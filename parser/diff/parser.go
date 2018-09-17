package diff

import (
	"compress/gzip"
	"context"
	"encoding/xml"
	"io"
	"log"
	"strconv"
	"time"

	"github.com/omniscale/go-osm/element"
	"github.com/pkg/errors"
)

type Element struct {
	Add  bool
	Mod  bool
	Del  bool
	Node *element.Node
	Way  *element.Way
	Rel  *element.Relation
}

// Parser is a stream based parser for OSM diff files (.osc).
// Parsing is handled in a background goroutine.
type Parser struct {
	reader io.Reader
	conf   Config
}

type Config struct {
	// IncludeMetadata indicates whether metadata like timestamps, versions and
	// user names should be parsed.
	IncludeMetadata bool

	// Elements specifies the destination for parsed elements.
	Elements chan Element

	// KeepOpen specifies whether the destination channel should be keept open
	// after Parse(). By default, the Elements channel is closed after Parse().
	KeepOpen bool
}

// New returns a parser from an io.Reader
func New(r io.Reader, conf Config) *Parser {
	return &Parser{reader: r, conf: conf}
}

// NewGZIP returns a parser from a GZIP compressed io.Reader
func NewGZIP(r io.Reader, conf Config) (*Parser, error) {
	r, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	return New(r, conf), nil
}

func (p *Parser) Parse(ctx context.Context) error {
	if !p.conf.KeepOpen {
		defer func() {
			if p.conf.Elements != nil {
				close(p.conf.Elements)
			}
		}()
	}
	decoder := xml.NewDecoder(p.reader)

	add := false
	mod := false
	del := false
	tags := make(map[string]string)
	newElem := false

	node := &element.Node{}
	way := &element.Way{}
	rel := &element.Relation{}

NextToken:
	for {
		token, err := decoder.Token()
		if err != nil {
			return errors.Wrap(err, "decoding next XML token")
		}

		switch tok := token.(type) {
		case xml.StartElement:
			switch tok.Name.Local {
			case "create":
				add = true
				mod = false
				del = false
			case "modify":
				add = false
				mod = true
				del = false
			case "delete":
				add = false
				mod = false
				del = true
			case "node":
				for _, attr := range tok.Attr {
					switch attr.Name.Local {
					case "id":
						node.ID, _ = strconv.ParseInt(attr.Value, 10, 64)
					case "lat":
						node.Lat, _ = strconv.ParseFloat(attr.Value, 64)
					case "lon":
						node.Long, _ = strconv.ParseFloat(attr.Value, 64)
					}
				}
				if p.conf.IncludeMetadata {
					setElemMetadata(tok.Attr, &node.OSMElem)
				}
			case "way":
				for _, attr := range tok.Attr {
					if attr.Name.Local == "id" {
						way.ID, _ = strconv.ParseInt(attr.Value, 10, 64)
					}
				}
				if p.conf.IncludeMetadata {
					setElemMetadata(tok.Attr, &way.OSMElem)
				}
			case "relation":
				for _, attr := range tok.Attr {
					if attr.Name.Local == "id" {
						rel.ID, _ = strconv.ParseInt(attr.Value, 10, 64)
					}
				}
				if p.conf.IncludeMetadata {
					setElemMetadata(tok.Attr, &rel.OSMElem)
				}
			case "nd":
				for _, attr := range tok.Attr {
					if attr.Name.Local == "ref" {
						ref, _ := strconv.ParseInt(attr.Value, 10, 64)
						way.Refs = append(way.Refs, ref)
					}
				}
			case "member":
				member := element.Member{}
				for _, attr := range tok.Attr {
					switch attr.Name.Local {
					case "type":
						var ok bool
						member.Type, ok = element.MemberTypeValues[attr.Value]
						if !ok {
							// ignore unknown member types
							continue NextToken
						}
					case "role":
						member.Role = attr.Value
					case "ref":
						var err error
						member.ID, err = strconv.ParseInt(attr.Value, 10, 64)
						if err != nil {
							// ignore invalid ref
							continue NextToken
						}
					}
				}
				rel.Members = append(rel.Members, member)
			case "tag":
				var k, v string
				for _, attr := range tok.Attr {
					if attr.Name.Local == "k" {
						k = attr.Value
					} else if attr.Name.Local == "v" {
						v = attr.Value
					}
				}
				tags[k] = v
			case "osmChange":
				// pass
			default:
				log.Println("unhandled XML tag ", tok.Name.Local, " in OSC")
			}
		case xml.EndElement:
			var e Element
			switch tok.Name.Local {
			case "node":
				if len(tags) > 0 {
					node.Tags = tags
				}
				e.Node = node
				node = &element.Node{}
				newElem = true
			case "way":
				if len(tags) > 0 {
					way.Tags = tags
				}
				e.Way = way
				way = &element.Way{}
				newElem = true
			case "relation":
				if len(tags) > 0 {
					rel.Tags = tags
				}
				e.Rel = rel
				rel = &element.Relation{}
				newElem = true
			case "osmChange":
				// EOF
				return nil
			}

			if newElem {
				e.Add = add
				e.Del = del
				e.Mod = mod
				if len(tags) > 0 {
					tags = make(map[string]string)
				}
				newElem = false
				p.conf.Elements <- e
			}
		}
	}

	return nil
}

func setElemMetadata(attrs []xml.Attr, elem *element.OSMElem) {
	elem.Metadata = &element.Metadata{}
	for _, attr := range attrs {
		switch attr.Name.Local {
		case "version":
			v, _ := strconv.ParseInt(attr.Value, 10, 64)
			elem.Metadata.Version = int32(v)
		case "uid":
			v, _ := strconv.ParseInt(attr.Value, 10, 64)
			elem.Metadata.UserID = int32(v)
		case "user":
			elem.Metadata.UserName = attr.Value
		case "changeset":
			v, _ := strconv.ParseInt(attr.Value, 10, 64)
			elem.Metadata.Changeset = v
		case "timestamp":
			ts, _ := time.Parse(time.RFC3339, attr.Value)
			elem.Metadata.Timestamp = ts.Unix()
		}
	}
}
