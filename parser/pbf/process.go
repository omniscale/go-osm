package pbf

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/omniscale/go-osm/element"
	"github.com/pkg/errors"
)

type Config struct {
	// IncludeMetadata indicates whether metadata like timestamps, versions and
	// user names should be parsed.
	IncludeMetadata bool

	// Nodes specifies the destination for parsed nodes. See also Coords below.
	// For efficiency, multiple nodes are passed in batches.
	Nodes chan []element.Node
	// Ways specifies the destination for parsed ways.
	// For efficiency, multiple wats are passed in batches.
	Ways chan []element.Way
	// Relations specifies the destination for parsed relations.
	// For efficiency, multiple relations are passed in batches.
	Relations chan []element.Relation

	// Coords specifies the destination for parsed nodes without any tags. This
	// can be used for more efficient storage/proceessing of nodes that are
	// only used as coordinates for ways and relations.
	// For efficiency, multiple nodes are passed in batches.
	//
	// If a Coords channel is specified, then nodes without tags are
	// not sent to the Nodes channel. However, the Coords channel will receive
	// all nodes.
	Coords chan []element.Node

	// KeepOpen specifies whether the destination channels should be keept open
	// after Parse(). By default, Nodes, Ways, Relations and Coords channels
	// are closed after Parse().
	KeepOpen bool

	// OnFirstWay defines an optional func that gets called when the the first
	// way is parsed. The callback should block until it is safe to fill the
	// Ways channel.
	//
	// This can be used when you require that all nodes are processed before
	// you start processing ways.
	//
	// This only works when the PBF file is ordered by type (nodes before ways
	// before relations).
	OnFirstWay func()

	// OnFirstRelation defines an optional func that gets called when the
	// the first relation is parsed. The callback should block until it is
	// safe to fill the Relations channel.
	//
	// This can be used when you require that all ways are processed before you
	// start processing relations.
	//
	// This only works when the PBF file is ordered by type (nodes before ways
	// before relations).
	OnFirstRelation func()

	// Concurrency specifies how many concurrent parsers are started. Defaults
	// to runtime.NumCPU if <= 0.
	Concurrency int
}

type Parser struct {
	conf    Config
	r       io.Reader
	header  *Header
	wg      sync.WaitGroup
	waySync *barrier
	relSync *barrier
}

// New creates a new PBF parser for the provided input. Config specifies the destinations for the parsed elements.
func New(r io.Reader, conf Config) *Parser {
	p := &Parser{
		r:    r,
		conf: conf,
	}

	if conf.Concurrency <= 0 {
		p.conf.Concurrency = runtime.NumCPU()
	}

	if conf.OnFirstWay != nil {
		p.waySync = newBarrier(conf.OnFirstWay)
		p.waySync.add(p.conf.Concurrency)
	}
	if conf.OnFirstRelation != nil {
		p.relSync = newBarrier(conf.OnFirstRelation)
		p.relSync.add(p.conf.Concurrency)
	}
	return p
}

// Header returns the header information from the PBF. Can be called before or
// after Parse().
func (p *Parser) Header() (*Header, error) {
	if p.header == nil {
		if err := p.parseHeader(); err != nil {
			return nil, err
		}
	}
	return p.header, nil
}

// Parse parses the PBF file and sends the parsed nodes, ways and relations
// into the channels provided to the Parsers Config.
// Context can be used to cancel the parsing.
func (p *Parser) Parse(ctx context.Context) error {
	if p.header == nil {
		if err := p.parseHeader(); err != nil {
			return err
		}
	}
	wg := sync.WaitGroup{}
	blocks := make(chan []byte)

	for i := 0; i < p.conf.Concurrency; i++ {
		wg.Add(1)
		go func() {
			for block := range blocks {
				p.parseBlock(block)
			}
			if p.waySync != nil {
				p.waySync.doneWait()
			}
			if p.relSync != nil {
				p.relSync.doneWait()
			}
			wg.Done()
		}()
	}

read:
	for {
		header, data, err := nextBlock(p.r)
		if err == io.EOF {
			break read
		}
		if err != nil {
			close(blocks)
			return errors.Wrap(err, "parsing next block")
		}
		if header.GetType() != "OSMData" {
			close(blocks)
			return errors.New("next block not of type OSMData but " + header.GetType())
		}
		select {
		case <-ctx.Done():
			fmt.Println("done")
			break read
		case blocks <- data:
		}
	}

	close(blocks)
	wg.Wait()

	if !p.conf.KeepOpen {
		if p.conf.Coords != nil {
			close(p.conf.Coords)
		}
		if p.conf.Nodes != nil {
			close(p.conf.Nodes)
		}
		if p.conf.Ways != nil {
			close(p.conf.Ways)
		}
		if p.conf.Relations != nil {
			close(p.conf.Relations)
		}
	}

	return ctx.Err()
}

func (p *Parser) parseHeader() error {
	if p.header != nil {
		return nil
	}
	var err error
	p.header, err = parseHeader(p.r)
	return err
}

func (p *Parser) parseBlock(blob []byte) error {
	block, err := decodePrimitiveBlock(blob)
	if err != nil {
		return err
	}
	stringtable := newStringTable(block.GetStringtable())

	for _, group := range block.Primitivegroup {
		if p.conf.Coords != nil || p.conf.Nodes != nil {
			dense := group.GetDense()
			if dense != nil {
				parsedCoords, parsedNodes := readDenseNodes(dense, block, stringtable, p.conf.Coords == nil)
				if len(parsedCoords) > 0 && p.conf.Coords != nil {
					p.conf.Coords <- parsedCoords
				}
				if len(parsedNodes) > 0 && p.conf.Nodes != nil {
					p.conf.Nodes <- parsedNodes
				}
			}
			if len(group.Nodes) > 0 {
				parsedCoords, parsedNodes := readNodes(group.Nodes, block, stringtable, p.conf.Coords == nil)
				if len(parsedCoords) > 0 && p.conf.Coords != nil {
					p.conf.Coords <- parsedCoords
				}
				if len(parsedNodes) > 0 && p.conf.Nodes != nil {
					p.conf.Nodes <- parsedNodes
				}
			}
		}
		if len(group.Ways) > 0 && p.conf.Ways != nil {
			parsedWays := readWays(group.Ways, block, stringtable)
			if len(parsedWays) > 0 {
				if p.waySync != nil {
					p.waySync.doneWait()
				}
				p.conf.Ways <- parsedWays
			}
		}
		if len(group.Relations) > 0 && p.conf.Relations != nil {
			parsedRelations := readRelations(group.Relations, block, stringtable)
			if len(parsedRelations) > 0 {
				if p.waySync != nil {
					p.waySync.doneWait()
				}
				if p.relSync != nil {
					p.relSync.doneWait()
				}
				p.conf.Relations <- parsedRelations
			}
		}
	}
	return nil
}

// barrier is a struct to synchronize multiple goroutines.
// Works similar to a WaitGroup. Except:
// Calls callback function once all goroutines called doneWait().
// doneWait() blocks until the callback returns. doneWait() does not
// block after all goroutines were blocked once.
type barrier struct {
	synced     int32
	wg         sync.WaitGroup
	once       sync.Once
	callbackWg sync.WaitGroup
	callback   func()
}

func newBarrier(callback func()) *barrier {
	s := &barrier{callback: callback}
	s.callbackWg.Add(1)
	return s
}

func (s *barrier) add(delta int) {
	s.wg.Add(delta)
}

func (s *barrier) doneWait() {
	if atomic.LoadInt32(&s.synced) == 1 {
		return
	}
	s.wg.Done()
	s.wg.Wait()
	s.once.Do(s.call)
	s.callbackWg.Wait()
}

func (s *barrier) call() {
	s.callback()
	atomic.StoreInt32(&s.synced, 1)
	s.callbackWg.Done()
}
