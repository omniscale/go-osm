package state

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/omniscale/go-osm/parser/pbf"
)

type DiffState struct {
	Time     time.Time
	Sequence int
	URL      string
}

func (d DiffState) write(w io.Writer) error {
	lines := []string{}
	lines = append(lines, "timestamp="+d.Time.Format(timestampFormat))
	if d.Sequence != 0 {
		lines = append(lines, "sequenceNumber="+fmt.Sprintf("%d", d.Sequence))
	}
	lines = append(lines, "replicationUrl="+d.URL)

	for _, line := range lines {
		_, err := w.Write([]byte(line + "\n"))
		if err != nil {
			return err
		}
	}
	return nil
}

func WriteFile(filename string, state *DiffState) error {
	tmpname := filename + "~"
	f, err := os.Create(tmpname)
	if err != nil {
		return errors.Wrap(err, "creating temp file for writing state file")
	}
	err = state.write(f)
	if err != nil {
		f.Close()
		os.Remove(tmpname)
		return errors.Wrapf(err, "writing state to %q", tmpname)
	}
	f.Close()
	return os.Rename(tmpname, filename)
}

// EstimateFromPBF
func EstimateFromPBF(filename string, before time.Duration, replicationURL string, replicationInterval time.Duration) (*DiffState, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, errors.Wrap(err, "opening PBF file")
	}
	defer f.Close()

	pbfparser := pbf.New(f, pbf.Config{})
	header, err := pbfparser.Header()

	var timestamp time.Time
	if err == nil && header.Time.Unix() != 0 {
		timestamp = header.Time
	} else {
		fstat, err := os.Stat(filename)
		if err != nil {
			return nil, errors.Wrapf(err, "reading mod time from %q", filename)
		}
		timestamp = fstat.ModTime()
	}

	if replicationURL == "" {
		replicationURL = "https://planet.openstreetmap.org/replication/minute/"
	}

	seq, err := estimateSequence(replicationURL, replicationInterval, timestamp)
	if err != nil {
		return nil, errors.Wrap(err, "fetching current sequence for estimated import sequence")
	}

	// start earlier
	seq -= int(math.Ceil(before.Minutes() / replicationInterval.Minutes()))
	return &DiffState{Time: timestamp, URL: replicationURL, Sequence: seq}, nil
}

func ParseFile(stateFile string) (*DiffState, error) {
	f, err := os.Open(stateFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Parse(f)
}

// Parse parses an INI style state.txt file.
// timestamp is required, sequenceNumber and replicationUrl can be empty.
func Parse(f io.Reader) (*DiffState, error) {
	values, err := parseSimpleIni(f)
	if err != nil {
		return nil, errors.Wrap(err, "parsing state file as INI")
	}

	timestamp, err := parseTimeStamp(values["timestamp"])
	if err != nil {
		return nil, err
	}
	sequence, err := parseSequence(values["sequenceNumber"])
	if err != nil {
		return nil, err
	}

	url := values["replicationUrl"]
	return &DiffState{
		Time:     timestamp,
		Sequence: sequence,
		URL:      url,
	}, nil
}

func parseSimpleIni(f io.Reader) (map[string]string, error) {
	result := make(map[string]string)

	reader := bufio.NewScanner(f)
	for reader.Scan() {
		line := reader.Text()
		if line != "" && line[0] == '#' {
			continue
		}
		if strings.Contains(line, "=") {
			keyVal := strings.SplitN(line, "=", 2)
			result[strings.TrimSpace(keyVal[0])] = strings.TrimSpace(keyVal[1])
		}

	}
	if err := reader.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

const timestampFormat = "2006-01-02T15\\:04\\:05Z"

func parseTimeStamp(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, errors.New("missing timestamp in state")
	}
	return time.Parse(timestampFormat, value)
}

func parseSequence(value string) (int, error) {
	if value == "" {
		return 0, nil
	}
	val, err := strconv.ParseInt(value, 10, 32)
	return int(val), err
}

func currentState(url string) (*DiffState, error) {
	resp, err := http.Get(url + "state.txt")
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, errors.New(fmt.Sprintf("invalid response: %v", resp))
	}
	defer resp.Body.Close()
	return Parse(resp.Body)
}

func estimateSequence(url string, interval time.Duration, timestamp time.Time) (int, error) {
	state, err := currentState(url)
	if err != nil {
		// discard first error and try a second time before failing
		time.Sleep(time.Second * 2)
		state, err = currentState(url)
		if err != nil {
			return 0, errors.Wrap(err, "fetching current state")
		}
	}

	behind := state.Time.Sub(timestamp)
	// Sequence unit depends on replication interval (minute, hour, day).
	return state.Sequence - int(math.Ceil(behind.Minutes()/interval.Minutes())), nil
}
