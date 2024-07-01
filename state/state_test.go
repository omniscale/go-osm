package state

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	s, err := ParseFile("964.state.txt")
	if err != nil {
		t.Fatal("parsing state", err)
	}

	if s.Sequence != 964 {
		t.Error("unexpected sequence", s)
	}
	if !s.Time.Equal(time.Date(2015, time.May, 4, 0, 0, 0, 0, time.UTC)) {
		t.Error("unexpected time", s)
	}
	if s.URL != "" {
		t.Error("unexpected URL", s)
	}
}

func TestWriteFile(t *testing.T) {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	defer os.Remove(f.Name())

	want := &DiffState{
		Time:     time.Date(2018, time.November, 22, 10, 42, 0, 0, time.UTC),
		URL:      "https://planet.openstreetmap.org/replication/minute/",
		Sequence: 123456,
	}
	err = WriteFile(f.Name(), want)
	if err != nil {
		t.Fatal("writing state", err)
	}

	content, err := ioutil.ReadFile(f.Name())
	if err != nil {
		t.Fatal("reading state file", err)
	}

	if string(content) != `timestamp=2018-11-22T10\:42\:00Z
sequenceNumber=123456
replicationUrl=https://planet.openstreetmap.org/replication/minute/
` {
		t.Error("unexpected content", string(content))
	}

	got, err := ParseFile(f.Name())
	if err != nil {
		t.Fatal("reading state file", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Error("parsed state differs", got, want)
	}
}
