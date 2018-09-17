package pbf

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/omniscale/go-osm/element"
)

func TestParser(t *testing.T) {
	checkParser(t)
}

func BenchmarkParser(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		checkParser(b)
	}
}

func checkParser(t testing.TB) {
	conf := Config{
		Coords:    make(chan []element.Node),
		Nodes:     make(chan []element.Node),
		Ways:      make(chan []element.Way),
		Relations: make(chan []element.Relation),
	}

	f, err := os.Open("./monaco-20150428.osm.pbf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	p := New(f, conf)

	wg := sync.WaitGroup{}

	var numNodes, numCoords, numWays, numRelations int64

	go func() {
		wg.Add(1)
		for nd := range conf.Nodes {
			numNodes += int64(len(nd))
		}
		wg.Done()
	}()

	go func() {
		wg.Add(1)
		for nd := range conf.Coords {
			numCoords += int64(len(nd))
		}
		wg.Done()
	}()

	go func() {
		wg.Add(1)
		for ways := range conf.Ways {
			numWays += int64(len(ways))
		}
		wg.Done()
	}()

	go func() {
		wg.Add(1)
		for rels := range conf.Relations {
			numRelations += int64(len(rels))
		}
		wg.Done()
	}()

	err = p.Parse(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	if numCoords != 17233 {
		t.Error("parsed an unexpected number of coords:", numCoords)
	}
	if numNodes != 978 {
		t.Error("parsed an unexpected number of nodes:", numNodes)
	}
	if numWays != 2398 {
		t.Error("parsed an unexpected number of ways:", numWays)
	}
	if numRelations != 108 {
		t.Error("parsed an unexpected number of relations:", numRelations)
	}
}

func TestParseCoords(t *testing.T) {
	conf := Config{
		Coords: make(chan []element.Node),
	}

	f, err := os.Open("./monaco-20150428.osm.pbf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	p := New(f, conf)

	wg := sync.WaitGroup{}

	var numCoords int64

	go func() {
		wg.Add(1)
		for nd := range conf.Coords {
			numCoords += int64(len(nd))
		}
		wg.Done()
	}()

	err = p.Parse(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	if numCoords != 17233 {
		t.Error("parsed an unexpected number of coords:", numCoords)
	}
}

func TestParseNodes(t *testing.T) {
	conf := Config{
		Nodes: make(chan []element.Node),
	}

	f, err := os.Open("./monaco-20150428.osm.pbf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	p := New(f, conf)

	wg := sync.WaitGroup{}

	var numNodes int64

	go func() {
		wg.Add(1)
		for nd := range conf.Nodes {
			numNodes += int64(len(nd))
		}
		wg.Done()
	}()

	err = p.Parse(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	if numNodes != 17233 {
		t.Error("parsed an unexpected number of nodes:", numNodes)
	}
}

func TestParserNotify(t *testing.T) {
	conf := Config{
		Coords:    make(chan []element.Node),
		Nodes:     make(chan []element.Node),
		Ways:      make(chan []element.Way),
		Relations: make(chan []element.Relation),
	}

	f, err := os.Open("./monaco-20150428.osm.pbf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	waysWg := sync.WaitGroup{}
	conf.OnFirstWay = func() {
		waysWg.Add(1)
		conf.Coords <- nil
		conf.Nodes <- nil
		waysWg.Done()
		waysWg.Wait()
	}

	p := New(f, conf)

	wg := sync.WaitGroup{}

	var numNodes, numCoords, numWays, numRelations int64

	waysWg.Add(1)
	go func() {
		wg.Add(1)
		for nd := range conf.Nodes {
			if nd == nil {
				waysWg.Done()
				waysWg.Wait()
				continue
			}
			numNodes += int64(len(nd))
		}
		wg.Done()
	}()

	waysWg.Add(1)
	go func() {
		wg.Add(1)
		for nd := range conf.Coords {
			if nd == nil {
				waysWg.Done()
				waysWg.Wait()
				continue
			}
			numCoords += int64(len(nd))
		}
		wg.Done()
	}()

	go func() {
		wg.Add(1)
		for ways := range conf.Ways {
			numWays += int64(len(ways))
		}
		wg.Done()
	}()

	go func() {
		wg.Add(1)
		for rels := range conf.Relations {
			numRelations += int64(len(rels))
		}
		wg.Done()
	}()

	err = p.Parse(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	if numCoords != 17233 {
		t.Error("parsed an unexpected number of coords:", numCoords)
	}
	if numNodes != 978 {
		t.Error("parsed an unexpected number of nodes:", numNodes)
	}
	if numWays != 2398 {
		t.Error("parsed an unexpected number of ways:", numWays)
	}
	if numRelations != 108 {
		t.Error("parsed an unexpected number of relations:", numRelations)
	}
}
func TestParseCancel(t *testing.T) {
	conf := Config{
		Nodes:       make(chan []element.Node),
		Ways:        make(chan []element.Way),
		Relations:   make(chan []element.Relation),
		Concurrency: 1,
	}

	f, err := os.Open("./monaco-20150428.osm.pbf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	p := New(f, conf)

	wg := sync.WaitGroup{}
	ctx, stop := context.WithCancel(context.Background())
	var numNodes, numWays, numRelations int64

	go func() {
		wg.Add(1)
		for nd := range conf.Nodes {
			numNodes += int64(len(nd))
			// stop after first parsed nodes
			stop()
		}
		wg.Done()
	}()
	go func() {
		wg.Add(1)
		for ways := range conf.Ways {
			numWays += int64(len(ways))
		}
		wg.Done()
	}()
	go func() {
		wg.Add(1)
		for rels := range conf.Relations {
			numRelations += int64(len(rels))
		}
		wg.Done()
	}()

	err = p.Parse(ctx)
	if err != context.Canceled {
		t.Fatal(err)
	}
	wg.Wait()

	// only two blocks of 8k nodes should be parsed before everything is stop()ed
	if numNodes != 16000 {
		t.Error("parsed an unexpected number of nodes:", numNodes)
	}
	if numWays != 0 {
		t.Error("parsed an unexpected number of ways:", numWays)
	}
	if numRelations != 0 {
		t.Error("parsed an unexpected number of relations:", numRelations)
	}
}

func TestBarrier(t *testing.T) {
	done := make(chan bool)
	check := int32(0)
	bar := newBarrier(func() {
		done <- true
		check = 1
	})
	bar.add(2)

	wait := func() {
		if check != 0 {
			panic("check set")
		}
		bar.doneWait()
		if check != 1 {
			panic("check not set")
		}
	}
	go wait()
	go wait()

	<-done

	// does not wait/block
	bar.doneWait()
}
