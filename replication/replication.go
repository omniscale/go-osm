package replication

import "time"

type Sequence struct {
	Filename      string
	StateFilename string
	Time          time.Time
	Sequence      int
	Error         error
}

type Source interface {
	Sequences() <-chan Sequence
	Stop()
}
