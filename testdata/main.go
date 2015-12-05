// Generate test data for streaming
// utm tags like utm medium and utm source
// generation is weightened
// generated stream saved in test-stream.log (by default)
// stats saved as blob gob serialized map[string]int in test-stat.dat (by default)
package main

import (
	"flag"
	"time"
)

var (
	numEvents int
	// TODO load data from files
	// utm medium tags
	utmMDict = []Tag{
		{"social", 4},
		{"fb", 2},
		{"gg", 1},
		{"vk", 1},
		{"mailru", 1},
	}
	// utm source tags
	utmSDict = []Tag{
		{"fun", 3},
		{"yemail", 1},
		{"camp", 1},
		{"experiment", 1},
		{"offer", 2},
	}
	// suffix for mutation utm source
	suffixes = []Tag{
		{"extra", 1},
		{"new", 1},
		{"fanboys", 1},
		{"blackfriday", 3},
	}
	// some identifiers of producers
	pids = []Tag{
		{"profile1", 1},
		{"profile2", 1},
		{"profile3", 2},
		{"profile4", 3},
	}
)

type Tag struct {
	Word   string
	Weight int
}

type Event struct {
	Pid       string
	UtmM      string
	UtmS      string
	TimeStamp int64
}

func init() {
	flag.IntVar(&numEvents, "ne", 1e+4, "number of events to generate")
	flag.Parse()
}

func main() {

}

// generates events with normal distribution with weights
func genNormalDistEvent(pids []Tag, utmms []Tag, utmss []Tag) Event {
	var pid, utmm, utms string
	ts := time.Now().UTC().Unix()
	return Event{
		Pid:       pid,
		UtmM:      utmm,
		UtmS:      utms,
		TimeStamp: ts,
	}
}
