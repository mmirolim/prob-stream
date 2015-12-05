// Generate test data for streaming
// utm tags like utm medium and utm source
// generation is weightened
// generated stream saved in test-stream.csv (by default)
// stats saved as blob gob serialized map[string]int in test-stat.dat (by default)
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/leesper/go_rng"
)

const (
	evNumberKey        = "events_count"
	uniquePid          = "unique_pid_count"
	uniquePidsUtms     = "unique_pid_utms_count"
	uniquePidsUtmm     = "unique_pid_utmm_count"
	uniquePidsUtmsUtmm = "unique_pid_utmm_utms_count"
)

var (
	numProf                    int    // number of profiles
	numEvents                  int    // number of event to generate
	testFileData, testFileStat string // files names for test stream and result stat
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
		{"profile1", 3},
		{"profile2", 3},
		{"profile3", 2},
		{"profile4", 2},
		{"profile5", 2},
		{"profile6", 2},
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
	flag.IntVar(&numEvents, "ne", 1e+6, "number of events to generate")
	flag.IntVar(&numProf, "np", 1e+6, "number of profiles")
	flag.StringVar(&testFileData, "td", "test-data.csv", "file name to save generated test stream")
	flag.StringVar(&testFileStat, "ts", "test-stat.dat", "file name to store exact stats for stream")
	flag.Parse()
}

func main() {
	// init map for stats
	stats := make(map[string]int)

	// process weights
	pids = enableWeights(pids)
	utmSDict = enableWeights(utmSDict)
	utmMDict = enableWeights(utmMDict)
	suffixes = enableWeights(suffixes)
	// add many different users
	genDummyPids(&pids, numProf)
	fmt.Println("number of profiles", len(pids))
	f, err := os.Create(testFileData)
	fatalOnErr(err)
	w := csv.NewWriter(f)
	var ev Event
	// headers to csv file
	fmt.Println("save test data events to " + testFileData)
	w.Write([]string{"pid", "utm_medium", "utm_source"})
	for i := 0; i < numEvents; i++ {
		ev = genNormalDistEvent(pids, utmMDict, utmSDict, suffixes)
		collectStat(&ev, &stats)
		w.Write([]string{ev.Pid, ev.UtmM, ev.UtmS})
		if i%10000 == 0 {
			fmt.Println("events created ", i)
		}
	}
	w.Flush()
	fatalOnErr(w.Error())
	fmt.Println("number of events generated", numEvents)

	fmt.Println("save generated stats to " + testFileStat)
	f, err = os.Create(testFileStat)
	fatalOnErr(err)
	w = csv.NewWriter(f)
	w.Write([]string{"counter_name", "count"})
	for k, v := range stats {
		w.Write([]string{k, strconv.Itoa(v)})
	}
	w.Flush()
}

// generate dummy profiles
func genDummyPids(pids *[]Tag, n int) {
	for i := 0; i < n; i++ {
		*pids = append(*pids, Tag{"profile" + strconv.Itoa(i+100), 1})
	}

}

// collect stat for event tags by all possible combinations
func collectStat(ev *Event, m *map[string]int) {
	if m == nil {
		*m = make(map[string]int)
	}
	// stats to map
	if _, ok := (*m)[ev.Pid]; !ok {
		(*m)[uniquePid]++
	}
	(*m)[ev.Pid]++

	(*m)[ev.UtmM]++
	(*m)[ev.UtmS]++

	if _, ok := (*m)[genKey(ev.Pid, ev.UtmM)]; !ok {
		(*m)[genKey(uniquePidsUtmm, ev.UtmM)]++
	}
	(*m)[genKey(ev.Pid, ev.UtmM)]++

	if _, ok := (*m)[genKey(ev.Pid, ev.UtmS)]; !ok {
		(*m)[genKey(uniquePidsUtms, ev.UtmS)]++
	}
	(*m)[genKey(ev.Pid, ev.UtmS)]++

	(*m)[genKey(ev.UtmM, ev.UtmS)]++

	if _, ok := (*m)[genKey(ev.Pid, ev.UtmM, ev.UtmS)]; !ok {
		(*m)[genKey(uniquePidsUtms, ev.UtmM, ev.UtmS)]++
	}
	(*m)[genKey(ev.Pid, ev.UtmM, ev.UtmS)]++

	(*m)[evNumberKey]++

}

func genKey(strs ...string) string {
	if len(strs) == 1 {
		return strs[0]
	}
	sep := ":"
	key := strs[0]
	for i := 1; i < len(strs); i++ {
		key = key + sep + strs[i]
	}
	return key
}

// TODO invert index? or use probability for each item
// to make list weighted copy number of items in list according to weight
func enableWeights(tags []Tag) []Tag {
	var dst []Tag
	for _, v := range tags {
		dst = append(dst, Tag{v.Word, 1})
		for i := 0; i < v.Weight-1; i++ {
			dst = append(dst, Tag{v.Word, 1})
		}
	}
	return dst
}

// generates events with normal distribution with weights
func genNormalDistEvent(pids []Tag, utmms []Tag, utmss []Tag, sufs []Tag) Event {
	// tags will have uniform distribution
	uniProb := rng.NewUniformGenerator(time.Now().UnixNano())
	ts := time.Now().UTC().Unix()
	return Event{
		Pid:       pids[uniProb.Int64n(int64(len(pids)))].Word,
		UtmM:      utmms[uniProb.Int64n(int64(len(utmms)))].Word,
		UtmS:      utmss[uniProb.Int64n(int64(len(utmss)))].Word + "_" + sufs[uniProb.Int64n(int64(len(sufs)))].Word,
		TimeStamp: ts,
	}
}

func fatalOnErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
