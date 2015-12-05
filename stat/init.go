package stats

import (
	"bytes"
	"database/sql"
	"io"
	"log"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mmirolim/BoomFilters"
)

var (
	// protect data while saving
	m            sync.Mutex
	hllPids      *boom.HyperLogLog
	cmsAnyKey    *boom.CountMinSketch
	topkPids     *boom.TopK
	topkUtmm     *boom.TopK
	topkUtms     *boom.TopK
	topkUtmmUtms *boom.TopK
)

func init() {
	var err error
	// TODO filters params store in configs and database
	// count unique pids
	hllPids, err = boom.NewDefaultHyperLogLog(0.1)
	fatalOnErr(err)

	cmsAnyKey = boom.NewCountMinSketch(0.001, 0.99)

	topkPids = boom.NewTopK(0.001, 0.99, 5)
	topkUtmm = boom.NewTopK(0.001, 0.99, 5)
	topkUtms = boom.NewTopK(0.001, 0.99, 5)
	topkUtmmUtms = boom.NewTopK(0.001, 0.99, 5)

}

type Serializable interface {
	WriteDataTo(w io.Writer) (int, error)
	ReadDataFrom(r io.Reader) (int, error)
}

// StatDB handle connection (safe for concurrent use)
// to ssdbs (currently)
type DB struct {
	stats *sql.DB
	delay time.Duration // persistent store after time
	tbl   string        // table name with serialized data
}

// interval in milliseconds
func Connect(file string, probTbl string, interval int) (*DB, error) {
	conn, err := sql.Open("sqlite3", file)
	if err != nil {
		return nil, err
	}

	// start goroutine to store persistently
	// data structures after duration
	db := &DB{
		stats: conn,
		delay: time.Duration(interval) * time.Millisecond,
		tbl:   probTbl,
	}
	// start watching collect stats
	db.watch()
	return db, nil
}

// collect different metrics and stores with some interval
func (db *DB) Collect(pid, utmm, utms string) {
	// count combinations
	cmsAnyKey.Add([]byte(pid)).
		Add([]byte(utmm)).
		Add([]byte(utms)).
		Add([]byte(genKey(pid, utmm))).
		Add([]byte(genKey(pid, utmm, utms))).
		Add([]byte(genKey(utmm, utms)))
	// collect unique pids number
	hllPids.Add([]byte(pid))
	// count top K 5 elements
	topkPids.Add([]byte(pid))
	topkUtmm.Add([]byte(utmm))
	topkUtms.Add([]byte(utms))
	topkUtmmUtms.Add([]byte(genKey(utmm, utms)))
}

func (db *DB) Count(key string) uint64 {
	return cmsAnyKey.Count([]byte(key))
}

func (db *DB) CountUniquePids() uint64 {
	return hllPids.Count()
}

func (db *DB) Top5Pid(pid string) []*boom.Element {
	return topkPids.Elements()
}

func (db *DB) Top5Utmm(utmm string) []*boom.Element {
	return topkUtmm.Elements()
}

func (db *DB) Top5Utms(utms string) []*boom.Element {
	return topkUtms.Elements()
}

func (db *DB) Top5UtmmUtms(utmmUtms string) []*boom.Element {
	return topkUtmmUtms.Elements()
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

func (db *DB) watch() {
	go func() {
		var err error
		// init if not exists
		m.Lock()
		err = db.save("hllpids")
		if err != nil {
			log.Println("hllpids save error", err)
		}
		err = db.save("cmsanykey")
		if err != nil {
			log.Println("hllpids save error", err)
		}
		m.Unlock()
		for {
			// wait for a delay and save data
			time.Sleep(db.delay)
			// protect save operation
			m.Lock()
			// TODO key names should be consistent
			// maybe contain filter params
			err = db.update("hllpids", hllPids)
			if err != nil {
				log.Println("hllpids update error", err)
			}
			err = db.update("cmsanykey", cmsAnyKey)
			if err != nil {
				log.Println("hllpids update error", err)
			}
			m.Unlock()
		}
	}()
}

// serialize data and update it
// TODO data should have params for constructor
func (db *DB) update(key string, data Serializable) error {
	buf := new(bytes.Buffer)
	_, err := data.WriteDataTo(buf)
	if err != nil {
		return err
	}
	_, err = db.stats.Exec(
		"UPDATE "+db.tbl+" SET data=$1 WHERE key=$2",
		buf.Bytes(),
		key,
	)
	return err
}

// init in database keys
func (db *DB) save(key string) error {
	_, err := db.stats.Exec(
		"INSERT INTO "+db.tbl+" (key, data) VALUES($1, $2)",
		key,
		[]byte{},
	)
	return err
}
func (db *DB) Close() error {
	return db.stats.Close()
}

func fatalOnErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
