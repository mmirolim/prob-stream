package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/mmirolim/prob-stream/api"
	"github.com/mmirolim/prob-stream/stat"
)

var (
	sqliteDbFile string
	port         string
	BuildVersion = "unknown"
)

func init() {
	flag.StringVar(&sqliteDbFile, "db", "/home/me/backups/sqlite3/stats.dat", "sqlite db file path")
	flag.StringVar(&port, "p", ":8082", "server port")
	flag.Parse()
}

func main() {
	log.Println("start service", " version: ", BuildVersion)
	// connect to sqlite db
	statdb, err := stats.Connect(sqliteDbFile, "prob_stats", 1000)
	fatalOnErr(err)

	// init api router
	mux, err := api.New(statdb, 5)
	fatalOnErr(err)

	// start http server
	fatalOnErr(http.ListenAndServe(port, mux))
}

func fatalOnErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
