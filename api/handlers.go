package api

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"

	"github.com/mmirolim/prob-stream/stat"

	"golang.org/x/net/context"
)

const (
	// url params name
	fileKey = "file"
	statKey = "key"
	pidKey  = "pid"
	utmmKey = "utmm"
	utmsKey = "utms"
)

func collectStat(ctx context.Context, w http.ResponseWriter, r *http.Request) *HttpError {
	stdb, ok := ctx.Value(statDBKey).(*stats.DB)
	if !ok {
		return &HttpError{ErrStatStorage, http.StatusInternalServerError}
	}

	// extract data from url
	vals, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return &HttpError{err, http.StatusInternalServerError}
	}
	file := vals.Get(fileKey)
	if file == "" {
		return &HttpError{nil, http.StatusBadRequest}
	}
	fmt.Println("file name ", file)
	fmt.Println("small test data ", "./testdata/small-data-1000/test-data.csv")
	fpath := "./testdata/small-data-1000/test-data.csv"
	// read file
	f, err := os.Open(fpath)
	if err != nil {
		return &HttpError{err, http.StatusInternalServerError}
	}
	fileReader := csv.NewReader(f)
	// read headers
	rec, err := fileReader.Read()
	if err != nil {
		return &HttpError{err, http.StatusInternalServerError}
	}
	fmt.Println("test data headers ", rec)
	counter := 0
	for {
		counter++
		rec, err = fileReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return &HttpError{err, http.StatusInternalServerError}
		}
		if len(rec) == 3 {
			stdb.Collect(rec[0], rec[1], rec[2])
			if counter%100 == 0 {
				fmt.Println("record from test data", rec)
			}
		}
	}

	respJson(w, "file "+file+" parsing finished")

	return nil
}

func realStat(ctx context.Context, w http.ResponseWriter, r *http.Request) *HttpError {
	stdb, ok := ctx.Value(statDBKey).(*stats.DB)
	if !ok {
		return &HttpError{ErrStatStorage, http.StatusInternalServerError}
	}
	// extract data from url
	vals, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return &HttpError{err, http.StatusInternalServerError}
	}
	result := stdb.Count(vals.Get(statKey))
	respJson(w, "stats for "+vals.Get(statKey)+" "+strconv.FormatUint(result, 10))
	return nil
}

func probUtmStat(ctx context.Context, w http.ResponseWriter, r *http.Request) *HttpError {
	var result uint64
	stdb, ok := ctx.Value(statDBKey).(*stats.DB)
	if !ok {
		return &HttpError{ErrStatStorage, http.StatusInternalServerError}
	}
	// extract data from url
	vals, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return &HttpError{err, http.StatusInternalServerError}
	}

	result = stdb.Count(vals.Get(statKey))
	respJson(w, "stats for "+vals.Get(statKey)+" "+strconv.FormatUint(result, 10))

	return nil
}

func probPidStat(ctx context.Context, w http.ResponseWriter, r *http.Request) *HttpError {
	var result uint64
	stdb, ok := ctx.Value(statDBKey).(*stats.DB)
	if !ok {
		return &HttpError{ErrStatStorage, http.StatusInternalServerError}
	}

	result = stdb.CountUniquePids()
	respJson(w, "unique pids number"+strconv.FormatUint(result, 10))

	return nil
}
