package api

import (
	"net/http"
	"net/url"
	"strconv"

	"github.com/mmirolim/prob-stream/stat"

	"golang.org/x/net/context"
)

const (
	// url params name
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

	pid := vals.Get(pidKey)
	utmm := vals.Get(utmmKey)
	utms := vals.Get(utmsKey)

	stdb.Collect(pid, utmm, utms)

	respJson(w, pid+":"+utmm+":"+utms)

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

func probStat(ctx context.Context, w http.ResponseWriter, r *http.Request) *HttpError {
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
