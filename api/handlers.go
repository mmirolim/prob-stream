package api

import (
	"net/http"

	"github.com/mmirolim/prob-stream/stat"

	"golang.org/x/net/context"
)

const (
	// url params name
	statKey = "key"
	pid     = "pid"
	utmm    = "utmm"
	utms    = "utms"
)

func collectStat(ctx context.Context, w http.ResponseWriter, r *http.Request) *HttpError {
	stdb, ok := ctx.Value(statDBKey).(*stats.DB)
	if !ok {
		return &HttpError{ErrStatStorage, http.StatusInternalServerError}
	}

	respJson(w, "stats collected")
	return nil
}

func realStat(ctx context.Context, w http.ResponseWriter, r *http.Request) *HttpError {
	stdb, ok := ctx.Value(statDBKey).(*stats.DB)
	if !ok {
		return &HttpError{ErrStatStorage, http.StatusInternalServerError}
	}

	respJson(w, "stats collected")
	return nil
}

func probStat(ctx context.Context, w http.ResponseWriter, r *http.Request) *HttpError {
	stdb, ok := ctx.Value(statDBKey).(*stats.DB)
	if !ok {
		return &HttpError{ErrStatStorage, http.StatusInternalServerError}
	}

	respJson(w, "stats collected")
	return nil
}
