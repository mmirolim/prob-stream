package api

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/mmirolim/prob-stream/stat"

	"golang.org/x/net/context"
)

const (
	// context keys
	ctxReqScopeKey = "req-scope"
	ctxReqParamKey = "req-params"

	statDBKey = "statDB"
)

var (
	// errors in handlers
	ErrNotFound     = HttpError{Err: errors.New("not found"), Status: http.StatusNotFound}
	ErrEventStorage = errors.New("event storage not defined")
	ErrStatStorage  = errors.New("stat storage not defined")
)

// HttpError returned by CtxHandlerFunc
type HttpError struct {
	Err    error
	Status int
}

// CtxHandlerFunc context enabled http handler func
type CtxHandlerFunc func(context.Context, http.ResponseWriter, *http.Request) *HttpError

func basicTelemetry(h CtxHandlerFunc) CtxHandlerFunc {
	return func(ctx context.Context, w http.ResponseWriter, r *http.Request) *HttpError {
		// set request scope
		ctx = context.WithValue(ctx, ctxReqScopeKey, r.Method+" "+r.URL.EscapedPath())
		// set request details
		ctx = context.WithValue(ctx, ctxReqParamKey, r.URL.RawQuery)
		// measure request process time
		s := time.Now()
		defer func(s time.Time) {
			// TODO send to metrics
			log.Println("req process time", time.Since(s), "scope ", ctx.Value(ctxReqScopeKey), " params ", ctx.Value(ctxReqParamKey))
		}(s)
		return h(ctx, w, r)
	}
}

func setCtx(stdb *stats.DB, timeout time.Duration, h CtxHandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		// pass connections to eventdb and statdb
		ctx = context.WithValue(ctx, statDBKey, stdb)
		defer cancel()
		err := h(ctx, w, r)
		if err != nil {
			respJson(w, err.Err, err.Status)
			return
		}
	}
}

// New return mux with handlers registered
// connections to event and stat database
// api timeout in seconds
func New(stdb *stats.DB, apiTimeout int) (*http.ServeMux, error) {
	if stdb == nil {
		return nil, ErrStatStorage
	}

	// default timeout for api handlers
	timeout := 5 * time.Second
	if apiTimeout > 0 {
		timeout = time.Duration(apiTimeout) * time.Second
	}
	// WARNING more specific routes should be first then more general
	m := http.NewServeMux()

	// gather stats
	m.HandleFunc("/collect-stats", setCtx(stdb, timeout, basicTelemetry(collectStat)))

	// get real stat for defined key
	m.HandleFunc("/real-stat", setCtx(stdb, timeout, basicTelemetry(realStat)))

	// get probabilistic stat for defined key
	m.HandleFunc("/prob-stat", setCtx(stdb, timeout, basicTelemetry(probUtmStat)))

	// get pids stat
	m.HandleFunc("/prob-stat-unique-pids", setCtx(stdb, timeout, basicTelemetry(probPidStat)))

	// and defined segment like edg, utm_source, utm_medium, device_type
	// TODO name params in standard way
	return m, nil
}

// reply in json format with status code
func respJson(w http.ResponseWriter, data interface{}, status ...int) {
	w.Header().Set("Content-Type", "application/json")
	// create struct for json marshaling
	// struct depends on concrete type of data
	switch v := data.(type) {
	case string:
		// if custom string passed
		data = struct{ Msg string }{v}
	case error:
		// if err passed
		data = struct{ Err string }{v.Error()}
	case int:
		// if http status passed as data for response
		w.WriteHeader(v)
		data = struct{ StatusCode int }{v}
	}
	// json encode data
	b, err := json.Marshal(data)
	// check for errors
	if err != nil {
		// set error as internal
		w.WriteHeader(http.StatusInternalServerError)
		// response with err msg
		w.Write([]byte(err.Error()))
	}

	// check if status code explicitly provided
	if len(status) == 1 {
		w.WriteHeader(status[0])
	}
	// write as response
	w.Write(b)
}
