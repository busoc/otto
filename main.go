package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/midbel/toml"
)

type Period struct {
	Starts time.Time `json:"dtstart"`
	Ends   time.Time `json:"dtend"`
}

func (p Period) isValid() bool {
	if p.Starts.IsZero() || p.Ends.IsZero() {
		return false
	}
	return p.Starts.Equal(p.Ends) || p.Starts.Before(p.Ends)
}

type Gap struct {
	Id    int       `json:"id"`
	When  time.Time `json:"time"`
	First int       `json:"first"`
	Last  int       `json:"last"`
	Period
}

type HRDGap struct {
	Gap
	When    time.Time `json:"time"`
	Channel string    `json:"channel"`
}

type VMUGap struct {
	Gap
	Source int    `json:"source"`
	UPI    string `json:"record"`
}

type RecordInfo struct {
	UPI   string `json:"record"`
	Count int    `json:"count"`
}

type SourceInfo struct {
	Source int `json:"source"`
	Count  int `json:"count"`
}

type ChannelInfo struct {
	Channel string `json:"source"`
	Count   int    `json:"count"`
}

type GapStore interface {
	FetchRecords() ([]RecordInfo, error)
	FetchSources() ([]SourceInfo, error)
	FetchChannels() ([]ChannelInfo, error)
	FetchGapsHRD(time.Time, time.Time, string) ([]HRDGap, error)
	FetchGapDetailHRD(int) (HRDGap, error)
	FetchGapsVMU(time.Time, time.Time, string) ([]VMUGap, error)
	FetchGapDetailVMU(int) (VMUGap, error)
}

type Replay struct {
	Id        int       `json:"id"`
	Status    string    `json:"status"`
	Priority  int       `json:"priority"`
	When      time.Time `json:"time"`
	Comment   string    `json:"comment"`
	Pass      int       `json:"pass"`
	Automatic bool      `json:"automatic"`
	Period
}

type StatusInfo struct {
	Id    int    `json:"id"`
	Name  string `json:"name"`
	Count int    `json:"count"`
	Order int    `json:"order"`
}

type ReplayStore interface {
	FetchStatus() ([]StatusInfo, error)
	FetchReplays(time.Time, time.Time, string) ([]Replay, error)
	FetchReplayDetail(int) (Replay, error)
	CancelReplay(int) error
	UpdateReplay(int, int) (Replay, error)
	RegisterReplay(Replay) (Replay, error)
}

type Variable struct {
	Id        int      `json:"id"`
	Name      string   `json:"name"`
	Value     string   `json:"value"`
	Range     []string `json:"range"`
	Hazardous bool     `json:"hazardous"`
}

type ConfigStore interface {
	FetchVariables() ([]Variable, error)
	UpdateVariable(int, string) (Variable, error)
	RegisterVariable(v Variable) (Variable, error)
}

type Store interface {
	Status() (interface{}, error)

	GapStore
	ReplayStore
	ConfigStore
}

type Handler func(r *http.Request) (interface{}, error)

var (
	ErrQuery  = errors.New("query")
	ErrEmpty  = errors.New("empty")
	ErrIntern = errors.New("internal")
	ErrExist  = errors.New("exist")
	ErrImpl   = errors.New("not implemented")
)

func main() {
	flag.Parse()

	conf := struct {
		Addr  string
		Quiet bool
		Mon   struct {
			Pid  string `toml:"pidfile"`
			Proc string `toml:"proc"`
		} `toml:"autobrm"`
		DB struct {
			Name   string `toml:"database"`
			Addr   string
			User   string
			Passwd string
		} `toml:"database"`
		Site struct {
			Base string `toml:"dir"`
			URL  string
		} `toml:"site"`
	}{}
	if err := toml.DecodeFile(flag.Arg(0), &conf); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	db, err := setupStore(conf.DB.Addr, conf.DB.User, conf.DB.Passwd, conf.DB.Name)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}

	handler := setupRoutes(db, []string{"*"})
	if !conf.Quiet {
		handler = handlers.LoggingHandler(os.Stdout, handler)
	}
	if err := http.ListenAndServe(conf.Addr, handler); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func setupRoutes(db Store, origins []string) http.Handler {
	routes := []struct {
		Do      Handler
		URL     string
		Methods []string
	}{
		{
			URL:     "/status/",
			Do:      listStatus(db),
			Methods: []string{http.MethodGet},
		},
		{
			URL:     "/requests/",
			Do:      listRequests(db),
			Methods: []string{http.MethodGet},
		},
		{
			URL:     "/requests/status/",
			Do:      listRegisteredStatus(db),
			Methods: []string{http.MethodGet},
		},
		{
			URL:     "/requests/",
			Do:      registerRequest(db),
			Methods: []string{http.MethodPost},
		},
		{
			URL:     "/requests/{id}",
			Do:      cancelRequest(db),
			Methods: []string{http.MethodDelete},
		},
		{
			URL:     "/requests/{id}",
			Do:      updateRequest(db),
			Methods: []string{http.MethodPut},
		},
		{
			URL:     "/requests/{id}",
			Do:      showRequest(db),
			Methods: []string{http.MethodGet},
		},
		{
			URL:     "/archives/vmu/gaps/",
			Do:      listGapsVMU(db),
			Methods: []string{http.MethodGet},
		},
		{
			URL:     "/archives/vmu/records/",
			Do:      listRecordsVMU(db),
			Methods: []string{http.MethodGet},
		},
		{
			URL:     "/archives/vmu/sources/",
			Do:      listSourcesVMU(db),
			Methods: []string{http.MethodGet},
		},
		{
			URL:     "/archives/vmu/gaps/{id}",
			Do:      showGapVMU(db),
			Methods: []string{http.MethodGet},
		},
		{
			URL:     "/archives/hrd/gaps/",
			Do:      listGapsHRD(db),
			Methods: []string{http.MethodGet},
		},
		{
			URL:     "/archives/hrd/channels/",
			Do:      listChannelsHRD(db),
			Methods: []string{http.MethodGet},
		},
		{
			URL:     "/archives/hrd/gaps/{id}",
			Do:      showGapHRD(db),
			Methods: []string{http.MethodGet},
		},
		{
			URL:     "/config/",
			Do:      listVariables(db),
			Methods: []string{http.MethodGet},
		},
		{
			URL:     "/config/{id}",
			Do:      updateVariable(db),
			Methods: []string{http.MethodPut},
		},
		{
			URL:     "",
			Do:      nil,
			Methods: []string{},
		},
	}
	r := mux.NewRouter()
	for _, route := range routes {
		next := wrapHandler(route.Do)
		r.Handle(route.URL, next).Methods(route.Methods...).Headers("Accept", "application/json")
	}
	methods := []string{
		http.MethodGet,
		http.MethodOptions,
		http.MethodDelete,
		http.MethodPut,
		http.MethodPost,
	}
	return handlers.CORS(handlers.AllowedOrigins(origins), handlers.AllowedMethods(methods))(r)
}

func setupStore(addr, user, passwd, name string) (Store, error) {
	if addr == "file" || addr == "dir" {
		return NewFileStore(name)
	}
	return NewDBStore(addr, name, user, passwd)
}

func wrapHandler(do Handler) http.Handler {
	next := func(w http.ResponseWriter, r *http.Request) {
		data, err := do(r)
		if err != nil {
			code := http.StatusInternalServerError
			switch {
			case errors.Is(err, ErrQuery):
				code = http.StatusBadRequest
			case errors.Is(err, ErrIntern):
			case errors.Is(err, ErrExist):
				code = http.StatusNotFound
			case errors.Is(err, ErrEmpty):
				code = http.StatusNoContent
			case errors.Is(err, ErrImpl):
				code = http.StatusNotImplemented
			}
			w.WriteHeader(code)
			return
		}
		code := http.StatusOK
		if r.Method == http.MethodPost {
			code = http.StatusCreated
		}
		if data == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(data)
	}
	return http.HandlerFunc(next)
}

const (
	fieldStart   = "dtstart"
	fieldEnd     = "dtend"
	fieldChannel = "channel"
	fieldId      = "id"
	fieldStatus  = "status"
	fieldRecord  = "record"
)

func listStatus(db Store) Handler {
	return func(r *http.Request) (interface{}, error) {
		return db.Status()
	}
}

func listRequests(db ReplayStore) Handler {
	return func(r *http.Request) (interface{}, error) {
		start, end, err := parsePeriod(r)
		if err != nil {
			return nil, fmt.Errorf("%w: err", ErrQuery, err)
		}
		return db.FetchReplays(start, end, r.URL.Query().Get(fieldStatus))
	}
}

func listRegisteredStatus(db ReplayStore) Handler {
	return func(r *http.Request) (interface{}, error) {
		return db.FetchStatus()
	}
}

func showRequest(db ReplayStore) Handler {
	return func(r *http.Request) (interface{}, error) {
		id, err := parseInt(r, fieldId)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", ErrQuery, err)
		}
		return db.FetchReplayDetail(id)
	}
}

func updateRequest(db ReplayStore) Handler {
	return func(r *http.Request) (interface{}, error) {
		id, err := parseInt(r, fieldId)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", ErrQuery, err)
		}
		v := struct {
			Priority int `json:"priority"`
		}{}
		if err := parseBody(r, &v); err != nil {
			return nil, fmt.Errorf("%w: %s", ErrQuery, err)
		}
		return db.UpdateReplay(id, v.Priority)
	}
}

func cancelRequest(db ReplayStore) Handler {
	return func(r *http.Request) (interface{}, error) {
		id, err := parseInt(r, fieldId)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", ErrQuery, err)
		}
		return nil, db.CancelReplay(id)
	}
}

func registerRequest(db ReplayStore) Handler {
	return func(r *http.Request) (interface{}, error) {
		var rp Replay
		if err := parseBody(r, &rp); err != nil {
			return nil, fmt.Errorf("%w: %s", ErrQuery, err)
		}
		return db.RegisterReplay(rp)
	}
}

func listGapsVMU(db GapStore) Handler {
	return func(r *http.Request) (interface{}, error) {
		start, end, err := parsePeriod(r)
		if err != nil {
			return nil, fmt.Errorf("%w: err", ErrQuery, err)
		}
		return db.FetchGapsVMU(start, end, r.URL.Query().Get(fieldRecord))
	}
}

func showGapVMU(db GapStore) Handler {
	return func(r *http.Request) (interface{}, error) {
		id, err := parseInt(r, fieldId)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", ErrQuery)
		}
		return db.FetchGapDetailVMU(id)
	}
}

func listRecordsVMU(db GapStore) Handler {
	return func(r *http.Request) (interface{}, error) {
		return db.FetchRecords()
	}
}

func listSourcesVMU(db GapStore) Handler {
	return func(r *http.Request) (interface{}, error) {
		return db.FetchSources()
	}
}

func listGapsHRD(db GapStore) Handler {
	return func(r *http.Request) (interface{}, error) {
		start, end, err := parsePeriod(r)
		if err != nil {
			return nil, fmt.Errorf("%w: err", ErrQuery, err)
		}
		return db.FetchGapsHRD(start, end, r.URL.Query().Get(fieldChannel))
	}
}

func listChannelsHRD(db GapStore) Handler {
	return func(r *http.Request) (interface{}, error) {
		return db.FetchChannels()
	}
}

func showGapHRD(db GapStore) Handler {
	return func(r *http.Request) (interface{}, error) {
		id, err := parseInt(r, fieldId)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", ErrQuery)
		}
		return db.FetchGapDetailHRD(id)
	}
}

func listVariables(db ConfigStore) Handler {
	return func(r *http.Request) (interface{}, error) {
		return db.FetchVariables()
	}
}

func updateVariable(db ConfigStore) Handler {
	return func(r *http.Request) (interface{}, error) {
		id, err := parseInt(r, fieldId)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", ErrQuery)
		}
		v := struct {
			Value string `json:"value"`
		}{}
		if err := parseBody(r, &v); err != nil {
			return nil, fmt.Errorf("%w: %s", ErrQuery, err)
		}
		return db.UpdateVariable(id, v.Value)
	}
}

func registerVariable(db ConfigStore) Handler {
	return func(r *http.Request) (interface{}, error) {
		var v Variable
		if err := parseBody(r, &v); err != nil {
			return nil, fmt.Errorf("%w: %s", ErrQuery, err)
		}
		return db.RegisterVariable(v)
	}
}

const MaxBodySize = 4 << 20

func parseBody(r *http.Request, body interface{}) error {
	defer r.Body.Close()
	rs := io.LimitedReader{
		R: r.Body,
		N: MaxBodySize,
	}
	return json.NewDecoder(&rs).Decode(body)
}

func parseInt(r *http.Request, field string) (int, error) {
	vars := mux.Vars(r)
	return strconv.Atoi(vars[field])
}

func parsePeriod(r *http.Request) (time.Time, time.Time, error) {
	var (
		start time.Time
		end   time.Time
		err   error
		query = r.URL.Query()
		str   string
	)
	str = query.Get(fieldStart)
	if start, err = parseDatetime(str); err != nil && str != "" {
		return start, end, err
	}
	str = query.Get(fieldEnd)
	if end, err = parseDatetime(str); err != nil && str != "" {
		return start, end, err
	}
	return start, end, nil
}

func parseDatetime(str string) (time.Time, error) {
	return time.Parse(time.RFC3339, str)
}
