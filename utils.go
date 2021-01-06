package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gorilla/mux"
)

const MaxBodySize = 4 << 20

func parseBody(r *http.Request, body interface{}) error {
	defer r.Body.Close()
	rs := io.LimitedReader{
		R: r.Body,
		N: MaxBodySize,
	}
	return json.NewDecoder(&rs).Decode(body)
}

func parseLimit(q url.Values) (int, int, error) {
	limit, err := parseIntQuery(q, fieldCount)
	if err != nil {
		return 0, 0, err
	}
	offset, err := parseIntQuery(q, fieldPage)
	if err != nil {
		return 0, 0, err
	}
	if offset > 0 {
		offset--
	}
	return limit, offset, nil
}

func parseBoolQuery(q url.Values, field string) (bool, error) {
	field = q.Get(field)
	if field == "" {
		return false, nil
	}
	return strconv.ParseBool(field)
}

func parseIntQuery(q url.Values, field string) (int, error) {
	field = q.Get(field)
	if field == "" {
		return 0, nil
	}
	return strconv.Atoi(field)
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
	if end.IsZero() {
		end = time.Now()
	}
	if start.IsZero() {
		start = end.Add(-time.Hour * 24)
	}
	return start, end, nil
}

func parseDatetime(str string) (time.Time, error) {
	return time.Parse(time.RFC3339, str)
}
