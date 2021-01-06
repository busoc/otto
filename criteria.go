package main

import (
	"net/http"
	"strings"

	"github.com/midbel/quel"
)

const (
	fieldStart     = "dtstart"
	fieldEnd       = "dtend"
	fieldChannel   = "channel"
	fieldId        = "id"
	fieldStatus    = "status"
	fieldRecord    = "record"
	fieldSource    = "source"
	fieldCount     = "limit"
	fieldPage      = "page"
	fieldCorrupted = "corrupted"
	fieldCompleted = "completed"
	fieldOrder     = "order"
	fieldBy        = "by"
)

type Criteria struct {
	Period
	Channel   string
	Status    string
	Record    string
	Source    string
	Corrupted bool
	Completed bool

	Field string
	Order string

	Limit  int
	Offset int
}

func FromRequest(r *http.Request) (Criteria, error) {
	var (
		c   Criteria
		err error
		q   = r.URL.Query()
	)
	if c.Starts, c.Ends, err = parsePeriod(r); err != nil {
		return c, err
	}
	c.Channel = q.Get(fieldChannel)
	c.Status = q.Get(fieldStatus)
	c.Record = q.Get(fieldRecord)
	c.Source = q.Get(fieldSource)

	c.Field = q.Get(fieldBy)
	c.Order = q.Get(fieldOrder)

	if c.Corrupted, err = parseBoolQuery(q, fieldCorrupted); err != nil {
		return c, err
	}
	if c.Completed, err = parseBoolQuery(q, fieldCompleted); err != nil {
		return c, err
	}
	if c.Limit, c.Offset, err = parseLimit(q); err != nil {
		return c, err
	}

	return c, nil
}

func (c Criteria) filterVMU() quel.SQLer {
	where := c.filterDates("g")
	if c.Record != "" {
		eq := quel.Equal(quel.NewIdent("phase", "g"), quel.Arg("record", c.Record))
		where = quel.And(where, eq)
	}
	if c.Source != "" {
		eq := quel.Equal(quel.NewIdent("source", "g"), quel.Arg("source", c.Source))
		where = quel.And(where, eq)
	}
	if !c.Corrupted {
		eq := quel.Equal(quel.NewIdent("corrupted", "g"), quel.NewLiteral(c.Corrupted))
		where = quel.And(where, eq)
	}
	if !c.Completed {
		eq := quel.Equal(quel.NewIdent("completed", "g"), quel.NewLiteral(c.Completed))
		where = quel.And(where, eq)
	}
	return where
}

func (c Criteria) filterHRD() quel.SQLer {
	where := c.filterDates("r")
	if c.Channel != "" {
		eq := quel.Equal(quel.NewIdent("channel"), quel.Arg("channel", c.Channel))
		where = quel.And(where, eq)
	}
	if !c.Corrupted {
		eq := quel.Equal(quel.NewIdent("corrupted", "r"), quel.NewLiteral(c.Corrupted))
		where = quel.And(where, eq)
	}
	if !c.Completed {
		eq := quel.Equal(quel.NewIdent("completed", "r"), quel.NewLiteral(c.Completed))
		where = quel.And(where, eq)
	}
	return where
}

func (c Criteria) filterReplay() quel.SQLer {
	where := c.filterDates("r")
	if c.Status != "" {
		eq := quel.Equal(quel.NewIdent("status", "r"), quel.Arg("status", c.Status))
		if where != nil {
			where = eq
		} else {
			where = quel.And(where, eq)
		}
	}
	return where
}

func (c Criteria) filterDates(alias string) quel.SQLer {
	var where quel.SQLer
	if c.Starts.IsZero() && !c.Ends.IsZero() {
		where = quel.LesserOrEqual(quel.NewIdent("timestamp", alias), quel.Arg("dtend", c.Ends))
	} else if !c.Starts.IsZero() && c.Ends.IsZero() {
		where = quel.GreaterOrEqual(quel.NewIdent("timestamp", alias), quel.Arg("dtstart", c.Starts))
	} else if !c.Starts.IsZero() && !c.Ends.IsZero() {
		fst := quel.GreaterOrEqual(quel.NewIdent("timestamp", alias), quel.Arg("dtstart", c.Starts))
		lst := quel.LesserOrEqual(quel.NewIdent("timestamp", alias), quel.Arg("dtend", c.Ends))
		where = quel.And(fst, lst)
	} else {

	}
	return where
}

func (c Criteria) limitResults() []quel.SelectOption {
	var options []quel.SelectOption
	if c.Limit > 0 {
		options = append(options, quel.SelectLimit(c.Limit))
		options = append(options, quel.SelectOffset(c.Offset*c.Limit))
	}
	return options
}

func (c Criteria) orderBy() quel.SelectOption {
	if c.Field == "" {
		c.Field = DefaultOrderField
	}
	var order quel.SQLer
	if strings.ToLower(c.Order) == "asc" {
		order = quel.Asc(c.Field)
	} else {
		order = quel.Desc(c.Field)
	}
	return quel.SelectOrderBy(order)
}

func (c Criteria) orderAndLimits() []quel.SelectOption {
	options := c.limitResults()
	options = append(options, c.orderBy())
	return options
}
