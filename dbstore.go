package main

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/midbel/quel"
)

type DBStore struct {
	db  *sql.DB
	mon Monitor
}

func NewDBStore(addr, name, user, passwd string, mon Monitor) (Store, error) {
	addr = fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true", user, passwd, addr, name)
	db, err := sql.Open("mysql", addr)
	if err != nil {
		return nil, fmt.Errorf("fail to connect: %w", err)
	}
	db.SetConnMaxLifetime(time.Second * 150)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	s := DBStore{
		db:  db,
		mon: mon,
	}
	return s, nil
}

func (s DBStore) Status() (interface{}, error) {
	status := map[string]interface{}{
		"autobrm": s.mon.readProcess(),
		"requests": map[string]interface{}{
			"count": s.countRequests(),
		},
		"hrd": map[string]interface{}{
			"count": s.countGapsHRD(),
		},
		"vmu": map[string]interface{}{
			"count": s.countGapsVMU(),
		},
	}
	return status, nil
}

func (s DBStore) countRequests() int {
	return s.countItems("replay")
}

func (s DBStore) countGapsHRD() int {
	return s.countItems("hrd_packet_gap")
}

func (s DBStore) countGapsVMU() int {
	return s.countItems("vmu_packet_gap")
}

func (s DBStore) countItems(table string) int {
	cid := quel.Count(quel.NewIdent("id"))
	q, err := quel.NewSelect(table, quel.SelectColumn(cid))
	if err != nil {
		return 0
	}
	query, _, err := q.SQL()
	if err != nil {
		return 0
	}
	var count int
	if err := s.db.QueryRow(query).Scan(&count); err != nil {
		return count
	}
	return count
}

func (s DBStore) normalizeInterval(start, end time.Time, table, column string) (time.Time, time.Time, error) {
	if !start.IsZero() && !end.IsZero() && start.After(end) {
		return start, end, ErrQuery
	}
	var err error
	if start.IsZero() && end.IsZero() {
		start, end, err = s.retrInterval(table, column)
	}
	return start, end, err
}

func (s DBStore) retrInterval(table, column string) (time.Time, time.Time, error) {
	var (
		dtstart time.Time
		dtend   time.Time
		max     = quel.Max(quel.NewIdent(column))
		min     = quel.Func("DATE_SUB", max, quel.Days(3))
		options = []quel.SelectOption{
			quel.SelectColumn(min),
			quel.SelectColumn(max),
		}
	)
	q, err := quel.NewSelect(table, options...)
	if err != nil {
		return dtstart, dtend, err
	}
	query, args, err := q.SQL()
	if err != nil {
		return dtstart, dtend, err
	}
	return dtstart, dtend, s.db.QueryRow(query, args...).Scan(&dtstart, &dtend)
}

func (s DBStore) FetchStatus() ([]StatusInfo, error) {
	q, err := prepareStatusInfoQuery()
	if err != nil {
		return nil, err
	}

	var vs []StatusInfo
	return vs, s.query(q, func(rows *sql.Rows) error {
		var (
			s   StatusInfo
			err error
		)
		if err = rows.Scan(&s.Id, &s.Name, &s.Order, &s.Count); err == nil {
			vs = append(vs, s)
		}
		return err
	})
}

func prepareStatusInfoQuery() (quel.SQLer, error) {
	qs, err := prepareStatusQuery()
	if err != nil {
		return nil, err
	}
	qc, err := prepareCountStatusQuery()
	if err != nil {
		return nil, err
	}
	var (
		cdt   = quel.Equal(quel.NewIdent("id", "s"), quel.NewIdent("replay_status_id", "c"))
		count = quel.Coalesce(quel.NewIdent("count", "c"), quel.Arg("count", 0))
	)
	return qs.LeftOuterJoin(quel.Alias("c", qc), cdt, quel.SelectColumn(count))
}

func prepareStatusQuery() (quel.Select, error) {
	options := []quel.SelectOption{
		quel.SelectColumn(quel.NewIdent("id", "s")),
		quel.SelectColumn(quel.NewIdent("name", "s")),
		quel.SelectColumn(quel.NewIdent("workflow", "s")),
		quel.SelectAlias("s"),
	}
	return quel.NewSelect("replay_status", options...)
}

func prepareCountStatusQuery() (quel.Select, error) {
	options := []quel.SelectOption{
		quel.SelectColumn(quel.NewIdent("replay_status_id")),
		quel.SelectColumn(quel.Alias("count", quel.Count(quel.NewIdent("replay_status_id")))),
		quel.SelectGroupBy(quel.NewIdent("replay_status_id")),
	}
	return quel.NewSelect("replay_job", options...)
}

func (s DBStore) FetchReplays(start time.Time, end time.Time, status string) ([]Replay, error) {
	start, end, err := s.normalizeInterval(start, end, "replay", "startdate")
	if err != nil {
		return nil, err
	}
	q, err := prepareSelectListReplay(start, end, status)
	if err != nil {
		return nil, err
	}
	var vs []Replay
	return vs, s.query(q, func(rows *sql.Rows) error {
		var (
			r   Replay
			err error
		)
		if err = rows.Scan(&r.Id, &r.When, &r.Starts, &r.Ends, &r.Priority, &r.Comment, &r.Status, &r.Automatic, &r.Cancellable); err == nil {
			vs = append(vs, r)
		}
		return err
	})
}

func prepareSelectListReplay(start, end time.Time, status string) (quel.SQLer, error) {
	var where quel.SQLer
	if start.IsZero() && !end.IsZero() {
		where = quel.LesserOrEqual(quel.NewIdent("startdate", "r"), quel.Arg("dtend", end))
	} else if !start.IsZero() && end.IsZero() {
		where = quel.GreaterOrEqual(quel.NewIdent("startdate", "r"), quel.Arg("dtstart", start))
	} else {
		fst := quel.GreaterOrEqual(quel.NewIdent("startdate", "r"), quel.Arg("dtstart", start))
		lst := quel.LesserOrEqual(quel.NewIdent("startdate", "r"), quel.Arg("dtend", end))
		where = quel.And(fst, lst)
	}
	if status != "" {
		where = quel.And(where, quel.Equal(quel.NewIdent("name", "s"), quel.Arg("status", status)))
	}
	return prepareSelectReplay(where)
}

func prepareSelectReplay(where quel.SQLer) (quel.SQLer, error) {
	var (
		cdt     quel.SQLer
		options = []quel.SelectOption{
			quel.SelectAlias("r"),
			quel.SelectColumn(quel.NewIdent("id", "r")),
			quel.SelectColumn(quel.NewIdent("timestamp", "r")),
			quel.SelectColumn(quel.NewIdent("startdate", "r")),
			quel.SelectColumn(quel.NewIdent("enddate", "r")),
			quel.SelectColumn(quel.Coalesce(quel.NewIdent("priority", "r"), quel.NewLiteral(-1))),
		}
	)
	q, err := quel.NewSelect("replay", options...)
	if err != nil {
		return nil, err
	}

	options = []quel.SelectOption{
		quel.SelectColumn(quel.NewIdent("replay_id")),
		quel.SelectColumn(quel.Alias("replay_status_id", quel.Max(quel.NewIdent("replay_status_id")))),
		quel.SelectGroupBy(quel.NewIdent("replay_id")),
	}
	jobs, err := quel.NewSelect("replay_job", options...)
	if err != nil {
		return nil, err
	}

	options = []quel.SelectOption{
		quel.SelectColumn(quel.NewIdent("replay_id", "j")),
		quel.SelectColumn(quel.NewIdent("text", "j")),
		quel.SelectAlias("j"),
	}
	latest, err := quel.NewSelect("replay_job", options...)
	if err != nil {
		return nil, err
	}
	cdt = quel.NewList(quel.NewIdent("replay_id"), quel.NewIdent("replay_status_id"))
	latest, err = latest.LeftInnerJoin(quel.Alias("m", jobs), cdt, quel.SelectColumn(quel.NewIdent("replay_status_id", "m")))
	if err != nil {
		return nil, err
	}

	cdt = quel.Equal(quel.NewIdent("id", "r"), quel.NewIdent("replay_id", "j"))
	options = []quel.SelectOption{
		quel.SelectColumn(quel.Coalesce(quel.NewIdent("text", "j"), quel.NewLiteral(""))),
	}
	q, err = q.LeftInnerJoin(quel.Alias("j", latest), cdt, options...)
	if err != nil {
		return nil, err
	}

	cdt = quel.Equal(quel.NewIdent("id", "s"), quel.NewIdent("replay_status_id", "j"))
	options = []quel.SelectOption{
		quel.SelectColumn(quel.NewIdent("name", "s")),
	}
	q, err = q.LeftInnerJoin(quel.Alias("s", quel.NewIdent("replay_status")), cdt, options...)
	if err != nil {
		return nil, err
	}

	gaps, err := quel.NewDistinct("gap_replay_list", quel.SelectColumns("replay_id"))
	if err != nil {
		return nil, err
	}

	options = []quel.SelectOption{
		quel.SelectColumn(quel.NewIdent("id")),
		quel.SelectOrderBy(quel.Desc("workflow")),
		quel.SelectLimit(4),
	}
	cancellable, err := quel.NewSelect("replay_status", options...)
	if err != nil {
		return nil, err
	}

	sub, err := quel.NewSelect("cancellable")
	if err != nil {
		return nil, err
	}
	cdt = quel.Equal(quel.NewIdent("id", "r"), quel.NewIdent("replay_id", "g"))
	options = []quel.SelectOption{
		quel.SelectColumn(quel.IsNull(quel.NewIdent("replay_id", "g"))),
		quel.SelectColumn(quel.NotIn(quel.NewIdent("replay_status_id"), sub)),
		quel.SelectWith("cancellable", cancellable, quel.NewIdent("id")),
		quel.SelectWhere(where),
	}
	return q.LeftOuterJoin(quel.Alias("g", gaps), cdt, options...)
}

func (s DBStore) FetchReplayDetail(id int) (Replay, error) {
	var r Replay
	return r, ErrImpl
}

func (s DBStore) CancelReplay(id int, comment string) (Replay, error) {
	var r Replay
	if err := s.shouldCancelReplay(id); err != nil {
		return r, err
	}
	get, err := prepareRetrCancelStatus("id")
	if err != nil {
		return r, err
	}
	tx, err := s.db.Begin()
	options := []quel.InsertOption{
		quel.InsertColumns("timestamp", "replay_id", "replay_status_id", "text"),
		quel.InsertValues(quel.Now(), quel.Arg("id", id), get, quel.Arg("comment", comment)),
	}
	i, err := quel.NewInsert("replay_job", options...)
	if err == nil {
		err = s.exec(tx, i, []string{"id", "comment"})
	}
	if err != nil {
		tx.Rollback()
		return r, err
	} else {
		tx.Commit()
	}
	return r, s.retrReplay(id, &r)
}

func (s DBStore) shouldCancelReplay(id int) error {
	sub, err := prepareRetrCancelStatus("id")
	if err != nil {
		return err
	}
	var (
		ideq  = quel.Equal(quel.NewIdent("replay_id"), quel.Arg("id", id))
		jobeq = quel.Equal(quel.NewIdent("replay_status_id"), sub)
		and   = quel.And(ideq, jobeq)
	)
	options := []quel.SelectOption{
		quel.SelectColumn(quel.NewIdent("replay_id")),
		quel.SelectWhere(and),
	}
	q, err := quel.NewSelect("replay_job", options...)
	if err != nil {
		return err
	}
	query, args, err := q.SQL()
	if err != nil {
		return err
	}
	err = s.db.QueryRow(query, args...).Scan(&id)
	if err == nil {
		err = fmt.Errorf("%w: replay job already cancelled", ErrQuery)
	} else if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return err
}

func (s DBStore) UpdateReplay(id int, priority int) (Replay, error) {
	var (
		r       Replay
		options = []quel.UpdateOption{
			quel.UpdateColumn("priority", quel.Arg("priority", priority)),
			quel.UpdateWhere(quel.Equal(quel.NewIdent("id"), quel.Arg("id", id))),
		}
	)
	q, err := quel.NewUpdate("replay", options...)
	if err != nil {
		return r, err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return r, err
	}
	if err = s.exec(tx, q, []string{"priority", "id"}); err != nil {
		tx.Rollback()
		return r, err
	}
	if err = tx.Commit(); err != nil {
		return r, err
	}
	return r, s.retrReplay(id, &r)
}

func (s DBStore) retrReplay(id int, r *Replay) error {
	where := quel.Equal(quel.NewIdent("id", "r"), quel.Arg("id", id))
	q, err := prepareSelectReplay(where)
	if err != nil {
		return err
	}
	query, args, err := q.SQL()
	if err != nil {
		return err
	}
	return s.db.QueryRow(query, args...).Scan(&r.Id, &r.When, &r.Starts, &r.Ends, &r.Priority, &r.Comment, &r.Status, &r.Automatic, &r.Cancellable)
}

func (s DBStore) RegisterReplay(r Replay) (Replay, error) {
	if !r.isValid() {
		return r, fmt.Errorf("%w: invalid period", ErrQuery)
	}
	tx, err := s.db.Begin()
	if err != nil {
		return r, err
	}
	if err := s.registerReplay(tx, &r); err != nil {
		tx.Rollback()
		return r, err
	}
	if err := s.registerReplayJob(tx, &r); err != nil {
		tx.Rollback()
		return r, err
	}
	return r, tx.Commit()
}

func (s DBStore) registerReplay(tx *sql.Tx, r *Replay) error {
	insert := []quel.InsertOption{
		quel.InsertColumns("timestamp", "startdate", "enddate", "priority"),
		quel.InsertValues(quel.Now(), quel.Arg("dtstart", r.Starts), quel.Arg("dtend", r.Ends), quel.Arg("priority", r.Priority)),
	}
	i, err := quel.NewInsert("replay", insert...)
	if err != nil {
		return err
	}
	if err := s.exec(tx, i, []string{"dtstart", "dtend", "priority"}); err != nil {
		return err
	}
	retrieve := []quel.SelectOption{
		quel.SelectColumn(quel.Func("LAST_INSERT_ID")),
		quel.SelectLimit(1),
	}
	q, err := quel.NewSelect("replay", retrieve...)
	if err != nil {
		return err
	}
	sql, _, err := q.SQL()
	if err != nil {
		return err
	}
	return tx.QueryRow(sql).Scan(&r.Id)
}

func (s DBStore) registerReplayJob(tx *sql.Tx, r *Replay) error {
	get, err := prepareRetrInitialStatus("id")
	if err != nil {
		return err
	}
	options := []quel.InsertOption{
		quel.InsertColumns("timestamp", "text", "replay_id", "replay_status_id"),
		quel.InsertValues(quel.Now(), quel.Arg("comment", r.Comment), quel.Arg("replay", r.Id), get),
	}
	i, err := quel.NewInsert("replay_job", options...)
	if err == nil {
		err = s.exec(tx, i, []string{"comment", "replay"})
	}
	return err
}

func prepareRetrCancelStatus(field string) (quel.Select, error) {
	var (
		max      = quel.Max(quel.NewIdent("workflow"))
		where, _ = quel.NewSelect("replay_status", quel.SelectColumn(max))
		options  = []quel.SelectOption{
			quel.SelectColumn(quel.NewIdent(field)),
			quel.SelectWhere(quel.Equal(quel.NewIdent("workflow"), where)),
			quel.SelectLimit(1),
		}
	)
	return quel.NewSelect("replay_status", options...)
}

func prepareRetrInitialStatus(field string) (quel.Select, error) {
	var (
		min      = quel.Min(quel.NewIdent("workflow"))
		where, _ = quel.NewSelect("replay_status", quel.SelectColumn(min))
		options  = []quel.SelectOption{
			quel.SelectColumn(quel.NewIdent(field)),
			quel.SelectWhere(quel.Equal(quel.NewIdent("workflow"), where)),
			quel.SelectLimit(1),
		}
	)
	return quel.NewSelect("replay_status", options...)
}

func (s DBStore) FetchChannels() ([]ChannelInfo, error) {
	ident := quel.NewIdent("chanel")
	options := []quel.SelectOption{
		quel.SelectColumn(ident),
		quel.SelectColumn(quel.Count(ident)),
		quel.SelectGroupBy(ident),
	}
	q, err := quel.NewSelect("hrd_packet_gap", options...)
	if err != nil {
		return nil, err
	}
	var vs []ChannelInfo
	return vs, s.query(q, func(rows *sql.Rows) error {
		var (
			c   ChannelInfo
			err error
		)
		if err = rows.Scan(&c.Channel, &c.Count); err == nil {
			vs = append(vs, c)
		}
		return err
	})
}

func (s DBStore) FetchGapsHRD(start time.Time, end time.Time, channel string) ([]HRDGap, error) {
	start, end, err := s.normalizeInterval(start, end, "hrd_packet_gap", "last_timestamp")
	if err != nil {
		return nil, err
	}

	var where quel.SQLer
	if start.IsZero() && !end.IsZero() {
		where = quel.LesserOrEqual(quel.NewIdent("last_timestamp", "r"), quel.Arg("dtend", end))
	} else if !start.IsZero() && end.IsZero() {
		where = quel.GreaterOrEqual(quel.NewIdent("last_timestamp", "r"), quel.Arg("dtstart", start))
	} else {
		fst := quel.GreaterOrEqual(quel.NewIdent("last_timestamp", "r"), quel.Arg("dtstart", start))
		lst := quel.LesserOrEqual(quel.NewIdent("last_timestamp", "r"), quel.Arg("dtend", end))
		where = quel.And(fst, lst)
	}
	if channel != "" {
		eq := quel.Equal(quel.NewIdent("chanel"), quel.Arg("channel", channel))
		where = quel.And(where, eq)
	}

	options := []quel.SelectOption{
		quel.SelectColumn(quel.NewIdent("id", "r")),
		quel.SelectColumn(quel.NewIdent("timestamp", "r")),
		quel.SelectColumn(quel.NewIdent("last_timestamp", "r")),
		quel.SelectColumn(quel.NewIdent("last_sequence_count", "r")),
		quel.SelectColumn(quel.NewIdent("next_timestamp", "r")),
		quel.SelectColumn(quel.NewIdent("next_sequence_count", "r")),
		quel.SelectColumn(quel.NewIdent("chanel", "r")),
		quel.SelectWhere(where),
		quel.SelectAlias("r"),
	}

	q, err := quel.NewSelect("hrd_packet_gap", options...)
	if err != nil {
		return nil, err
	}
	var vs []HRDGap
	return vs, s.query(q, func(rows *sql.Rows) error {
		var (
			g   HRDGap
			err error
		)
		if err = rows.Scan(&g.Id, &g.When, &g.Starts, &g.First, &g.Ends, &g.Last, &g.Channel); err == nil {
			vs = append(vs, g)
		}
		return err
	})
}

func (s DBStore) FetchGapDetailHRD(id int) (HRDGap, error) {
	var h HRDGap
	return h, ErrImpl
}

func (s DBStore) FetchGapsVMU(start time.Time, end time.Time, record string) ([]VMUGap, error) {
	start, end, err := s.normalizeInterval(start, end, "vmu_packet_gap", "last_timestamp")
	if err != nil {
		return nil, err
	}
	options := []quel.SelectOption{
		quel.SelectColumn(quel.NewIdent("id", "g")),
		quel.SelectColumn(quel.NewIdent("timestamp", "g")),
		quel.SelectColumn(quel.NewIdent("last_timestamp", "g")),
		quel.SelectColumn(quel.NewIdent("last_sequence_count", "g")),
		quel.SelectColumn(quel.NewIdent("next_timestamp", "g")),
		quel.SelectColumn(quel.NewIdent("next_sequence_count", "g")),
		quel.SelectAlias("g"),
	}
	q, err := quel.NewSelect("vmu_packet_gap", options...)
	if err != nil {
		return nil, err
	}

	var where quel.SQLer
	if start.IsZero() && !end.IsZero() {
		where = quel.LesserOrEqual(quel.NewIdent("last_timestamp", "g"), quel.Arg("dtend", end))
	} else if !start.IsZero() && end.IsZero() {
		where = quel.GreaterOrEqual(quel.NewIdent("last_timestamp", "g"), quel.Arg("dtstart", start))
	} else {
		fst := quel.GreaterOrEqual(quel.NewIdent("last_timestamp", "g"), quel.Arg("dtstart", start))
		lst := quel.LesserOrEqual(quel.NewIdent("last_timestamp", "g"), quel.Arg("dtend", end))
		where = quel.And(fst, lst)
	}
	if record != "" {
		eq := quel.Equal(quel.NewIdent("phase", "r"), quel.Arg("record", record))
		where = quel.And(where, eq)
	}

	options = []quel.SelectOption{
		quel.SelectColumn(quel.NewIdent("source", "r")),
		quel.SelectColumn(quel.Coalesce(quel.NewIdent("phase", "r"), quel.NewLiteral(""))),
		quel.SelectWhere(where),
	}
	cdt := quel.Equal(quel.NewIdent("vmu_record_id", "g"), quel.NewIdent("id", "r"))
	q, err = q.LeftInnerJoin(quel.Alias("r", quel.NewIdent("vmu_record")), cdt, options...)
	if err != nil {
		return nil, err
	}

	var vs []VMUGap
	return vs, s.query(q, func(rows *sql.Rows) error {
		var (
			g   VMUGap
			err error
		)
		if err = rows.Scan(&g.Id, &g.When, &g.Starts, &g.First, &g.Ends, &g.Last, &g.Source, &g.UPI); err == nil {
			vs = append(vs, g)
		}
		return err
	})
}

func (s DBStore) FetchGapDetailVMU(id int) (VMUGap, error) {
	var v VMUGap
	return v, ErrImpl
}

func (s DBStore) FetchSources() ([]SourceInfo, error) {
	options := []quel.SelectOption{
		quel.SelectColumn(quel.NewIdent("vmu_record_id")),
		quel.SelectColumn(quel.Alias("total", quel.Count(quel.NewIdent("vmu_record_id")))),
		quel.SelectGroupBy(quel.NewIdent("vmu_record_id")),
	}
	sub, err := quel.NewSelect("vmu_packet_gap", options...)
	if err != nil {
		return nil, err
	}

	options = []quel.SelectOption{
		quel.SelectColumn(quel.NewIdent("source", "r")),
		quel.SelectAlias("r"),
	}
	q, err := quel.NewSelect("vmu_record", options...)
	if err != nil {
		return nil, err
	}
	cdt := quel.Equal(quel.NewIdent("id", "r"), quel.NewIdent("vmu_record_id", "g"))
	options = []quel.SelectOption{
		quel.SelectColumn(quel.Sum(quel.NewIdent("total", "g"))),
		quel.SelectWhere(quel.IsNotNullTest(quel.NewIdent("source", "r"))),
		quel.SelectGroupBy(quel.NewIdent("source", "r")),
	}
	q, err = q.LeftInnerJoin(quel.Alias("g", sub), cdt, options...)
	if err != nil {
		return nil, err
	}
	var rs []SourceInfo
	return rs, s.query(q, func(rows *sql.Rows) error {
		var (
			r   SourceInfo
			err error
		)
		if err = rows.Scan(&r.Source, &r.Count); err == nil {
			rs = append(rs, r)
		}
		return err
	})
}

func (s DBStore) FetchRecords() ([]RecordInfo, error) {
	options := []quel.SelectOption{
		quel.SelectColumn(quel.NewIdent("vmu_record_id")),
		quel.SelectColumn(quel.Alias("total", quel.Count(quel.NewIdent("vmu_record_id")))),
		quel.SelectGroupBy(quel.NewIdent("vmu_record_id")),
	}
	sub, err := quel.NewSelect("vmu_packet_gap", options...)
	if err != nil {
		return nil, err
	}

	options = []quel.SelectOption{
		quel.SelectColumn(quel.NewIdent("phase", "r")),
		quel.SelectAlias("r"),
	}
	q, err := quel.NewSelect("vmu_record", options...)
	if err != nil {
		return nil, err
	}
	cdt := quel.Equal(quel.NewIdent("id", "r"), quel.NewIdent("vmu_record_id", "g"))
	options = []quel.SelectOption{
		quel.SelectColumn(quel.Sum(quel.NewIdent("total", "g"))),
		quel.SelectWhere(quel.IsNotNullTest(quel.NewIdent("phase", "r"))),
		quel.SelectGroupBy(quel.NewIdent("phase", "r")),
	}
	q, err = q.LeftInnerJoin(quel.Alias("g", sub), cdt, options...)
	if err != nil {
		return nil, err
	}
	var rs []RecordInfo
	return rs, s.query(q, func(rows *sql.Rows) error {
		var (
			r   RecordInfo
			err error
		)
		if err = rows.Scan(&r.UPI, &r.Count); err == nil {
			rs = append(rs, r)
		}
		return err
	})
}

func (s DBStore) FetchVariables() ([]Variable, error) {
	q, err := quel.NewSelect("variable", quel.SelectColumns("id", "name", "value"))
	if err != nil {
		return nil, err
	}
	var vs []Variable
	return vs, s.query(q, func(rows *sql.Rows) error {
		var (
			v   Variable
			err error
		)
		if err = rows.Scan(&v.Id, &v.Name, &v.Value); err == nil {
			vs = append(vs, v)
		}
		return err
	})
}

func (s DBStore) UpdateVariable(id int, value string) (Variable, error) {
	var (
		v       Variable
		options = []quel.UpdateOption{
			quel.UpdateColumn("value", quel.Arg("value", value)),
			quel.UpdateWhere(quel.Equal(quel.NewIdent("id"), quel.Arg("id", id))),
		}
	)
	q, err := quel.NewUpdate("variable", options...)
	if err != nil {
		return v, err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return v, err
	}
	if err = s.exec(tx, q, []string{"value", "id"}); err != nil {
		tx.Rollback()
		return v, err
	}
	if err = tx.Commit(); err != nil {
		return v, err
	}
	return v, s.retrVariable(id, &v)
}

func (s DBStore) retrVariable(id int, v *Variable) error {
	options := []quel.SelectOption{
		quel.SelectColumns("id", "name", "value"),
		quel.SelectWhere(quel.Equal(quel.NewIdent("id"), quel.Arg("id", id))),
	}
	q, err := quel.NewSelect("variable", options...)
	if err != nil {
		return err
	}
	query, args, err := q.SQL()
	if err != nil {
		return err
	}
	return s.db.QueryRow(query, args...).Scan(&v.Id, &v.Name, &v.Value)
}

func (s DBStore) RegisterVariable(v Variable) (Variable, error) {
	return v, ErrImpl
}

func (s DBStore) exec(tx *sql.Tx, q quel.SQLer, names []string) error {
	query, args, err := q.SQL()
	if err != nil {
		return err
	}
	if len(args) != len(names) {
		return fmt.Errorf("number of arguments mismatched!")
	}
	_, err = tx.Exec(query, args...)
	return err
}

func (s DBStore) query(q quel.SQLer, scan func(rows *sql.Rows) error) error {
	query, args, err := q.SQL()
	if err != nil {
		return err
	}
	rows, err := s.db.Query(query, args...)
	switch err {
	case nil:
	case sql.ErrNoRows:
		return nil
	default:
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := scan(rows); err != nil {
			return err
		}
	}
	return nil
}
