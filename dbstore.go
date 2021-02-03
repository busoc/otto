package main

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/midbel/quel"
)

const DefaultOrderField = "timestamp"

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
	where := quel.Equal(quel.NewIdent("timestamp"), quel.Func("current_date"))
	status := map[string]interface{}{
		"autobrm": s.mon.readProcess(),
		"requests": map[string]interface{}{
			"count": s.countRequests(where),
			"duration": s.pendingTime(),
		},
		"hrd": map[string]interface{}{
			"count": s.countGapsHRD(where),
		},
		"vmu": map[string]interface{}{
			"count": s.countGapsVMU(where),
		},
	}
	return status, nil
}

func (s DBStore) FetchStatusHRD(days int) ([]PacketInfo, error) {
	if days <= 0 {
		days = 30
	}
	var (
		expr    = quel.Func("DATE_SUB", quel.NewIdent("CURRENT_DATE"), quel.Days(days))
		options = []quel.SelectOption{
			quel.SelectColumn(quel.NewIdent("label")),
			quel.SelectColumn(quel.NewIdent("timestamp")),
			quel.SelectColumn(quel.NewIdent("channel")),
			quel.SelectColumn(quel.NewIdent("count")),
			quel.SelectWhere(quel.GreaterOrEqual(quel.NewIdent("timestamp"), expr)),
		}
	)
	q, err := quel.NewSelect("hrd_status_list", options...)
	if err != nil {
		return nil, err
	}
	var vs []PacketInfo
	return vs, s.query(q, func(rows *sql.Rows) error {
		var (
			i   PacketInfo
			err error
		)
		if err = rows.Scan(&i.Label, &i.When, &i.Channel, &i.Count); err == nil {
			vs = append(vs, i)
		}
		return err
	})
}

func (s DBStore) FetchCounts(days int) ([]ItemInfo, error) {
	if days <= 0 {
		days = 30
	}
	var (
		expr    = quel.Func("DATE_SUB", quel.NewIdent("CURRENT_DATE"), quel.Days(days))
		options = []quel.SelectOption{
			quel.SelectColumn(quel.NewIdent("label")),
			quel.SelectColumn(quel.NewIdent("origin")),
			quel.SelectColumn(quel.NewIdent("date")),
			quel.SelectColumn(quel.NewIdent("count")),
			quel.SelectColumn(quel.NewIdent("duration")),
			quel.SelectWhere(quel.GreaterOrEqual(quel.NewIdent("date"), expr)),
		}
	)
	q, err := quel.NewSelect("items_count", options...)
	if err != nil {
		return nil, err
	}
	var vs []ItemInfo
	return vs, s.query(q, func(rows *sql.Rows) error {
		var (
			i   ItemInfo
			err error
		)
		if err = rows.Scan(&i.Label, &i.Origin, &i.When, &i.Count, &i.Duration); err == nil {
			vs = append(vs, i)
		}
		return err
	})
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

func (s DBStore) FetchReplayStats(days int) ([]JobStatus, error) {
	if days <= 0 {
		days = 30
	}
	var (
		expr    = quel.Func("DATE_SUB", quel.NewIdent("CURRENT_DATE"), quel.Days(days))
		options = []quel.SelectOption{
			quel.SelectColumns("label", "timestamp", "count"),
			quel.SelectWhere(quel.GreaterOrEqual(quel.NewIdent("timestamp"), expr)),
		}
	)
	q, err := quel.NewSelect("jobs_status", options...)
	if err != nil {
		return nil, err
	}
	var vs []JobStatus
	return vs, s.query(q, func(rows *sql.Rows) error {
		var (
			j   JobStatus
			err error
		)
		if err = rows.Scan(&j.Status, &j.When, &j.Count); err == nil {
			vs = append(vs, j)
		}
		return err
	})
}

func (s DBStore) FetchReplays(query Criteria) (int, []Replay, error) {
	var (
		where = query.filterReplay()
		count = s.countItems("replay_list", "r", where)
	)
	q, err := prepareSelectReplay(where, query.orderAndLimits())
	if err != nil {
		return 0, nil, err
	}
	var vs []Replay
	return count, vs, s.query(q, func(rows *sql.Rows) error {
		var (
			r   Replay
			err error
		)
		if err = rows.Scan(&r.Id, &r.When, &r.Starts, &r.Ends, &r.Priority, &r.Comment, &r.Status, &r.Automatic, &r.Cancellable, &r.Corrupted, &r.Missing); err == nil {
			vs = append(vs, r)
		}
		r.When = r.When.UTC()
		r.Starts = r.Starts.UTC()
		r.Ends = r.Ends.UTC()
		return err
	})
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

func (s DBStore) FetchChannels() ([]ChannelInfo, error) {
	options := []quel.SelectOption{
		quel.SelectColumn(quel.NewIdent("channel")),
		quel.SelectColumn(quel.NewIdent("total")),
	}
	q, err := quel.NewSelect("channel_infos", options...)
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

func (s DBStore) FetchGapsHRD(query Criteria) (int, []HRDGap, error) {
	var (
		where = query.filterHRD()
		count = s.countItems("hrd_gap_list", "r", where)
	)
	q, err := prepareSelectGapsHRD(where, query.orderAndLimits())
	if err != nil {
		return 0, nil, err
	}
	var vs []HRDGap
	return count, vs, s.query(q, func(rows *sql.Rows) error {
		var (
			g   HRDGap
			err error
		)
		if err = rows.Scan(&g.Id, &g.When, &g.Starts, &g.First, &g.Ends, &g.Last, &g.Channel, &g.Replay, &g.Completed); err == nil {
			vs = append(vs, g)
		}
		g.When = g.When.UTC()
		g.Starts = g.Starts.UTC()
		g.Ends = g.Ends.UTC()
		return err
	})
}

func (s DBStore) FetchGapDetailHRD(id int) (HRDGap, error) {
	var h HRDGap
	return h, ErrImpl
}

func (s DBStore) FetchGapsVMU(query Criteria) (int, []VMUGap, error) {
	var (
		where = query.filterVMU()
		count = s.countItems("vmu_gap_list", "g", where)
	)

	q, err := prepareSelectGapsVMU(where, query.orderAndLimits())
	if err != nil {
		return 0, nil, err
	}

	var vs []VMUGap
	return count, vs, s.query(q, func(rows *sql.Rows) error {
		var (
			g   VMUGap
			err error
		)
		if err = rows.Scan(&g.Id, &g.When, &g.Starts, &g.First, &g.Ends, &g.Last, &g.Source, &g.UPI, &g.Replay, &g.Completed); err == nil {
			vs = append(vs, g)
		}
		g.When = g.When.UTC()
		g.Starts = g.Starts.UTC()
		g.Ends = g.Ends.UTC()
		return err
	})
}

func (s DBStore) FetchGapDetailVMU(id int) (VMUGap, error) {
	var v VMUGap
	return v, ErrImpl
}

func (s DBStore) FetchSources() ([]SourceInfo, error) {
	options := []quel.SelectOption{
		quel.SelectColumn(quel.NewIdent("source")),
		quel.SelectColumn(quel.NewIdent("total")),
	}
	q, err := quel.NewSelect("source_infos", options...)
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
		quel.SelectColumn(quel.NewIdent("phase")),
		quel.SelectColumn(quel.NewIdent("total")),
	}
	q, err := quel.NewSelect("record_infos", options...)
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

func (s DBStore) RegisterVariable(v Variable) (Variable, error) {
	return v, ErrImpl
}

func (s DBStore) exec(tx *sql.Tx, q quel.SQLer, names []string) error {
	query, args, err := q.SQL()
	if err != nil {
		return err
	}
	_, err = tx.Exec(query, args...)
	return err
}

func (s DBStore) query(q quel.SQLer, scan func(rows *sql.Rows) error) error {
	query, args, err := q.SQL()
	if err != nil {
		return err
	}
	fmt.Println(query, args)
	rows, err := s.db.Query(query, args...)
	switch err {
	case nil:
	case sql.ErrNoRows:
		return ErrEmpty
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

func (s DBStore) retrReplay(id int, r *Replay) error {
	where := quel.Equal(quel.NewIdent("id", "r"), quel.Arg("id", id))
	q, err := prepareSelectReplay(where, nil)
	if err != nil {
		return err
	}
	query, args, err := q.SQL()
	if err != nil {
		return err
	}
	return s.db.QueryRow(query, args...).Scan(&r.Id, &r.When, &r.Starts, &r.Ends, &r.Priority, &r.Comment, &r.Status, &r.Automatic, &r.Cancellable, &r.Corrupted, &r.Missing)
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

func (s DBStore) countRequests(where quel.SQLer) int {
	return s.countItems("replay", "r", where)
}

func (s DBStore) pendingTime() int {
	q, err := quel.NewSelect("pending_duration", quel.SelectColumn(quel.NewIdent("duration")))
	if err != nil {
		return 0
	}
	query, args, err := q.SQL()
	if err != nil {
		return 0
	}
	var count int
	if err := s.db.QueryRow(query, args...).Scan(&count); err != nil {
		return count
	}
	return count
}

func (s DBStore) countGapsHRD(where quel.SQLer) int {
	return s.countItems("hrd_packet_gap", "r", where)
}

func (s DBStore) countGapsVMU(where quel.SQLer) int {
	return s.countItems("vmu_packet_gap", "r", where)
}

func (s DBStore) countItems(table, alias string, where quel.SQLer) int {
	options := []quel.SelectOption{
		quel.SelectColumn(quel.Count(quel.NewIdent("id"))),
		quel.SelectAlias(alias),
	}
	if where != nil {
		options = append(options, quel.SelectWhere(where))
	}
	q, err := quel.NewSelect(table, options...)
	if err != nil {
		return 0
	}
	query, args, err := q.SQL()
	if err != nil {
		return 0
	}
	var count int
	if err := s.db.QueryRow(query, args...).Scan(&count); err != nil {
		return count
	}
	return count
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

func prepareSelectReplay(where quel.SQLer, limits []quel.SelectOption) (quel.SQLer, error) {
	options := []quel.SelectOption{
		quel.SelectAlias("r"),
		quel.SelectColumn(quel.NewIdent("id", "r")),
		quel.SelectColumn(quel.NewIdent("timestamp", "r")),
		quel.SelectColumn(quel.NewIdent("startdate", "r")),
		quel.SelectColumn(quel.NewIdent("enddate", "r")),
		quel.SelectColumn(quel.NewIdent("priority", "r")),
		quel.SelectColumn(quel.NewIdent("comment", "r")),
		quel.SelectColumn(quel.NewIdent("status", "r")),
		quel.SelectColumn(quel.NewIdent("automatic", "r")),
		quel.SelectColumn(quel.NewIdent("cancellable", "r")),
		quel.SelectColumn(quel.NewIdent("corrupted", "r")),
		quel.SelectColumn(quel.NewIdent("missing", "r")),
		quel.SelectWhere(where),
	}
	options = append(options, limits...)
	return quel.NewSelect("replay_list", options...)
}

func prepareSelectGapsVMU(where quel.SQLer, limits []quel.SelectOption) (quel.SQLer, error) {
	options := []quel.SelectOption{
		quel.SelectColumn(quel.NewIdent("id", "g")),
		quel.SelectColumn(quel.NewIdent("timestamp", "g")),
		quel.SelectColumn(quel.NewIdent("last_timestamp", "g")),
		quel.SelectColumn(quel.NewIdent("last_sequence_count", "g")),
		quel.SelectColumn(quel.NewIdent("next_timestamp", "g")),
		quel.SelectColumn(quel.NewIdent("next_sequence_count", "g")),
		quel.SelectColumn(quel.NewIdent("source", "g")),
		quel.SelectColumn(quel.NewIdent("phase", "g")),
		quel.SelectColumn(quel.NewIdent("replay", "g")),
		quel.SelectColumn(quel.NewIdent("completed", "g")),
		quel.SelectAlias("g"),
		quel.SelectWhere(where),
	}
	options = append(options, limits...)
	return quel.NewSelect("vmu_gap_list", options...)
}

func prepareSelectGapsHRD(where quel.SQLer, limits []quel.SelectOption) (quel.SQLer, error) {
	options := []quel.SelectOption{
		quel.SelectColumn(quel.NewIdent("id", "r")),
		quel.SelectColumn(quel.NewIdent("timestamp", "r")),
		quel.SelectColumn(quel.NewIdent("last_timestamp", "r")),
		quel.SelectColumn(quel.NewIdent("last_sequence_count", "r")),
		quel.SelectColumn(quel.NewIdent("next_timestamp", "r")),
		quel.SelectColumn(quel.NewIdent("next_sequence_count", "r")),
		quel.SelectColumn(quel.NewIdent("channel", "r")),
		quel.SelectColumn(quel.NewIdent("replay", "r")),
		quel.SelectColumn(quel.NewIdent("completed", "r")),
		quel.SelectWhere(where),
		quel.SelectAlias("r"),
	}
	options = append(options, limits...)
	return quel.NewSelect("hrd_gap_list", options...)
}
