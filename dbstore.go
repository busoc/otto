package main

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/midbel/quel"
)

type DBStore struct {
	db *sql.DB
}

func NewDBStore(addr, name, user, passwd string) (Store, error) {
	addr = fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true", user, passwd, addr, name)
	db, err := sql.Open("mysql", addr)
	if err != nil {
		return nil, fmt.Errorf("fail to connect: %w", err)
	}
	db.SetConnMaxLifetime(time.Second * 150)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	return DBStore{db: db}, nil
}

func (s DBStore) Status() (interface{}, error) {
	return nil, ErrImpl
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
	cdt := quel.Equal(quel.NewIdent("id", "s"), quel.NewIdent("replay_status_id", "c"))
	return qs.LeftOuterJoin(quel.Alias("c", qc), cdt, quel.SelectColumn(quel.NewIdent("count", "c")))
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
	return nil, ErrImpl
}

func (s DBStore) FetchReplayDetail(id int) (Replay, error) {
	var r Replay
	return r, ErrImpl
}

func (s DBStore) CancelReplay(id int) error {
	return ErrImpl
}

func (s DBStore) UpdateReplay(id int, priority int) (Replay, error) {
	var r Replay
	return r, ErrImpl
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

func (s DBStore) FetchGapsHRD(start time.Time, end time.Time, channel string) ([]HRDGap, error) {
	return nil, ErrImpl
}

func (s DBStore) FetchGapDetailHRD(id int) (HRDGap, error) {
	var h HRDGap
	return h, ErrImpl
}

func (s DBStore) FetchGapsVMU(start time.Time, end time.Time, record string) ([]VMUGap, error) {
	return nil, ErrImpl
}

func (s DBStore) FetchGapDetailVMU(id int) (VMUGap, error) {
	var v VMUGap
	return v, ErrImpl
}

func (s DBStore) FetchRecords() ([]RecordInfo, error) {
	return nil, ErrImpl
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
	var v Variable
	return v, ErrImpl
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
	query, _, err := q.SQL()
	if err != nil {
		return err
	}
	rows, err := s.db.Query(query)
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
