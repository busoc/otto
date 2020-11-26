package main

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type DBStore struct {
	db *sql.DB
}

func NewDBStore(addr, name, user, passwd string) (Store, error) {
	addr = fmt.Sprintf("%s:%s@%s/%s?parseTime=true", user, passwd, addr, name)
	db, err := sql.Open("mysql", addr)
	if err != nil {
		return nil, err
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
	return nil, ErrImpl
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
	return r, ErrImpl
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
	return nil, ErrImpl
}

func (s DBStore) UpdateVariable(id int, value string) (Variable, error) {
	var v Variable
	return v, ErrImpl
}

func (s DBStore) RegisterVariable(v Variable) (Variable, error) {
	return v, ErrImpl
}
