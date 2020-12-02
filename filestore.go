package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

var errDone = errors.New("done")

type FileStore struct {
	dir string
}

func NewFileStore(dir string) (Store, error) {
	i, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	if !i.IsDir() {
		return nil, fmt.Errorf("%s: not a directory", dir)
	}
	return FileStore{dir: dir}, nil
}

func (s FileStore) Status() (interface{}, error) {
	rand.Seed(time.Now().Unix())
	stats := map[string]interface{}{
		"autobrm": "zombie",
		"memory":  rand.Intn(16 << 20),
		"uptime":  rand.Intn(16 << 20),
		"hrd": map[string]interface{}{
			"gap":      rand.Intn(1 << 14),
			"size":     rand.Intn(1 << 20),
			"duration": rand.Intn(1 << 20),
		},
		"vmu": map[string]interface{}{
			"gap":      rand.Intn(1 << 14),
			"size":     rand.Intn(1 << 20),
			"duration": rand.Intn(1 << 20),
		},
	}
	return stats, nil
}

func (s FileStore) FetchStatus() ([]StatusInfo, error) {
	var rs []StatusInfo
	return rs, s.readFile("replay", func(row []string) error {
		x := sort.Search(len(rs), func(i int) bool {
			return rs[i].Name >= row[6]
		})
		if x >= len(rs) || rs[x].Name != row[6] {
			s := StatusInfo{Name: row[6]}
			rs = append(rs, s)
			sort.Slice(rs, func(i, j int) bool {
				return rs[i].Name < rs[j].Name
			})
		}
		rs[x].Count++
		return nil
	})
}

func (s FileStore) FetchReplays(start time.Time, end time.Time, status string) ([]Replay, error) {
	var rs []Replay
	return rs, s.readFile("replay", func(row []string) error {
		rp, err := parseReplay(row)
		if err == nil {
			rs = append(rs, rp)
		}
		return err
	})
}

func (s FileStore) FetchReplayDetail(id int) (Replay, error) {
	var (
		r   Replay
		str = strconv.Itoa(id)
		err error
	)
	return r, s.readFile("replay", func(row []string) error {
		if row[0] != str {
			return nil
		}
		r, err = parseReplay(row)
		if err == nil {
			err = errDone
		}
		return err
	})
}

func (s FileStore) CancelReplay(id int, comment string) (Replay, error) {
	var r Replay
	return r, ErrImpl
}

func (s FileStore) UpdateReplay(id int, priority int) (Replay, error) {
	var r Replay
	return r, ErrImpl
}

func (s FileStore) RegisterReplay(r Replay) (Replay, error) {
	return r, ErrImpl
}

func (s FileStore) FetchGapsHRD(start time.Time, end time.Time, channel string) ([]HRDGap, error) {
	var rs []HRDGap
	return rs, s.readFile("hrdgap", func(row []string) error {
		gap, err := parseHRD(row)
		if err == nil {
			rs = append(rs, gap)
		}
		return nil
	})
}

func (s FileStore) FetchChannels() ([]ChannelInfo, error) {
	return nil, ErrImpl
}

func (s FileStore) FetchGapDetailHRD(id int) (HRDGap, error) {
	var h HRDGap
	return h, ErrImpl
}

func (s FileStore) FetchGapsVMU(start time.Time, end time.Time, record string) ([]VMUGap, error) {
	var rs []VMUGap
	return rs, s.readFile("vmugap", func(row []string) error {
		gap, err := parseVMU(row)
		if err == nil {
			rs = append(rs, gap)
		}
		return err
	})
}

func (s FileStore) FetchGapDetailVMU(id int) (VMUGap, error) {
	var v VMUGap
	return v, ErrImpl
}

func (s FileStore) FetchRecords() ([]RecordInfo, error) {
	var rs []RecordInfo
	return rs, s.readFile("vmugap", func(row []string) error {
		x := sort.Search(len(rs), func(i int) bool {
			return rs[i].UPI >= row[7]
		})
		if x >= len(rs) {
			r := RecordInfo{UPI: row[7]}
			rs = append(rs, r)
		}
		rs[x].Count++
		return nil
	})
}

func (s FileStore) FetchSources() ([]SourceInfo, error) {
	return nil, ErrImpl
}

func (s FileStore) FetchVariables() ([]Variable, error) {
	var rs []Variable
	return rs, s.readFile("variables", func(row []string) error {
		var (
			v   Variable
			err error
		)
		v.Id, err = strconv.Atoi(row[0])
		v.Name = row[1]
		v.Value = row[2]

		if err == nil {
			rs = append(rs, v)
		}
		return err
	})
}

func (s FileStore) UpdateVariable(id int, value string) (Variable, error) {
	var v Variable
	return v, ErrImpl
}

func (s FileStore) RegisterVariable(v Variable) (Variable, error) {
	return v, ErrImpl
}

func (s FileStore) readFile(file string, parse func(rs []string) error) error {
	r, err := os.Open(filepath.Join(s.dir, file+".csv"))
	if err != nil {
		return err
	}
	defer r.Close()

	rs := csv.NewReader(r)
	rs.Comma = '\t'
	rs.ReuseRecord = true

	rs.Read()
	for {
		row, err := rs.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err = parse(row); err != nil {
			if errors.Is(err, errDone) {
				break
			}
			return err
		}
	}
	return nil
}

const timePattern = "2006-01-02 15:04:05"

func parseHRD(row []string) (HRDGap, error) {
	var (
		gap HRDGap
		err error
	)
	gap.Id, err = strconv.Atoi(row[0])
	gap.When, err = time.Parse(timePattern, row[1])
	gap.Starts, err = time.Parse(timePattern, row[2])
	gap.First, err = strconv.Atoi(row[3])
	gap.Ends, err = time.Parse(timePattern, row[4])
	gap.Last, err = strconv.Atoi(row[5])
	gap.Channel = row[6]

	return gap, err
}

func parseVMU(row []string) (VMUGap, error) {
	var (
		gap VMUGap
		err error
	)
	gap.Id, err = strconv.Atoi(row[0])
	gap.When, err = time.Parse(timePattern, row[1])
	gap.Starts, err = time.Parse(timePattern, row[2])
	gap.First, err = strconv.Atoi(row[3])
	gap.Ends, err = time.Parse(timePattern, row[4])
	gap.Last, err = strconv.Atoi(row[5])
	gap.Source, err = strconv.Atoi(row[6])
	gap.UPI = row[7]

	return gap, err
}

func parseReplay(row []string) (Replay, error) {
	var (
		rp  Replay
		err error
	)
	rp.Id, err = strconv.Atoi(row[0])
	rp.When, err = time.Parse(timePattern, row[1])
	rp.Starts, err = time.Parse(timePattern, row[2])
	rp.Ends, err = time.Parse(timePattern, row[3])
	rp.Priority, err = strconv.Atoi(row[4])
	rp.Comment = row[5]
	rp.Status = row[6]
	rp.Automatic, err = strconv.ParseBool(row[7])

	return rp, err
}
