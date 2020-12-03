package main

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"path/filepath"
	"strings"
)

type Monitor struct {
	Pid  string `toml:"pidfile"`
	Proc string `toml:"proc"`
}

func (m Monitor) readProcess() map[string]string {
	buf, err := ioutil.ReadFile(m.Pid)
	status := map[string]string{
		"cmdline": "unknown",
		"name":    "unknown",
		"state":   "unknown",
		"pid":     "unknown",
		"vmsize":  "unknown",
		"vmrss":   "unknown",
	}
	if err != nil {
		return status
	}
	pid := strings.TrimSpace(string(buf))
	status = readCommandStatus(filepath.Join(m.Proc, pid))
	return status
}

func readCommandLine(dir string) string {
	buf, _ := ioutil.ReadFile(filepath.Join(dir, "cmdline"))
	return strings.ReplaceAll(string(buf), "\x00", " ")
}

func readCommandStatus(dir string) map[string]string {
	stats := make(map[string]string)
	stats["cmdline"] = readCommandLine(dir)

	buf, err := ioutil.ReadFile(filepath.Join(dir, "status"))
	if err != nil {
		return stats
	}
	scan := bufio.NewScanner(bytes.NewReader(buf))
	for scan.Scan() {
		parts := strings.SplitN(scan.Text(), ":", 2)
		switch field := strings.ToLower(parts[0]); field {
		case "name", "pid", "state", "vmrss", "vmsize":
			stats[field] = strings.TrimSpace(parts[1])
		default:
		}
	}
	return stats
}
