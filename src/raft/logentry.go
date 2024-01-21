package raft

import (
	"errors"
	"sync"
)

var ErrLogNotFound = errors.New("log not found")

type LogEntry struct {
	Term    int         // Term when the log entries create
	Index   int         // Index of the Log entries
	Command interface{} // generic Command to hold log command
}

// inspired by https://github.com/hashicorp/raft/blob/main/inmem_store.go
// I chose map instead of slice to hold log data to gain o(1) Logsearching and LogInserting
type LogStore struct {
	lock      sync.RWMutex
	logs      map[int]*LogEntry // logEntries
	lowIndex  int
	highIndex int
}

func (ls *LogStore) getLog(idx int, l *LogEntry) error {
	ls.lock.RLock()
	defer ls.lock.RUnlock()

	log, ok := ls.logs[idx]
	if !ok {
		return ErrLogNotFound
	}

	*l = *log
	return nil
}

func (ls *LogStore) setLogs(logs []*LogEntry) error {
	ls.lock.Lock()
	defer ls.lock.Unlock()

	for _, l := range logs {
		ls.logs[l.Index] = l

		if ls.lowIndex == 0 {
			ls.lowIndex = l.Index
		}
		if l.Index > ls.highIndex {
			ls.highIndex = l.Index
		}
	}

	return nil
}

func (ls *LogStore) setLog(log *LogEntry) error {
	return ls.setLogs([]*LogEntry{log})
}

func (ls *LogStore) newLogStore() *LogStore {
	i := &LogStore{
		logs: make(map[int]*LogEntry),
	}

	return i
}

func (ls *LogStore) DeleteRange(min, max int) error {
	ls.lock.Lock()
	defer ls.lock.Unlock()
	for j := min; j <= max; j++ {
		delete(ls.logs, j)
	}
	if min <= ls.lowIndex {
		ls.lowIndex = max + 1
	}
	if max >= ls.highIndex {
		ls.highIndex = min - 1
	}
	if ls.lowIndex > ls.highIndex {
		ls.lowIndex = 0
		ls.highIndex = 0
	}
	return nil
}
