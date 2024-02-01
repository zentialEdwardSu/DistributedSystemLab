package raft

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

var ErrLogNotFound = errors.New("log not found")

type LogEntry struct {
	Term    int         // Term when the log entries create
	Index   int         // Index of the Log entries
	Command interface{} // generic Command to hold log command
}

func (l LogEntry) String() string {
	return fmt.Sprintf("{index:%d,term:%d,Command:%v}", l.Index, l.Term, l.Command)
}

// LogStore inspired by https://github.com/hashicorp/raft/blob/main/inmem_store.go
// we chose map instead of slice to hold log data to gain o(1) LogSearching and LogInserting
type LogStore struct {
	l         sync.RWMutex
	logs      map[int]LogEntry // logEntries
	lowIndex  int
	highIndex int
}

func (ls *LogStore) String() string {
	return fmt.Sprintf("{highIndex:%d,lowIndex:%d,logs:%v}", ls.highIndex, ls.lowIndex, ls.logs)
}

func (ls *LogStore) getLog(idx int, l *LogEntry) error {
	ls.l.RLock()
	defer ls.l.RUnlock()

	log, ok := ls.logs[idx]
	if !ok {
		return ErrLogNotFound
	}
	*l = log
	return nil
}

func (ls *LogStore) setLogs(logs []LogEntry) error {
	ls.l.Lock()
	defer ls.l.Unlock()

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

func (ls *LogStore) setLog(log LogEntry) error {
	return ls.setLogs([]LogEntry{log})
}

func (ls *LogStore) unwrapLogs() []LogEntry {
	r := make([]LogEntry, 0)
	ls.l.RLock()

	// Since map is unordered, we must get the keys and sort them
	// to make sure the slice we get by key is ordered
	keys := make([]int, 0, len(ls.logs))
	for k := range ls.logs {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	for _, k := range keys {
		r = append(r, ls.logs[k])
	}
	ls.l.RUnlock()

	return r
}

func newLogStore() *LogStore {
	l := new(LogStore)
	l.logs = make(map[int]LogEntry)
	return l
}

func (ls *LogStore) DeleteRange(min, max int) error {
	ls.l.Lock()
	defer ls.l.Unlock()
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

func (ls *LogStore) FirstIndex() (int, error) {
	ls.l.RLock()
	defer ls.l.RUnlock()
	return ls.lowIndex, nil
}

func (ls *LogStore) LastIndex() (int, error) {
	ls.l.RLock()
	defer ls.l.RUnlock()
	return ls.highIndex, nil
}
