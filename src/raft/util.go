package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// max return the larger value between a and b
func max(a, b int) int {
	if a >= b {
		return a
	}
	return b
}

// min return the larger value between a and b
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// // toMilliseconds convert int timeout to duration
func toMilliseconds(t int) time.Duration {
	return time.Millisecond * time.Duration(t)
}

// randomTimeout returns a value that is between the minVal and 2x minVal.
func randomTimeout(minVal time.Duration) <-chan time.Time {
	if minVal == 0 {
		return nil
	}
	extra := time.Duration(rand.Int63()) % minVal
	return time.After(minVal + extra)
}
