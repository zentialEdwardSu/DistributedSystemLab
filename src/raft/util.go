package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync/atomic"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.Lmicroseconds)
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

// toMilliseconds convert int timeout to duration
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

// asyncNotifyCh is used to do an async channel send
// to a single channel without blocking.
func asyncNotifyCh(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// when get nil while load, return 0
func parseAtomicValueInt(atom atomic.Value) int {
	if val := atom.Load(); val == nil {
		return 0
	} else {
		return val.(int)
	}
}

func calculateRetryTime(base time.Duration, fails uint32, maxFails uint32, timeLimit time.Duration) time.Duration {
	if fails > maxFails {
		return timeLimit
	}

	retryTime := base * time.Duration(1<<fails)
	if retryTime > timeLimit {
		return timeLimit
	}
	return retryTime
}

func BuddhaBless(b bool) {
	////////////////////////////////////////////////////////////////////
	//                          _ooOoo_                               //
	//                         o8888888o                              //
	//                         88" . "88                              //
	//                         (| ^_^ |)                              //
	//                         O\  =  /O                              //
	//                      ____/`---'\____                           //
	//                    .'  \\|     |//  `.                         //
	//                   /  \\|||  :  |||//  \                        //
	//                  /  _||||| -:- |||||-  \                       //
	//                  |   | \\\  -  /// |   |                       //
	//                  | \_|  ''\---/''  |   |                       //
	//                  \  .-\__  `-`  ___/-. /                       //
	//                ___`. .'  /--.--\  `. . ___                     //
	//              ."" '<  `.___\_<|>_/___.'  >'"".                  //
	//            | | :  `- \`.;`\ _ /`;.`/ - ` : | |                 //
	//            \  \ `-.   \_ __\ /__ _/   .-` /  /                 //
	//      ========`-.____`-.___\_____/___.-`____.-'========         //
	//                           `=---='                              //
	//      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^        //
	////////////////////////////////////////////////////////////////////
	if b {
		fmt.Println("菩提本无树 \t明镜亦非台 \n 本来无BUG \t 何必常修改")
	}
}

type intSlice4Sort []int

func (p intSlice4Sort) Len() int           { return len(p) }
func (p intSlice4Sort) Less(i, j int) bool { return p[i] < p[j] }
func (p intSlice4Sort) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
