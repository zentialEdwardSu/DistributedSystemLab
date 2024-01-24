package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync/atomic"
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

// when get nil while load, return 0
func parseAtomicValueInt(atom atomic.Value) int {
	if val := atom.Load(); val == nil {
		return 0
	} else {
		return val.(int)
	}
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
