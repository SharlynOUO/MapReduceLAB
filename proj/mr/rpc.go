package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type WokerRequest struct {
	Worker_id int
	Task_type string
	//处理完成的任务类型：map or reduce,通过MAPTYPE/REDUCETYPE/NOTASK和确认
	//一定先判断此变量

	//tasktype是map时range[0,nmap)
	//tasktype是reduce时range[0,nReduce)
	TaskNO int
}

type RequestReply struct {
	Task_type string
	NReduce   int
	NMap      int

	//tasktype是map时range[0,nmap)
	//tasktype是reduce时range[0,nReduce)
	TaskNO int

	Mapfilename string
}

type AliveAlarm struct {
	Worker_id int
}

// task_type可能返回任务类型或状态信息
func MAPTYPE() string {
	return "map"
}
func REDUCETYPE() string {
	return "reduce"
}
func NOTASK() string {
	return "notask"
}
func CALLFAILD() string {
	return "fail"
}
func WAIT() string {
	return "wait"
}
func FRESH() string {
	return "fresh"
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
