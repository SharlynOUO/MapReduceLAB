package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"

	"encoding/json"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func GetReduceChan(key string, nReduce int) int {
	return ihash(key) % nReduce
}

// 和文件有关de操作
func CreateTempIntermidiateFiles(MapNO int, ReduceNO int) (*os.File, error) {
	return ioutil.TempFile("./", "temp"+strconv.Itoa(MapNO)+"-"+strconv.Itoa(ReduceNO)+"_*.json")
}
func RenameTempIntermidiateFiles(mapNO int, reduceNO int, fpath string) error {
	//return "./intermidiate/mr-" + strconv.Itoa(mapNO) + "-" + strconv.Itoa(reduceNO) + ".json"
	return os.Rename(fpath, "./mr-"+strconv.Itoa(mapNO)+"-"+strconv.Itoa(reduceNO)+".json")
}
func GetReduceTaskFileName(TaskNum int, ReduceNO int) []string {
	retlist := make([]string, TaskNum)
	for i := 0; i < TaskNum; i++ {
		retlist[i] = "./mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(ReduceNO) + ".json"
	}
	//fmt.Println(retlist)
	return retlist
}

// 两个任务函数
func DoMapTask(task RequestReply, mapf func(string, string) []KeyValue) {
	//获得临时文件句柄
	var files [](*os.File) = make([]*os.File, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		files[i], _ = CreateTempIntermidiateFiles(task.TaskNO, i)
	}

	//获取分配到的任务的内容
	content, err := ioutil.ReadFile(task.Mapfilename)
	if err != nil {
		log.Fatalf("cannot read %v", task.Mapfilename)
	}
	//获取kv
	//fmt.Println("mapf")
	//fmt.Println((time.Now()))

	kva := mapf(task.Mapfilename, string(content))

	//把kv们hash进reduce桶里
	//用缓冲区保存进内存里
	//fmt.Println("mapf done,getfile start")
	//fmt.Println((time.Now()))
	tongs := make([][]KeyValue, task.NReduce)
	for i := 0; i < len(kva); i++ {
		idxt := GetReduceChan(kva[i].Key, task.NReduce)
		tongs[idxt] = append(tongs[idxt], kva[i])
	}
	//把缓冲区中的内容刷进磁盘后给临时文件改名字并关闭文件
	for i := 0; i < task.NReduce; i++ {

		//以json格式编码
		encoder := json.NewEncoder(files[i])
		encoder.Encode(tongs[i])

		RenameTempIntermidiateFiles(task.TaskNO, i, files[i].Name())
		files[i].Close()

	}
	//fmt.Println((time.Now()))

}
func DoReduceTask(task RequestReply, reducef func(string, []string) string) {
	tasklen := task.NMap
	var filenames = GetReduceTaskFileName(tasklen, task.TaskNO)
	outfilename := "mr-out-" + strconv.Itoa(task.TaskNO)
	outfile, _ := os.Create(outfilename)

	var taskbuffer []KeyValue

	for ifile := 0; ifile < tasklen; ifile++ {
		//把解码的中间文件内容存入buffer中
		filePtr, _ := os.Open(filenames[ifile])
		decoder := json.NewDecoder(filePtr)
		var buffer []KeyValue
		decoder.Decode(&buffer)
		filePtr.Close()

		taskbuffer = append(taskbuffer, buffer...)
	}
	sort.Sort(ByKey(taskbuffer))

	//统计该文件中的词并将它们写入outfile中
	i := 0
	for i < len(taskbuffer) {
		j := i + 1
		for j < len(taskbuffer) && taskbuffer[j].Key == taskbuffer[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, taskbuffer[k].Value)
		}
		output := reducef(taskbuffer[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outfile, "%v %v\n", taskbuffer[i].Key, output)

		i = j
	}

	outfile.Close()
}

/*
func GetTasks(WorkerID int, backkkk chan bool, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	backkkk <- true

}
*/
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	//fmt.Println("Woker starting...")

	// Your worker implementation here.
	//worker_num := 3
	//workerstat := make([]chan bool, worker_num)
	//记录是否返回,如果有未返回的会阻塞不退出防止任务做到一半主goroutine返回了导致有的任务被杀死
	//for i := 0; i < worker_num; i++ {
	//workerstat[i] = make(chan bool)
	// go GetTasks(sync.stat, workerstat[i], mapf, reducef)
	//}
	WorkerID := os.Getpid()
	//fmt.Println("worker ", WorkerID, "has been woken up!")

	//新人领任务

	go AlarmLuncher(WorkerID)

	NowTask := CallForTask(-1, FRESH(), WorkerID)

	for NowTask.Task_type != NOTASK() {

		if NowTask.Task_type == WAIT() {
			time.Sleep(time.Second)
			NowTask = CallForTask(NowTask.TaskNO, NowTask.Task_type, WorkerID)
			continue
		} else if NowTask.Task_type == CALLFAILD() {
			fmt.Println("Worker ", WorkerID, ": can't connect to coordinator!")
			time.Sleep(time.Second)
			NowTask = CallForTask(NowTask.TaskNO, NowTask.Task_type, WorkerID)
			continue
		} else if NowTask.Task_type == MAPTYPE() {
			DoMapTask(NowTask, mapf)
		} else if NowTask.Task_type == REDUCETYPE() {
			DoReduceTask(NowTask, reducef)
		}

		//接着申请新的任务

		NowTask = CallForTask(NowTask.TaskNO, NowTask.Task_type, WorkerID)
	}
}

// call coordinator for task
func CallForTask(NOofdonetask int, TYPEofdonetask string, workerid int) RequestReply {
	// declare an argument structure.
	args := WokerRequest{}

	// fill in the argument(s).
	args.Task_type = TYPEofdonetask
	args.TaskNO = NOofdonetask
	args.Worker_id = workerid

	// declare a reply structure.
	reply := RequestReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.HandOut", &args, &reply)

	if !ok {
		// reply.Y should be 100.
		fmt.Printf("Call for task failed!\n")
		reply.Task_type = CALLFAILD()

	} /*else {
		fmt.Printf("-------------------\n")
		fmt.Println("worker", workerid, " do task:", reply)

	}*/
	return reply
}

// 每隔6s告诉coordinator自己还在工作
func AlarmLuncher(workerid int) {
	time.Sleep(6 * time.Second)
	for AliveCall(workerid) {
		time.Sleep(6 * time.Second)
	}
}

func AliveCall(workerid int) bool {
	// declare an argument structure.
	args := AliveAlarm{}

	// fill in the argument(s).
	args.Worker_id = workerid
	// declare a reply structure.
	reply := false

	ok := call("Coordinator.AliveWorkerSign", &args, &reply)

	if !ok {
		fmt.Printf("Alive Call failed!\n")
	}

	return reply

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	//fmt.Println("try connecting to coodinator...")
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// 通信栗子
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)

	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}
