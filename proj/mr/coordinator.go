package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	inputfilenames []string //给文件分配map tasks编号

	//维护的状态表
	//取值：0表示啥也没动，1表示有woker正在做，2表示做完了
	mapstat    []int //一个nmap大小的二维数组
	reducestat []int //一个nreduce大小的数组

	//请求的worker信息:
	worker_num int
	//key-id ,value-上一次请求时间
	workerRequestTime sync.Map
	//key-id,value-正在做的任务
	workerTask sync.Map
	//key-id,value-任务类型
	//仅区分woker类型，当task等于-1时没有用
	workerType sync.Map

	//阶段任务是否完成
	IsMapDONE    bool
	IsReduceDONE bool

	//任务数量
	nMap    int
	nReduce int
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.

	/*c.workerRequestTime = make(map[int]time.Time)
	c.workerTask = make(map[int]int)
	c.workerType = make(map[int]string)*/

	//map部分初始化
	c.inputfilenames = files
	c.nMap = len(files)
	c.IsMapDONE = false
	c.mapstat = make([]int, c.nMap)
	for i := 0; i < c.nMap; i++ {
		c.mapstat[i] = 0
		//测试reduce阶段时改为2

	}

	//reduce部分初始化
	c.nReduce = nReduce
	c.IsReduceDONE = false
	c.reducestat = make([]int, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.reducestat[i] = 0
	}
	//os.Mkdir("intermidiate", os.ModePerm)
	//fmt.Println("Initialization:", c.inputfilenames, c.mapstat, c.reducestat, c.nMap, c.nReduce)

	c.server()
	return &c
}

// woker发送请求时由此函数进行回复
// 同时它还有维护coordinator的功能
func (c *Coordinator) HandOut(request *WokerRequest, reply *RequestReply) error {

	//fmt.Println("Get request: ", request)

	c.workerRequestTime.Store(request.Worker_id, time.Now())
	//处理可能的task完成信息
	if request.Task_type == FRESH() {
		c.worker_num++
	} else if request.Task_type == MAPTYPE() {
		c.mapstat[request.TaskNO] = 2

	} else if request.Task_type == REDUCETYPE() {
		c.reducestat[request.TaskNO] = 2
	}

	c.workerTask.Store(request.Worker_id, -1)

	var task int

	//Map阶段
	if !c.IsMapDONE { //加上判断条件防止reduce阶段多遍历nReduce次浪费资源
		task = c.GetAvailableTask(MAPTYPE())
		if task == -2 {
			c.IsMapDONE = true
		} else if task == -1 {
			reply.Task_type = WAIT()
			return nil
		} else {
			reply.TaskNO = task
			reply.Mapfilename = c.inputfilenames[task]
			reply.Task_type = MAPTYPE()
			reply.NReduce = c.nReduce
			reply.NMap = c.nMap
			c.mapstat[task] = 1 //返回前更改任务状态为占用
			//先改type后改task顺序不能换！
			c.workerType.Store(request.Worker_id, MAPTYPE())
			c.workerTask.Store(request.Worker_id, task)

			return nil
		}
	}

	//reduce阶段
	task = c.GetAvailableTask(REDUCETYPE())
	if task == -2 {
		c.IsReduceDONE = true
	} else if task == -1 {
		reply.Task_type = WAIT()
		return nil
	} else {
		reply.TaskNO = task
		reply.Task_type = REDUCETYPE()
		reply.NReduce = c.nReduce
		reply.NMap = c.nMap
		c.reducestat[task] = 1 //返回前更改任务状态为占用
		c.workerType.Store(request.Worker_id, REDUCETYPE())
		c.workerTask.Store(request.Worker_id, task)
		return nil
	}

	//如果所有的工作都做完了也要给worker一个反馈
	reply.Task_type = NOTASK()
	return nil
}

func (c *Coordinator) AliveWorkerSign(aliveworker *AliveAlarm, rep *bool) error {
	c.workerRequestTime.Store(aliveworker.Worker_id, time.Now())
	*rep = true
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.

// 返回值：0-X表示位置，-1表示无空闲但有任务没做完，-2表示全部做完了
func (c *Coordinator) GetAvailableTask(taskstage string) int {
	ret := -2

	var n int
	var stat []int

	if taskstage == MAPTYPE() {
		n = c.nMap
		stat = c.mapstat
	} else if taskstage == REDUCETYPE() {
		n = c.nReduce
		stat = c.reducestat

	}

	for i := 0; i < n; i++ {

		if stat[i] == 0 {
			stat[i] = 1
			return i
		} else if stat[i] == 1 {
			ret = -1
		}
	}

	return ret
}

/*
	func is_state_change(oldlist []int, newlist []int, n int) bool {
		for i := 0; i < n; i++ {
			if oldlist[i] != newlist[i] {
				return true
			}
		}
		return false
	}
*/

func (c *Coordinator) Done() bool {

	// Your code here.

	if c.IsMapDONE && c.IsReduceDONE {
		/*
			cmd := exec.Command("ls", "-l", "/var/log/")
			err := cmd.Run()
			if err != nil {
				log.Fatalf("cmd.Run() failed with %s\n", err)
			}*/
		return true
	}

	//打印任务状态
	/*var lastMapStat []int = make([]int, c.nMap)
	var lastReduceStat []int = make([]int, c.nReduce)
	if is_state_change(lastMapStat, c.mapstat, c.nMap) || is_state_change(lastReduceStat, c.reducestat, c.nReduce) {
	*/
	//fmt.Println("map tasks statlist: ", c.mapstat)
	//fmt.Println("reduce tasks statlist: ", c.reducestat)
	//}

	if c.worker_num != 0 {
		nowatime := time.Now()
		var overtime []int
		c.workerRequestTime.Range(func(key, value interface{}) bool {
			worker := key.(int)
			lastrequesttime := value.(time.Time)
			workertype, _ := c.workerType.Load(worker)
			lasttask, _ := c.workerTask.Load(worker)

			if nowatime.Sub(lastrequesttime).Seconds() > 10 {
				if lasttask != -1 {
					if workertype.(string) == MAPTYPE() {
						c.mapstat[lasttask.(int)] = 0
					} else {
						c.reducestat[lasttask.(int)] = 0
					}
				}
				overtime = append(overtime, worker)
			}

			return true

		})

		for ovt_worker := range overtime {
			c.workerRequestTime.Delete(ovt_worker)
			c.workerTask.Delete(ovt_worker)
			c.workerType.Delete(ovt_worker)
		}

		/*
			for worker := range c.workerRequestTime {
				if nowatime.Sub(c.workerRequestTime[worker]).Seconds() > 10 { //如果超时
					if c.workerTask[worker] == -1 {
						continue
					}
					//如果有没做完的任务，则需要重新做
					//也就是修改任务列表等待下一个请求任务的worker
					if c.workerType[worker] == MAPTYPE() {
						c.mapstat[c.workerTask[worker]] = 0
					} else {
						c.reducestat[c.workerTask[worker]] = 0
					}

				}
			}*/
	}

	//test
	//ret := true

	return false
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//不太有用的东c

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

/*
func CreateReduceChan(mapNO int, nReduce int) {

	for i := 0; i < nReduce; i++ {
		fname := "./intermidiate/mr-" + strconv.Itoa(mapNO) + "-" + strconv.Itoa(i)
		ifile, _ := os.Create(fname)
		ifile.Close()
	}

}*/
