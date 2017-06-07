package master

import (
	"golang.org/x/net/context"
	"math/rand"
	"net"
	"sync"
	"time"

	kmrpb "github.com/naturali/kmr/pb"
	"github.com/naturali/kmr/util/log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	mapPhase    = "map"
	reducePhase = "reduce"

	HEARTBEAT_CODE_PULSE  = 0
	HEARTBEAT_CODE_DEAD   = 1
	HEARTBEAT_CODE_FINISH = 2

	HEARTBEAT_TIMEOUT = 20 * time.Second
)

// Master is a map-reduce controller. It stores the state for each task and other runtime progress statuses.
type Master struct {
	//sync.Mutex

	JobName string   // Name of currently executing job
	DataDir string   // Directory of intermediate files
	Files   []string // Input files
	NReduce int      // Number of reduce partitions

	taskChan      chan *kmrpb.TaskInfo // Channel for piping pending task
	wg            sync.WaitGroup       // WaitGroup for waiting all of tasks finished on each phase
	tasks         []*kmrpb.TaskInfo    // All TaskInfo
	taskWorkerID  []int64              // Current workerID of each task
	taskHeartBeat []chan int           // Each task keeps a heartbeat which indicates the status of each task progress
}

// CheckHeartbeat keeps checking the heartbeat of each task. It is either DEAD, PULSE, FINISH or losing signal of
// heartbeat.
// If the task is DEAD (occur error while the worker is doing the task) or cannot detect heartbeat in time. Master
// will releases the task, so that another work can takeover
// Master will check the heartbeat every 5 seconds. If master cannot detect any heartbeat in the meantime, master
// regards it as a DEAD worker.
func (master *Master) CheckHeartbeat(taskID int) {
	for {
		timeout := time.After(HEARTBEAT_TIMEOUT)
		select {
		case <-timeout:
			// the worker fuck up, release the task
			master.taskWorkerID[taskID] = 0
			go func() { master.taskChan <- master.tasks[taskID] }()
			return
		case heartbeatCode := <-master.taskHeartBeat[taskID]:
			// the worker is doing his job
			switch heartbeatCode {
			case HEARTBEAT_CODE_DEAD:
				// the worker fuck up, release the task
				master.taskWorkerID[taskID] = 0
				go func() { master.taskChan <- master.tasks[taskID] }()
				return
			case HEARTBEAT_CODE_FINISH:
				master.wg.Done()
				return
			case HEARTBEAT_CODE_PULSE:
				continue
			}
		}
	}
}

// Schedule pipes into tasks for the phase (map or reduce). It will return after all the tasks are finished.
func (master *Master) Schedule(phase string) {
	var ntasks int
	switch phase {
	case mapPhase:
		ntasks = len(master.Files)
	case reducePhase:
		ntasks = master.NReduce
	}

	master.tasks = make([]*kmrpb.TaskInfo, ntasks)
	master.taskWorkerID = make([]int64, ntasks)
	master.taskHeartBeat = make([]chan int, ntasks)
	for i := 0; i < ntasks; i++ {
		task := &kmrpb.TaskInfo{
			JobName:         master.JobName,
			Phase:           phase,
			IntermediateDir: master.DataDir,
			TaskID:          int32(i),
			NReduce:         int32(master.NReduce),
			NMap:            int32(len(master.Files)),
		}
		if phase == mapPhase {
			task.File = master.Files[i]
		}
		master.tasks[i] = task
		master.taskWorkerID[i] = 0
		master.taskHeartBeat[i] = make(chan int)
	}

	master.wg.Add(ntasks)
	// pipe in tasks
	go func(ch chan *kmrpb.TaskInfo) {
		for i := 0; i < ntasks; i++ {
			ch <- master.tasks[i]
		}
	}(master.taskChan)
	master.wg.Wait()
}

type server struct {
	master *Master
}

// RequestTask is to deliver a task to worker.
func (s *server) RequestTask(ctx context.Context, in *kmrpb.RegisterParams) (*kmrpb.Task, error) {
	log.Debugf("register %s", in.JobName)
	ret := &kmrpb.Task{
		Retcode: -1,
	}
	select {
	case task := <-s.master.taskChan:
		ret.Taskinfo = task
		ret.WorkerID = rand.Int63()
		s.master.taskWorkerID[task.TaskID] = ret.WorkerID
		go s.master.CheckHeartbeat(int(task.TaskID))
		ret.Retcode = 0
		log.Debug("deliver a task")
	default:
		log.Debug("no task right now")
	}
	return ret, nil
}

// ReportTask is for executor to report its progress state to master.
func (s *server) ReportTask(ctx context.Context, in *kmrpb.ReportInfo) (*kmrpb.Response, error) {
	log.Debugf("get heartbeat phase=%s, taskid=%d, workid=%d", in.Phase, in.TaskID, in.WorkerID)
	if s.master.taskWorkerID[in.TaskID] == in.WorkerID {
		var heartbeatCode int
		switch in.Retcode {
		case kmrpb.ReportInfo_FINISH:
			heartbeatCode = HEARTBEAT_CODE_FINISH
		case kmrpb.ReportInfo_DOING:
			heartbeatCode = HEARTBEAT_CODE_PULSE
		case kmrpb.ReportInfo_ERROR:
			heartbeatCode = HEARTBEAT_CODE_DEAD
		default:
			panic("unknown ReportInfo")
		}
		go func() {
			s.master.taskHeartBeat[in.TaskID] <- heartbeatCode
		}()
	}

	return &kmrpb.Response{Retcode: 0}, nil
}

// NewMapReduce creates a map-reduce job.
func NewMapReduce(port string, jobName string, inputFiles []string, dataDir string, nReduce int) {
	master := &Master{
		JobName:  jobName,
		Files:    inputFiles,
		NReduce:  nReduce,
		DataDir:  dataDir,
		taskChan: make(chan *kmrpb.TaskInfo),
	}

	go func() {
		lis, err := net.Listen("tcp", port)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		log.Infof("listen %s", port)
		s := grpc.NewServer()
		kmrpb.RegisterMasterServer(s, &server{master: master})
		reflection.Register(s)
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	startTime := time.Now()
	master.Schedule(mapPhase)
	log.Debug("Map DONE")
	master.Schedule(reducePhase)
	log.Debug("Reduce DONE")
	log.Debug("Finish", time.Since(startTime))
}
