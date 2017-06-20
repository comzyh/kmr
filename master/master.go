package master

import (
	"math/rand"
	"net"
	"sync"
	"time"

	"golang.org/x/net/context"

	kmrpb "github.com/naturali/kmr/pb"
	"github.com/naturali/kmr/util/log"
	"k8s.io/client-go/kubernetes"

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

	STATE_IDLE       = 0
	STATE_INPROGRESS = 1
	STATE_COMPLETED  = 2
)

type Task struct {
	state        int
	workers      map[int64]int
	taskInfo     *kmrpb.TaskInfo
	commitWorker int64
}

// Master is a map-reduce controller. It stores the state for each task and other runtime progress statuses.
type Master struct {
	sync.Mutex

	JobName string   // Name of currently executing job
	DataDir string   // Directory of intermediate files
	Files   []string // Input files
	NReduce int      // Number of reduce partitions

	wg        sync.WaitGroup     // WaitGroup for waiting all of tasks finished on each phase
	tasks     []*Task            // Holding all of tasks
	heartbeat map[int64]chan int // Heartbeat channel for each worker

	k8sclient *kubernetes.Clientset
	namespace string

	commitMappers []int64
}

// CheckHeartbeatForEachWorker
// CheckHeartbeat keeps checking the heartbeat of each worker. It is either DEAD, PULSE, FINISH or losing signal of
// heartbeat.
// If the task is DEAD (occur error while the worker is doing the task) or cannot detect heartbeat in time. Master
// will releases the task, so that another work can takeover
// Master will check the heartbeat every 5 seconds. If master cannot detect any heartbeat in the meantime, master
// regards it as a DEAD worker.
func (master *Master) CheckHeartbeatForEachWorker(taskID int, workerID int64, heartbeat chan int) {
	for {
		timeout := time.After(HEARTBEAT_TIMEOUT)
		select {
		case <-timeout:
			// the worker fuck up, release the task
			master.Lock()
			if master.tasks[taskID].state == STATE_INPROGRESS {
				delete(master.tasks[taskID].workers, workerID)
				if len(master.tasks[taskID].workers) == 0 {
					master.tasks[taskID].state = STATE_IDLE
				}
			}
			master.Unlock()
			return
		case heartbeatCode := <-heartbeat:
			// the worker is doing his job
			switch heartbeatCode {
			case HEARTBEAT_CODE_DEAD:
				// the worker fuck up, release the task
				master.Lock()
				if master.tasks[taskID].state == STATE_INPROGRESS {
					delete(master.tasks[taskID].workers, workerID)
					if len(master.tasks[taskID].workers) == 0 {
						master.tasks[taskID].state = STATE_IDLE
					}
				}
				master.Unlock()
				return
			case HEARTBEAT_CODE_FINISH:
				master.Lock()
				if master.tasks[taskID].state != STATE_COMPLETED {
					master.tasks[taskID].state = STATE_COMPLETED
					master.tasks[taskID].commitWorker = workerID
				}
				master.Unlock()
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
	var nTasks int
	switch phase {
	case mapPhase:
		nTasks = len(master.Files)
	case reducePhase:
		nTasks = master.NReduce
	}

	master.Lock()
	master.tasks = make([]*Task, nTasks)
	master.heartbeat = make(map[int64]chan int)
	for i := 0; i < nTasks; i++ {
		taskInfo := &kmrpb.TaskInfo{
			JobName:         master.JobName,
			Phase:           phase,
			IntermediateDir: master.DataDir,
			TaskID:          int32(i),
			NReduce:         int32(master.NReduce),
			NMap:            int32(len(master.Files)),
		}
		if phase == mapPhase {
			taskInfo.File = master.Files[i]
		}
		if phase == reducePhase {
			taskInfo.CommitMappers = master.commitMappers
		}
		master.tasks[i] = &Task{
			state:    STATE_IDLE,
			workers:  make(map[int64]int),
			taskInfo: taskInfo,
		}
	}
	master.wg.Add(nTasks)
	master.Unlock()
	master.wg.Wait()

	if phase == mapPhase {
		master.commitMappers = make([]int64, 0)
		for _, t := range master.tasks {
			master.commitMappers = append(master.commitMappers, t.commitWorker)
		}
	}
}

type server struct {
	master *Master
}

// RequestTask is to deliver a task to worker.
func (s *server) RequestTask(ctx context.Context, in *kmrpb.RegisterParams) (*kmrpb.Task, error) {
	log.Debugf("register %s", in.JobName)
	s.master.Lock()
	defer s.master.Unlock()

	for id, t := range s.master.tasks {
		if t.state == STATE_IDLE {
			workerID := rand.Int63()
			t.workers[workerID] = id
			t.state = STATE_INPROGRESS
			s.master.heartbeat[workerID] = make(chan int)
			log.Debug("deliver a task")
			go s.master.CheckHeartbeatForEachWorker(id, workerID, s.master.heartbeat[workerID])
			return &kmrpb.Task{
				WorkerID: workerID,
				Retcode:  0,
				Taskinfo: t.taskInfo,
			}, nil
		}
	}
	log.Debug("no task right now")
	return &kmrpb.Task{
		Retcode: -1,
	}, nil
}

// ReportTask is for executor to report its progress state to master.
func (s *server) ReportTask(ctx context.Context, in *kmrpb.ReportInfo) (*kmrpb.Response, error) {
	log.Debugf("get heartbeat phase=%s, taskid=%d, workid=%d", in.Phase, in.TaskID, in.WorkerID)
	s.master.Lock()
	defer s.master.Unlock()

	if _, ok := s.master.tasks[in.TaskID].workers[in.WorkerID]; ok {
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
			s.master.heartbeat[in.WorkerID] <- heartbeatCode
		}()
	}

	return &kmrpb.Response{Retcode: 0}, nil
}

// NewMapReduce creates a map-reduce job.
func NewMapReduce(port string, jobName string, inputFiles []string, dataDir string,
	nReduce int, k8sclient *kubernetes.Clientset, namespace string) {
	master := &Master{
		JobName:   jobName,
		Files:     inputFiles,
		NReduce:   nReduce,
		DataDir:   dataDir,
		k8sclient: k8sclient,
		namespace: namespace,
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
