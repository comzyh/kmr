/*
CheckPoint file format:

map <id> <commitworker>
...
reduce <id> <commitworker>
...
*/
package util

import (
	"bufio"
	"os"

	"fmt"
	"github.com/naturali/kmr/util/log"
	"strconv"
	"strings"
)

type CompletedMapDesc struct {
	TaskID       int
	CommitWorker int64
}

type CompletedReduceDesc struct {
	TaskID       int
	CommitWorker int64
}

// ReduceDescription
type MapReduceCheckPoint struct {
	CompletedMaps    []*CompletedMapDesc
	CompletedReduces []*CompletedReduceDesc
}

func RestoreCheckPointFromFile(filename string) *MapReduceCheckPoint {
	cp := &MapReduceCheckPoint{
		CompletedMaps:    make([]*CompletedMapDesc, 0),
		CompletedReduces: make([]*CompletedReduceDesc, 0),
	}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	var line string
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}
		parts := strings.Split(strings.TrimSpace(line), " ")
		switch parts[0] {
		case "map":
			tid, _ := strconv.Atoi(parts[1])
			cm, _ := strconv.ParseInt(parts[2], 10, 64)
			cp.CompletedMaps = append(cp.CompletedMaps, &CompletedMapDesc{
				TaskID:       tid,
				CommitWorker: cm,
			})
		case "reduce":
			tid, _ := strconv.Atoi(parts[1])
			cm, _ := strconv.ParseInt(parts[2], 10, 64)
			cp.CompletedReduces = append(cp.CompletedReduces, &CompletedReduceDesc{
				TaskID:       tid,
				CommitWorker: cm,
			})
		default:
			log.Errorf("unknown line on checkpoint file: %s", line)
		}
	}
	return cp
}

func AppendCheckPoint(writer *os.File, phase string, taskID int, commitWorker int64) {
	writer.WriteString(fmt.Sprintf("%s %d %d\n", phase, taskID, commitWorker))
}
