package job

import (
	"fmt"
	"hash/fnv"

	"github.com/naturali/kmr/records"
)

type Context struct {
	writers []records.RecordWriter
	jobconf *JobConfig
}

func GetContext(task *Task, jobconf JobConfig) Context {
	// TODO: instantial by jobconf
	// if phase == map
	// 	get writers
	writers := make([]records.RecordWriter, jobconf.ShardCount)
	for i := 0; i < jobconf.ShardCount; i++ {
		records.MakeRecordWriter("file", map[string]interface{}{
			"filename": fmt.Sprintf("%s_%s_%d", jobconf.JobName, task.Phase, i),
		})
	}
	return Context{
		writers: writers,
		jobconf: &jobconf,
	}
}

func (ctx *Context) Write(records []records.Record) {
	for _, r := range records {
		h := fnv.New32a()
		h.Write(r.Key)
		writer := ctx.writers[h.Sum32()%uint32(ctx.jobconf.ReducerCount)]
		writer.WriteRecord(r)
	}
}
