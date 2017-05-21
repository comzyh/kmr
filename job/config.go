package job

type JobConfig struct {
	JobName string

	readerClass string
	writerClass string

	ShardCount   int
	MapperCount  int
	ReducerCount int
}
