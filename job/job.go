package job

import "encoding/json"

// MapDescription MapDescription
type MapDescription struct {
	NWorker    int      `json:"nWorker"`
	Objects    []string `json:"objects"`
	Image      string   `json:"image"`
	Command    []string `json:"command"`
	ReaderType string   `json:"readerType"`
}

// ReduceDescription ReduceDescription
type ReduceDescription struct {
	NWorker int      `json:"nWorker"`
	NReduce int      `json:"nReduce"`
	Image   string   `json:"image"`
	Command []string `json:"command"`
}

// BucketConfig Parameters of bucket
type BucketConfig map[string]interface{}

// BucketDescription BucketDescription
type BucketDescription struct {
	BucketType string       `json:"bucketType"`
	Config     BucketConfig `json:"config"`
}

// JobDescription description of a job
type JobDescription struct {
	MapBucket     BucketDescription `json:"mapBucket"`
	InterBucket   BucketDescription `json:"interBucket"`
	ReduceBucket  BucketDescription `json:"reduceBucket"`
	NWorker       int               `json:"nWorker"`
	Map           MapDescription    `json:"map"`
	Reduce        ReduceDescription `json:"reduce"`
	Image         string            `json:"image"`
	Command       []string          `json:"command"`
	CPULimit      string            `json:"cpulimit"`
	MaxBackupTask int               `json:"maxBackupTask"`
}

func (bucket *BucketDescription) Marshal() string {
	ret, _ := json.Marshal(*bucket)
	return string(ret)
}
