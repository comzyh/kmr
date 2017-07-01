package master

// MapDescription
type MapDescription struct {
	NWorker    int      `json:"nWorker"`
	Objects    []string `json:"objects"`
	Image      string   `json:"image"`
	Command    []string `json:"command"`
	ReaderType string   `json:"readerType"`
}

// ReduceDescription
type ReduceDescription struct {
	NWorker int      `json:"nWorker"`
	NReduce int      `json:"nReduce"`
	Image   string   `json:"image"`
	Command []string `json:"command"`
}

type BucketDescription struct {
	BucketType string                 `json: "bucketType"`
	Config     map[string]interface{} `json: "config"`
}

// JobDescription
type JobDescription struct {
	MapBucket    BucketDescription `json: "mapBucket"`
	InterBucket  BucketDescription `json: "interBucket"`
	ReduceBucket BucketDescription `json: "reduceBucket"`
	NWorker      int               `json: "nWorker"`
	Map          MapDescription    `json: "map"`
	Reduce       ReduceDescription `json: "reduce"`
	Image        string            `json: "image"`
	Command      []string          `json: "command"`
	CPULimit     string            `json: "cpulimit"`
}
