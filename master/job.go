package master

// MapDescription
type MapDescription struct {
	NWorker int      `json: "nWorker"`
	Objects []string `json: "objects"`
	Image   string   `json: "image"`
	commad  []string `json: "command"`
}

// ReduceDescription
type ReduceDescription struct {
	NWorker int      `json: "nWorker"`
	NReduce int      `json: "nReduce"`
	Image   string   `json: "image"`
	commad  []string `json: "command"`
}

// JobDescription
type JobDescription struct {
	MapBucket    string            `json: "mapBucket"`
	InterBucket  string            `json: "interBucket"`
	ReduceBucket string            `json: "reduceBucket"`
	Map          MapDescription    `json: "map"`
	Reduce       ReduceDescription `json: "reduce"`
}
