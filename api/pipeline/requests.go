package pipeline

type JobData struct {
	ComputeTiles       bool                `json:"compute_tiles"`
	DataPaths          []string            `json:"data_paths"`
	DocIds             []string            `json:"doc_ids"`
	IsIndicator        bool                `json:"is_indicator"`
	ModelId            string              `json:"model_id"`
	QualifierMap       map[string][]string `json:"qualifier_map"`
	RunId              string              `json:"run_id"`
	TemporalResolution string              `json:"temporal_resolution"`
}

// EnqueueRequestData defines the minimum fields upstream callers need to specify in order to run
// a data pipeline job.  Additional parameters will not be validated and will be passed through
// to prefect.
type EnqueueRequestData struct {
	ModelID     string   `json:"model_id"`
	RunID       string   `json:"run_id"`
	DataPaths   []string `json:"data_paths"`
	RequestData []byte
}

// KeyedEnqueueRequestData adds an internally generated hash key to support checks for
// duplicate requests.
type KeyedEnqueueRequestData struct {
	EnqueueRequestData
	RequestKey int32
}
