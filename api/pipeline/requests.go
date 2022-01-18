package pipeline

import "time"

// EnqueueRequestData defines the minimum fields upstream callers need to specify in order to run
// a data pipeline job.  Additional parameters will not be validated and will be passed through
// to prefect.
type EnqueueRequestData struct {
	ModelID     string   `json:"model_id"`
	RunID       string   `json:"run_id"`
	DataPaths   []string `json:"data_paths"`
	DocIDs      []string `json:"doc_ids"`
	IsIndicator bool     `json:"is_indicator"`
	RequestData []byte
}

// FlowData is used to keep track of what flows we have that haven't failed or succeded
// in the data pipeline
type FlowData struct {
	Request   EnqueueRequestData
	StartTime time.Time
}

// KeyedEnqueueRequestData adds an internally generated hash key to support checks for
// duplicate requests.
type KeyedEnqueueRequestData struct {
	EnqueueRequestData
	RequestKey int32
	StartTime  time.Time
	Labels     []string
}

type SubmitParams struct {
	Force          bool
	ProvidedLabels []string
}
