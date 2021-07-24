package routes

import (
	"net/http"

	"gitlab.uncharted.software/WM/wm-request-queue/api/pipeline"
	"gitlab.uncharted.software/WM/wm-request-queue/config"
)

// StopRequest stops datapipeline runner.  Requests can still be enqueued, but the queue will not be serviced.
func StopRequest(cfg *config.Config, runner *pipeline.DataPipelineRunner) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		runner.Stop()
	}
}
