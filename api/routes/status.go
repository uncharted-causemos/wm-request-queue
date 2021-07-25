package routes

import (
	"net/http"

	"github.com/pkg/errors"
	"gitlab.uncharted.software/WM/wm-request-queue/api/pipeline"
	"gitlab.uncharted.software/WM/wm-request-queue/api/queue"
	"gitlab.uncharted.software/WM/wm-request-queue/config"
)

// StatusResponse provides the number of items currently queued, and whether or not the
// the pipeline runner routine has been stopped, or is running.
type StatusResponse struct {
	Count int `json:"count"`
	Running bool `json:"running"`
}

// StatusRequest creates a get request handler that will return status info for the request queue and pipeline runner.
func StatusRequest(cfg *config.Config, queue queue.RequestQueue, runner *pipeline.DataPipelineRunner) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		count := queue.Size()
		running := runner.Running()
		if err := handleJSON(w, StatusResponse{count, running}); err != nil {
			handleErrorType(w, errors.New("failed to generate response"), http.StatusInternalServerError, cfg.Logger)
		}
	}
}