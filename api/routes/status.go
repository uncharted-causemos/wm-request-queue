package routes

import (
	"net/http"

	"github.com/pkg/errors"
	"gitlab.uncharted.software/WM/wm-request-queue/api/pipeline"
	"gitlab.uncharted.software/WM/wm-request-queue/api/queue"
	"gitlab.uncharted.software/WM/wm-request-queue/config"
)

// WaitingResponse provides the number of jobs waiting in the queue
type StatusResponse struct {
	Count int `json:"count"`
	Running bool `json:"running"`
}

// Waiting creates a get request handler that will return the number of items currently waiting in the queue
func StatusRequest(cfg *config.Config, queue queue.RequestQueue, runner *pipeline.DataPipelineRunner) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		count := queue.Size()
		running := runner.Running()
		if err := handleJSON(w, StatusResponse{count, running}); err != nil {
			handleErrorType(w, errors.New("failed to generate response"), http.StatusInternalServerError, cfg.Logger)
		}
	}
}