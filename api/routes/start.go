package routes

import (
	"net/http"

	"gitlab.uncharted.software/WM/wm-request-queue/api/pipeline"
	"gitlab.uncharted.software/WM/wm-request-queue/config"
)

// StartRequest will start the pipeline runner task.  If its already running then the request does nothing.
func StartRequest(cfg *config.Config, runner *pipeline.DataPipelineRunner) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		runner.Start()
	}
}
