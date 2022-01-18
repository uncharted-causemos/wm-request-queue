package routes

import (
	"net/http"
	"strings"

	"gitlab.uncharted.software/WM/wm-request-queue/api/pipeline"
	"gitlab.uncharted.software/WM/wm-request-queue/api/queue"
	"gitlab.uncharted.software/WM/wm-request-queue/config"
)

// ForceDispatchRequest submits the next item in the queue regardless of prefect's busy status
// or whether or not the data pipeline is running
func ForceDispatchRequest(cfg *config.Config, requestQueue queue.RequestQueue, runner *pipeline.DataPipelineRunner) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		labelsParam := r.URL.Query().Get("labels")
		labels := strings.Split(labelsParam, ",")
		runner.Submit(pipeline.SubmitParams{Force: true, ProvidedLabels: labels})
	}
}
