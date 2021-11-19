package routes

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/pkg/errors"
	"gitlab.uncharted.software/WM/wm-request-queue/api/helpers"
	"gitlab.uncharted.software/WM/wm-request-queue/api/pipeline"
	"gitlab.uncharted.software/WM/wm-request-queue/api/queue"
	"gitlab.uncharted.software/WM/wm-request-queue/config"
)

// ForceDispatchRequest submits the next item in the queue regardless of prefect's busy status
// or whether or not the data pipeline is running
func RetryFlowRequest(cfg *config.Config, requestQueue queue.RequestQueue, runner *pipeline.DataPipelineRunner) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		path := strings.Split(r.URL.Path, "/")
		flow_run_id := path[len(path)-1]
		// r.Context().Value(interface{name: ""})
		requestData := runner.RetrieveByFlowRunID(flow_run_id)

		var enqueueMsg pipeline.EnqueueRequestData
		err := json.Unmarshal(requestData, &enqueueMsg)
		if err != nil {
			handleErrorType(w, errors.Wrap(err, "failed to unmarshal request body"), http.StatusBadRequest, cfg.Logger)
			return
		}
		// Store the full request body for forwarding to prefect
		enqueueMsg.RequestData = requestData

		err = helpers.CheckEnqueueParams(enqueueMsg)
		if err != nil {
			handleErrorType(w, err, http.StatusBadRequest, cfg.Logger)
		}

		result, err := helpers.AddToQueue(enqueueMsg, *cfg, requestQueue)
		if err != nil {
			handleErrorType(w, err, http.StatusInternalServerError, cfg.Logger)
			return
		} else if !result {
			handleErrorType(w, err, http.StatusServiceUnavailable, cfg.Logger)
			return
		}
	}
}
