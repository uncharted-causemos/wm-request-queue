package routes

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/pkg/errors"
	"gitlab.uncharted.software/WM/wm-request-queue/api/helpers"
	"gitlab.uncharted.software/WM/wm-request-queue/api/pipeline"
	"gitlab.uncharted.software/WM/wm-request-queue/api/queue"
	"gitlab.uncharted.software/WM/wm-request-queue/config"
)

// RetryFlowRequest resubmits a flow given it's run_id in prefect
func RetryFlowRequest(cfg *config.Config, requestQueue queue.RequestQueue, runner *pipeline.DataPipelineRunner) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		labelsParam := r.URL.Query().Get("labels")
		labels := strings.Split(labelsParam, ",")
		path := strings.Split(r.URL.Path, "/")
		flowRunID := path[len(path)-1]
		var enqueueParams map[string]interface{}

		body, err := ioutil.ReadAll(r.Body)
		defer r.Body.Close()
		if err != nil {
			handleErrorType(w, errors.Wrap(err, "failed to read enqueue request body"), http.StatusBadRequest, cfg.Logger)
			return
		}
		hasNewParams := len(body) > 0
		if len(body) > 0 {
			err = json.Unmarshal(body, &enqueueParams)
			if err != nil {
				handleErrorType(w, errors.Wrap(err, "failed to unmarshal request body"), http.StatusBadRequest, cfg.Logger)
				return
			}
		}

		if !runner.IsFlowDone(flowRunID) {
			handleErrorType(w, errors.New("flow has not finished yet"), http.StatusBadRequest, cfg.Logger)
			return
		}

		requestData := runner.RetrieveByFlowRunID(flowRunID)

		var enqueueMsgTemp map[string]interface{}
		err = json.Unmarshal(requestData, &enqueueMsgTemp)
		if err != nil {
			handleErrorType(w, errors.Wrap(err, "failed to unmarshal request body"), http.StatusBadRequest, cfg.Logger)
			return
		}

		if hasNewParams {
			for key, val := range enqueueParams {
				enqueueMsgTemp[key] = val
			}
		}

		// Re-marshal to create RequestData
		enqueueBody, err := json.Marshal(enqueueMsgTemp)
		if err != nil {
			handleErrorType(w, errors.Wrap(err, "failed to nmarshal request body"), http.StatusBadRequest, cfg.Logger)
			return
		}
		// re-unmarshal to convert from dict -> struct easily
		var enqueueMsg pipeline.EnqueueRequestData
		err = json.Unmarshal(enqueueBody, &enqueueMsg)
		if err != nil {
			handleErrorType(w, errors.Wrap(err, "failed to unmarshal request body"), http.StatusBadRequest, cfg.Logger)
			return
		}
		// Store the full request body for forwarding to prefect
		enqueueMsg.RequestData = enqueueBody

		err = helpers.CheckEnqueueParams(enqueueMsg)
		if err != nil {
			handleErrorType(w, err, http.StatusBadRequest, cfg.Logger)
		}

		result, err := helpers.AddToQueue(enqueueMsg, *cfg, requestQueue, labels)
		if err != nil {
			handleErrorType(w, err, http.StatusInternalServerError, cfg.Logger)
			return
		} else if !result {
			handleErrorType(w, err, http.StatusServiceUnavailable, cfg.Logger)
			return
		}
	}
}
