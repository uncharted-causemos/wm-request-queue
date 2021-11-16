package routes

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/pkg/errors"
	"gitlab.uncharted.software/WM/wm-request-queue/api/helpers"
	"gitlab.uncharted.software/WM/wm-request-queue/api/pipeline"
	"gitlab.uncharted.software/WM/wm-request-queue/api/queue"
	"gitlab.uncharted.software/WM/wm-request-queue/config"
)

// BulkEnqueueRequest adds a list of requests to the queue if there is space, or returns an error if
// the queue is currently at maximum capacity. Will add until reaching capacity.
func BulkEnqueueRequest(cfg *config.Config, requestQueue queue.RequestQueue) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		// Read the body into a byte array
		body, err := ioutil.ReadAll(r.Body)
		defer r.Body.Close()
		if err != nil {
			handleErrorType(w, errors.Wrap(err, "failed to read bulk enqueue request body"), http.StatusBadRequest, cfg.Logger)
			return
		}

		// Decode and respond with a 400 on failure
		var requests []interface{}
		err = json.Unmarshal(body, &requests)
		if err != nil {
			handleErrorType(w, errors.Wrap(err, "failed to unmarshal bulk enqueue request body"), http.StatusBadRequest, cfg.Logger)
			return
		}

		for i := 0; i < len(requests); i++ {
			body, err := json.Marshal(requests[i])
			if err != nil {
				handleErrorType(w, errors.Wrap(err, "failed to marshal enqueue request body"), http.StatusBadRequest, cfg.Logger)
				return
			}

			var enqueueMsg pipeline.EnqueueRequestData
			err = json.Unmarshal(body, &enqueueMsg)
			if err != nil {
				handleErrorType(w, errors.Wrap(err, "failed to unmarshal enqueue request body"), http.StatusBadRequest, cfg.Logger)
				return
			}

			// Store the full request body for forwarding to prefect
			enqueueMsg.RequestData = body

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
}
