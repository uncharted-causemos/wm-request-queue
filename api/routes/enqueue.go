package routes

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/pkg/errors"

	"github.com/vova616/xxhash"
	"gitlab.uncharted.software/WM/wm-request-queue/config"
)

// EnqueueRequestData defines the minimum fields upstream callers need to specify in order to run
// a data pipeline job.  Additional parameters will not be validated and will be passed through
// to prefect.
type EnqueueRequestData struct {
	ModelID      string   `json:"model_id"`
	RunID        string   `json:"run_id"`
	DataPaths    []string `json:"data_paths"`
	RequestData []byte
}

// KeyedEnqueueRequestData adds an internally generated hash key to support checks for
// duplicate requests.
type KeyedEnqueueRequestData struct {
	EnqueueRequestData
	RequestKey int32
}

// EnqueueRequest adds a request to the queue if there is space, or returns an error if
// the queue is currently at maximum capacity.
func EnqueueRequest(cfg *config.Config) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var enqueueMsg EnqueueRequestData

		// Read the body into a byte array
		body, err := ioutil.ReadAll(r.Body)
		defer r.Body.Close()
		if err != nil {
			handleErrorType(w, errors.Wrap(err, "failed to read enqueue request body"), http.StatusBadRequest, cfg.Logger)
			return
		}

		// Decode and respond with a 400 on failure
		err = json.Unmarshal(body, &enqueueMsg)
		if err != nil {
			handleErrorType(w, errors.Wrap(err, "failed to unmarshal enqueue request body"), http.StatusBadRequest, cfg.Logger)
			return
		}
		// Store the full request body for forwarding to prefect
		enqueueMsg.RequestData = body

		// Make sure we've got valid data for the minimum field set
		if enqueueMsg.ModelID == "" {
			handleErrorType(w, errors.New("model_id missing"), http.StatusBadRequest, cfg.Logger)
		}
		if enqueueMsg.RunID == "" {
			handleErrorType(w, errors.New("run_id_missing"), http.StatusBadRequest, cfg.Logger)
		}
		if len(enqueueMsg.DataPaths) == 0 {
			handleErrorType(w, errors.New("data_paths missing"), http.StatusBadRequest, cfg.Logger)
		}
		for _, path := range enqueueMsg.DataPaths {
			if path == "" {
				handleErrorType(w, errors.New("data_paths missing"), http.StatusBadRequest, cfg.Logger)
			}
		}

		// Create a hash from the request data
		paramHash := xxhash.Checksum32(enqueueMsg.RequestData)

		// Relevant info to run the request downstream
		keyed := KeyedEnqueueRequestData{
			EnqueueRequestData: enqueueMsg,
			RequestKey:         int32(paramHash),
		}

		// Enqueue the request if there's room, otherwise let the caller know that the service
		// is unavailable.
		if !cfg.RequestQueue.EnqueueHashed(int(keyed.RequestKey), keyed) {
			handleErrorType(w, errors.New("request queue full"), http.StatusServiceUnavailable, cfg.Logger)
			return
		}
	}
}
