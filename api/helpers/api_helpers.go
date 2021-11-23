package helpers

import (
	"errors"
	"time"

	"github.com/vova616/xxhash"
	"gitlab.uncharted.software/WM/wm-request-queue/api/pipeline"
	"gitlab.uncharted.software/WM/wm-request-queue/api/queue"
	"gitlab.uncharted.software/WM/wm-request-queue/config"
)

// CheckEnqueueParams checks if a job has all required information
func CheckEnqueueParams(enqueueMsg pipeline.EnqueueRequestData) error {

	// Make sure we've got valid data for the minimum field set
	if enqueueMsg.ModelID == "" {
		// handleErrorType(w, errors.New("model_id missing"), http.StatusBadRequest, cfg.Logger)
		return errors.New("model_id missing")
	}
	if enqueueMsg.RunID == "" {
		return errors.New("run_id missing")
	}
	if len(enqueueMsg.DataPaths) == 0 {
		return errors.New("data_paths missing")
	}
	for _, path := range enqueueMsg.DataPaths {
		if path == "" {
			return errors.New("data_paths missing")
		}
	}

	return nil
}

// AddToQueue takes a given job and adds it to the queue
func AddToQueue(enqueueMsg pipeline.EnqueueRequestData, cfg config.Config, requestQueue queue.RequestQueue) (bool, error) {
	// Create a hash from the request data
	paramHash := xxhash.Checksum32(enqueueMsg.RequestData)

	// Relevant info to run the request downstream
	keyed := pipeline.KeyedEnqueueRequestData{
		EnqueueRequestData: enqueueMsg,
		RequestKey:         int32(paramHash),
		StartTime:          time.Now(),
	}

	// Enqueue the request if there's room, otherwise let the caller know that the service
	// is unavailable.
	if config.UseQueueIdempotency(cfg.Environment.DataPipelineIdempotencyChecks) {
		result, err := requestQueue.EnqueueHashed(int(keyed.RequestKey), keyed)
		if err != nil {
			return result, err
		} else if !result {
			return result, errors.New("request queue full")
		}
	} else {
		result, err := requestQueue.Enqueue(keyed)
		if err != nil {
			return result, err
		} else if !result {
			return result, errors.New("request queue full")
		}
	}
	return true, nil
}
