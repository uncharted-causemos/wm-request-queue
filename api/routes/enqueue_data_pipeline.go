package routes

import (
	"encoding/json"
	"net/http"

	"gitlab.uncharted.software/WM/wm-request-queue/config"
)

// EnqueuePipelineData defines the fields necessary to run a data pipeline job.
type EnqueuePipelineData struct {
	ModelID      string   `json:"model_id"`
	RunID        string   `json:"run_id"`
	DataPaths    []string `json:"data_paths"`
	ComputeTiles bool     `json:"compute_tiles"`
}

// EnqueueDataPipelineRequest adds a request to the queue if there is space, or returns an error if
// the queue is currently at maximum capacity.
func EnqueueDataPipelineRequest(cfg *config.Config) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var enqueueMsg EnqueuePipelineData

		// Decode and respond with a 400 on failure
		err := json.NewDecoder(r.Body).Decode(&enqueueMsg)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Make sure we've got valid field data
		if enqueueMsg.ModelID == "" {
			http.Error(w, "model_id missing", http.StatusBadRequest)
		}
		if enqueueMsg.RunID == "" {
			http.Error(w, "run_id missing", http.StatusBadRequest)
		}
		if len(enqueueMsg.DataPaths) == 0 {
			http.Error(w, "data_paths missing", http.StatusBadRequest)
		}
		for _, path := range enqueueMsg.DataPaths {
			if path == "" {
				http.Error(w, "data_paths missing", http.StatusBadRequest)
			}
		}

		// Would be nice to perform up-front validation of the request contents here, but its not terribly
		// practical.

		// Enqueue the request if there's room, otherwise let the caller know that the service
		// is unavailable
		if !cfg.RequestQueue.Enqueue(enqueueMsg) {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
	}
}
