package routes

import (
	"net/http"

	"gitlab.uncharted.software/WM/wm-request-queue/api/queue"
	"gitlab.uncharted.software/WM/wm-request-queue/config"
)

// ClearRequest clears the request queue.
func ClearRequest(cfg *config.Config, requestQueue queue.RequestQueue) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		requestQueue.Clear()
	}
}
