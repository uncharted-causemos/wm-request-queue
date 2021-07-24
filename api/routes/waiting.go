package routes

import (
	"net/http"

	"github.com/pkg/errors"
	"gitlab.uncharted.software/WM/wm-request-queue/api/queue"
	"gitlab.uncharted.software/WM/wm-request-queue/config"
)

// WaitingResponse provides the number of jobs waiting in the queue
type WaitingResponse struct {
	Count int `json:"count"`
}

// Waiting creates a get request handler that will return the number of items currently waiting in the queue
func Waiting(cfg *config.Config, queue queue.RequestQueue) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		count := queue.Size()
		if err := handleJSON(w, WaitingResponse{count}); err != nil {
			handleErrorType(w, errors.New("failed to generate response"), http.StatusInternalServerError, cfg.Logger)
		}
	}
}
