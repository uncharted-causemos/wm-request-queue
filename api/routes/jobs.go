package routes

import (
	"net/http"

	"github.com/pkg/errors"
	"gitlab.uncharted.software/WM/wm-request-queue/api/pipeline"
	"gitlab.uncharted.software/WM/wm-request-queue/api/queue"
	"gitlab.uncharted.software/WM/wm-request-queue/config"
)

// JobsRequest returns the contents in the queue.
func JobsRequest(cfg *config.Config, queue queue.RequestQueue) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		queueContents, err := queue.GetAll()
		if err != nil {
			handleErrorType(w, err, http.StatusInternalServerError, cfg.Logger)
			return
		}
		jobData := make([]pipeline.EnqueueRequestData, len(queueContents))
		for i := 0; i < len(queueContents); i++ {
			request, ok := queueContents[i].(pipeline.KeyedEnqueueRequestData)
			jobData[i] = request.EnqueueRequestData
			if !ok {
				handleErrorType(w, errors.New("failed to generate response, unexpected datatype found"), http.StatusBadRequest, cfg.Logger)
			}
		}
		if err := handleJSON(w, jobData); err != nil {
			handleErrorType(w, errors.New("failed to generate response"), http.StatusInternalServerError, cfg.Logger)
		}
	}
}
