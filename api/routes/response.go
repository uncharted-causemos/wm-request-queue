package routes

import (
	"encoding/json"
	"net/http"

	"go.uber.org/zap"
)

func handleJSON(w http.ResponseWriter, data interface{}) error {
	// marshal data
	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	// write response
	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}

func handleErrorType(w http.ResponseWriter, err error, code int, logger *zap.SugaredLogger) {
	logger.Errorf("%+v", err)
	errMessage := "An error occured on the server while processing the request"
	http.Error(w, errMessage, code)
}
