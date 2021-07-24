package main

import (
	"fmt"
	"log"
	"net/http"

	"gitlab.uncharted.software/WM/wm-request-queue/api"
	"gitlab.uncharted.software/WM/wm-request-queue/api/pipeline"
	"gitlab.uncharted.software/WM/wm-request-queue/api/queue"
	"gitlab.uncharted.software/WM/wm-request-queue/config"
	"go.uber.org/zap"
)

const envFile = "wm.env"

func main() {
	// Load environment
	env, err := config.Load(envFile)
	if err != nil {
		log.Fatal(err)
	}

	// Setup logging
	var logger *zap.Logger
	switch env.Mode {
	case "dev":
		logger, err = zap.NewDevelopment()
	case "prod":
		logger, err = zap.NewProduction()

	default:
		err = fmt.Errorf("Invalid 'mode' flag: %s", env.Mode)
	}
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		_ = logger.Sync()
	}()
	sugar := logger.Sugar()

	config := config.Config{
		Logger:       sugar,
		Environment:  env,
	}

	// Setup the request queue
	requestQueue := queue.NewListFIFOQueue(env.DataPipelineQueueSize)

	// Setup the prefect mediator
	dataPipelineRunner := pipeline.NewDataPipelineRunner(&config, requestQueue)

	// Setup router
	r, err := api.NewRouter(config, requestQueue, dataPipelineRunner)
	if err != nil {
		log.Fatal(err)
	}

	// Log config
	sugar.Info(env)

	// Start listening for updates
	dataPipelineRunner.Start()

	// Start listening
	sugar.Infof("Listening on %s", env.Addr)
	sugar.Fatal(http.ListenAndServe(env.Addr, r))
}
