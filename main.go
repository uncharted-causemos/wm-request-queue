package main

import (
	"fmt"
	"log"
	"net/http"

	"gitlab.uncharted.software/WM/wm-request-queue/config"
	"gitlab.uncharted.software/WM/wm-request-queue/msg"
	"gitlab.uncharted.software/WM/wm-request-queue/queue"
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

	// Setup the request queue
	requestQueue := queue.NewSimpleRequestQueue(env.DataPipelineQueueSize)

	// Setup router
	routerConfig := config.Config{
		Logger:       sugar,
		Environment:  env,
		RequestQueue: requestQueue,
	}
	r, err := msg.NewRouter(routerConfig)
	if err != nil {
		log.Fatal(err)
	}

	// Setup the prefect mediator
	dataPipelineRunner := msg.NewDataPipelineRunner(&routerConfig)
	dataPipelineRunner.Start()

	// Log config
	sugar.Info(env)

	// Start listening
	sugar.Infof("Listening on %s", env.Addr)
	sugar.Fatal(http.ListenAndServe(env.Addr, r))
}
