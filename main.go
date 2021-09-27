package main

import (
	"encoding/gob"
	"fmt"
	"log"
	"net/http"
	"time"

	"gitlab.uncharted.software/WM/wm-request-queue/api"
	"gitlab.uncharted.software/WM/wm-request-queue/api/pipeline"
	"gitlab.uncharted.software/WM/wm-request-queue/api/queue"
	"gitlab.uncharted.software/WM/wm-request-queue/config"
	"go.uber.org/zap"
)

const envFile = "wm.env"

var (
	// populated at compile time based on data injected by the makefile
	version   = "unset"
	timestamp = "unset"
)

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
		Logger:      sugar,
		Environment: env,
	}

	// Log version
	sugar.Infof("Version: %s Timestamp: %s", version, timestamp)

	// Log config
	sugar.Info(env)

	// Setup the request queue
	var requestQueue queue.RequestQueue
	if env.DataPipelinePersistedQueue {
		// The gob package that the persisted queue uses for storing data requires a one-time registration
		// of any structures that it stores.  TODO: Could be added to the params of the New call below.
		gob.Register(pipeline.EnqueueRequestData{})
		gob.Register(pipeline.KeyedEnqueueRequestData{})
		requestQueue, err = queue.NewPersistedFIFOQueue(env.DataPipelineQueueSize, env.DataPipelineQueueDir, env.DataPipelineQueueName)
		sugar.Infof("Loaded queue with %d entries from %s%s", requestQueue.Size(), env.DataPipelineQueueDir, env.DataPipelineQueueName)
		if err != nil {
			sugar.Fatal(err)
		}
	} else {
		// in-memory queue, data does not survive a restart
		requestQueue = queue.NewListFIFOQueue(env.DataPipelineQueueSize)
	}

	// Setup the prefect mediator
	dataPipelineRunner := pipeline.NewDataPipelineRunner(&config, requestQueue)

	// Setup router
	r, err := api.NewRouter(config, requestQueue, dataPipelineRunner)
	if err != nil {
		sugar.Fatal(err)
	}

	// Start listening for updates
	dataPipelineRunner.Start()

	currentTime := time.Now()
	pauseTime, err := time.Parse(time.RFC3339, env.PauseTime)
	if err != nil {
		sugar.Fatal(err)
	}
	// ignores time configuration if given pause time date is before when code is ran
	if currentTime.Before(pauseTime) {
		sugar.Info("Datapipeline pause time: %s", pauseTime)
		go pauseAndResume(&pauseTime, dataPipelineRunner.Stop, sugar)

		resumeTime, err := time.Parse(time.RFC3339, env.ResumeTime)
		if err != nil {
			sugar.Fatal(err)
		}
		sugar.Info("Datapipeline resume time: %s", resumeTime)
		go pauseAndResume(&resumeTime, dataPipelineRunner.Start, sugar)
	}

	// Start listening
	sugar.Infof("Listening on %s", env.Addr)
	sugar.Fatal(http.ListenAndServe(env.Addr, r))

}

func pauseAndResume(resumeTime *time.Time, dataPipelineOperation func(), sugar *zap.SugaredLogger) {
	currentTime := time.Now()
	resumeHour := time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day(), resumeTime.Hour(), resumeTime.Minute(), resumeTime.Second(), 0, resumeTime.Location())
	difference := resumeHour.Sub(currentTime)

	if difference < 0 {
		resumeHour = resumeHour.Add(24 * time.Hour)
		difference = resumeHour.Sub(currentTime)
	}
	for {
		time.Sleep(difference)
		difference = 24 * time.Hour
		dataPipelineOperation()

	}
}
