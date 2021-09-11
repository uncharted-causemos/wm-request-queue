package config

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
	"github.com/pkg/errors"
)

// Environment contains the imported environment variables.
type Environment struct {
	// Debug vs Deploy
	Mode string `default:"dev"`
	// Port to listen on
	Addr string `default:":4040"`
	// Prefect server address including port
	DataPipelineAddr string `default:"http://localhost:4200" split_words:"true"`
	// Prefect server request timeout
	DataPipelineTimeoutSec int `default:"10" split_words:"true"`
	// Data pipeline queue request size
	DataPipelineQueueSize int `default:"100" split_words:"true"`
	// Prefect polling interaval
	DataPipelinePollIntervalSec int `default:"5" split_words:"true"`
	// Flow version group ID for the baseline tile flow
	DataPipelineTileFlowID string `default:"4d8d9239-2594-45af-9ec9-d24eafb1f1af" split_words:"true"`
	// Enable idempotency checks on prefect
	DataPipelineIdempotencyChecks string `default:"all" split_words:"true"`
	// Maximum number of flows to run in parallel
	DataPipelineParallelism int `default:"1" split_words:"true"`
	// Use persisted queue or default (memory only) queue.
	DataPipelinePersistedQueue bool `default:"true" split_workds:"true"`
	// Directory to store the queue data in when persisted queue is used.
	DataPipelineQueueDir string `default:"./" split_words:"true"`
	// Name of queue when persisted queue is used.
	DataPipelineQueueName string `default:"request_queue" split_words:"true"`
}

const (
	// IdempotencyAll applies all idempotency checks
	IdempotencyAll = "all"
	// IdempotencyNone skips all idempotency checks
	IdempotencyNone = "none"
	// IdempotencyQueue ignores duplicate requests when enqueuing
	IdempotencyQueue = "queue"
	// IdempotencyPrefect uses prefect's idempotency checks to skip requests that have already
	// been run
	IdempotencyPrefect = "prefect"
)

func (e Environment) String() string {
	settings, err := json.MarshalIndent(e, "", "    ")
	if err != nil {
		return fmt.Errorf("Failed to marshal env: %v", err).Error()
	}
	return fmt.Sprintf("Environment Settings:\n%s\n", string(settings))
}

// Load imports the environment variables and returns them in an Specification.
func Load(envFile string) (*Environment, error) {
	testEnv := os.Getenv("WM_MODE")
	// if no env var in existing environment, load environment file from the .env file,
	// otherwise (in production) just check existing host environment
	if "" == testEnv {
		err := godotenv.Load(envFile)
		if err != nil {
			return nil, errors.Wrapf(err, "Error loading %s file", envFile)
		}
	}

	var env Environment
	err := envconfig.Process("wm", &env)
	if err != nil {
		return nil, errors.Wrap(err, "Error processing environment config")
	}
	return &env, err
}

// UsePrefectIdempotency checks if the supplied arg calls for the use of prefect's idempotency
// functionalty, which skips execution of a previously run request.
func UsePrefectIdempotency(idempotencyType string) bool {
	return idempotencyType == IdempotencyAll || idempotencyType == IdempotencyPrefect
}

// UseQueueIdempotency checks if the supplied arg calls for queue level idempotency, which skips
// enqueue requests for a currently enqueued job.
func UseQueueIdempotency(idempotencyType string) bool {
	return idempotencyType == IdempotencyAll || idempotencyType == IdempotencyQueue
}
