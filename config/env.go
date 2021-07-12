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
	DataPipelineAddr string `default:"http://localhost:4200"`
	// Prefect server request timeout
	DataPipelineTimeoutSec int `default:"10"`
	// Data pipeline queue request size
	DataPipelineQueueSize int `default:"100"`
	// Prefect polling interaval
	DataPipelinePollIntervalSec int `default:"5"`
	// Flow version group ID for the baseline tile flow
	DataPipelineTileFlowID string `default:"4d8d9239-2594-45af-9ec9-d24eafb1f1af"`
	// Enable idempotency checks on prefect
	DataPipelineIdempotencyChecks bool `default:"true"`
}

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
	// if no env var in existing environment, load environment file from the .env file, otherwise (in production) just check existing host environment
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
