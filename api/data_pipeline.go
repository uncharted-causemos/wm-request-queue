package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/machinebox/graphql"
	"github.com/pkg/errors"
	"gitlab.uncharted.software/WM/wm-request-queue/api/routes"
	"gitlab.uncharted.software/WM/wm-request-queue/config"
)

// DataPipelineRunner services the request queue
type DataPipelineRunner struct {
	config.Config
	client *graphql.Client
	done   chan bool
}

// NewDataPipelineRunner creates a new instance of a data pipeline runner.
func NewDataPipelineRunner(cfg *config.Config) *DataPipelineRunner {
	httpClient := &http.Client{Timeout: time.Second * time.Duration(cfg.Environment.DataPipelineTimeoutSec)}
	graphQLClient := graphql.NewClient(cfg.Environment.DataPipelineAddr, graphql.WithHTTPClient(httpClient))

	return &DataPipelineRunner{
		Config: config.Config{
			Logger:       cfg.Logger,
			Environment:  cfg.Environment,
			RequestQueue: cfg.RequestQueue,
		},
		client: graphQLClient,
		done:   make(chan bool),
	}
}

// Start initiates request queue servicing.
func (d *DataPipelineRunner) Start() {
	// Read from the queue until we get shut down.
	go func() {
		for {
			select {
			case <-d.done:
				// break out of the loop on shutdown
				break
			default:
				// Check to see if prefect is busy.  If not run the next flow request in the
				// queue.
				running, err := d.getActiveFlowRuns()
				if err != nil {
					d.Logger.Error(err)
				} else {
					activeFlowRuns := len(running.FlowRun)
					if activeFlowRuns == 0 {
						request, ok := d.Config.RequestQueue.Dequeue().(routes.KeyedEnqueueRequestData)
						if !ok {
							d.Logger.Error(errors.Errorf("unhandled request type %s", reflect.TypeOf(request)))
						}
						if err := d.submitFlowRunRequest(request); err != nil {
							d.Logger.Error(err)
						}

					}
				}
				time.Sleep(time.Duration(d.Environment.DataPipelinePollIntervalSec) * time.Second)
			}
		}
	}()
}

// Stop ends request servicing.
func (d *DataPipelineRunner) Stop() {
	d.done <- true
}

// Status of prefect flow runs.
type activeFlowRuns struct {
	FlowRun []struct {
		ID    string
		State string
		Flow  struct {
			ID   string
			Name string
		}
	} `json:"flow_run"`
}

// Fetches the scheduled/running tasks from prefect.
func (d *DataPipelineRunner) getActiveFlowRuns() (*activeFlowRuns, error) {
	query := graphql.NewRequest(
		`query {
			flow_run(where: {
			  _or: [
				{state: {_eq: "Submitted"}}
				{state: {_eq: "Scheduled"}}
				{state: {_eq: "Running"}}
			  ]
			}) {
			  id
			  state
			  flow {
				id
				name
			  }
			}
		  }`,
	)

	// run it and capture the response
	var respData activeFlowRuns
	if err := d.client.Run(context.Background(), query, &respData); err != nil {
		return nil, errors.Wrap(err, "failed to fetch running flows")
	}
	return &respData, nil
}

// Submits a flow run request to prefect.
func (d *DataPipelineRunner) submitFlowRunRequest(request routes.KeyedEnqueueRequestData) error {

	runName := fmt.Sprintf("%s:%s", request.ModelID, request.RunID)
	flowParameters, err := json.Marshal(request)
	if err != nil {
		return errors.Wrap(err, "failed to marshal flow request params")
	}
	// prefect server expects JSON to be escaped
	escaped := strings.ReplaceAll(string(flowParameters), `"`, `\"`)

	// Define a task submission query
	// ** NOTE: Using a GraphQL variable for the `parameters` field generates an error on the server,
	// and the same error can be replicated through the Prefect "Interactive API" in the UI.  It seems
	// to a bug in how the prefect server parses the JSON stored in the parameters string.  For now the
	// best we can do is include the JSON through string formatting.
	requestStr := fmt.Sprintf("mutation($id: String, $runName: String, $key: String) {"+
		"create_flow_run(input: { "+
		"   idempotency_key: $key, "+
		"	version_group_id: $id, "+
		"	flow_run_name: $runName, "+
		"	parameters: \"%s\""+
		"}) { "+
		"	id "+
		"}"+
		"}", escaped)

	mutation := graphql.NewRequest(requestStr)

	mutation.Var("id", d.Environment.DataPipelineTileFlowID)
	mutation.Var("runName", runName)

	// set the key to use for prefect's idempotency checks - if a pipeline is run to completion,
	// SUCESSFULLY or UNSUCESSFULLY, an attempt to re-run with the same key will result in it being
	// skipped.
	if d.Config.Environment.DataPipelineIdempotencyChecks {
		idempotencyKey := strconv.FormatUint(uint64(request.RequestKey), 16)
		mutation.Var("key", idempotencyKey)
	}

	// run it and capture the response
	var respData activeFlowRuns
	if err := d.client.Run(context.Background(), mutation, &respData); err != nil {
		return errors.Wrap(err, "failed to run flow")
	}
	return nil
}
