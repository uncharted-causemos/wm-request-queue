package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/machinebox/graphql"
	"github.com/pkg/errors"
	"gitlab.uncharted.software/WM/wm-request-queue/api/queue"
	"gitlab.uncharted.software/WM/wm-request-queue/config"
)

// DataPipelineRunner services the request queue
type DataPipelineRunner struct {
	config.Config
	client  *graphql.Client
	queue   queue.RequestQueue
	done    chan bool
	running bool
	mutex   *sync.RWMutex
}

// NewDataPipelineRunner creates a new instance of a data pipeline runner.
func NewDataPipelineRunner(cfg *config.Config, requestQueue queue.RequestQueue) *DataPipelineRunner {
	// standard http client with our timeout
	httpClient := &http.Client{Timeout: time.Second * time.Duration(cfg.Environment.DataPipelineTimeoutSec)}

	// graphql client that uses our http  client - our timeout is applied transitively
	graphQLClient := graphql.NewClient(cfg.Environment.DataPipelineAddr, graphql.WithHTTPClient(httpClient))

	return &DataPipelineRunner{
		Config: config.Config{
			Logger:      cfg.Logger,
			Environment: cfg.Environment,
		},
		queue:   requestQueue,
		client:  graphQLClient,
		done:    make(chan bool),
		running: false,
		mutex:   &sync.RWMutex{},
	}
}

// Start initiates request queue servicing.
func (d *DataPipelineRunner) Start() {
	d.mutex.RLock()
	if d.running {
		d.mutex.RUnlock()
		return
	}
	d.mutex.RUnlock()

	// Read from the queue until we get shut down.
	go func() {
		d.mutex.Lock()
		d.running = true
		d.mutex.Unlock()

		for {
			select {
			case <-d.done:
				// break out of the loop on shutdown
				d.mutex.Lock()
				d.running = false
				d.mutex.Unlock()
				return
			default:
				// Check to see if prefect is busy.  If not run the next flow request in the
				// queue.
				running, err := d.getActiveFlowRuns()
				if err != nil {
					d.Logger.Error(err)
				} else {
					activeFlowRuns := len(running.FlowRun)
					if activeFlowRuns < d.Config.Environment.DataPipelineParallelism {
						d.Submit()
					}
				}
				time.Sleep(time.Duration(d.Environment.DataPipelinePollIntervalSec) * time.Second)
			}
		}
	}()
}

// Submit submits the next item in the queue
func (d *DataPipelineRunner) Submit() {
	if d.queue.Size() == 0 {
		return
	}
	data, err := d.queue.Dequeue()
	if err != nil {
		d.Logger.Error(err)
	}
	request, ok := data.(KeyedEnqueueRequestData)
	if !ok {
		d.Logger.Error(errors.Errorf("unhandled request type %s", reflect.TypeOf(request)))
	}
	if err := d.submitFlowRunRequest(&request); err != nil {
		d.Logger.Error(err)
	}
}

// Stop ends request servicing.
func (d *DataPipelineRunner) Stop() {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	if d.running {
		d.done <- true
	}
}

// Running indicates whether or not the pipeline runner routine has been stopped,
// or is currently running.
func (d *DataPipelineRunner) Running() bool {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	return d.running
}

// Status of prefect flow runs.
type activeFlowRuns struct {
	FlowRun []struct {
		ID    string
		State string
		Flow  struct {
			ID             string
			Name           string
			VersionGroupID string `json:"version_group_id"`
		}
	} `json:"flow_run"`
}

// Fetches the scheduled/running tasks from prefect.
func (d *DataPipelineRunner) getActiveFlowRuns() (*activeFlowRuns, error) {
	query := graphql.NewRequest(
		`query {
			flow_run(where: {
			  _and: [{
				_or: [
					{state: {_eq: "Submitted"}}
					{state: {_eq: "Scheduled"}}
					{state: {_eq: "Running"}}
				]
			  }, {
				  flow: {version_group_id: {_eq: "` + d.Config.Environment.DataPipelineTileFlowID + `"}}
			  }
			  ]
			}) {
			  id
			  state
			  flow {
				id
				name
				version_group_id
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
func (d *DataPipelineRunner) submitFlowRunRequest(request *KeyedEnqueueRequestData) error {
	// compose the run name
	runName := fmt.Sprintf("%s:%s", request.ModelID, request.RunID)

	// prefect server expects JSON to be escaped and without newlines/tabs
	buffer := bytes.Buffer{}
	if err := json.Compact(&buffer, request.RequestData); err != nil {
		return errors.Wrap(err, "failed to compact request JSON")
	}
	escaped := strings.ReplaceAll(buffer.String(), `"`, `\"`)

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
	if config.UsePrefectIdempotency(d.Environment.DataPipelineIdempotencyChecks) {
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
