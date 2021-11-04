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
	agents  prefectAgents
}

// NewDataPipelineRunner creates a new instance of a data pipeline runner.
func NewDataPipelineRunner(cfg *config.Config, requestQueue queue.RequestQueue) *DataPipelineRunner {
	// standard http client with our timeout
	httpClient := &http.Client{Timeout: time.Second * time.Duration(cfg.Environment.DataPipelineTimeoutSec)}

	// graphql client that uses our http  client - our timeout is applied transitively
	graphQLClient := graphql.NewClient(cfg.Environment.DataPipelineAddr, graphql.WithHTTPClient(httpClient))

	dataPipeline := &DataPipelineRunner{
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

	dataPipeline.SetAgents()

	return dataPipeline
}

type agent struct {
	ID     string
	Name   string
	Labels []string
}

// Current non-dask agents in prefect
type prefectAgents struct {
	Agents []agent `json:"agent"`
}

// SetAgents Gets non-dask labelled agents to track
func (d *DataPipelineRunner) SetAgents() {
	// query := graphql.NewRequest(
	// 	`query {
	// 		agent(
	// 			where: {
	// 			  _not:{
	// 				labels:{_contains: "non-dask"}
	// 			  }
	// 			}
	// 		  ) {
	// 			id
	// 			name
	// 			labels
	// 		  }
	// 	}`,
	// )
	query := graphql.NewRequest(
		`query {
			agent(
				where: {
				  labels:{_contains: "non-dask"}
				}
			  ) {
				id
				name
				labels
			  }
		}`,
	)

	var respData prefectAgents
	if err := d.client.Run(context.Background(), query, &respData); err != nil {
		d.Logger.Error(err)
	}

	d.mutex.Lock()
	d.agents = respData
	d.mutex.Unlock()
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
				d.Submit(false)
				time.Sleep(time.Duration(d.Environment.DataPipelinePollIntervalSec) * time.Second)
			}
		}
	}()
}

// Submit submits the next item in the queue
func (d *DataPipelineRunner) Submit(force bool) {
	running, err := d.getActiveFlowRuns()
	labels := d.getLabelsToRunFlow(running)
	if force {
		d.submit(labels)
		return
	}
	// Check to see if prefect is busy.  If not run the next flow request in the
	// queue.
	if err != nil {
		d.Logger.Error(err)
	} else {
		activeFlowRuns := len(running.FlowRun)
		if activeFlowRuns < d.Config.Environment.DataPipelineParallelism {
			d.submit(labels)
		}
	}
}

func (d *DataPipelineRunner) getLabelsToRunFlow(flowRuns *activeFlowRuns) []string {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	for _, trackedAgent := range d.agents.Agents {
		found := true
		for _, flowRun := range flowRuns.FlowRun {
			// if agent is occupied, then we don't want to use it
			if trackedAgent.ID == flowRun.Agent.ID {
				found = false
				break
			}
		}
		if found {
			return trackedAgent.Labels
		}
	}
	return []string{}
}

func (d *DataPipelineRunner) submit(labels []string) {
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
	if err := d.submitFlowRunRequest(&request, labels); err != nil {
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
		Agent agent
	} `json:"flow_run"`
}

// GetAmountOfRunningFlows returns the number of running/scheduled flows
func (d *DataPipelineRunner) GetAmountOfRunningFlows() (int, error) {
	running, err := d.getActiveFlowRuns()
	if err != nil {
		return 0, err
	}
	return len(running.FlowRun), nil
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
			  agent {
				  id
				  name
				  labels
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
func (d *DataPipelineRunner) submitFlowRunRequest(request *KeyedEnqueueRequestData, labels []string) error {
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
	requestStr := fmt.Sprintf("mutation($id: String, $runName: String, $labels: [String!], $key: String) {"+
		"create_flow_run(input: { "+
		"   idempotency_key: $key, "+
		"	version_group_id: $id, "+
		"	flow_run_name: $runName, "+
		"	labels: $labels, "+
		"	parameters: \"%s\""+
		"}) { "+
		"	id "+
		"}"+
		"}", escaped)

	mutation := graphql.NewRequest(requestStr)

	mutation.Var("id", d.Environment.DataPipelineTileFlowID)
	mutation.Var("runName", runName)
	if len(labels) > 0 {
		mutation.Var("labels", labels)
	}
	// mutation.Var("labels", []string{"non-dask", "wm-prefect-server.openstack.uncharted.software"})
	// mutation.Var("labels", []string{"three-medium", "wm-prefect-server.openstack.uncharted.software"})

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
