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
	client         *graphql.Client
	queue          queue.RequestQueue
	done           chan bool
	running        bool
	mutex          *sync.RWMutex
	currentFlowIDs map[string]FlowData
	httpClient     http.Client
	agents         prefectAgents
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
		queue:          requestQueue,
		client:         graphQLClient,
		done:           make(chan bool),
		running:        false,
		mutex:          &sync.RWMutex{},
		currentFlowIDs: make(map[string]FlowData),
		httpClient:     *httpClient,
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
	query := graphql.NewRequest(
		`query {
			agent(
				where: {
				  _not:{
					labels:{_contains: "` + d.Environment.AgentLabelToIgnore + `"}
				  }
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

// updateCurrentFlows notifies causemos for failed jobs, removes them from
// currentFlowIDs if they failed or succeeded
func (d *DataPipelineRunner) updateCurrentFlows() {
	flowIds := d.getIDString()
	if flowIds != "[]" {
		currentFlows, err := d.getFlowRunsByIds(flowIds)
		if err != nil {
			d.Logger.Error(err)
			return
		}
		d.mutex.Lock()
		for i := 0; i < len(currentFlows.FlowRun); i++ {
			// check if a flow we're tracking has failed
			if currentFlows.FlowRun[i].State == "Failed" {
				values := map[string]interface{}{"run_id": d.currentFlowIDs[currentFlows.FlowRun[i].ID].Request.RunID,
					"data_id":      d.currentFlowIDs[currentFlows.FlowRun[i].ID].Request.ModelID,
					"doc_ids":      d.currentFlowIDs[currentFlows.FlowRun[i].ID].Request.DocIDs,
					"is_indicator": d.currentFlowIDs[currentFlows.FlowRun[i].ID].Request.IsIndicator}
				payload_json, _ := json.Marshal(values)

				req, err := http.NewRequest(http.MethodPut, d.Config.Environment.CauseMosAddr+"/api/maas/pipeline-reporting/processing-failed", bytes.NewBuffer(payload_json))
				if err != nil {
					d.Logger.Error(err)
					continue
				}
				req.Header.Set("Content-type", "application/json")
				resp, err := d.httpClient.Do(req)
				if err != nil {
					d.Logger.Error(err)
				} else {
					resp.Body.Close()
				}
				delete(d.currentFlowIDs, currentFlows.FlowRun[i].ID)
			} else if currentFlows.FlowRun[i].State == "Success" {
				values := map[string]interface{}{"run_id": d.currentFlowIDs[currentFlows.FlowRun[i].ID].Request.RunID,
					"data_id":      d.currentFlowIDs[currentFlows.FlowRun[i].ID].Request.ModelID,
					"doc_ids":      d.currentFlowIDs[currentFlows.FlowRun[i].ID].Request.DocIDs,
					"is_indicator": d.currentFlowIDs[currentFlows.FlowRun[i].ID].Request.IsIndicator,
					"start_time":   d.currentFlowIDs[currentFlows.FlowRun[i].ID].StartTime.Unix(),
					"end_time":     time.Now().Unix()}
				payload_json, _ := json.Marshal(values)

				req, err := http.NewRequest(http.MethodPut, d.Config.Environment.CauseMosAddr+"/api/maas/pipeline-reporting/processing-succeeded", bytes.NewBuffer(payload_json))
				if err != nil {
					d.Logger.Error(err)
					continue
				}
				req.Header.Set("Content-type", "application/json")
				resp, err := d.httpClient.Do(req)
				if err != nil {
					d.Logger.Error(err)
				} else {
					resp.Body.Close()
				}
				delete(d.currentFlowIDs, currentFlows.FlowRun[i].ID)
			}
		}
		d.mutex.Unlock()
	}
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
	d.updateCurrentFlows()
}

func (d *DataPipelineRunner) getLabelsToRunFlow(flowRuns *flowRuns) []string {
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
	flowID, err := d.submitFlowRunRequest(&request, labels)

	if err != nil {
		d.Logger.Error(err)
	}

	// track flow
	if flowID != "" {
		d.mutex.Lock()
		d.currentFlowIDs[flowID] = FlowData{Request: request.EnqueueRequestData, StartTime: time.Now()}
		d.mutex.Unlock()
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
type flowRuns struct {
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
func (d *DataPipelineRunner) getActiveFlowRuns() (*flowRuns, error) {
	queryString :=
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
		  }`

	return d.runGraphqlRequest(queryString)
}

func (d *DataPipelineRunner) getIDString() string {
	result := "["
	d.mutex.Lock()
	for k := range d.currentFlowIDs {
		result += "\"" + k + "\"" + ","
	}
	d.mutex.Unlock()

	if result != "[" {
		return result[:len(result)-1] + "]"
	}
	return "[]"
}

func (d *DataPipelineRunner) getFlowRunsByIds(ids string) (*flowRuns, error) {
	queryString :=
		`query {
			flow_run(where: {
			  _and: [{
					id: {_in: ` + ids + `}
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
		  }`

	return d.runGraphqlRequest(queryString)
}

func (d *DataPipelineRunner) runGraphqlRequest(queryString string) (*flowRuns, error) {
	query := graphql.NewRequest(queryString)

	// run it and capture the response
	var respData flowRuns
	if err := d.client.Run(context.Background(), query, &respData); err != nil {
		return nil, errors.Wrap(err, "failed to fetch running flows")
	}
	return &respData, nil
}

type flowSubmissionResponse struct {
	CreateFlowRun struct {
		ID string
	} `json:"create_flow_run"`
}

// Submits a flow run request to prefect.
func (d *DataPipelineRunner) submitFlowRunRequest(request *KeyedEnqueueRequestData, labels []string) (string, error) {
	// compose the run name
	runName := fmt.Sprintf("%s:%s", request.ModelID, request.RunID)

	// prefect server expects JSON to be escaped and without newlines/tabs
	buffer := bytes.Buffer{}
	if err := json.Compact(&buffer, request.RequestData); err != nil {
		return "", errors.Wrap(err, "failed to compact request JSON")
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

	// set the key to use for prefect's idempotency checks - if a pipeline is run to completion,
	// SUCESSFULLY or UNSUCESSFULLY, an attempt to re-run with the same key will result in it being
	// skipped.
	if config.UsePrefectIdempotency(d.Environment.DataPipelineIdempotencyChecks) {
		idempotencyKey := strconv.FormatUint(uint64(request.RequestKey), 16)
		mutation.Var("key", idempotencyKey)
	}

	var respData flowSubmissionResponse
	// run it and capture the response
	if err := d.client.Run(context.Background(), mutation, &respData); err != nil {
		return respData.CreateFlowRun.ID, errors.Wrap(err, "failed to run flow")
	}
	return respData.CreateFlowRun.ID, nil
}
