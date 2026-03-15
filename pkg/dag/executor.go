package dag

import (
	"context"
	"fmt"
	"log"
	"net/http"
)

type Persist interface {
	Create(table string, data map[string]interface{}) (interface{}, error)
	Retrieve(table string, select_ []string, where map[string]interface{}) ([]interface{}, error)
	Update(
		table string,
		data map[string]interface{},
		where map[string]interface{},
	) (interface{}, error)
	Delete(table string, where map[string]interface{}) (interface{}, error)

	GetTableNames() ([]string, error)
	GetColumns(table string) (map[string]string, error)
}

type ParsedResponse struct {
	Data       interface{}
	Raw        *http.Response
	StatusCode int
}

type Http interface {
	Post(
		url string,
		query map[string]interface{},
		body map[string]interface{},
		headers map[string]string,
	) (*ParsedResponse, error)
	Get(
		url string,
		query map[string]interface{},
		headers map[string]string,
	) (*ParsedResponse, error)
	Put(
		url string,
		body map[string]interface{},
		query map[string]interface{},
		headers map[string]string,
	) (*ParsedResponse, error)
	Delete(
		url string,
		query map[string]interface{},
		headers map[string]string,
	) (*ParsedResponse, error)
	Patch(
		url string,
		body map[string]interface{},
		query map[string]interface{},
		headers map[string]string,
	) (*ParsedResponse, error)
}

// Runner handles the execution of a DAG with parallel processing capabilities
type Runner struct {
	graphSize       int
	nodesDict       map[string]*Node[*Action]
	successorsMap   map[string][]string
	predecessorsMap map[string][]string
	rootNodes       []*Node[*Action]
	graph           *Graph[*Action, any]
	db              *Persist
	httpClient      *Http
}

// NewExecutor creates a new DAG executor
func NewRunner(db Persist, http Http, dag *Graph[*Action, any]) *Runner {
	graphSize := dag.Size()
	nodesMap := dag.NodesDict()
	rootNodes := dag.RootNodes()

	return &Runner{
		graphSize:       graphSize,
		rootNodes:       rootNodes,
		successorsMap:   dag.FanOutEdges(),
		predecessorsMap: dag.FanInEdges(),
		nodesDict:       nodesMap,
		db:              &db,
		httpClient:      &http,
		graph:           dag,
	}
}

// ExecutionResult contains the results and any leaf node errors
type ExecutionResult struct {
	Results map[string]interface{}
	Errors  map[string]error
}

// Execute runs the DAG with parallel execution of steps.
// If a non-leaf node fails, returns immediately with the error.
// If a leaf node fails, the error is captured in ExecutionResult.Errors.
func (e *Runner) Execute(input map[string]interface{}) (*ExecutionResult, error) {
	return e.ExecuteWithContext(context.Background(), input)
}

// ExecuteWithContext runs the DAG with support for external cancellation.
func (e *Runner) ExecuteWithContext(ctx context.Context, input map[string]interface{}) (*ExecutionResult, error) {
	log.Printf("[dag] starting execution with input: %v", input)
	execution := NewExecutionContext(input, e)

	// Start execution from the entry step
	for _, step := range e.rootNodes {
		execution.wg.Add(1)
		go execution.initExecution(step)
	}

	// Wait for either completion or early termination
	done := make(chan struct{})
	go func() {
		execution.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Normal completion
	case errEvt := <-execution.errorChannel:
		// Non-leaf node failed - return immediately
		execution.cancel() // Ensure everything shuts down
		execution.wg.Wait() // Wait for cleanup
		log.Printf("[dag] execution failed at step %s: %v", errEvt.StepID, errEvt.Err)
		return nil, fmt.Errorf("step %s failed: %w", errEvt.StepID, errEvt.Err)
	case <-ctx.Done():
		// External cancellation
		execution.cancel()
		execution.wg.Wait()
		return nil, fmt.Errorf("execution cancelled: %w", ctx.Err())
	}

	log.Printf("[dag] execution completed - results: %d, errors: %d",
		len(execution.context.Results), len(execution.context.Errors))

	return &ExecutionResult{
		Results: execution.context.Results,
		Errors:  execution.context.Errors,
	}, nil
}
