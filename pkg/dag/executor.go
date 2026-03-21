package dag

import (
	"context"
	"fmt"
	"log/slog"
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

// Runner handles the execution of a DAG with parallel processing capabilities.
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

// NewRunner creates a new DAG runner.
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

// ExecutionResult contains successful step outputs and any non-terminal step errors.
type ExecutionResult struct {
	Results map[string]interface{}
	Errors  map[string]error
}

// Execute runs the DAG with the default execution policy.
// By default, any non-leaf node failure terminates the run immediately,
// while leaf node failures are captured in ExecutionResult.Errors.
func (e *Runner) Execute(input map[string]interface{}) (*ExecutionResult, error) {
	return e.ExecuteWithContext(context.Background(), input)
}

// ExecuteWithContext runs the DAG with support for external cancellation.
// The default criticality policy treats non-leaf nodes as execution-critical.
func (e *Runner) ExecuteWithContext(ctx context.Context, input map[string]interface{}) (*ExecutionResult, error) {
	return e.executeWithPolicy(ctx, input, NewStructuralExecutionPolicy(e))
}

// ExecuteForResponse runs the DAG using a response-critical execution policy.
// Only nodes required to produce the provided response shape — and their
// transitive dependencies — are treated as terminal on failure. Failures
// outside that critical set are captured in ExecutionResult.Errors.
func (e *Runner) ExecuteForResponse(
	ctx context.Context,
	input map[string]interface{},
	response map[string]interface{},
) (*ExecutionResult, error) {
	criticalSet, err := ExtractResponseDependencyClosure(response, e.graph)
	if err != nil {
		return nil, err
	}

	stepIDs := make([]string, 0, len(criticalSet))
	for stepID := range criticalSet {
		stepIDs = append(stepIDs, stepID)
	}

	return e.executeWithPolicy(ctx, input, NewCriticalSetExecutionPolicy(stepIDs))
}

func (e *Runner) executeWithPolicy(
	ctx context.Context,
	input map[string]interface{},
	policy ExecutionPolicy,
) (*ExecutionResult, error) {
	slog.Info("starting DAG execution", "input", input)

	execution := NewExecutionContext(ctx, input, e, policy)

	for _, step := range e.rootNodes {
		execution.wg.Add(1)
		go execution.initExecution(step)
	}

	done := make(chan struct{})
	go func() {
		execution.wg.Wait()
		close(done)
	}()

	var runErr error

	select {
	case errEvt := <-execution.errorChannel:
		execution.cancel()
		runErr = fmt.Errorf("step %s failed: %w", errEvt.StepID, errEvt.Err)
	case <-ctx.Done():
		execution.cancel()
		runErr = fmt.Errorf("execution cancelled: %w", ctx.Err())
	case <-done:
		select {
		case errEvt := <-execution.errorChannel:
			runErr = fmt.Errorf("step %s failed: %w", errEvt.StepID, errEvt.Err)
		case <-ctx.Done():
			runErr = fmt.Errorf("execution cancelled: %w", ctx.Err())
		default:
		}
	}

	execution.wg.Wait()

	if runErr != nil {
		slog.Error("DAG execution failed", "error", runErr)
		return nil, runErr
	}

	slog.Info(
		"DAG execution completed",
		"results_count", len(execution.context.Results),
		"errors_count", len(execution.context.Errors),
	)

	return &ExecutionResult{
		Results: execution.context.Results,
		Errors:  execution.context.Errors,
	}, nil
}
