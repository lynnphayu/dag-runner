package dag

import (
	"fmt"
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
	graph           *Graph[*Action]
	db              *Persist
	httpClient      *Http
}

// NewExecutor creates a new DAG executor
func NewRunner(db Persist, http Http, dag *Graph[*Action]) *Runner {
	graphSize := dag.GetSize()
	nodesMap := dag.GetNodesDict()
	rootNodes := dag.GetRootNodes()

	return &Runner{
		graphSize:       graphSize,
		rootNodes:       rootNodes,
		successorsMap:   dag.GetFanOutEdges(),
		predecessorsMap: dag.GetFanInEdges(),
		nodesDict:       nodesMap,
		db:              &db,
		httpClient:      &http,
		graph:           dag,
	}
}

// Execute runs the DAG with parallel execution of steps
func (e *Runner) Execute(input map[string]interface{}) (map[string]interface{}, error) {
	execution := NewExecutionContext(input, e)

	// Start execution from the entry step
	for _, step := range e.rootNodes {
		execution.wg.Add(1)
		go execution.initExecution(step)
	}

	execution.wg.Wait()

	// Check for any errors
	select {
	case err := <-execution.errorChannel:
		return nil, fmt.Errorf("step %s failed: %w", err.StepID, err.Err)
	default:
		// No errors occurred
	}

	return execution.context.Results, nil
}
