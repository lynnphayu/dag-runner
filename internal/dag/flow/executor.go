package flow

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/lynnphayu/dag-runner/internal/dag/domain"
	http "github.com/lynnphayu/dag-runner/internal/dag/repositories/http"
	postgres "github.com/lynnphayu/dag-runner/internal/dag/repositories/postgres"
	"github.com/xeipuuv/gojsonschema"
)

// Executor handles the execution of a DAG with parallel processing capabilities
type Executor struct {
	db         *postgres.Postgres
	httpClient *http.Http
}

type ErrEvt struct {
	StepID string
	Err    error
}

type Execution struct {
	dag      *domain.DAG
	stepsMap map[string]*domain.Step
	context  *domain.Context
	output   interface{}

	waitList          *sync.Map
	executor          *Executor
	wg                *sync.WaitGroup
	errorChannel      chan ErrEvt
	completionChannel chan string
}

// NewExecutor creates a new DAG executor
func NewExecutor(connStr string) (*Executor, error) {
	db, err := postgres.NewPostgres(connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to create repository: %w", err)
	}
	httpClient, err := http.NewHttp(connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to create http client: %w", err)
	}
	return &Executor{
		db:         db,
		httpClient: httpClient,
	}, nil
}

// Execute runs the DAG with parallel execution of steps
func (e *Executor) Execute(dag *domain.DAG, input map[string]interface{}) (interface{}, error) {
	if err := validateSchema(dag.InputSchema, input); err != nil {
		return nil, fmt.Errorf("input validation failed: %w", err)
	}

	stepsMap, err := e.mapSteps(dag)
	if err != nil {
		return nil, err
	}

	execution := &Execution{
		dag:      dag,
		stepsMap: stepsMap,
		context: &domain.Context{
			Results: &map[string]interface{}{},
			Input:   &input,
		},
		executor:          e,
		waitList:          &sync.Map{},
		wg:                &sync.WaitGroup{},
		errorChannel:      make(chan ErrEvt, 1),
		completionChannel: make(chan string, 100),
	}

	// Create wait group for tracking goroutines

	// Start execution from the entry step
	independentSteps := e.independentSteps(dag)
	for _, step := range independentSteps {
		execution.wg.Add(1)
		go execution.initExecution(execution.stepsMap[step])
	}

	execution.wg.Wait()

	// Check for any errors
	select {
	case err := <-execution.errorChannel:
		return nil, fmt.Errorf("step %s failed: %w", err.StepID, err.Err)
	default:
		// No errors occurred
	}

	// Get the final step result
	// result, ok := (*execution.context.Results)[dag.Result]
	// if !ok {
	// 	return nil, fmt.Errorf("final step result not found")
	// }

	// result := resolveString[interface{}](dag.Result, execution.context)

	// Validate output against schema
	if err := validateSchema(dag.OutputSchema, execution.output); err != nil {
		return nil, fmt.Errorf("output validation failed: %w", err)
	}

	return execution.output.(interface{}), nil
}

func (e *Executor) mapSteps(dag *domain.DAG) (map[string]*domain.Step, error) {
	steps := make(map[string]*domain.Step)
	for _, step := range dag.Steps {
		if _, ok := steps[step.ID]; ok {
			return nil, fmt.Errorf("Duplicate step ID: %s", step.ID)
		}
		steps[step.ID] = &step
	}
	return steps, nil
}

func (e *Executor) independentSteps(dag *domain.DAG) []string {
	dependentSteps := make(map[string]bool)
	independentSteps := make([]string, 0)
	for _, step := range dag.Steps {
		for _, dep := range step.Then {
			dependentSteps[dep] = true
		}
		// only for Condition type
		for _, dep := range step.Else {
			dependentSteps[dep] = true
		}
	}
	for _, step := range dag.Steps {
		if !dependentSteps[step.ID] && len(step.DependsOn) == 0 {
			independentSteps = append(independentSteps, step.ID)
		}
	}
	return independentSteps
}

func ParseDAG(dagString []byte) (domain.DAG, error) {
	var dag domain.DAG
	err := json.Unmarshal([]byte(dagString), &dag)
	if err != nil {
		return domain.DAG{}, err
	}
	return dag, nil
}

func validateSchema(schema domain.Schema, data interface{}) error {
	schemaLoader := gojsonschema.NewGoLoader(schema)
	dataLoader := gojsonschema.NewGoLoader(data)

	result, err := gojsonschema.Validate(schemaLoader, dataLoader)
	if err != nil {
		return err
	}

	if !result.Valid() {
		return fmt.Errorf("validation errors: %v", result.Errors())
	}

	return nil
}
