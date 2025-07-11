package dag

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/xeipuuv/gojsonschema"
)

type Persist interface {
	Create(table string, data map[string]interface{}) (interface{}, error)
	Retrieve(table string, select_ []string, where map[string]interface{}) ([]interface{}, error)
	Update(table string, data map[string]interface{}, where map[string]interface{}) (interface{}, error)
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
	Post(url string, query map[string]interface{}, body map[string]interface{}, headers map[string]string) (*ParsedResponse, error)
	Get(url string, query map[string]interface{}, headers map[string]string) (*ParsedResponse, error)
	Put(url string, body map[string]interface{}, query map[string]interface{}, headers map[string]string) (*ParsedResponse, error)
	Delete(url string, query map[string]interface{}, headers map[string]string) (*ParsedResponse, error)
	Patch(url string, body map[string]interface{}, query map[string]interface{}, headers map[string]string) (*ParsedResponse, error)
}

// Executor handles the execution of a DAG with parallel processing capabilities
type Executor struct {
	db         *Persist
	httpClient *Http
}

// NewExecutor creates a new DAG executor
func NewExecutor(db Persist, http Http) (*Executor, error) {
	return &Executor{
		db:         &db,
		httpClient: &http,
	}, nil
}

// Execute runs the DAG with parallel execution of steps
func (e *Executor) Execute(dag *DAG, input map[string]interface{}) (interface{}, error) {
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
		context: &Context{
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
	// Find the output step
	var outputStepID string
	for _, step := range dag.Steps {
		if step.Name == "output" {
			outputStepID = step.ID
			break
		}
	}

	if outputStepID == "" {
		return nil, fmt.Errorf("output step not found")
	}
	outputStep := execution.stepsMap[outputStepID]
	fmt.Println(execution.output)
	fmt.Println(outputStep.Schema)
	// Validate output against schema
	if err := validateSchema(outputStep.Schema, execution.output); err != nil {
		return nil, fmt.Errorf("output validation failed: %w", err)
	}

	return execution.output.(interface{}), nil
}

func (e *Executor) mapSteps(dag *DAG) (map[string]*Step, error) {
	steps := make(map[string]*Step)
	for _, step := range dag.Steps {
		if _, ok := steps[step.ID]; ok {
			return nil, fmt.Errorf("Duplicate step ID: %s", step.ID)
		}
		steps[step.ID] = &step
	}
	return steps, nil
}

func (e *Executor) independentSteps(dag *DAG) []string {
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

func ParseDAG(dagString []byte) (DAG, error) {
	var dag DAG
	err := json.Unmarshal([]byte(dagString), &dag)
	if err != nil {
		return DAG{}, err
	}
	return dag, nil
}

func validateSchema(schema Schema, data interface{}) error {
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
