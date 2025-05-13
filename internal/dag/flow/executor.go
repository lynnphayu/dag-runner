package flow

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/lynnphayu/swift/dagflow/internal/dag/db"
	"github.com/lynnphayu/swift/dagflow/internal/dag/domain"
	"github.com/xeipuuv/gojsonschema"
)

// Executor handles the execution of a DAG with parallel processing capabilities
type Executor struct {
	repo *db.Repository
	// Track step results - no lock needed as step IDs are unique
	results map[string]interface{}
	context *domain.Context
	// Error channel for collecting errors from goroutines
	errChan        chan error
	completionChan chan string
}

// NewExecutor creates a new DAG executor
func NewExecutor(connStr string) (*Executor, error) {
	repo, err := db.NewRepository(connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to create repository: %w", err)
	}
	return &Executor{
		repo:           repo,
		results:        make(map[string]interface{}),
		completionChan: make(chan string, 1),
		errChan:        make(chan error, 100),
	}, nil
}

// Execute runs the DAG with parallel execution of steps
func (e *Executor) Execute(dag *domain.DAG, input map[string]interface{}) (interface{}, error) {
	// Validate input against schema
	fmt.Println("Validating input...")
	fmt.Println("DAG:", dag)
	if err := validateSchema(dag.InputSchema, input); err != nil {
		return nil, fmt.Errorf("input validation failed: %w", err)
	}

	// Create wait group for tracking goroutines
	var wg sync.WaitGroup

	// Start execution from the entry step
	var Entry domain.Step
	for _, step := range dag.Steps {
		if step.ID == dag.Entry {
			Entry = step
			break
		}
	}
	if Entry.ID == "" {
		return nil, fmt.Errorf("entry step '%s' not found", dag.Entry)
	}

	// Execute entry step and start recursive execution
	e.results["input"] = input
	e.context = &domain.Context{
		Results: &e.results,
		Input:   &input,
	}
	for _, dep := range dag.Steps {
		wg.Add(1)
		go e.executeStepAsync(&dep, &wg, dag)
	}

	// Wait for all steps to complete
	wg.Wait()
	// close(e.completionChan)
	fmt.Println("All steps completed")

	// Check for any errors
	select {
	case err := <-e.errChan:
		return nil, err
	default:
		// No errors occurred
	}

	// Get the final step result
	result, ok := e.results[dag.Result]
	if !ok {
		return nil, fmt.Errorf("final step result not found")
	}

	fmt.Println("Before validation :", result)
	// Validate output against schema
	if err := validateSchema(dag.OutputSchema, result); err != nil {

		return nil, fmt.Errorf("output validation failed: %w", err)
	}
	fmt.Println("Validation passed :", result)

	return result.(interface{}), nil
}

// executeStepAsync executes a single step asynchronously and triggers dependent steps
func (e *Executor) executeStepAsync(step *domain.Step, wg *sync.WaitGroup, dag *domain.DAG) {
	defer wg.Done()

	fmt.Println("Executing step:", step.ID)
	// Check if all dependencies are completed
	if len(step.DependsOn) > 0 {
		for _, depID := range step.DependsOn {
			if depID == "input" {
				continue
			}
			if _, completed := e.results[depID]; !completed {
				// Wait for dependency to complete
				for completedStep := range e.completionChan {
					if completedStep == depID {
						break
					}
				}
			}
		}
	}

	// Execute the step
	result, err := e.executeStep(step)
	fmt.Println("Step result:", step.ID, result)
	if err != nil {
		e.errChan <- fmt.Errorf("failed to execute step %s: %w", step.ID, err)
		return
	}

	// Store the result
	e.results[step.ID] = result
	for _, dep := range dag.Steps {
		if contains(dep.DependsOn, step.ID) {
			e.completionChan <- step.ID
			break
		}
	}
}

// executeStep executes a single step
func (e *Executor) executeStep(step *domain.Step) (interface{}, error) {
	// Handle different step types
	switch step.Type {
	case "query":
		return e.executeQuery(step)
	case "join":
		return e.executeJoin(step)
	default:
		return nil, fmt.Errorf("unsupported step type: %s", step.Type)
	}
}

func (e *Executor) executeQuery(step *domain.Step) (interface{}, error) {
	// Build query
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(step.Params.Select, ", "), step.Params.Table)
	if len(step.Params.Where) > 0 {
		whereClause, args := db.BuildWhereClause(step.Params.Where, e.context)
		query += " WHERE " + whereClause

		fmt.Println("Query:", query)
		fmt.Println("Args:", args)
		// Execute query using repository
		return e.repo.ExecuteQuery(query, args...)
	}

	// Execute query without where clause
	return e.repo.ExecuteQuery(query)
}

func (e *Executor) executeJoin(step *domain.Step) (interface{}, error) {
	// Get input data
	var datasets [][]map[string]interface{}
	if len(step.DependsOn) != 2 {
		return nil, fmt.Errorf("join step requires exactly two dependent steps")
	}
	if step.Params.Left == "" {
		return nil, fmt.Errorf("join step requires left parameter")
	}
	if step.Params.Right == "" {
		return nil, fmt.Errorf("join step requires right parameter")
	}
	if !contains(strings.Split(step.DependsOn[0], "."), step.Params.Left) && !contains(strings.Split(step.DependsOn[0], "."), step.Params.Left) {
		return nil, fmt.Errorf("join step left parameter must be one of the dependent steps")
	}
	if !contains(strings.Split(step.DependsOn[0], "."), step.Params.Right) && !contains(strings.Split(step.DependsOn[0], "."), step.Params.Right) {
		return nil, fmt.Errorf("join step right parameter must be one of the dependent steps")
	}
	if step.Params.Left == step.Params.Right {
		return nil, fmt.Errorf("join step left and right parameters cannot be the same")
	}
	if (*e.context.Results)[step.DependsOn[0]] == nil {
		return nil, fmt.Errorf("join step left dependent step %s not found", step.DependsOn[0])
	}
	if (*e.context.Results)[step.DependsOn[1]] == nil {
		return nil, fmt.Errorf("join step right dependent step %s not found", step.DependsOn[1])
	}
	if v, ok := (*e.context.Results)[step.DependsOn[0]].([]map[string]interface{}); ok {
		datasets = append(datasets, v)
	} else {
		return nil, fmt.Errorf("join step left dependent step %s is not a slice", step.DependsOn[0])
	}
	if v, ok := (*e.context.Results)[step.DependsOn[1]].([]map[string]interface{}); ok {
		datasets = append(datasets, v)
	} else {
		return nil, fmt.Errorf("join step right dependent step %s is not a slice", step.DependsOn[1])
	}

	return performJoin(datasets, step.Params.On, step.Params.Type)
}

func contains(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
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
	fmt.Println("Validating schema...", schema, data)

	schemaLoader := gojsonschema.NewGoLoader(schema)
	dataLoader := gojsonschema.NewGoLoader(data)

	fmt.Println("loaded:", schemaLoader, dataLoader)
	result, err := gojsonschema.Validate(schemaLoader, dataLoader)
	if err != nil {
		return err
	}

	if !result.Valid() {
		return fmt.Errorf("validation errors: %v", result.Errors())
	}

	return nil
}
