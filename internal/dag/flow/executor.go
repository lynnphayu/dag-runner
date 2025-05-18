package flow

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/lynnphayu/swift/dagflow/internal/dag/domain"
	http "github.com/lynnphayu/swift/dagflow/internal/dag/repositories/http"
	postgres "github.com/lynnphayu/swift/dagflow/internal/dag/repositories/postgres"
	"github.com/xeipuuv/gojsonschema"

	utils "github.com/lynnphayu/swift/dagflow/pkg/utils"
)

// Executor handles the execution of a DAG with parallel processing capabilities
type Executor struct {
	db         *postgres.Postgres
	httpClient *http.Http
}

type Execution struct {
	dag      *domain.DAG
	stepsMap map[string]*domain.Step
	context  *domain.Context
	output   interface{}

	executor     *Executor
	wg           *sync.WaitGroup
	errorChannel chan error
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
		executor:     e,
		wg:           &sync.WaitGroup{},
		errorChannel: make(chan error, 1),
	}

	// Create wait group for tracking goroutines

	// Start execution from the entry step
	independentSteps := e.independentSteps(dag)
	for _, step := range independentSteps {
		execution.wg.Add(1)
		go execution.executeStepAsync(execution.stepsMap[step])
	}

	execution.wg.Wait()

	// Check for any errors
	select {
	case err := <-execution.errorChannel:
		return nil, err
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
		if !dependentSteps[step.ID] {
			independentSteps = append(independentSteps, step.ID)
		}
	}
	return independentSteps
}

// executeStepAsync executes a single step asynchronously and triggers dependent steps
func (e *Execution) executeStepAsync(step *domain.Step) {
	defer e.wg.Done()

	fmt.Println("Executing step:", step.ID)

	// Execute the step
	result, err := e.executeStep(step)
	fmt.Println("Step result:", step.ID, result)
	if err != nil {
		e.errorChannel <- fmt.Errorf("failed to execute step %s: %w", step.ID, err)
		return
	}

	// Store the result
	(*e.context.Results)[step.ID] = result
	if step.Output != "" {
		e.output = resolveV2[interface{}](step.Output, e.context)
	}

	for _, dep := range step.Then {
		e.wg.Add(1)

		go e.executeStepAsync(e.stepsMap[dep])
	}
}

// executeStep executes a single step
func (e *Execution) executeStep(step *domain.Step) (interface{}, error) {
	// Handle different step types
	switch step.Type {
	case "query":
		return e.executeQuery(step)
	case "insert":
		return e.executeInsert(step)
	// case "join":
	// 	return e.executeJoin(step)
	case "http":
		return e.executeHTTP(step)
	case "condition":
		return e.executeCondition(step)
	default:
		return nil, fmt.Errorf("unsupported step type: %s", step.Type)
	}
}

func eveluateCondition(left interface{}, right interface{}, operator domain.Operator, ctx *domain.Context) bool {
	if v, ok := left.(string); ok {
		resolvedLeft := resolveV2[interface{}](v, ctx)
		left = resolvedLeft
	} else if v, ok := left.(domain.Condition); ok {
		left = eveluateCondition(v.Left, v.Right, v.Operator, ctx)
	}

	if v, ok := right.(string); ok {
		resolvedRight := resolveV2[interface{}](v, ctx)
		right = resolvedRight
	} else if v, ok := right.(domain.Condition); ok {
		right = eveluateCondition(v.Left, v.Right, v.Operator, ctx)
	}
	// Convert left and right to the same type for comparison
	switch {
	case utils.IsNumeric(left) || utils.IsNumeric(right):
		// Convert both to float64 for numeric comparisons
		leftNum := utils.ToFloat64(left)
		rightNum := utils.ToFloat64(right)
		left = leftNum
		right = rightNum
	case utils.IsString(left) || utils.IsString(right):
		// Convert both to strings for string comparisons
		left = fmt.Sprintf("%v", left)
		right = fmt.Sprintf("%v", right)
	case utils.IsBool(left) || utils.IsBool(right):
		// Convert both to bools for boolean comparisons
		leftBool, leftOk := left.(bool)
		rightBool, rightOk := right.(bool)
		if !leftOk || !rightOk {
			return false
		}
		left = leftBool
		right = rightBool
	}
	switch operator {
	case domain.EQ:
		return left == right
	case domain.NE:
		return left != right
	case domain.GT:
		return left.(float64) > right.(float64)
	case domain.GTE:
		return left.(float64) >= right.(float64)
	case domain.LT:
		return left.(float64) < right.(float64)
	case domain.LTE:
		return left.(float64) <= right.(float64)
	case domain.IN:
		return contains(right.([]string), left.(string))
	case domain.NOTIN:
		return !contains(right.([]string), left.(string))
	case domain.AND:
		return left.(bool) && right.(bool)
	case domain.OR:
		return left.(bool) || right.(bool)
	default:
		return false
	}

}

func (e *Execution) executeCondition(step *domain.Step) (interface{}, error) {
	left := step.If.Left
	right := step.If.Right
	operator := step.Params.If.Operator

	result := eveluateCondition(left, right, operator, e.context)
	elseStep := step.Else
	if !result {
		for _, dep := range elseStep {
			e.wg.Add(1)
			go e.executeStepAsync(e.stepsMap[dep])
		}
	}
	return nil, nil
}

func (e *Execution) executeInsert(step *domain.Step) (interface{}, error) {
	query, args, err := postgres.BuildInsertQuery(step.Params.Table, step.Params.Map, e.context)
	if err != nil {
		return nil, fmt.Errorf("failed to build insert query: %w", err)
	}
	return e.executor.db.Insert(query, args...)
}

func (e *Execution) executeQuery(step *domain.Step) (interface{}, error) {
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(step.Params.Select, ", "), step.Params.Table)
	if len(step.Params.Where) > 0 {
		resolvedQuery := resolveValues(step.Params.Where, e.context)
		whereClause, args := postgres.BuildWhereClause(resolvedQuery.(map[string]interface{}))
		query += " WHERE " + whereClause
		return e.executor.db.Query(query, args...)
	}
	return e.executor.db.Query(query)
}

func (e *Execution) executeHTTP(step *domain.Step) (interface{}, error) {
	result, err := e.executor.httpClient.Execute(
		step.Params.Method,
		step.Params.URL,
		resolveValues(step.Params.Query, e.context).(map[string]interface{}),
		resolveValues(step.Params.Body, e.context).(map[string]interface{}),
		resolveValues(step.Params.Headers, e.context).(map[string]string),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to execute HTTP request: %w", err)
	}
	return result, nil
}

// func (e *Execution) executeJoin(step *domain.Step) (interface{}, error) {
// 	// Get input data
// 	var datasets [][]map[string]interface{}
// 	if len(step.DependsOn) != 2 {
// 		return nil, fmt.Errorf("join step requires exactly two dependent steps")
// 	}
// 	if step.Params.Left == "" {
// 		return nil, fmt.Errorf("join step requires left parameter")
// 	}
// 	if step.Params.Right == "" {
// 		return nil, fmt.Errorf("join step requires right parameter")
// 	}
// 	if !contains(strings.Split(step.DependsOn[0], "."), step.Params.Left) && !contains(strings.Split(step.DependsOn[0], "."), step.Params.Left) {
// 		return nil, fmt.Errorf("join step left parameter must be one of the dependent steps")
// 	}
// 	if !contains(strings.Split(step.DependsOn[0], "."), step.Params.Right) && !contains(strings.Split(step.DependsOn[0], "."), step.Params.Right) {
// 		return nil, fmt.Errorf("join step right parameter must be one of the dependent steps")
// 	}
// 	if step.Params.Left == step.Params.Right {
// 		return nil, fmt.Errorf("join step left and right parameters cannot be the same")
// 	}
// 	if (*e.context.Results)[step.DependsOn[0]] == nil {
// 		return nil, fmt.Errorf("join step left dependent step %s not found", step.DependsOn[0])
// 	}
// 	if (*e.context.Results)[step.DependsOn[1]] == nil {
// 		return nil, fmt.Errorf("join step right dependent step %s not found", step.DependsOn[1])
// 	}
// 	if v, ok := (*e.context.Results)[step.DependsOn[0]].([]map[string]interface{}); ok {
// 		datasets = append(datasets, v)
// 	} else {
// 		return nil, fmt.Errorf("join step left dependent step %s is not a slice", step.DependsOn[0])
// 	}
// 	if v, ok := (*e.context.Results)[step.DependsOn[1]].([]map[string]interface{}); ok {
// 		datasets = append(datasets, v)
// 	} else {
// 		return nil, fmt.Errorf("join step right dependent step %s is not a slice", step.DependsOn[1])
// 	}

// 	return performJoin(datasets, step.Params.On, step.Params.Type)
// }

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
