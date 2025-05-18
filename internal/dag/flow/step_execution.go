package flow

import (
	"fmt"
	"sync"

	"github.com/lynnphayu/dag-runner/internal/dag/domain"
	utils "github.com/lynnphayu/dag-runner/pkg/utils"
)

type ErrEvt struct {
	StepID string
	Err    error
}

type Context struct {
	Input   *map[string]interface{}
	Results *map[string]interface{}
}

type Execution struct {
	dag      *domain.DAG
	stepsMap map[string]*domain.Step
	context  *Context
	output   interface{}

	waitList          *sync.Map
	executor          *Executor
	wg                *sync.WaitGroup
	errorChannel      chan ErrEvt
	completionChannel chan string
}

// initExecution executes a single step asynchronously and triggers dependent steps
func (e *Execution) initExecution(step *domain.Step) {
	defer e.wg.Done()

	for _, dep := range step.DependsOn {
		if _, ok := (*e.context.Results)[dep]; !ok {
			// wait for dependent steps to complete from completion channel
			for stepId := range e.completionChannel {
				if dep == stepId {
					break
				}
			}
		}
	}

	fmt.Println("Executing step:", step.ID)
	result, err := e.executeStep(step)
	fmt.Println("Step result:", step.ID, result)

	if err != nil {
		e.errorChannel <- ErrEvt{
			StepID: step.ID,
			Err:    err,
		}
		return
	}

	// Store the result
	(*e.context.Results)[step.ID] = result
	e.completionChannel <- step.ID
	if step.Output != nil && step.Output != "" {
		e.output = resolveValues(step.Output, e.context)
	}

	for _, dep := range step.Then {
		if _, ok := e.waitList.Load(dep); !ok {
			e.waitList.Store(dep, true)
			e.wg.Add(1)
			go e.initExecution(e.stepsMap[dep])
		}

	}
}

// executeStep executes a single step
func (e *Execution) executeStep(step *domain.Step) (interface{}, error) {
	// Handle different step types
	switch step.Type {
	case domain.Query:
		return e.executeQuery(step)
	case domain.Insert:
		return e.executeInsert(step)
	case domain.Update:
		return e.executeUpdate(step)
	case domain.Delete:
		return e.executeDelete(step)
	case domain.Join:
		return e.executeJoin(step)
	case domain.HTTP:
		return e.executeHTTP(step)
	case domain.Cond:
		return e.executeCondition(step)
	case domain.Filter:
		return e.executeFilter(step)
	default:
		return nil, fmt.Errorf("unsupported step type: %s", step.Type)
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
			go e.initExecution(e.stepsMap[dep])
		}
	}
	return nil, nil
}

func (e *Execution) executeInsert(step *domain.Step) (interface{}, error) {
	data := resolveValues(step.Params.Filter, e.context).(map[string]interface{})
	return (*e.executor.db).Create(step.Params.Table, data)
}

func (e *Execution) executeQuery(step *domain.Step) ([]interface{}, error) {
	where := resolveValues(step.Params.Where, e.context).(map[string]interface{})
	return (*e.executor.db).Retrieve(step.Params.Table, step.Params.Select, where)
}

func (e *Execution) executeUpdate(step *domain.Step) (interface{}, error) {
	data := resolveValues(step.Params.Filter, e.context).(map[string]interface{})
	where := resolveValues(step.Params.Where, e.context).(map[string]interface{})
	return (*e.executor.db).Update(step.Params.Table, data, where)
}

func (e *Execution) executeDelete(step *domain.Step) (interface{}, error) {
	where := resolveValues(step.Params.Where, e.context).(map[string]interface{})
	return (*e.executor.db).Delete(step.Params.Table, where)
}

func (e *Execution) executeHTTP(step *domain.Step) (interface{}, error) {

	query := resolveValues(step.Params.Query, e.context).(map[string]interface{})
	body := resolveValues(step.Params.Body, e.context).(map[string]interface{})
	headers := resolveValues(step.Params.Headers, e.context).(map[string]string)
	url := resolveV2[string](step.Params.URL, e.context)
	switch step.Params.Method {
	case domain.GET:
		return (*e.executor.httpClient).Get(url, query, headers)
	case domain.POST:
		return (*e.executor.httpClient).Post(url, query, body, headers)
	case domain.PUT:
		return (*e.executor.httpClient).Put(url, body, query, headers)
	case domain.DELETE:
		return (*e.executor.httpClient).Delete(url, query, headers)
	case domain.PATCH:
		return (*e.executor.httpClient).Patch(url, body, query, headers)
	default:
		return nil, fmt.Errorf("unsupported HTTP method: %s", step.Params.Method)

	}
}

func (e *Execution) executeJoin(step *domain.Step) (interface{}, error) {
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

	if v, ok := (*e.context.Results)[step.Params.Left]; ok {
		datasets = append(datasets, v.([]map[string]interface{}))
	} else {
		return nil, fmt.Errorf("join step left dependent step %s is not a slice", step.DependsOn[0])
	}
	if v, ok := (*e.context.Results)[step.Params.Right]; ok {
		datasets = append(datasets, v.([]map[string]interface{}))
	} else {
		return nil, fmt.Errorf("join step right dependent step %s is not a slice", step.DependsOn[1])
	}

	return performJoin(datasets, step.Params.On, step.Params.Type)
}

func (e *Execution) executeFilter(step *domain.Step) (interface{}, error) {
	// Get input data
	var dataset []interface{}
	if len(step.DependsOn) != 1 {
		return nil, fmt.Errorf("filter step requires exactly one dependent step")
	}
	if v, ok := (*e.context.Results)[step.DependsOn[0]]; ok {
		dataset = v.([]interface{})
	} else {
		return nil, fmt.Errorf("filter step dependent step %s is not a slice", step.DependsOn[0])
	}
	return applyFilter(dataset, step.Params.Filter)
}

func eveluateCondition(left interface{}, right interface{}, operator domain.Operator, ctx *Context) bool {
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

func contains(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}
