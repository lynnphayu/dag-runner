package dag

import (
	"fmt"
	"sync"

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
	dag      *DAG
	stepsMap map[string]*Step
	context  *Context
	output   interface{}

	waitList          *sync.Map
	executor          *Executor
	wg                *sync.WaitGroup
	errorChannel      chan ErrEvt
	completionChannel chan string
}

// initExecution executes a single step asynchronously and triggers dependent steps
func (e *Execution) initExecution(step *Step) {
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
func (e *Execution) executeStep(step *Step) (interface{}, error) {
	// Handle different step types
	switch step.Type {
	case Query:
		return e.executeQuery(step)
	case Insert:
		return e.executeInsert(step)
	case Update:
		return e.executeUpdate(step)
	case Delete:
		return e.executeDelete(step)
	case Join:
		return e.executeJoin(step)
	case HTTP:
		return e.executeHTTP(step)
	case Cond:
		return e.executeCondition(step)
	case Filter:
		return e.executeFilter(step)
	default:
		return nil, fmt.Errorf("unsupported step type: %s", step.Type)
	}
}

func (e *Execution) executeCondition(step *Step) (interface{}, error) {
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

func (e *Execution) executeInsert(step *Step) (interface{}, error) {
	data := resolveValues(step.Params.Map, e.context).(map[string]interface{})
	return (*e.executor.db).Create(step.Params.Table, data)
}

func (e *Execution) executeQuery(step *Step) ([]interface{}, error) {
	where := resolveValues(step.Params.Where, e.context).(map[string]interface{})
	return (*e.executor.db).Retrieve(step.Params.Table, step.Params.Select, where)
}

func (e *Execution) executeUpdate(step *Step) (interface{}, error) {
	data := resolveValues(step.Params.Filter, e.context).(map[string]interface{})
	where := resolveValues(step.Params.Where, e.context).(map[string]interface{})
	return (*e.executor.db).Update(step.Params.Table, data, where)
}

func (e *Execution) executeDelete(step *Step) (interface{}, error) {
	where := resolveValues(step.Params.Where, e.context).(map[string]interface{})
	return (*e.executor.db).Delete(step.Params.Table, where)
}

func (e *Execution) executeHTTP(step *Step) (interface{}, error) {

	query := resolveValues(step.Params.Query, e.context).(map[string]interface{})
	body := resolveValues(step.Params.Body, e.context).(map[string]interface{})
	headers := resolveValues(step.Params.Headers, e.context).(map[string]string)
	url := resolveV2[string](step.Params.URL, e.context)
	switch step.Params.Method {
	case GET:
		return (*e.executor.httpClient).Get(url, query, headers)
	case POST:
		return (*e.executor.httpClient).Post(url, query, body, headers)
	case PUT:
		return (*e.executor.httpClient).Put(url, body, query, headers)
	case DELETE:
		return (*e.executor.httpClient).Delete(url, query, headers)
	case PATCH:
		return (*e.executor.httpClient).Patch(url, body, query, headers)
	default:
		return nil, fmt.Errorf("unsupported HTTP method: %s", step.Params.Method)

	}
}

func (e *Execution) executeJoin(step *Step) (interface{}, error) {
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

func (e *Execution) executeFilter(step *Step) (interface{}, error) {
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

func eveluateCondition(left interface{}, right interface{}, operator Operator, ctx *Context) bool {
	if v, ok := left.(string); ok {
		resolvedLeft := resolveV2[interface{}](v, ctx)
		left = resolvedLeft
	} else if v, ok := left.(Condition); ok {
		left = eveluateCondition(v.Left, v.Right, v.Operator, ctx)
	}

	if v, ok := right.(string); ok {
		resolvedRight := resolveV2[interface{}](v, ctx)
		right = resolvedRight
	} else if v, ok := right.(Condition); ok {
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
	case EQ:
		return left == right
	case NE:
		return left != right
	case GT:
		return left.(float64) > right.(float64)
	case GTE:
		return left.(float64) >= right.(float64)
	case LT:
		return left.(float64) < right.(float64)
	case LTE:
		return left.(float64) <= right.(float64)
	case IN:
		return contains(right.([]string), left.(string))
	case NOTIN:
		return !contains(right.([]string), left.(string))
	case AND:
		return left.(bool) && right.(bool)
	case OR:
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
