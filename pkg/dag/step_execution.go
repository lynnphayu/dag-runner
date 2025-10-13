package dag

import (
	"fmt"
	"slices"
	"sync"

	utils "github.com/lynnphayu/dag-runner/pkg/utils"
)

type ErrEvt struct {
	StepID string
	Err    error
}

type Context struct {
	Input   map[string]interface{}
	Results map[string]interface{}
}

type ExecutionContext struct {
	successorsMap   map[string][]string
	predecessorsMap map[string][]string
	context         Context
	output          interface{}

	waitList          sync.Map
	executor          *Runner
	wg                sync.WaitGroup
	errorChannel      chan ErrEvt
	completionChannel chan string
}

func NewExecutionContext(
	input map[string]interface{},
	executor *Runner,
) *ExecutionContext {
	context := Context{
		Results: map[string]interface{}{},
		Input:   input,
	}
	successorsMap := executor.successorsMap
	predecessorsMap := executor.predecessorsMap
	graphSize := executor.graphSize

	return &ExecutionContext{
		successorsMap:     successorsMap,
		predecessorsMap:   predecessorsMap,
		context:           context,
		executor:          executor,
		waitList:          sync.Map{},
		wg:                sync.WaitGroup{},
		errorChannel:      make(chan ErrEvt, graphSize),
		completionChannel: make(chan string, graphSize),
	}
}

// initExecution executes a single step asynchronously and triggers dependent steps
func (e *ExecutionContext) initExecution(step *Node[*Action]) {
	defer e.wg.Done()

	for _, dep := range e.predecessorsMap[step.ID] {
		if _, ok := (e.context.Results)[dep]; !ok {
			// wait for dependent steps to complete from completion channel
			for stepId := range e.completionChannel {
				if dep == stepId {
					break
				}
			}
		}
	}

	result, err := step.Data.Execute(e)
	if err != nil {
		e.errorChannel <- ErrEvt{
			StepID: step.ID,
			Err:    err,
		}
		return
	}

	(e.context.Results)[step.ID] = result
	e.completionChannel <- step.ID

	for _, dep := range e.successorsMap[step.ID] {
		if _, ok := e.waitList.Load(dep); !ok {
			e.waitList.Store(dep, true)
			e.wg.Add(1)
			go e.initExecution(e.executor.nodesDict[dep])
		}
	}
}

func eveluateCondition(left interface{}, right interface{}, operator Operator, ctx *Context) bool {
	if v, ok := left.(string); ok {
		resolvedLeft := ResolveV2[interface{}](v, nil, ctx)
		left = resolvedLeft
	} else if v, ok := left.(Condition); ok {
		left = eveluateCondition(v.Left, v.Right, v.Operator, ctx)
	}

	if v, ok := right.(string); ok {
		resolvedRight := ResolveV2[interface{}](v, nil, ctx)
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
		return slices.Contains(right.([]string), left.(string))
	case NOTIN:
		return !slices.Contains(right.([]string), left.(string))
	case AND:
		return left.(bool) && right.(bool)
	case OR:
		return left.(bool) || right.(bool)
	default:
		return false
	}
}
