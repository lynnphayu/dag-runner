package dag

import (
	"context"
	"fmt"
	"log"
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
	Errors  map[string]error // Leaf node errors captured here
}

type ExecutionContext struct {
	successorsMap   map[string][]string
	predecessorsMap map[string][]string
	context         Context

	resultsLock  sync.RWMutex
	errorsLock   sync.RWMutex
	waitList     sync.Map
	executor     *Runner
	wg           sync.WaitGroup
	errorChannel chan ErrEvt
	// stepDone holds one channel per node; closed (not sent on) when the step
	// finishes — whether successfully or with an error. Closing broadcasts to
	// all goroutines waiting on the same dependency simultaneously.
	stepDone map[string]chan struct{}
	// ctx is used for early termination on non-leaf node failures
	ctx    context.Context
	cancel context.CancelFunc
}

func NewExecutionContext(input map[string]interface{}, executor *Runner) *ExecutionContext {
	stepDone := make(map[string]chan struct{}, len(executor.nodesDict))
	for id := range executor.nodesDict {
		stepDone[id] = make(chan struct{})
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &ExecutionContext{
		successorsMap:   executor.successorsMap,
		predecessorsMap: executor.predecessorsMap,
		context:         Context{Input: input, Results: map[string]interface{}{}, Errors: map[string]error{}},
		executor:        executor,
		errorChannel:    make(chan ErrEvt, executor.graphSize),
		stepDone:        stepDone,
		ctx:             ctx,
		cancel:          cancel,
	}
}

// failNonLeaf signals a non-leaf node failure - triggers immediate termination
func (e *ExecutionContext) failNonLeaf(stepID string, err error) {
	select {
	case e.errorChannel <- ErrEvt{StepID: stepID, Err: err}:
		e.cancel() // Cancel context to signal early termination
	default:
		// Error already sent, just cancel
		e.cancel()
	}
	close(e.stepDone[stepID])
}

// failLeaf captures a leaf node error in the Errors map
func (e *ExecutionContext) failLeaf(stepID string, err error) {
	e.errorsLock.Lock()
	e.context.Errors[stepID] = err
	e.errorsLock.Unlock()
	log.Printf("[step:%s] leaf node failed (captured in Errors): %v", stepID, err)
	close(e.stepDone[stepID])
}

// initExecution executes a single step asynchronously and triggers dependent steps.
func (e *ExecutionContext) initExecution(step *Node[*Action]) {
	defer e.wg.Done()

	// Check for early termination (non-leaf failure occurred)
	select {
	case <-e.ctx.Done():
		log.Printf("[step:%s] skipped: execution cancelled", step.ID)
		close(e.stepDone[step.ID])
		return
	default:
	}

	for _, dep := range e.predecessorsMap[step.ID] {
		// Block until the dependency finishes. Closing (not sending) means every
		// goroutine waiting on the same dep unblocks simultaneously.
		<-e.stepDone[dep]

		// Check for cancellation mid-wait
		select {
		case <-e.ctx.Done():
			log.Printf("[step:%s] skipped: execution cancelled during dependency wait", step.ID)
			close(e.stepDone[step.ID])
			return
		default:
		}

		// Presence in Results means success; absence means the dep failed.
		e.resultsLock.RLock()
		_, depSucceeded := e.context.Results[dep]
		e.resultsLock.RUnlock()

		if !depSucceeded {
			log.Printf("[step:%s] skipped: dependency %s failed", step.ID, dep)
			// Check if this is a leaf node - if so, capture error; otherwise propagate
			if step.IsLeaf() {
				e.failLeaf(step.ID, fmt.Errorf("dependency %s failed", dep))
			} else {
				e.failNonLeaf(step.ID, fmt.Errorf("dependency %s failed", dep))
			}
			return
		}
	}

	log.Printf("[step:%s] starting execution (type=%s)", step.ID, step.Data.Type)
	result, err := step.Data.Execute(e)
	if err != nil {
		log.Printf("[step:%s] failed: %v", step.ID, err)
		// Distinguish leaf vs non-leaf failures
		if step.IsLeaf() {
			e.failLeaf(step.ID, err)
		} else {
			e.failNonLeaf(step.ID, err)
		}
		return
	}

	log.Printf("[step:%s] completed successfully: %+v", step.ID, result)
	e.resultsLock.Lock()
	e.context.Results[step.ID] = result
	e.resultsLock.Unlock()
	close(e.stepDone[step.ID])

	// Don't spawn dependents if execution was cancelled
	select {
	case <-e.ctx.Done():
		return
	default:
	}

	for _, dep := range e.successorsMap[step.ID] {
		if _, alreadyQueued := e.waitList.LoadOrStore(dep, true); !alreadyQueued {
			e.wg.Add(1)
			go e.initExecution(e.executor.nodesDict[dep])
		}
	}
}

func evaluateCondition(left interface{}, right interface{}, operator Operator, ctx *Context) bool {
	if v, ok := left.(string); ok {
		left = ResolveV2[interface{}](v, nil, ctx)
	} else if v, ok := left.(Condition); ok {
		left = evaluateCondition(v.Left, v.Right, v.Operator, ctx)
	}

	if v, ok := right.(string); ok {
		right = ResolveV2[interface{}](v, nil, ctx)
	} else if v, ok := right.(Condition); ok {
		right = evaluateCondition(v.Left, v.Right, v.Operator, ctx)
	}

	switch {
	case utils.IsNumeric(left) || utils.IsNumeric(right):
		left = utils.ToFloat64(left)
		right = utils.ToFloat64(right)
	case utils.IsString(left) || utils.IsString(right):
		left = fmt.Sprintf("%v", left)
		right = fmt.Sprintf("%v", right)
	case utils.IsBool(left) || utils.IsBool(right):
		if _, ok := left.(bool); !ok {
			return false
		}
		if _, ok := right.(bool); !ok {
			return false
		}
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
