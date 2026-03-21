package dag

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"

	utils "github.com/lynnphayu/dag-runner/pkg/utils"
)

type ErrEvt struct {
	StepID string
	Err    error
}

type Context struct {
	Input   map[string]any
	Results map[string]any
	Errors  map[string]error
}

type StepStatus string

const (
	StepStatusPending StepStatus = "pending"
	StepStatusSuccess StepStatus = "success"
	StepStatusFailed  StepStatus = "failed"
	StepStatusSkipped StepStatus = "skipped"
)

type StepState struct {
	Status StepStatus
	Err    error
}

type ExecutionPolicy interface {
	IsCritical(stepID string) bool
}

type StructuralExecutionPolicy struct {
	executor *Runner
}

func NewStructuralExecutionPolicy(executor *Runner) ExecutionPolicy {
	return StructuralExecutionPolicy{executor: executor}
}

func (p StructuralExecutionPolicy) IsCritical(stepID string) bool {
	node, ok := p.executor.nodesDict[stepID]
	if !ok || node == nil {
		return true
	}
	return !node.IsLeaf()
}

type CriticalSetExecutionPolicy struct {
	critical map[string]struct{}
}

func NewCriticalSetExecutionPolicy(stepIDs []string) ExecutionPolicy {
	critical := make(map[string]struct{}, len(stepIDs))
	for _, id := range stepIDs {
		critical[id] = struct{}{}
	}
	return CriticalSetExecutionPolicy{critical: critical}
}

func (p CriticalSetExecutionPolicy) IsCritical(stepID string) bool {
	_, ok := p.critical[stepID]
	return ok
}

type ExecutionContext struct {
	successorsMap   map[string][]string
	predecessorsMap map[string][]string
	context         Context

	resultsLock sync.RWMutex
	errorsLock  sync.RWMutex
	stateLock   sync.RWMutex
	branchLock  sync.RWMutex

	waitList sync.Map
	executor *Runner
	wg       sync.WaitGroup

	errorChannel chan ErrEvt

	stepCompletion map[string]chan struct{}
	stepStates     map[string]StepState
	policy         ExecutionPolicy
	ctx            context.Context
	cancel         context.CancelFunc

	rootFailure sync.Once
}

func NewExecutionContext(
	parent context.Context,
	input map[string]interface{},
	executor *Runner,
	policy ExecutionPolicy,
) *ExecutionContext {
	stepCompletion := make(map[string]chan struct{}, len(executor.nodesDict))
	stepStates := make(map[string]StepState, len(executor.nodesDict))
	for id := range executor.nodesDict {
		stepCompletion[id] = make(chan struct{})
		stepStates[id] = StepState{Status: StepStatusPending}
	}

	if parent == nil {
		parent = context.Background()
	}

	if policy == nil {
		policy = NewStructuralExecutionPolicy(executor)
	}

	ctx, cancel := context.WithCancel(parent)

	successorsMap := make(map[string][]string, len(executor.successorsMap))
	for stepID, successors := range executor.successorsMap {
		successorsMap[stepID] = append([]string(nil), successors...)
	}

	predecessorsMap := make(map[string][]string, len(executor.predecessorsMap))
	for stepID, predecessors := range executor.predecessorsMap {
		predecessorsMap[stepID] = append([]string(nil), predecessors...)
	}

	return &ExecutionContext{
		successorsMap:   successorsMap,
		predecessorsMap: predecessorsMap,
		context: Context{
			Input:   input,
			Results: map[string]any{},
			Errors:  map[string]error{},
		},
		executor:       executor,
		errorChannel:   make(chan ErrEvt, 1),
		stepCompletion: stepCompletion,
		stepStates:     stepStates,
		policy:         policy,
		ctx:            ctx,
		cancel:         cancel,
	}
}

func (e *ExecutionContext) completeStep(stepID string, status StepStatus, err error) {
	e.stateLock.Lock()
	e.stepStates[stepID] = StepState{
		Status: status,
		Err:    err,
	}
	doneCh := e.stepCompletion[stepID]
	e.stateLock.Unlock()

	close(doneCh)
}

func (e *ExecutionContext) stepState(stepID string) StepState {
	e.stateLock.RLock()
	defer e.stateLock.RUnlock()
	return e.stepStates[stepID]
}

func (e *ExecutionContext) setSuccessors(stepID string, successors []string) {
	e.branchLock.Lock()
	e.successorsMap[stepID] = append([]string(nil), successors...)
	e.branchLock.Unlock()
}

func (e *ExecutionContext) successors(stepID string) []string {
	e.branchLock.RLock()
	defer e.branchLock.RUnlock()
	return append([]string(nil), e.successorsMap[stepID]...)
}

func (e *ExecutionContext) isCritical(stepID string) bool {
	if e.policy == nil {
		return true
	}
	return e.policy.IsCritical(stepID)
}

func (e *ExecutionContext) failCritical(stepID string, err error) {
	e.rootFailure.Do(func() {
		e.errorsLock.Lock()
		e.context.Errors[stepID] = err
		e.errorsLock.Unlock()

		e.completeStep(stepID, StepStatusFailed, err)

		select {
		case e.errorChannel <- ErrEvt{StepID: stepID, Err: err}:
		default:
		}

		e.cancel()
	})
}

func (e *ExecutionContext) failNonCritical(stepID string, err error) {
	e.errorsLock.Lock()
	e.context.Errors[stepID] = err
	e.errorsLock.Unlock()

	slog.Warn("non-critical step failed", "step_id", stepID, "error", err)
	e.completeStep(stepID, StepStatusFailed, err)
}

func (e *ExecutionContext) handleFailure(stepID string, err error) {
	if e.isCritical(stepID) {
		e.failCritical(stepID, err)
		return
	}
	e.failNonCritical(stepID, err)
}

func (e *ExecutionContext) skipStep(stepID string, reason error) {
	slog.Info("step skipped", "step_id", stepID, "reason", reason)
	e.completeStep(stepID, StepStatusSkipped, reason)
}

func (e *ExecutionContext) dependencyFailure(dep string) error {
	depState := e.stepState(dep)
	if depState.Err != nil {
		return fmt.Errorf("dependency %s failed: %w", dep, depState.Err)
	}
	return fmt.Errorf("dependency %s did not complete successfully", dep)
}

func (e *ExecutionContext) initExecution(step *Node[*Action]) {
	defer e.wg.Done()

	select {
	case <-e.ctx.Done():
		e.skipStep(step.ID, e.ctx.Err())
		return
	default:
	}

	for _, dep := range e.predecessorsMap[step.ID] {
		<-e.stepCompletion[dep]

		depState := e.stepState(dep)
		if depState.Status != StepStatusSuccess {
			e.handleFailure(step.ID, e.dependencyFailure(dep))
			return
		}

		select {
		case <-e.ctx.Done():
			e.skipStep(step.ID, e.ctx.Err())
			return
		default:
		}
	}

	slog.Info("starting step execution", "step_id", step.ID, "step_type", step.Data.Type)
	result, err := step.Data.Execute(e)
	if err != nil {
		slog.Error("step execution failed", "step_id", step.ID, "error", err)
		e.handleFailure(step.ID, err)
		return
	}

	slog.Info("step execution completed", "step_id", step.ID, "result", result)

	e.resultsLock.Lock()
	e.context.Results[step.ID] = result
	e.resultsLock.Unlock()

	e.completeStep(step.ID, StepStatusSuccess, nil)

	select {
	case <-e.ctx.Done():
		return
	default:
	}

	for _, dep := range e.successors(step.ID) {
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
