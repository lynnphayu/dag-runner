package dag

import (
	"encoding/json"
	"fmt"
)

type ActionInterface interface {
	Validate(execCtx *ExecutionContext, action *Action) error
	Execute(execCtx *ExecutionContext, action *Action) (interface{}, error)
}

type Action struct {
	Type ActionType             `json:"type"  bson:"type"`
	Meta map[string]interface{} `bson:"meta" json:"meta"`
	node *Node[*Action]
}

func (a *Action) SetBackRef(node *Node[*Action]) {
	a.node = node
}

type ActionType string

const (
	Type_Query  ActionType = "query"
	Type_Insert ActionType = "insert"
	Type_Update ActionType = "update"
	Type_Delete ActionType = "delete"
	Type_Join   ActionType = "join"
	Type_Filter ActionType = "filter"
	Type_Map    ActionType = "map"
	Type_Cond   ActionType = "cond"
	Type_HTTP   ActionType = "http"
)

type JoinType string

const (
	InnerJoin JoinType = "inner"
	LeftJoin  JoinType = "left"
	RightJoin JoinType = "right"
)

type HTTPMethod string

const (
	GET    HTTPMethod = "GET"
	POST   HTTPMethod = "POST"
	PUT    HTTPMethod = "PUT"
	DELETE HTTPMethod = "DELETE"
	PATCH  HTTPMethod = "PATCH"
)

type Query struct {
	Table  string                 `json:"table"  bson:"table"`
	Select []string               `json:"select" bson:"select"`
	Where  map[string]interface{} `json:"where"  bson:"where"`
}

func (q *Query) Execute(execCtx *ExecutionContext, action *Action) (interface{}, error) {
	resolvedWhere := ResolveValues(q.Where, nil, &execCtx.context).(map[string]interface{})
	result, err := (*execCtx.executor.db).Retrieve(q.Table, q.Select, resolvedWhere)
	return result, err
}

func (q *Query) Validate(execCtx *ExecutionContext, action *Action) error {
	return nil
}

type Insert struct {
	Table string                 `json:"table" bson:"table"`
	Map   map[string]interface{} `json:"map"   bson:"map"`
}

func (i *Insert) Execute(execCtx *ExecutionContext, action *Action) (interface{}, error) {
	resolvedMap := ResolveValues(i.Map, nil, &execCtx.context).(map[string]interface{})
	return (*execCtx.executor.db).Create(i.Table, resolvedMap)
}

func (i *Insert) Validate(execCtx *ExecutionContext, action *Action) error {
	return nil
}

type Update struct {
	Table string                 `json:"table" bson:"table"`
	Set   map[string]interface{} `json:"set"   bson:"set"`
	Where map[string]interface{} `json:"where" bson:"where"`
}

func (u *Update) Execute(execCtx *ExecutionContext, action *Action) (interface{}, error) {
	resolvedSet := ResolveValues(u.Set, nil, &execCtx.context).(map[string]interface{})
	resolvedWhere := ResolveValues(u.Where, nil, &execCtx.context).(map[string]interface{})
	return (*execCtx.executor.db).Update(u.Table, resolvedSet, resolvedWhere)
}

func (u *Update) Validate(execCtx *ExecutionContext, action *Action) error {
	return nil
}

type Delete struct {
	Table string                 `json:"table" bson:"table"`
	Where map[string]interface{} `json:"where" bson:"where"`
}

func (d *Delete) Execute(execCtx *ExecutionContext, action *Action) (interface{}, error) {
	resolvedWhere := ResolveValues(d.Where, nil, &execCtx.context).(map[string]interface{})
	return (*execCtx.executor.db).Delete(d.Table, resolvedWhere)
}

func (d *Delete) Validate(execCtx *ExecutionContext, action *Action) error {
	return nil
}

type Join struct {
	On    map[string]string `json:"on"    bson:"on"`
	Type  JoinType          `json:"type"  bson:"type"`
	Left  string            `json:"left"  bson:"left"`
	Right string            `json:"right" bson:"right"`
}

func (j *Join) Execute(execCtx *ExecutionContext, action *Action) (interface{}, error) {
	var datasets [][]map[string]interface{}
	left := ResolveValues(j.Left, nil, &execCtx.context).([]map[string]interface{})
	right := ResolveValues(j.Right, nil, &execCtx.context).([]map[string]interface{})
	datasets = append(datasets, left)
	datasets = append(datasets, right)
	return performJoin(datasets, j.On, j.Type)
}

func (j *Join) Validate(execCtx *ExecutionContext, action *Action) error {
	dependencies := action.node.Dependencies
	if len(dependencies) != 2 {
		return fmt.Errorf("join action requires exactly two dependent steps")
	}
	if j.Left == "" {
		return fmt.Errorf("join action requires left parameter")
	}
	if j.Right == "" {
		return fmt.Errorf("join action requires right parameter")
	}
	return nil
}

type Filter struct {
	On     map[string]string      `json:"on"     bson:"on"`
	Filter map[string]interface{} `json:"filter" bson:"filter"`
}

func (f *Filter) Execute(execCtx *ExecutionContext, action *Action) (interface{}, error) {
	if on, ok := ResolveValues(f.On, nil, &execCtx.context).([]interface{}); ok {
		return applyFilter(on, f.Filter)
	} else {
		return nil, fmt.Errorf("filter action requires an array of maps as input")
	}
}

func (f *Filter) Validate(execCtx *ExecutionContext, action *Action) error {
	if len(action.node.Dependencies) != 1 {
		return fmt.Errorf("filter step requires exactly one dependent step")
	}
	return nil
}

type Map struct {
	On     map[string]string      `json:"on"     bson:"on"`
	Mapper map[string]interface{} `json:"mapper" bson:"mapper"`
}

func (m *Map) Execute(execCtx *ExecutionContext, action *Action) (interface{}, error) {
	if on, ok := ResolveValues(m.On, nil, &execCtx.context).([]interface{}); ok {
		return applyMap(on, m.Mapper)
	}
	return nil, fmt.Errorf("map action requires an array of maps as input")
}

func (m *Map) Validate(execCtx *ExecutionContext, action *Action) error {
	return nil
}

type Operator string

const (
	EQ    Operator = "eq"
	NE    Operator = "ne"
	GT    Operator = "gt"
	GTE   Operator = "gte"
	LT    Operator = "lt"
	LTE   Operator = "lte"
	IN    Operator = "in"
	NOTIN Operator = "notin"
	AND   Operator = "and"
	OR    Operator = "or"
)

type ConditionOperand interface {
	string | *Condition
}

type Condition struct {
	Operator Operator `json:"operator" bson:"operator"`
	Left     string   `json:"left"     bson:"left"`
	Right    string   `json:"right"    bson:"right"`
	If       []string `json:"if"       bson:"if"`
	Else     []string `json:"else"     bson:"else"`
}

func (c *Condition) Execute(execCtx *ExecutionContext, action *Action) (interface{}, error) {
	result := eveluateCondition(
		c.Left,
		c.Right,
		c.Operator,
		&execCtx.context,
	)
	if result {
		execCtx.successorsMap[action.node.ID] = c.If
	} else {
		execCtx.successorsMap[action.node.ID] = c.Else
	}
	return result, nil
}

func (c *Condition) Validate(execCtx *ExecutionContext, action *Action) error {
	return nil
}

type HTTP struct {
	Method  HTTPMethod             `json:"method"            bson:"method"`
	URL     string                 `json:"url"               bson:"url"`
	Headers map[string]string      `json:"headers,omitempty" bson:"headers,omitempty"`
	Body    map[string]interface{} `json:"body,omitempty"    bson:"body,omitempty"`
	Query   map[string]interface{} `json:"query,omitempty"   bson:"query,omitempty"`
}

func (h *HTTP) Execute(execCtx *ExecutionContext, action *Action) (interface{}, error) {
	query := ResolveValues(h.Query, nil, &execCtx.context).(map[string]interface{})
	body := ResolveValues(h.Body, nil, &execCtx.context).(map[string]interface{})
	headers := ResolveValues(h.Headers, nil, &execCtx.context).(map[string]string)
	url := ResolveV2[string](h.URL, nil, &execCtx.context)
	switch h.Method {
	case GET:
		return (*execCtx.executor.httpClient).Get(url, query, headers)
	case POST:
		return (*execCtx.executor.httpClient).Post(url, query, body, headers)
	case PUT:
		return (*execCtx.executor.httpClient).Put(url, body, query, headers)
	case DELETE:
		return (*execCtx.executor.httpClient).Delete(url, query, headers)
	case PATCH:
		return (*execCtx.executor.httpClient).Patch(url, body, query, headers)
	default:
		return nil, fmt.Errorf("unsupported HTTP method: %s", h.Method)
	}
}

func (h *HTTP) Validate(execCtx *ExecutionContext, action *Action) error {
	return nil
}

var actionRegistry = map[ActionType]func() ActionInterface{
	Type_Query:  func() ActionInterface { return &Query{} },
	Type_Insert: func() ActionInterface { return &Insert{} },
	Type_Update: func() ActionInterface { return &Update{} },
	Type_Delete: func() ActionInterface { return &Delete{} },
	Type_Join:   func() ActionInterface { return &Join{} },
	Type_Filter: func() ActionInterface { return &Filter{} },
	Type_Map:    func() ActionInterface { return &Map{} },
	Type_Cond:   func() ActionInterface { return &Condition{} },
	Type_HTTP:   func() ActionInterface { return &HTTP{} },
}

func (a *Action) Execute(c *ExecutionContext) (interface{}, error) {
	rawMeta, err := json.Marshal(a.Meta)
	var internal ActionInterface
	if err != nil {
		return nil, fmt.Errorf("failed to marshal meta: %v", err)
	}
	if fn, ok := actionRegistry[a.Type]; ok {
		internal = fn()
		if err := json.Unmarshal(rawMeta, internal); err != nil {
			return nil, fmt.Errorf("failed to unmarshal meta: %v", err)
		}
	}
	if internal == nil {
		return nil, fmt.Errorf("unknown action type: %s", a.Type)
	}
	return internal.Execute(c, a)
}
