package domain

type Context struct {
	Input   *map[string]interface{}
	Results *map[string]interface{}
}

// DAG represents a directed acyclic graph of processing steps
type DAG struct {
	ID           string `json:"id"`
	Description  string `json:"description"`
	InputSchema  Schema `json:"inputSchema"`
	OutputSchema Schema `json:"outputSchema"`
	Steps        []Step `json:"steps"`
	Entry        string `json:"entry"`
	Result       string `json:"result"`
}

// Schema represents a JSON schema for input/output validation
type Schema struct {
	Type       string            `json:"type"`
	Properties map[string]Schema `json:"properties,omitempty"`
	Items      *Schema           `json:"items,omitempty"`
	Required   []string          `json:"required,omitempty"`
}

// Step represents a single step in the DAG
type Step struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	// Input     interface{} `json:"input,omitempty"` // Can be string or []string
	Params
	// Next   []string `json:"next,omitempty"`
	Then   []string `json:"then,omitempty"`
	Output string   `json:"output,omitempty"`
}

type Params struct {
	DbOperationParams
	JoinParams
	FilterParams
	MapParams
	ConditionParams
	HTTPParams
}

type DbOperationParams struct {
	Table string `json:"table"`
	QueryParams
	InsertParams
}

type QueryParams struct {
	Select []string               `json:"select"`
	Where  map[string]interface{} `json:"where"`
}
type InsertParams struct {
	Map map[string]string `json:"map"`
}

type JoinParams struct {
	On    map[string]string `json:"on"`
	Type  string            `json:"type"`
	Left  string            `json:"left"`
	Right string            `json:"right"`
}

type FilterParams struct {
	Filter map[string]interface{} `json:"filter"`
}

type MapParams struct {
	Function string `json:"function"`
}

type ConditionParams struct {
	If   Condition `json:"if"`
	Else []string  `json:"else"`
}

type HTTPParams struct {
	Method  string                 `json:"method"`
	URL     string                 `json:"url"`
	Headers map[string]string      `json:"headers,omitempty"`
	Body    map[string]interface{} `json:"body,omitempty"`
	Query   map[string]interface{} `json:"query,omitempty"`
}

// StepParams represents the parameters for different step types
type StepParams struct {
	// Query params
	Table  string                 `json:"table,omitempty"`
	Select []string               `json:"select,omitempty"`
	Where  map[string]interface{} `json:"where,omitempty"`

	// Join params
	On    map[string]string `json:"on,omitempty"`
	Type  string            `json:"type,omitempty"`
	Left  string            `json:"left,omitempty"`
	Right string            `json:"right,omitempty"`

	// Filter params
	Filter map[string]interface{} `json:"filter,omitempty"`

	// Map params
	Function string `json:"function,omitempty"`

	// Insert params
	Map map[string]string `json:"map,omitempty"`

	// Condition params
	If   Condition `json:"if,omitempty"`
	Else string    `json:"else,omitempty"`

	// HTTP params
	Method  string                 `json:"method,omitempty"`
	URL     string                 `json:"url,omitempty"`
	Headers map[string]string      `json:"headers,omitempty"`
	Body    map[string]interface{} `json:"body,omitempty"`
	Query   map[string]interface{} `json:"query,omitempty"`
}

type Operator string

const (
	EQ    Operator = "="
	NE    Operator = "!="
	GT    Operator = ">"
	GTE   Operator = ">="
	LT    Operator = "<"
	LTE   Operator = "<="
	IN    Operator = "in"
	NOTIN Operator = "not in"
	AND   Operator = "and"
	OR    Operator = "or"
)

type ConditionOperand interface {
	string | *Condition
}

// Condition represents a condition in the DAG
type Condition struct {
	Left     interface{} `json:"left"` // can be string or *Condition
	Right    interface{} `json:"right"`
	Operator Operator    `json:"operator"`
}
