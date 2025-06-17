package dag

// DAG represents a directed acyclic graph of processing steps
type DAG struct {
	ID          string `json:"id" bson:"id"`
	Name        string `json:"name" bson:"name"`
	Description string `json:"description,omitempty" bson:"description,omitempty"`
	InputSchema Schema `json:"inputSchema,omitempty" bson:"inputSchema,omitempty"`
	// OutputSchema Schema `json:"outputSchema,omitempty" bson:"outputSchema,omitempty"`
	Steps []Step `json:"steps" bson:"steps"`
}

// Schema represents a JSON schema for input/output validation
type Schema struct {
	Type       string            `json:"type" bson:"type"`
	Properties map[string]Schema `json:"properties,omitempty" bson:"properties,omitempty"`
	Items      *Schema           `json:"items,omitempty" bson:"items,omitempty"`
	Required   []string          `json:"required,omitempty" bson:"required,omitempty"`
}

type StepType string

const (
	Query  StepType = "query"
	Insert StepType = "insert"
	Update StepType = "update"
	Delete StepType = "delete"
	Cond   StepType = "condition"
	HTTP   StepType = "http"
	Map    StepType = "map"
	Join   StepType = "join"
	Filter StepType = "filter"
	Output StepType = "output"
)

// Step represents a single step in the DAG
type Step struct {
	ID   string   `json:"id" bson:"id"`
	Name string   `json:"name" bson:"name"`
	Type StepType `json:"type" bson:"type"`
	// Input     interface{} `json:"input,omitempty"` // Can be string or []string
	Params
	Then      []string `json:"then,omitempty" bson:"then,omitempty"` // next steps
	DependsOn []string `json:"dependsOn,omitempty" bson:"dependsOn,omitempty"`
	// Output    interface{} `json:"output,omitempty" bson:"output,omitempty"`
}

type Params struct {
	DbOperationParams
	JoinParams
	FilterParams
	MapParams
	ConditionParams
	HTTPParams
	OutputParams
}

type OutputParams struct {
	Schema Schema `json:"schema" bson:"schema"`
	Source string `json:"source" bson:"source"`
	// Key    string `json:"key,omitempty" bson:"key,omitempty"`
}

type DbOperationParams struct {
	Table string                 `json:"table" bson:"table"`
	Where map[string]interface{} `json:"where,omitempty" bson:"where,omitempty"`
	QueryParams
	InsertParams
	UpdateParams
	DeleteParams
}

type QueryParams struct {
	Select []string `json:"select" bson:"select"`
}
type InsertParams struct {
	Map map[string]interface{} `json:"map" bson:"map"`
}
type UpdateParams struct {
	Set map[string]interface{} `json:"set" bson:"set"`
}
type DeleteParams struct{}

type JoinType string

const (
	Inner JoinType = "inner"
	Left  JoinType = "left"
	Right JoinType = "right"
	// Full  JoinType = "full" // still not sure this should be supported
)

type JoinParams struct {
	On    map[string]string `json:"on" bson:"on"`
	Type  JoinType          `json:"type" bson:"type"`
	Left  string            `json:"left" bson:"left"`
	Right string            `json:"right" bson:"right"`
}

type FilterParams struct {
	Filter map[string]interface{} `json:"filter" bson:"filter"`
}

type MapParams struct {
	Function string `json:"function" bson:"function"`
}

type ConditionParams struct {
	If   Condition `json:"if" bson:"if"`
	Else []string  `json:"else" bson:"else"`
}

type SupportedHTTPMethods string

const (
	GET    SupportedHTTPMethods = "GET"
	POST   SupportedHTTPMethods = "POST"
	PUT    SupportedHTTPMethods = "PUT"
	DELETE SupportedHTTPMethods = "DELETE"
	PATCH  SupportedHTTPMethods = "PATCH"
)

type HTTPParams struct {
	Method  SupportedHTTPMethods   `json:"method" bson:"method"`
	URL     string                 `json:"url" bson:"url"`
	Headers map[string]string      `json:"headers,omitempty" bson:"headers,omitempty"`
	Body    map[string]interface{} `json:"body,omitempty" bson:"body,omitempty"`
	Query   map[string]interface{} `json:"query,omitempty" bson:"query,omitempty"`
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

// Condition represents a condition in the DAG
type Condition struct {
	Left     interface{} `json:"left" bson:"left"` // can be string or *Condition
	Right    interface{} `json:"right" bson:"right"`
	Operator Operator    `json:"operator" bson:"operator"`
}
