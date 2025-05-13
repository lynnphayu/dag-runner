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

// // Property represents a JSON schema property
// type Property struct {
// 	Type    string      `json:"type"`
// 	Items   *Schema     `json:"items,omitempty"`
// 	Default interface{} `json:"default,omitempty"`
// }

// Step represents a single step in the DAG
type Step struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	// Input     interface{} `json:"input,omitempty"` // Can be string or []string
	Params    StepParams `json:"params"`
	DependsOn []string   `json:"depends_on,omitempty"`
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
	If   string `json:"if,omitempty"`
	Then string `json:"then,omitempty"`
	Else string `json:"else,omitempty"`

	// HTTP params
	Method  string            `json:"method,omitempty"`
	URL     string            `json:"url,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    interface{}       `json:"body,omitempty"`

	// Log params
	Message string `json:"message,omitempty"`
	Include bool   `json:"include,omitempty"`
}
