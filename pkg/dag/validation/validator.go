package validation

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/lynnphayu/dag-runner/pkg/dag"
)

var (
	pathRegex   = regexp.MustCompile(`^/[a-zA-Z0-9/_\-\.~]*$`)
	nodeIDRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
)

type ValidationError struct {
	Field   string
	Message string
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

type ValidationErrors []ValidationError

func (e ValidationErrors) Error() string {
	if len(e) == 0 {
		return "validation failed"
	}
	if len(e) == 1 {
		return e[0].Error()
	}
	messages := make([]string, len(e))
	for i, err := range e {
		messages[i] = err.Error()
	}
	return fmt.Sprintf("%d validation errors: %s", len(e), strings.Join(messages, "; "))
}

func (e ValidationErrors) HasErrors() bool {
	return len(e) > 0
}

func ValidateDAG(g *dag.Graph[*dag.Action, any]) error {
	var errors ValidationErrors

	if g == nil {
		return ValidationError{Field: "graph", Message: "graph is nil"}
	}

	if strings.TrimSpace(g.Name) == "" {
		errors = append(errors, ValidationError{Field: "name", Message: "graph name is required"})
	}

	if len(g.Nodes) == 0 {
		errors = append(errors, ValidationError{Field: "nodes", Message: "graph must have at least one node"})
	}

	nodeIDs := make(map[string]bool)
	for nodeID, node := range g.Nodes {
		if node == nil {
			errors = append(errors, ValidationError{
				Field:   fmt.Sprintf("nodes[%s]", nodeID),
				Message: "node is nil",
			})
			continue
		}

		if !nodeIDRegex.MatchString(node.ID) {
			errors = append(errors, ValidationError{
				Field:   fmt.Sprintf("nodes[%s].id", nodeID),
				Message: "node ID must contain only alphanumeric characters, hyphens, and underscores",
			})
		}

		if nodeIDs[node.ID] {
			errors = append(errors, ValidationError{
				Field:   fmt.Sprintf("nodes[%s].id", nodeID),
				Message: fmt.Sprintf("duplicate node ID: %s", node.ID),
			})
		}
		nodeIDs[node.ID] = true

		if strings.TrimSpace(node.Name) == "" {
			errors = append(errors, ValidationError{
				Field:   fmt.Sprintf("nodes[%s].name", node.ID),
				Message: "node name is required",
			})
		}

		if node.Data == nil {
			errors = append(errors, ValidationError{
				Field:   fmt.Sprintf("nodes[%s].data", node.ID),
				Message: "node action is required",
			})
			continue
		}

		if node.Data.Type == "" {
			errors = append(errors, ValidationError{
				Field:   fmt.Sprintf("nodes[%s].type", node.ID),
				Message: "action type is required",
			})
			continue
		}

		for _, depID := range node.Dependencies {
			if _, exists := g.Nodes[depID]; !exists {
				errors = append(errors, ValidationError{
					Field:   fmt.Sprintf("nodes[%s].dependencies", node.ID),
					Message: fmt.Sprintf("dependency references non-existent node: %s", depID),
				})
			}
		}

		if err := validateAction(node.Data, node); err != nil {
			errors = append(errors, ValidationError{
				Field:   fmt.Sprintf("nodes[%s]", node.ID),
				Message: err.Error(),
			})
		}
	}

	if err := g.ValidateDAG(); err != nil {
		errors = append(errors, ValidationError{Field: "graph", Message: err.Error()})
	}

	if errors.HasErrors() {
		return errors
	}
	return nil
}

func validateAction(action *dag.Action, node *dag.Node[*dag.Action]) error {
	rawMeta, err := json.Marshal(action.Meta)
	if err != nil {
		return fmt.Errorf("failed to marshal action meta: %w", err)
	}

	if fn, ok := actionTypeRegistry[action.Type]; ok {
		internal := fn()
		if err := json.Unmarshal(rawMeta, internal); err != nil {
			return fmt.Errorf("failed to unmarshal action meta: %w", err)
		}
		return internal.Validate(nil, action)
	}

	return fmt.Errorf("unknown action type: %s", action.Type)
}

var actionTypeRegistry = map[dag.ActionType]func() dag.ActionInterface{
	dag.Type_Query:  func() dag.ActionInterface { return &dag.Query{} },
	dag.Type_Insert: func() dag.ActionInterface { return &dag.Insert{} },
	dag.Type_Update: func() dag.ActionInterface { return &dag.Update{} },
	dag.Type_Delete: func() dag.ActionInterface { return &dag.Delete{} },
	dag.Type_Join:   func() dag.ActionInterface { return &dag.Join{} },
	dag.Type_Filter: func() dag.ActionInterface { return &dag.Filter{} },
	dag.Type_Map:    func() dag.ActionInterface { return &dag.Map{} },
	dag.Type_Cond:   func() dag.ActionInterface { return &dag.Condition{} },
	dag.Type_HTTP:   func() dag.ActionInterface { return &dag.HTTP{} },
}

func ValidateAdapter(adapter *dag.Adapter[any]) error {
	var errors ValidationErrors

	if adapter == nil {
		return ValidationError{Field: "adapter", Message: "adapter is nil"}
	}

	switch adapter.Type {
	case dag.Adapter_Http:
		errors = append(errors, validateHTTPAdapter(adapter)...)
	case dag.Adapter_Schedular:
		errors = append(errors, validateSchedulerAdapter(adapter)...)
	default:
		errors = append(errors, ValidationError{
			Field:   "type",
			Message: fmt.Sprintf("unknown adapter type: %s", adapter.Type),
		})
	}

	if strings.TrimSpace(adapter.Name) == "" {
		errors = append(errors, ValidationError{Field: "name", Message: "adapter name is required"})
	}

	if errors.HasErrors() {
		return errors
	}
	return nil
}

func validateHTTPAdapter(adapter *dag.Adapter[any]) ValidationErrors {
	var errors ValidationErrors

	meta, ok := any(adapter.Meta).(dag.HttpAdapter)
	if !ok {
		if len(adapter.MetaRaw) > 0 {
			errors = append(errors, ValidationError{
				Field:   "meta",
				Message: "HTTP adapter meta could not be parsed",
			})
			return errors
		}
		errors = append(errors, ValidationError{Field: "meta", Message: "invalid HTTP adapter meta"})
		return errors
	}

	if strings.TrimSpace(meta.Path) == "" {
		errors = append(errors, ValidationError{Field: "path", Message: "adapter path is required"})
	} else if !pathRegex.MatchString(meta.Path) {
		errors = append(errors, ValidationError{
			Field:   "path",
			Message: "path must start with / and contain only valid URL path characters",
		})
	}

	method := string(meta.Method)
	if method == "" {
		errors = append(errors, ValidationError{Field: "method", Message: "adapter method is required"})
	} else {
		validMethods := []string{string(dag.GET), string(dag.POST), string(dag.PUT), string(dag.DELETE), string(dag.PATCH)}
		found := false
		for _, m := range validMethods {
			if method == m {
				found = true
				break
			}
		}
		if !found {
			errors = append(errors, ValidationError{
				Field:   "method",
				Message: fmt.Sprintf("invalid HTTP method: %s", method),
			})
		}
	}

	if len(meta.Response) == 0 {
		errors = append(errors, ValidationError{Field: "response", Message: "adapter response selector is required"})
	} else {
		for key, value := range meta.Response {
			if err := validateResponseSelector(key, value); err != nil {
				errors = append(errors, ValidationError{
					Field:   fmt.Sprintf("response.%s", key),
					Message: err.Error(),
				})
			}
		}
	}

	if err := validateAdapterAuth(meta); err != nil {
		errors = append(errors, ValidationError{Field: "auth", Message: err.Error()})
	}

	return errors
}

func validateSchedulerAdapter(adapter *dag.Adapter[any]) ValidationErrors {
	var errors ValidationErrors

	meta, ok := any(adapter.Meta).(dag.SchedularAdapter)
	if !ok {
		errors = append(errors, ValidationError{Field: "meta", Message: "invalid scheduler adapter meta"})
		return errors
	}

	if strings.TrimSpace(meta.Cron) == "" {
		errors = append(errors, ValidationError{Field: "cron", Message: "cron expression is required"})
	} else {
		fields := strings.Fields(meta.Cron)
		if len(fields) < 5 || len(fields) > 7 {
			errors = append(errors, ValidationError{
				Field:   "cron",
				Message: "cron expression must have 5-7 space-separated fields",
			})
		}
	}

	return errors
}

func validateAdapterAuth(meta dag.HttpAdapter) error {
	switch meta.AuthType {
	case dag.Auth_None:
		return nil
	case dag.Auth_Basic:
		return validateBasicAuth(meta.Auth)
	case dag.Auth_Bearer:
		return validateBearerAuth(meta.Auth)
	case dag.Auth_ApiKey:
		return validateAPIKeyAuth(meta.Auth)
	default:
		return fmt.Errorf("unsupported auth type: %s", meta.AuthType)
	}
}

func validateBasicAuth(auth map[string]interface{}) error {
	username, uok := auth["username"].(string)
	password, pok := auth["password"].(string)
	if !uok || !pok || strings.TrimSpace(username) == "" || strings.TrimSpace(password) == "" {
		return fmt.Errorf("basic auth requires username and password")
	}
	return nil
}

func validateBearerAuth(auth map[string]interface{}) error {
	hasSecret := false
	if v, ok := auth["secret"].(string); ok && v != "" {
		hasSecret = true
	}
	if v, ok := auth["hmacSecret"].(string); ok && v != "" {
		hasSecret = true
	}
	if v, ok := auth["sharedSecret"].(string); ok && v != "" {
		hasSecret = true
	}

	hasJwks := false
	if raw := auth["jwks"]; raw != nil {
		hasJwks = true
	}
	if url, ok := auth["jwksUrl"].(string); ok && strings.TrimSpace(url) != "" {
		hasJwks = true
	}

	if !hasSecret && !hasJwks {
		return fmt.Errorf("bearer auth requires 'secret', 'hmacSecret', 'sharedSecret', 'jwks', or 'jwksUrl'")
	}

	if hasSecret {
		if alg, ok := auth["alg"].(string); ok && alg != "" {
			validAlgs := map[string]bool{"HS256": true, "HS384": true, "HS512": true}
			if !validAlgs[alg] {
				return fmt.Errorf("bearer auth HMAC alg must be HS256, HS384, or HS512")
			}
		}
	}

	return nil
}

func validateAPIKeyAuth(auth map[string]interface{}) error {
	name, nok := auth["name"].(string)
	if !nok || strings.TrimSpace(name) == "" {
		return fmt.Errorf("apiKey auth requires 'name'")
	}

	if in, ok := auth["in"].(string); ok && in != "" {
		in = strings.ToLower(in)
		if in != "header" && in != "query" && in != "cookie" {
			return fmt.Errorf("apiKey auth 'in' must be header, query, or cookie")
		}
	}

	return nil
}

func validateResponseSelector(key string, value interface{}) error {
	switch v := value.(type) {
	case string:
		if strings.TrimSpace(v) == "" {
			return fmt.Errorf("response selector cannot be empty")
		}
		if strings.HasPrefix(v, "$") {
			if !strings.HasPrefix(v, "$input.") && !strings.HasPrefix(v, "$results.") {
				return fmt.Errorf("response reference must start with $input. or $results.")
			}
		}
	case map[string]interface{}:
		for k, val := range v {
			if err := validateResponseSelector(k, val); err != nil {
				return err
			}
		}
	case []interface{}:
		for i, item := range v {
			if err := validateResponseSelector(fmt.Sprintf("%s[%d]", key, i), item); err != nil {
				return err
			}
		}
	case nil:
	default:
		return fmt.Errorf("invalid response selector type: %T", value)
	}
	return nil
}

func ValidateResponseReferences(response map[string]interface{}, graph *dag.Graph[*dag.Action, any]) error {
	referenced := dag.ExtractReferencedResultNodeIDs(response)
	if len(referenced) == 0 {
		return nil
	}

	var missing []string
	for nodeID := range referenced {
		if _, exists := graph.Nodes[nodeID]; !exists {
			missing = append(missing, nodeID)
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf("response references unknown nodes: %s", strings.Join(missing, ", "))
	}

	return nil
}

func IsValidMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch,
		http.MethodHead, http.MethodOptions, http.MethodConnect, http.MethodTrace:
		return true
	}
	return false
}
