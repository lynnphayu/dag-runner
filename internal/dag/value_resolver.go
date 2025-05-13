package dag

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/lynnphayu/swift/dagflow/internal/dag/domain"
	"github.com/tidwall/gjson"
)

// resolveValue resolves a value from the step results and converts it to the appropriate type
func resolveValue(value interface{}, context *domain.Context) (interface{}, error) {
	// Handle string values that might be step references
	fmt.Println("Value:", context.Input, context.Results, value)
	if strVal, ok := value.(string); ok && strings.HasPrefix(strVal, "$") {
		jsonStr := ""
		if strings.HasPrefix(strVal, "$step.") {
			contextValue := *context.Results
			if contextValue == nil {
				return nil, fmt.Errorf("step results not found")
			}

			jsonBytes, err := json.Marshal(contextValue)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal step results: %w", err)
			}
			jsonStr = string(jsonBytes)
		} else if strings.HasPrefix(strVal, "$input.") {
			contextValue := *context.Input
			if contextValue == nil {
				return nil, fmt.Errorf("input context not found")
			}

			jsonBytes, err := json.Marshal(contextValue)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal input context: %w", err)
			}
			jsonStr = string(jsonBytes)
		}
		// Extract the path after the prefix ($step. or $input.)
		var pathAfterPrefix string
		if strings.HasPrefix(strVal, "$step.") {
			pathAfterPrefix = strings.TrimPrefix(strVal, "$step.")
		} else {
			pathAfterPrefix = strings.TrimPrefix(strVal, "$input.")
		}
		result := gjson.Get(jsonStr, pathAfterPrefix)
		fmt.Println("GJSON Path:", pathAfterPrefix, result, jsonStr)

		if !result.Exists() {
			return nil, fmt.Errorf("value not found for path '%s' in context", strVal)
		}

		// For array access, ensure the result is valid
		if strings.Contains(pathAfterPrefix, "[") && result.Type == gjson.Null {
			return nil, fmt.Errorf("array element not found for path '%s'", strVal)
		}

		// Handle type conversion based on the gjson value type
		switch result.Type {
		case gjson.String:
			// Try to convert string to number if it looks like one
			if num, err := strconv.ParseInt(result.String(), 10, 64); err == nil {
				return num, nil
			}
			if num, err := strconv.ParseFloat(result.String(), 64); err == nil {
				return num, nil
			}
			return result.String(), nil
		case gjson.Number:
			return result.Value(), nil
		case gjson.True, gjson.False:
			return result.Bool(), nil
		case gjson.Null:
			return nil, nil
		default:
			return result.Value(), nil
		}
	}

	// Return the original value if it's not a context reference
	return value, nil
}
