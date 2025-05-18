package flow

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/expr-lang/expr"
	"github.com/lynnphayu/swift/dagflow/internal/dag/domain"
	"github.com/tidwall/gjson"
)

// scanRows scans SQL rows into a slice of maps
func scanRows(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]interface{}, len(columns))
	scans := make([]interface{}, len(columns))
	for i := range values {
		scans[i] = &values[i]
	}

	result := make([]map[string]interface{}, 0)
	for rows.Next() {
		if err := rows.Scan(scans...); err != nil {
			return nil, err
		}

		row := make(map[string]interface{})
		for i, value := range values {
			switch v := value.(type) {
			case []byte:
				row[columns[i]] = string(v)
			default:
				row[columns[i]] = v
			}
		}

		result = append(result, row)
	}

	return result, nil
}

func mergeMaps(maps ...map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for _, m := range maps {
		for k, v := range m {
			result[k] = v
		}
	}
	return result
}

// performJoin joins two datasets based on join conditions
func performJoin(datasets [][]map[string]interface{}, on map[string]string, joinType domain.JoinType) ([]map[string]interface{}, error) {
	if len(datasets) != 2 {
		return nil, fmt.Errorf("join requires exactly two datasets")
	}

	// Convert datasets to slices of maps
	left := datasets[0]
	right := datasets[1]

	// Perform join
	result := make([]map[string]interface{}, 0)
	switch joinType {
	case domain.Inner:
		for _, leftRow := range left {
			for _, rightRow := range right {
				if matchJoinConditions(leftRow, rightRow, on) {
					result = append(result, mergeMaps(leftRow, rightRow))
				}
			}
		}
	case domain.Left:
		for _, leftRow := range left {
			matched := false
			for _, rightRow := range right {
				if matchJoinConditions(leftRow, rightRow, on) {
					result = append(result, mergeMaps(leftRow, rightRow))
					matched = true
					break
				}
			}
			if !matched {
				result = append(result, mergeMaps(leftRow))
			}
		}
	case domain.Right:
		for _, rightRow := range right {
			matched := false
			for _, leftRow := range left {
				if matchJoinConditions(leftRow, rightRow, on) {
					result = append(result, mergeMaps(leftRow, rightRow))
					matched = true
					break
				}
			}
			if !matched {
				result = append(result, mergeMaps(rightRow))
			}
		}
	}

	return result, nil
}

// matchJoinConditions checks if two rows match based on join conditions
func matchJoinConditions(left, right map[string]interface{}, on map[string]string) bool {
	for leftKey, rightKey := range on {
		leftVal := left[leftKey]
		rightVal := right[rightKey]
		if !reflect.DeepEqual(leftVal, rightVal) {
			return false
		}
	}
	return true
}

// applyFilter filters data based on conditions
func applyFilter(data interface{}, conditions map[string]interface{}) (interface{}, error) {
	dataset, ok := data.([]map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("filter input must be array of maps")
	}

	result := make([]map[string]interface{}, 0)
	for _, item := range dataset {
		if matchConditions(item, conditions) {
			result = append(result, item)
		}
	}

	return result, nil
}

// matchConditions checks if an item matches filter conditions
func matchConditions(item map[string]interface{}, conditions map[string]interface{}) bool {
	for key, condition := range conditions {
		switch cond := condition.(type) {
		case map[string]interface{}:
			for op, value := range cond {
				if !evaluateOperator(item[key], op, value) {
					return false
				}
			}
		default:
			if !reflect.DeepEqual(item[key], condition) {
				return false
			}
		}
	}
	return true
}

// evaluateOperator evaluates a comparison operator
func evaluateOperator(left interface{}, operator string, right interface{}) bool {
	switch operator {
	case "gt":
		return compareValues(left, right) > 0
	case "gte":
		return compareValues(left, right) >= 0
	case "lt":
		return compareValues(left, right) < 0
	case "lte":
		return compareValues(left, right) <= 0
	case "ne":
		return !reflect.DeepEqual(left, right)
	default:
		return false
	}
}

// compareValues compares two values
func compareValues(a, b interface{}) int {
	// Convert to comparable types if needed
	va := reflect.ValueOf(a)
	vb := reflect.ValueOf(b)

	// Handle nil values
	if !va.IsValid() || !vb.IsValid() {
		return strings.Compare(fmt.Sprint(a), fmt.Sprint(b))
	}

	// Convert both values to float64 if one of them is float
	if (va.Kind() == reflect.Float32 || va.Kind() == reflect.Float64) ||
		(vb.Kind() == reflect.Float32 || vb.Kind() == reflect.Float64) {
		var fa, fb float64
		switch va.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			fa = float64(va.Int())
		case reflect.Float32, reflect.Float64:
			fa = va.Float()
		default:
			return strings.Compare(fmt.Sprint(a), fmt.Sprint(b))
		}
		switch vb.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			fb = float64(vb.Int())
		case reflect.Float32, reflect.Float64:
			fb = vb.Float()
		default:
			return strings.Compare(fmt.Sprint(a), fmt.Sprint(b))
		}
		if fa > fb {
			return 1
		} else if fa < fb {
			return -1
		}
		return 0
	}

	// Handle integer types
	if va.Kind() >= reflect.Int && va.Kind() <= reflect.Int64 &&
		vb.Kind() >= reflect.Int && vb.Kind() <= reflect.Int64 {
		return int(va.Int() - vb.Int())
	}

	// Default to string comparison
	return strings.Compare(fmt.Sprint(a), fmt.Sprint(b))
}

// executeInsert executes an insert operation
func executeInsert(db *sql.DB, table string, mapping map[string]string, data interface{}) (interface{}, error) {
	// Build insert query
	columns := make([]string, 0, len(mapping))
	placeholders := make([]string, 0, len(mapping))
	values := make([]interface{}, 0, len(mapping))

	// Convert data to map if needed
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		// Try to convert slice of maps, take first item
		if dataSlice, ok := data.([]map[string]interface{}); ok && len(dataSlice) > 0 {
			dataMap = dataSlice[0]
		} else {
			// If data is not a map or slice of maps, create empty map
			dataMap = make(map[string]interface{})
		}
	}

	// Process each mapping
	for col, field := range mapping {
		columns = append(columns, col)
		placeholders = append(placeholders, "?")

		// Get value from data map using field name
		val, exists := dataMap[field]
		if !exists {
			// If field doesn't exist in data, use field value as literal
			val = field
		}
		values = append(values, val)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	// Execute query
	result, err := db.Exec(query, values...)
	if err != nil {
		return nil, err
	}

	// Get inserted ID
	id, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}

	return id, nil
}

func resolveValues(input interface{}, context *domain.Context) interface{} {
	// Handle different types of inputs

	switch v := input.(type) {
	case map[string]interface{}:
		resolvedMap := make(map[string]interface{})
		for key, value := range v {
			if str, ok := value.(string); ok {
				resolvedMap[key] = resolveV2[interface{}](str, context)
			} else if integer, ok := value.(int); ok {
				resolvedMap[key] = integer
			} else if boolean, ok := value.(bool); ok {
				resolvedMap[key] = boolean
			} else if obj, ok := value.(map[string]interface{}); ok {
				resolvedMap[key] = resolveValues(obj, context)
			} else if slice, ok := value.([]map[string]interface{}); ok {
				resolvedMap[key] = resolveValues(slice, context)
			} else {
				resolvedMap[key] = value
			}
		}
		return resolvedMap
	case []map[string]interface{}:
		resolvedSlice := make([]map[string]interface{}, len(v))
		for i, item := range v {
			resolvedItem := resolveValues(item, context)
			resolvedSlice[i] = resolvedItem.(map[string]interface{})
		}
		return resolvedSlice
	case []interface{}:
		resolvedSlice := make([]interface{}, len(v))
		for i, item := range v {
			resolvedItem := resolveValues(item, context)
			resolvedSlice[i] = resolvedItem
		}
		return resolvedSlice
	case string:
		resolvedValue, err := resolveV1(v, context)
		if err != nil {
			return v
		}
		return resolvedValue
	case bool:
		return v
	case int:
		return v
	default:
		return v
	}

}

func resolveV2[T []map[string]T | map[string]T | string | bool | int | interface{}](str string, context *domain.Context) T {
	// Handle string interpolation for ${var} syntax
	env := map[string]interface{}{
		"input":   context.Input,
		"results": context.Results,
	}

	// Handle string interpolation with ${var} syntax
	if strings.Contains(str, "${") {
		result := str
		for {
			start := strings.Index(result, "${")
			if start == -1 {
				break
			}
			end := strings.Index(result[start:], "}") + start
			if end == -1 {
				break
			}

			template := result[start+2 : end]
			evaluated, err := expr.Eval(template, env)
			if err == nil {
				result = result[:start] + fmt.Sprint(evaluated) + result[end+1:]
			}
		}
		return any(result).(T)
	}

	// Handle direct expression evaluation with $ prefix
	if strings.HasPrefix(str, "$") {
		result, err := expr.Eval(str[1:], env)
		if err == nil {
			// Try direct type assertion
			if converted, ok := result.(T); ok {
				return converted
			}
			// Try string conversion for bool/int types
			return any(result).(T)
		} else {
			return any(str).(T)
		}
	}

	return any(str).(T)
}

// resolveV1 resolves a value from the step results and converts it to the appropriate type
func resolveV1(value interface{}, context *domain.Context) (interface{}, error) {
	// Handle string values that might be step references
	if strVal, ok := value.(string); ok && strings.HasPrefix(strVal, "$") {
		jsonStr := ""
		if strings.HasPrefix(strVal, "$results.") {
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
		if strings.HasPrefix(strVal, "$results.") {
			pathAfterPrefix = strings.TrimPrefix(strVal, "$results.")
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

// ApplyFilter filters data based on conditions
func ApplyFilter(data interface{}, conditions map[string]interface{}) (interface{}, error) {
	dataset, ok := data.([]map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("filter input must be array of maps")
	}

	var result []map[string]interface{}
	for _, item := range dataset {
		if matchConditions(item, conditions) {
			result = append(result, item)
		}
	}

	return result, nil
}
