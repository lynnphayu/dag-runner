package flow

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/expr-lang/expr"
	"github.com/lynnphayu/swift/dagflow/internal/dag/domain"
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

// performJoin joins two datasets based on join conditions
func performJoin(datasets [][]map[string]interface{}, on map[string]string, joinType string) ([]map[string]interface{}, error) {
	if len(datasets) != 2 {
		return nil, fmt.Errorf("join requires exactly two datasets")
	}

	// Convert datasets to slices of maps
	left := datasets[0]
	right := datasets[1]

	// Perform join
	result := make([]map[string]interface{}, 0)
	for _, leftRow := range left {
		for _, rightRow := range right {
			if matchJoinConditions(leftRow, rightRow, on) {
				// Merge rows
				merged := make(map[string]interface{})
				for k, v := range leftRow {
					merged[k] = v
				}
				for k, v := range rightRow {
					merged[k] = v
				}
				result = append(result, merged)
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
	case "$gt":
		return compareValues(left, right) > 0
	case "$gte":
		return compareValues(left, right) >= 0
	case "$lt":
		return compareValues(left, right) < 0
	case "$lte":
		return compareValues(left, right) <= 0
	case "$ne":
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

	switch va.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return int(va.Int() - vb.Int())
	case reflect.Float32, reflect.Float64:
		diff := va.Float() - vb.Float()
		if diff > 0 {
			return 1
		} else if diff < 0 {
			return -1
		}
		return 0
	default:
		return strings.Compare(fmt.Sprint(a), fmt.Sprint(b))
	}
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

func resolveValues[T []map[string]T | map[string]T | string | bool | int | interface{}, R any](input T, context *domain.Context) R {
	// Handle different types of inputs

	switch v := any(input).(type) {
	case map[string]interface{}:
		resolvedMap := make(map[string]interface{})
		for key, value := range v {
			if str, ok := value.(string); ok {
				resolvedMap[key] = resolveString[interface{}](str, context)
			} else if integer, ok := value.(int); ok {
				resolvedMap[key] = integer
			} else if boolean, ok := value.(bool); ok {
				resolvedMap[key] = boolean
			} else if obj, ok := value.(map[string]interface{}); ok {
				resolvedMap[key] = resolveValues[map[string]interface{}, map[string]interface{}](obj, context)
			} else if slice, ok := value.([]map[string]interface{}); ok {
				resolvedMap[key] = resolveValues[[]map[string]interface{}, []map[string]interface{}](slice, context)
			}
		}
		if _, ok := any(map[string]interface{}{}).(R); ok {
			return any(resolvedMap).(R)
		}
	case []map[string]any:
		resolvedSlice := make([]map[string]interface{}, len(v))
		for i, item := range v {
			resolvedItem := resolveValues[map[string]interface{}, map[string]interface{}](item, context)
			resolvedSlice[i] = resolvedItem
		}
		if _, ok := any(map[string]interface{}{}).(R); ok {
			return any(resolvedSlice).(R)
		}
	case string:
		resolvedValue := resolveString[string](v, context)
		if _, ok := any(resolvedValue).(string); ok {
			return any(v).(R)
		}
		if _, ok := any(resolvedValue).(bool); ok {
			b, _ := strconv.ParseBool(v)
			return any(b).(R)
		}
		if _, ok := any(resolvedValue).(int); ok {
			i, _ := strconv.Atoi(v)
			return any(i).(R)
		}
	case bool:
		if _, ok := any(v).(bool); ok {
			return any(v).(R)
		}
	case int:
		if _, ok := any(v).(int); ok {
			return any(v).(R)
		}
	default:
		return any(v).(R)
	}
	var zeroValue R
	return zeroValue
}

func resolveString[T []map[string]T | map[string]T | string | bool | int | interface{}](str string, context *domain.Context) T {
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

func isNumeric(value interface{}) bool {
	switch value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return true
	default:
		return false
	}
}

func isString(value interface{}) bool {
	_, ok := value.(string)
	return ok
}

func isBool(value interface{}) bool {
	_, ok := value.(bool)
	return ok
}

func toFloat64(value interface{}) float64 {
	switch v := value.(type) {
	case int:
	case int8:
	case int16:
	case int32:
	case int64:
	case uint:
	case uint8:
	case uint16:
	case uint32:
	case uint64:
	case float32:
	case float64:
		return float64(v)
	default:
		return 0
	}
	switch v := value.(type) {
	case int:
		return float64(v)
	case int8:
		return float64(v)
	case int16:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	case uint:
		return float64(v)
	case uint8:
		return float64(v)
	case uint16:
		return float64(v)
	case uint32:
		return float64(v)
	case uint64:
		return float64(v)
	case float32:
		return float64(v)
	case float64:
		return v
	default:
		return 0
	}
}
