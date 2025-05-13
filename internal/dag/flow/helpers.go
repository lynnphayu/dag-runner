package flow

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/expr-lang/expr"
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

// applyMapping applies a mapping function to data
func applyMapping(data interface{}, function string) (interface{}, error) {
	// Convert function name to actual mapping logic
	mapper, err := getMappingFunction(function)
	if err != nil {
		return nil, err
	}

	return mapper(data)
}

// getMappingFunction returns a mapping function by name
func getMappingFunction(name string) (func(interface{}) (interface{}, error), error) {
	// Add your mapping functions here
	mappings := map[string]func(interface{}) (interface{}, error){
		"createOrderSummary":   createOrderSummary,
		"combineUserAndOrders": combineUserAndOrders,
	}

	if fn, ok := mappings[name]; ok {
		return fn, nil
	}
	return nil, fmt.Errorf("mapping function %s not found", name)
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

// evaluateCondition evaluates a condition expression
func evaluateCondition(condition string, data interface{}) (bool, error) {
	env := map[string]interface{}{
		"input": data,
	}

	result, err := expr.Eval(condition, env)
	if err != nil {
		return false, err
	}

	bool, ok := result.(bool)
	if !ok {
		return false, fmt.Errorf("condition did not evaluate to boolean")
	}

	return bool, nil
}

// Example mapping functions
func createOrderSummary(data interface{}) (interface{}, error) {
	orders, ok := data.([]map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid input format for createOrderSummary")
	}

	var total float64
	for _, order := range orders {
		if t, ok := order["total"].(float64); ok {
			total += t
		}
	}

	return map[string]interface{}{
		"orders":     orders,
		"totalSpent": total,
	}, nil
}

func combineUserAndOrders(data interface{}) (interface{}, error) {
	inputs, ok := data.([]interface{})
	if !ok || len(inputs) != 2 {
		return nil, fmt.Errorf("invalid input format for combineUserAndOrders")
	}

	user, ok := inputs[0].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid user data format")
	}

	orders, ok := inputs[1].([]map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid orders data format")
	}

	var total float64
	for _, order := range orders {
		if t, ok := order["total"].(float64); ok {
			total += t
		}
	}

	return map[string]interface{}{
		"user": map[string]interface{}{
			"id":   user["id"],
			"name": user["name"],
		},
		"orders":     orders,
		"totalSpent": total,
	}, nil
}
