package operations

import (
	"fmt"
	"reflect"
	"strings"
)

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
