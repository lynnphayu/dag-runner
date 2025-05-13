package operations

import (
	"fmt"
	"reflect"
)

// PerformJoin joins two datasets based on join conditions
func PerformJoin(datasets [][]map[string]interface{}, on map[string]string, joinType string) ([]map[string]interface{}, error) {
	if len(datasets) != 2 {
		return nil, fmt.Errorf("join requires exactly two datasets")
	}

	// Convert datasets to slices of maps
	left := datasets[0]
	right := datasets[1]

	// Perform join
	var result []map[string]interface{}
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
