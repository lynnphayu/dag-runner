package operations

import (
	"fmt"
)

// ApplyMapping applies a mapping function to data
func ApplyMapping(data interface{}, function string) (interface{}, error) {
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
