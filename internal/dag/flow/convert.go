package flow

import (
	"fmt"
)

// convertToType converts a string value to the target type T
func convertToType[T []map[string]T | map[string]T | string | bool | int | interface{}, R any](value T) R {
	var zero T

	switch any(zero).(type) {
	case []map[string]T:
		return any(value).(R)
	case map[string]T:
		return any(value).(R)
	case string:
		return any(value).(R)
	case bool:
		return any(value).(R)
	case int:
		return any(value).(R)
	case interface{}:
		return any(value).(R)
	}
	return any(fmt.Sprint(value)).(R)
}
