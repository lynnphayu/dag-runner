package db

import (
	"database/sql"
	"fmt"
	"strings"
)

// ExecuteInsert executes an insert operation
func ExecuteInsert(db *sql.DB, table string, mapping map[string]string, data interface{}) (interface{}, error) {
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
