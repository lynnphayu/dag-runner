package db

import (
	"database/sql"
)

// ScanRows scans SQL rows into a slice of maps
func ScanRows(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]interface{}, len(columns))
	scans := make([]interface{}, len(columns))
	for i := range values {
		scans[i] = &values[i]
	}

	var result []map[string]interface{}
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
