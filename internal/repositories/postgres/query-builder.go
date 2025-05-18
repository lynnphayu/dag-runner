package respositories

import (
	"fmt"
	"strings"
)

func BuildSelectQuery(table string, columns []string, where map[string]interface{}) (string, []interface{}) {
	if len(columns) == 0 {
		columns = append(columns, "*")
	}
	base := fmt.Sprintf(
		"SELECT %s FROM %s",
		strings.Join(columns, ", "),
		table,
	)
	if len(where) != 0 {
		whereClause, whereArgs := BuildWhereClause(table, where)
		base = fmt.Sprintf(
			"%s WHERE %s",
			base,
			whereClause,
		)
		return base, whereArgs
	}
	return base, nil
}

// BuildWhereClause constructs a WHERE clause from the given conditions
func BuildWhereClause(table string, conditions map[string]interface{}) (string, []interface{}) {
	var clauses []string
	var args []interface{}
	var i int

	for field, value := range conditions {
		// Handle special operators
		switch v := value.(type) {
		case map[string]interface{}:
			for op, val := range v {
				switch op {
				case "eq":
					clauses = append(clauses, fmt.Sprintf("%s = $%d", field, i+1))
					args = append(args, val)
				case "gt":
					clauses = append(clauses, fmt.Sprintf("%s > $%d", field, i+1))
					args = append(args, val)
				case "lt":
					clauses = append(clauses, fmt.Sprintf("%s < $%d", field, i+1))
					args = append(args, val)
				case "gte":
					clauses = append(clauses, fmt.Sprintf("%s >= $%d", field, i+1))
					args = append(args, val)
				case "lte":
					clauses = append(clauses, fmt.Sprintf("%s <= $%d", field, i+1))
					args = append(args, val)
				case "like":
					clauses = append(clauses, fmt.Sprintf("%s LIKE $%d", field, i+1))
					args = append(args, val)
				case "in":
					if arr, ok := val.([]interface{}); ok {
						placeholders := make([]string, len(arr))
						for j := range arr {
							placeholders[j] = fmt.Sprintf("$%d", i+j+1)
							args = append(args, arr[j])
						}
						clauses = append(clauses, fmt.Sprintf("%s IN (%s)", field, strings.Join(placeholders, ",")))
						i += len(arr) - 1
					}
				}
				i++
			}
		default:

			clauses = append(clauses, fmt.Sprintf("%s = $%d", field, i+1))
			args = append(args, value)
			i++
		}
	}

	return strings.Join(clauses, " AND "), args
}

// BuildInsertQuery constructs an INSERT query from the given parameters
func BuildInsertQuery(table string, mapping map[string]interface{}) (string, []interface{}, error) {
	var columns []string
	var placeholders []string
	var args []interface{}

	// Build ordered columns first to maintain consistency
	for col := range mapping {
		columns = append(columns, col)
	}

	placeholders = make([]string, len(columns))
	for i, col := range columns {
		field := mapping[col]
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		if field == nil {
			return "", nil, fmt.Errorf("field '%s' cannot be null", field)
		}
		args = append(args, field)
	}
	placeholders = []string{fmt.Sprintf("(%s)", strings.Join(placeholders, ", "))}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	return query, args, nil
}

func BuildUpdateQuery(table string, mapping map[string]interface{}, where map[string]interface{}) (string, []interface{}) {
	var setClauses []string
	var args []interface{}
	var i int
	for field, value := range mapping {
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", field, i+1))
		args = append(args, value)
		i++
	}
	whereClause, whereArgs := BuildWhereClause(table, where)
	args = append(args, whereArgs...)
	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		table,
		strings.Join(setClauses, ", "),
		whereClause,
	)
	return query, args
}

func BuildDeleteQuery(table string, where map[string]interface{}) (string, []interface{}) {
	whereClause, whereArgs := BuildWhereClause(table, where)
	query := fmt.Sprintf(
		"DELETE FROM %s WHERE %s",
		table,
		whereClause,
	)
	return query, whereArgs
}
