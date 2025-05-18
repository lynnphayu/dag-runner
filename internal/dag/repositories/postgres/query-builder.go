package respositories

import (
	"fmt"
	"strings"

	"github.com/expr-lang/expr"
	"github.com/lynnphayu/swift/dagflow/internal/dag/domain"
)

// BuildWhereClause constructs a WHERE clause from the given conditions
func BuildWhereClause(conditions map[string]interface{}) (string, []interface{}) {
	var clauses []string
	var args []interface{}
	var i int

	for field, value := range conditions {
		// Handle special operators
		switch v := value.(type) {
		case map[string]interface{}:
			for op, val := range v {
				switch op {
				case "$eq":
					clauses = append(clauses, fmt.Sprintf("%s = $%d", field, i+1))
					args = append(args, val)
				case "$gt":
					clauses = append(clauses, fmt.Sprintf("%s > $%d", field, i+1))
					args = append(args, val)
				case "$lt":
					clauses = append(clauses, fmt.Sprintf("%s < $%d", field, i+1))
					args = append(args, val)
				case "$gte":
					clauses = append(clauses, fmt.Sprintf("%s >= $%d", field, i+1))
					args = append(args, val)
				case "$lte":
					clauses = append(clauses, fmt.Sprintf("%s <= $%d", field, i+1))
					args = append(args, val)
				case "$like":
					clauses = append(clauses, fmt.Sprintf("%s LIKE $%d", field, i+1))
					args = append(args, val)
				case "$in":
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
func BuildInsertQuery(table string, mapping map[string]string, context *domain.Context) (string, []interface{}, error) {
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
		val, err := resolveValue(field, context)
		if err != nil {
			return "", nil, fmt.Errorf("error resolving field '%s': %v", field, err)
		}
		if val == nil {
			return "", nil, fmt.Errorf("field '%s' cannot be null", field)
		}
		args = append(args, val)
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

// resolveValue resolves a value from the context if it's a reference
func resolveValue(value interface{}, context *domain.Context) (interface{}, error) {
	if str, ok := value.(string); ok && strings.HasPrefix(str, "$") {
		env := map[string]interface{}{
			"input":   context.Input,
			"results": context.Results,
		}
		result, err := expr.Eval(str[1:], env)
		if err != nil {
			return nil, fmt.Errorf("error evaluating expression '%s': %v", str, err)
		}
		return result, nil
	}
	return value, nil
}
