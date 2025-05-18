package respositories

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Postgres handles database operations for the DAG executor
type Postgres struct {
	pool *pgxpool.Pool
}

// NewPostgres creates a new PostgreSQL repository
func NewPostgres(connStr string) (*Postgres, error) {
	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	// Configure connection pool
	config.MaxConns = 25
	config.MinConns = 5
	config.MaxConnLifetime = 5 * time.Minute

	// Create connection pool
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Verify connection
	if err := pool.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &Postgres{pool: pool}, nil
}

// Close closes the database connection pool
func (r *Postgres) Close() error {
	r.pool.Close()
	return nil
}

// Query executes a query and returns the results
func (r *Postgres) query(query string, args ...interface{}) ([]interface{}, error) {
	// Execute query
	rows, err := r.pool.Query(context.Background(), query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Get field descriptions
	fieldDescriptions := rows.FieldDescriptions()
	columns := make([]string, len(fieldDescriptions))
	for i, fd := range fieldDescriptions {
		columns[i] = string(fd.Name)
	}

	// Prepare result
	result := make([]interface{}, 0)

	// Scan rows using Values() for more efficient scanning
	for rows.Next() {
		// Get row values directly using Values()
		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("failed to get row values: %w", err)
		}

		// Create map for current row
		row := make(map[string]interface{}, len(columns))
		for i, col := range columns {
			row[col] = values[i]
		}

		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during row iteration: %w", err)
	}

	return result, nil
}

// Insert executes an insert query and returns the number of affected rows
func (r *Postgres) mutate(query string, args ...interface{}) (int64, error) {
	result, err := r.pool.Exec(context.Background(), query, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to execute insert: %w", err)
	}

	return result.RowsAffected(), nil
}

// ExecuteInTransaction executes the given function within a transaction
func (r *Postgres) executeInTransaction(fn func(*pgx.Tx) error) error {
	tx, err := r.pool.Begin(context.Background())
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	if err := fn(&tx); err != nil {
		if rbErr := tx.Rollback(context.Background()); rbErr != nil {
			return fmt.Errorf("failed to rollback transaction: %v (original error: %v)", rbErr, err)
		}
		return err
	}

	if err := tx.Commit(context.Background()); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (r *Postgres) Create(table string, mapping map[string]interface{}) (interface{}, error) {
	query, args, err := BuildInsertQuery(table, mapping)
	if err != nil {
		return nil, err
	}
	return r.mutate(query, args...)
}

func (r *Postgres) Update(table string, mapping map[string]interface{}, where map[string]interface{}) (interface{}, error) {
	query, args := BuildUpdateQuery(table, mapping, where)
	return r.mutate(query, args...)
}

func (r *Postgres) Retrieve(table string, columns []string, where map[string]interface{}) ([]interface{}, error) {
	query, args := BuildSelectQuery(table, columns, where)
	return r.query(query, args...)
}

func (r *Postgres) Delete(table string, where map[string]interface{}) (interface{}, error) {
	query, args := BuildDeleteQuery(table, where)
	return r.mutate(query, args...)
}

func (r *Postgres) GetTableNames() ([]string, error) {
	rows, err := r.Retrieve("information_schema.tables", []string{"table_name"}, map[string]interface{}{"table_schema": "public"})
	if err != nil {
		return nil, err
	}
	var tableNames []string
	for _, row := range rows {
		tableNames = append(tableNames, row.(map[string]interface{})["table_name"].(string))
	}
	return tableNames, nil
}

func (r *Postgres) GetColumns(tableName string) (map[string]string, error) {
	rows, err := r.Retrieve("information_schema.columns", []string{"column_name", "udt_name"}, map[string]interface{}{"table_name": tableName, "table_schema": "public"})
	if err != nil {
		return nil, err
	}
	schema := make(map[string]string)
	for _, row := range rows {
		columnName := row.(map[string]interface{})["column_name"].(string)
		dataType := row.(map[string]interface{})["udt_name"].(string)
		schema[columnName] = dataType
	}
	return schema, nil
}
