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
func (r *Postgres) Query(query string, args ...interface{}) ([]map[string]interface{}, error) {
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
	result := make([]map[string]interface{}, 0)

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
func (r *Postgres) Insert(query string, args ...interface{}) (int64, error) {
	result, err := r.pool.Exec(context.Background(), query, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to execute insert: %w", err)
	}

	return result.RowsAffected(), nil
}

// ExecuteInTransaction executes the given function within a transaction
func (r *Postgres) ExecuteInTransaction(fn func(*pgx.Tx) error) error {
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
