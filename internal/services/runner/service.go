package runner

import (
	"log"

	httpClient "github.com/lynnphayu/dag-runner/internal/repositories/http"
	postgres "github.com/lynnphayu/dag-runner/internal/repositories/postgres"
	dag "github.com/lynnphayu/dag-runner/pkg/dag"
)

type RunnerService struct {
	executor *dag.Executor
	db       *postgres.Postgres
}

func NewRunnerService(connString string) *RunnerService {
	persistent, err := postgres.NewPostgres(connString)
	if err != nil {
		log.Fatalf("failed to create postgres: %v", err)
	}
	httpClient, err := httpClient.NewHttp()
	if err != nil {
		log.Fatalf("failed to create http: %v", err)
	}
	executor, err := dag.NewExecutor(persistent, httpClient)
	if err != nil {
		log.Fatalf("failed to create executor: %v", err)
	}
	return &RunnerService{
		executor,
		persistent,
	}
}

func (r *RunnerService) Execute(dag *dag.DAG, input map[string]interface{}) (interface{}, error) {
	return r.executor.Execute(dag, input)
}

func (r *RunnerService) GetTableNames() ([]string, error) {
	return r.db.GetTableNames()
}

func (r *RunnerService) GetColumns(tableName string) (map[string]string, error) {
	return r.db.GetColumns(tableName)
}
