package runner

import (
	"fmt"
	"log"

	httpClient "github.com/lynnphayu/dag-runner/internal/repositories/http"
	mongodb "github.com/lynnphayu/dag-runner/internal/repositories/mongodb"
	postgres "github.com/lynnphayu/dag-runner/internal/repositories/postgres"
	dag "github.com/lynnphayu/dag-runner/pkg/dag"
	"go.mongodb.org/mongo-driver/bson"
)

type RunnerService struct {
	postgresdb *postgres.Postgres
	mongodb    *mongodb.MongoDB
	httpClient *httpClient.Http
}

func NewRunnerService(
	postgresURI string,
	mongoURI string,
) *RunnerService {
	postgresdb, err := postgres.NewPostgres(postgresURI)
	if err != nil {
		log.Fatalf("failed to create postgres: %v", err)
	}
	httpClient, err := httpClient.NewHttp()
	if err != nil {
		log.Fatalf("failed to create http: %v", err)
	}
	mongodb, err := mongodb.NewMongoDB(mongoURI, "dag_manager")
	if err != nil {
		log.Fatalf("failed to create mongodb connection: %v", err)
	}
	return &RunnerService{
		postgresdb,
		mongodb,
		httpClient,
	}
}

func (r *RunnerService) GetHTTPHandlerPreference(
	graphId string,
) (*dag.Runner, *dag.Adapter[dag.HttpAdapter], error) {
	graphs, err := r.mongodb.Retrieve("dags", []string{"*"}, map[string]interface{}{"id": graphId})
	if err != nil {
		return nil, nil, err
	}
	if len(graphs) == 0 {
		return nil, nil, fmt.Errorf("graph not found")
	}

	var graph dag.Graph[*dag.Action]
	bsonBytes, err := bson.Marshal(graphs[0])
	if err != nil {
		return nil, nil, err
	}
	err = bson.Unmarshal(bsonBytes, &graph)
	if err != nil {
		return nil, nil, err
	}

	adapters, err := r.mongodb.Retrieve("adapters", []string{"*"}, map[string]interface{}{"graphId": graphId, "type": "http"})
	if err != nil {
		return nil, nil, err
	}
	if len(adapters) == 0 {
		return nil, nil, fmt.Errorf("no HTTP adapter found for graph ID: %s", graphId)
	}
	var adapter dag.Adapter[dag.HttpAdapter]
	bsonBytes, err = bson.Marshal(adapters[0])
	if err != nil {
		return nil, nil, err
	}
	err = bson.Unmarshal(bsonBytes, &adapter)
	if err != nil {
		return nil, nil, err
	}
	runner := dag.NewRunner(r.postgresdb, r.httpClient, &graph)
	return runner, &adapter, nil
}

func (r *RunnerService) GetTableNames() ([]string, error) {
	return r.postgresdb.GetTableNames()
}

func (r *RunnerService) GetColumns(tableName string) (map[string]string, error) {
	return r.postgresdb.GetColumns(tableName)
}
