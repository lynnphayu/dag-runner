package manager

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/google/uuid"
	mongodb "github.com/lynnphayu/dag-runner/internal/repositories/mongodb"
	"github.com/lynnphayu/dag-runner/pkg/dag"
	"go.mongodb.org/mongo-driver/bson"
)

type ManagerService struct {
	db *mongodb.MongoDB
}

func NewManagerService(mongoURI string) *ManagerService {
	db, err := mongodb.NewMongoDB(mongoURI, "dag_manager")
	if err != nil {
		log.Fatalf("failed to create mongodb connection: %v", err)
	}
	return &ManagerService{
		db: db,
	}
}

// SaveDAG stores a DAG definition in MongoDB
func (m *ManagerService) SaveDAG(dag *dag.DAG) error {
	collection := "dags"
	uuid, err := uuid.NewRandom()
	if err != nil {
		return fmt.Errorf("failed to generate UUID: %w", err)
	}
	marshalDag, err := json.Marshal(dag)
	if err != nil {
		return fmt.Errorf("failed to marshal input schema: %w", err)
	}

	data := map[string]interface{}{
		"id": uuid.String(),
	}
	json.Unmarshal(marshalDag, &data)

	r, err := m.db.Create(collection, data)
	fmt.Println(err, r)
	if err != nil {
		return fmt.Errorf("failed to save DAG: %w", err)
	}
	return nil
}

// GetDAG retrieves a DAG definition by ID
func (m *ManagerService) GetDAG(id string) (*dag.DAG, error) {
	collection := "dags"
	filter := map[string]interface{}{
		"id": id,
	}

	results, err := m.db.Retrieve(collection, []string{}, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve DAG: %w", err)
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("DAG not found: %s", id)
	}

	// First unmarshal to a map to handle the MongoDB document structure
	var rawData map[string]interface{}
	bsonBytes, err := bson.Marshal(results[0])
	if err != nil {
		return nil, fmt.Errorf("failed to marshal BSON: %w", err)
	}

	if err = bson.Unmarshal(bsonBytes, &rawData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal BSON to map: %w", err)
	}

	// Now marshal the map to JSON and then unmarshal to DAG struct
	jsonBytes, err := json.Marshal(rawData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal to JSON: %w", err)
	}

	var dagData dag.DAG
	if err := json.Unmarshal(jsonBytes, &dagData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal to DAG: %w", err)
	}

	return &dagData, nil
}

// ListDAGs retrieves all stored DAG definitions
func (m *ManagerService) ListDAGs() ([]dag.DAG, error) {
	collection := "dags"
	results, err := m.db.Retrieve(collection, []string{}, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to list DAGs: %w", err)
	}

	dags := make([]dag.DAG, len(results))
	for i, result := range results {
		var dagData dag.DAG
		bsonBytes, err := bson.Marshal(result)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal BSON: %w", err)
		}

		if err := bson.Unmarshal(bsonBytes, &dagData); err != nil {
			return nil, fmt.Errorf("failed to unmarshal to DAG: %w", err)
		}

		dags[i] = dagData
	}
	return dags, nil
}

// DeleteDAG removes a DAG definition by ID
func (m *ManagerService) DeleteDAG(id string) error {
	collection := "dags"
	filter := map[string]interface{}{
		"id": id,
	}

	_, err := m.db.Delete(collection, filter)
	if err != nil {
		return fmt.Errorf("failed to delete DAG: %w", err)
	}
	return nil
}

// UpdateDAG updates an existing DAG definition
func (m *ManagerService) UpdateDAG(dag *dag.DAG) (interface{}, error) {
	collection := "dags"
	filter := map[string]interface{}{
		"id": dag.ID,
	}

	marshalDag, err := json.Marshal(dag)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal input schema: %w", err)
	}

	data := map[string]interface{}{}
	json.Unmarshal(marshalDag, &data)

	r, err := m.db.Update(collection, data, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to update DAG: %w", err)
	}
	return r, nil
}
