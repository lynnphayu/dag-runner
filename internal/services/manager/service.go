package manager

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"

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

func (m *ManagerService) GetAdapter(id string) (*dag.Adapter[any], error) {
	collection := "adapters"
	filter := map[string]interface{}{
		"id": id,
	}

	results, err := m.db.Retrieve(collection, []string{}, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve adapter: %w", err)
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("adapter not found: %s", id)
	}

	var adapter dag.Adapter[any]
	bsonBytes, err := bson.Marshal(results[0])
	if err != nil {
		return nil, fmt.Errorf("failed to marshal BSON: %w", err)
	}

	if err = bson.Unmarshal(bsonBytes, &adapter); err != nil {
		return nil, fmt.Errorf("failed to unmarshal BSON to map: %w", err)
	}

	return &adapter, nil
}

func validateHTTPAuth(meta dag.HttpAdapter) error {
	switch meta.AuthType {
	case dag.Auth_None:
		return nil
	case dag.Auth_Basic:
		username, uok := meta.Auth["username"].(string)
		password, pok := meta.Auth["password"].(string)
		if !uok || !pok || strings.TrimSpace(username) == "" || strings.TrimSpace(password) == "" {
			return fmt.Errorf("basic auth requires username and password")
		}
		return nil
	case dag.Auth_Bearer:
		// Require either jwks or jwksUrl
		if raw := meta.Auth["jwks"]; raw != nil {
			// accept map or string; basic presence check is enough here
			return nil
		}
		if url, ok := meta.Auth["jwksUrl"].(string); ok && strings.TrimSpace(url) != "" {
			return nil
		}
		return fmt.Errorf("bearer auth requires 'jwks' or 'jwksUrl'")
	case dag.Auth_ApiKey:
		name, nok := meta.Auth["name"].(string)
		val, vok := meta.Auth["value"].(string)
		key, kok := meta.Auth["key"].(string)
		in := "header"
		if v, ok := meta.Auth["in"].(string); ok && v != "" {
			in = strings.ToLower(v)
		}
		if in != "header" && in != "query" && in != "cookie" {
			return fmt.Errorf("apiKey auth 'in' must be header, query, or cookie")
		}
		if !nok || strings.TrimSpace(name) == "" {
			return fmt.Errorf("apiKey auth requires 'name'")
		}
		if (!vok || strings.TrimSpace(val) == "") && (!kok || strings.TrimSpace(key) == "") {
			return fmt.Errorf("apiKey auth requires 'value' or 'key'")
		}
		return nil
	default:
		return fmt.Errorf("unsupported auth type: %s", meta.AuthType)
	}
}

func validateHTTPAdapter(adapter *dag.Adapter[any]) error {
	meta, ok := any(adapter.Meta).(dag.HttpAdapter)
	if !ok {
		return fmt.Errorf("invalid http adapter meta")
	}
	if strings.TrimSpace(meta.Path) == "" {
		return fmt.Errorf("adapter path is required")
	}
	if strings.TrimSpace(string(meta.Method)) == "" {
		return fmt.Errorf("adapter method is required")
	}
	if strings.TrimSpace(meta.Response) == "" {
		return fmt.Errorf("adapter response selector is required")
	}
	return validateHTTPAuth(meta)
}

func (m *ManagerService) SaveAdapter(adapter *dag.Adapter[any]) error {
	// Validate adapter before save
	if adapter.Type == dag.Adapter_Http {
		if err := validateHTTPAdapter(adapter); err != nil {
			return err
		}
	}

	collection := "adapters"
	uuid, err := uuid.NewRandom()
	if err != nil {
		return fmt.Errorf("failed to generate UUID: %w", err)
	}

	marshalAdapter, err := json.Marshal(adapter)

	if err != nil {
		return fmt.Errorf("failed to marshal adapter: %w", err)
	}

	var data map[string]interface{}
	json.Unmarshal(marshalAdapter, &data)
	data["id"] = uuid.String()
	data["user_id"] = "12345"

	_, err = m.db.Create(collection, data)
	if err != nil {
		return fmt.Errorf("failed to save adapter: %w", err)
	}
	return nil
}

// SaveDAG stores a DAG definition in MongoDB
func (m *ManagerService) SaveDAG(dag *dag.Graph[*dag.Action]) error {
	collection := "dags"
	uuid, err := uuid.NewRandom()
	if err != nil {
		return fmt.Errorf("failed to generate UUID: %w", err)
	}
	marshalDag, err := json.Marshal(dag)
	if err != nil {
		return fmt.Errorf("failed to marshal input schema: %w", err)
	}

	data := map[string]interface{}{}
	json.Unmarshal(marshalDag, &data)
	data["id"] = uuid.String()
	data["user_id"] = "12345"

	_, err = m.db.Create(collection, data)
	if err != nil {
		return fmt.Errorf("failed to save DAG: %w", err)
	}
	return nil
}

// GetDAG retrieves a DAG definition by ID
func (m *ManagerService) GetDAG(id string) (*dag.Graph[*dag.Action], error) {
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

	var dagData dag.Graph[*dag.Action]
	if err := json.Unmarshal(jsonBytes, &dagData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal to DAG: %w", err)
	}

	return &dagData, nil
}

// ListDAGs retrieves all stored DAG definitions
func (m *ManagerService) ListDAGs() ([]dag.Graph[*dag.Action], error) {
	collection := "dags"
	results, err := m.db.Retrieve(collection, []string{}, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to list DAGs: %w", err)
	}

	dags := make([]dag.Graph[*dag.Action], len(results))
	for i, result := range results {
		var dagData dag.Graph[*dag.Action]
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
func (m *ManagerService) UpdateDAG(dag *dag.Graph[*dag.Action]) (interface{}, error) {
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
