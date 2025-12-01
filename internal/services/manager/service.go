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

func (m *ManagerService) ListAdapters(userID, graphID string) ([]dag.Adapter[any], error) {
	collection := "adapters"
	filter := map[string]interface{}{}

	if userID != "" {
		filter["user_id"] = userID
	}
	if graphID != "" {
		filter["graphId"] = graphID
	}

	results, err := m.db.Retrieve(collection, []string{}, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve adapters: %w", err)
	}

	adapters := make([]dag.Adapter[any], len(results))
	for i, result := range results {
		bsonBytes, err := bson.Marshal(result)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal BSON: %w", err)
		}

		// Convert BSON to JSON to properly populate MetaRaw
		var rawData map[string]interface{}
		if err := bson.Unmarshal(bsonBytes, &rawData); err != nil {
			return nil, fmt.Errorf("failed to unmarshal BSON to map: %w", err)
		}

		jsonBytes, err := json.Marshal(rawData)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal to JSON: %w", err)
		}

		var adapter dag.Adapter[any]
		if err := json.Unmarshal(jsonBytes, &adapter); err != nil {
			return nil, fmt.Errorf("failed to unmarshal to adapter: %w", err)
		}

		adapters[i] = adapter
	}

	return adapters, nil
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

// UpdateAdapter updates an existing adapter definition
func (m *ManagerService) UpdateAdapter(adapter *dag.Adapter[any]) (interface{}, error) {
	// Validate adapter before update
	if adapter.Type == dag.Adapter_Http {
		if err := validateHTTPAdapter(adapter); err != nil {
			return nil, err
		}
	}

	collection := "adapters"
	filter := map[string]interface{}{
		"id": adapter.ID,
	}

	marshalAdapter, err := json.Marshal(adapter)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal adapter: %w", err)
	}

	data := map[string]interface{}{}
	json.Unmarshal(marshalAdapter, &data)

	r, err := m.db.Update(collection, data, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to update adapter: %w", err)
	}
	return r, nil
}

// SaveDAG stores a DAG definition in MongoDB
func (m *ManagerService) SaveDAG(g *dag.Graph[*dag.Action]) error {
	collection := "dags"
	uuid, err := uuid.NewRandom()
	if err != nil {
		return fmt.Errorf("failed to generate UUID: %w", err)
	}
	marshalDag, err := json.Marshal(g)
	if err != nil {
		return fmt.Errorf("failed to marshal input schema: %w", err)
	}

	data := map[string]interface{}{}
	json.Unmarshal(marshalDag, &data)
	data["id"] = uuid.String()
	data["user_id"] = "12345"
	data["version"] = 1
	data["subversion"] = 1
	data["status"] = string(dag.Status_Draft)

	_, err = m.db.Create(collection, data)
	if err != nil {
		return fmt.Errorf("failed to save DAG: %w", err)
	}
	return nil
}

// GetDAG retrieves a DAG definition by ID
func (m *ManagerService) GetDAG(id string) (*dag.Graph[*dag.Action], error) {
	collection := "dags"
	// fetch all entries with the same business id and select the latest (max version, then max subversion)
	groupResults, err := m.db.Retrieve(collection, []string{}, map[string]interface{}{"id": id})
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve DAG group: %w", err)
	}
	candidates := groupResults
	if len(candidates) == 0 {
		return nil, fmt.Errorf("DAG not found: %s", id)
	}
	// Find the doc with highest (version, subversion)
	best := candidates[0]
	bestV, bestSV := extractVS(best)
	for _, doc := range candidates[1:] {
		v, sv := extractVS(doc)
		if v > bestV || (v == bestV && sv > bestSV) {
			best = doc
			bestV, bestSV = v, sv
		}
	}
	// Marshal/unmarshal into strongly typed DAG
	bsonBytes, err := bson.Marshal(best)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal BSON: %w", err)
	}
	var rawData map[string]interface{}
	if err = bson.Unmarshal(bsonBytes, &rawData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal BSON to map: %w", err)
	}
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

// extractVS reads version and subversion from a generic document, defaulting to 0 if absent
func extractVS(doc bson.M) (int, int) {
	getInt := func(v interface{}) int {
		switch t := v.(type) {
		case int:
			return t
		case int32:
			return int(t)
		case int64:
			return int(t)
		case float64:
			return int(t)
		default:
			return 0
		}
	}
	v := getInt(doc["version"])
	sv := getInt(doc["subversion"])
	return v, sv
}

// ListDAGs retrieves all stored DAG definitions
func (m *ManagerService) ListDAGs() ([]dag.Graph[*dag.Action], error) {
	collection := "dags"
	// Use aggregation to group by id and pick the latest by (version, subversion)
	pipeline := []bson.M{
		{"$sort": bson.M{"id": 1, "version": -1, "subversion": -1}},
		{
			"$group": bson.M{
				"_id": "$id",
				"doc": bson.M{"$first": "$$ROOT"},
			},
		},
		{"$replaceRoot": bson.M{"newRoot": "$doc"}},
	}
	results, err := m.db.Aggregate(collection, pipeline)
	if err != nil {
		return nil, fmt.Errorf("failed to list DAGs: %w", err)
	}

	dags := make([]dag.Graph[*dag.Action], 0, len(results))
	for _, best := range results {
		// Marshal/unmarshal through map->json to preserve nested types correctly
		bsonBytes, err := bson.Marshal(best)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal BSON: %w", err)
		}
		var rawData map[string]interface{}
		if err := bson.Unmarshal(bsonBytes, &rawData); err != nil {
			return nil, fmt.Errorf("failed to unmarshal BSON to map: %w", err)
		}
		jsonBytes, err := json.Marshal(rawData)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal to JSON: %w", err)
		}
		var dagData dag.Graph[*dag.Action]
		if err := json.Unmarshal(jsonBytes, &dagData); err != nil {
			return nil, fmt.Errorf("failed to unmarshal to DAG: %w", err)
		}
		dags = append(dags, dagData)
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
func (m *ManagerService) UpdateDAG(g *dag.Graph[*dag.Action]) (interface{}, error) {
	collection := "dags"
	// read current to inspect status and version
	current, err := m.db.Retrieve(collection, []string{}, map[string]interface{}{"id": g.ID})
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve existing DAG: %w", err)
	}
	if len(current) == 0 {
		return nil, fmt.Errorf("DAG not found: %s", g.ID)
	}
	// pick latest by (version, subversion)
	currentDoc := current[0]
	currentVersion, currentSubversion := extractVS(currentDoc)
	for _, doc := range current[1:] {
		v, sv := extractVS(doc)
		if v > currentVersion || (v == currentVersion && sv > currentSubversion) {
			currentDoc = doc
			currentVersion, currentSubversion = v, sv
		}
	}
	status, _ := currentDoc["status"].(string)
	// read current version/subversion

	// prepare updated data from payload
	marshalDag, err := json.Marshal(g)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal input schema: %w", err)
	}
	newData := map[string]interface{}{}
	_ = json.Unmarshal(marshalDag, &newData)
	// preserve user_id if present
	if userID, ok := currentDoc["user_id"]; ok {
		newData["user_id"] = userID
	}
	// Always enforce status transitions via Publish; keep current status in new entry
	newData["status"] = string(dag.Status_Draft)
	// keep same business id
	newData["id"] = g.ID
	// If not published, bump subversion only in the new entry
	if status != string(dag.Status_Published) {
		newData["version"] = currentVersion
		newData["subversion"] = currentSubversion + 1
		if _, err := m.db.Create(collection, newData); err != nil {
			return nil, fmt.Errorf("failed to create new draft revision: %w", err)
		}
		return map[string]interface{}{
			"id":         newData["id"],
			"version":    newData["version"],
			"subversion": newData["subversion"],
			"status":     newData["status"],
		}, nil
	}
	// If published, create new version entry (bump version, reset subversion)
	newData["version"] = currentVersion + 1
	newData["subversion"] = 1
	log.Println("newData", newData)
	log.Println("verions", currentVersion, currentSubversion)
	if _, err := m.db.Create(collection, newData); err != nil {
		return nil, fmt.Errorf("failed to create new published version: %w", err)
	}
	return map[string]interface{}{
		"id":         newData["id"],
		"version":    newData["version"],
		"subversion": newData["subversion"],
		"status":     newData["status"],
	}, nil
}

// PublishDAG creates a new entry with status=published (no bumps)
func (m *ManagerService) PublishDAG(id string) (string, int, error) {
	collection := "dags"
	current, err := m.db.Retrieve(collection, []string{}, map[string]interface{}{"id": id})
	if err != nil {
		return "", 0, fmt.Errorf("failed to retrieve existing DAG: %w", err)
	}
	if len(current) == 0 {
		return "", 0, fmt.Errorf("DAG not found: %s", id)
	}
	// pick latest by (version, subversion)
	currentDoc := current[0]
	currentVersion, currentSubversion := extractVS(currentDoc)
	for _, doc := range current[1:] {
		v, sv := extractVS(doc)
		if v > currentVersion || (v == currentVersion && sv > currentSubversion) {
			currentDoc = doc
			currentVersion, currentSubversion = v, sv
		}
	}
	// Update the latest entry in place to set status=published
	filter := map[string]interface{}{
		"_id": currentDoc["_id"],
	}
	update := map[string]interface{}{
		"status": string(dag.Status_Published),
	}
	if _, err := m.db.Update(collection, update, filter); err != nil {
		return "", 0, fmt.Errorf("failed to publish DAG: %w", err)
	}
	return currentDoc["id"].(string), currentVersion, nil
}

// ListDAGVersions returns all historical versions for a DAG id
func (m *ManagerService) ListDAGVersions(id string) ([]dag.Graph[*dag.Action], error) {
	collection := "dag_versions"
	filter := map[string]interface{}{
		"graph_id": id,
	}
	results, err := m.db.Retrieve(collection, []string{}, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve DAG versions: %w", err)
	}
	dags := make([]dag.Graph[*dag.Action], len(results))
	for i, result := range results {
		bsonBytes, err := bson.Marshal(result)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal BSON: %w", err)
		}
		var dagData dag.Graph[*dag.Action]
		if err := bson.Unmarshal(bsonBytes, &dagData); err != nil {
			return nil, fmt.Errorf("failed to unmarshal to DAG: %w", err)
		}
		dags[i] = dagData
	}
	return dags, nil
}

// GetDAGVersion retrieves a specific version for a DAG id
func (m *ManagerService) GetDAGVersion(id string, version int) (*dag.Graph[*dag.Action], error) {
	collection := "dag_versions"
	filter := map[string]interface{}{
		"graph_id": id,
		"version":  version,
	}
	results, err := m.db.Retrieve(collection, []string{}, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve DAG version: %w", err)
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("DAG version not found: %s v%d", id, version)
	}
	var dagData dag.Graph[*dag.Action]
	bsonBytes, err := bson.Marshal(results[0])
	if err != nil {
		return nil, fmt.Errorf("failed to marshal BSON: %w", err)
	}
	if err := bson.Unmarshal(bsonBytes, &dagData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal to DAG: %w", err)
	}
	return &dagData, nil
}
