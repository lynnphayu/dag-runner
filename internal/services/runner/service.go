package runner

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/mux"
	"github.com/lestrrat-go/jwx/jwk"
	"github.com/lynnphayu/dag-runner/internal/constants"
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
	routeMu    sync.RWMutex
	routes     map[string]*mux.Route
	graphIndex map[string]string
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
		postgresdb: postgresdb,
		mongodb:    mongodb,
		httpClient: httpClient,
		routes:     make(map[string]*mux.Route),
		graphIndex: make(map[string]string),
	}
}

func (r *RunnerService) GetHTTPHandlerAdapter(
	graphId string,
) (*dag.Runner, *dag.Adapter[dag.HttpAdapter], error) {

	// Load latest graph metadata (by version/subversion) and compose nodes from separate collection
	graphDocs, err := r.mongodb.Retrieve(constants.GRAPH_COLLECTION, []string{}, map[string]interface{}{
		"id":     graphId,
		"status": string(dag.Status_Published),
	})
	if err != nil {
		return nil, nil, err
	}
	if len(graphDocs) == 0 {
		return nil, nil, fmt.Errorf("published graph not found: %s", graphId)
	}
	best := graphDocs[0]
	bestV, bestSV := extractVersion(best)
	for _, doc := range graphDocs[1:] {
		v, sv := extractVersion(doc)
		if v > bestV || (v == bestV && sv > bestSV) {
			best = doc
			bestV, bestSV = v, sv
		}
	}
	var graph dag.Graph[*dag.Action, any]
	bb, err := bson.Marshal(best)
	if err != nil {
		return nil, nil, err
	}
	var raw map[string]interface{}
	if err := bson.Unmarshal(bb, &raw); err != nil {
		return nil, nil, err
	}
	j, err := json.Marshal(raw)
	if err != nil {
		return nil, nil, err
	}
	if err := json.Unmarshal(j, &graph); err != nil {
		return nil, nil, err
	}
	// Attach nodes for the selected version
	nodeDocs, err := r.mongodb.Retrieve(constants.NODE_COLLECTION, []string{}, map[string]interface{}{
		"graphId":    graphId,
		"version":    bestV,
		"subversion": bestSV,
	})
	if err != nil {
		return nil, nil, err
	}
	graph.Nodes = make(map[string]*dag.Node[*dag.Action])
	for _, nd := range nodeDocs {
		nb, err := bson.Marshal(nd)
		if err != nil {
			return nil, nil, err
		}
		var m map[string]interface{}
		if err := bson.Unmarshal(nb, &m); err != nil {
			return nil, nil, err
		}
		jb, err := json.Marshal(m)
		if err != nil {
			return nil, nil, err
		}
		var node dag.Node[*dag.Action]
		if err := json.Unmarshal(jb, &node); err != nil {
			return nil, nil, err
		}
		if node.Data != nil {
			node.Data.SetBackRef(&node)
		}
		if node.Dependents == nil {
			node.Dependents = make([]string, 0)
		}
		n := node
		graph.Nodes[node.ID] = &n
	}
	// Rebuild dependents from dependencies
	for _, n := range graph.Nodes {
		n.Dependents = make([]string, 0)
	}
	for _, n := range graph.Nodes {
		for _, depID := range n.Dependencies {
			if depNode, ok := graph.Nodes[depID]; ok {
				depNode.Dependents = append(depNode.Dependents, n.ID)
			}
		}
	}
	adapterDocs, err := r.mongodb.Retrieve(constants.ADAPTER_COLLECTION, []string{}, map[string]interface{}{
		"graphId":    graphId,
		"version":    bestV,
		"subversion": bestSV,
	})
	if err != nil {
		return nil, nil, err
	}
	var adapter dag.Adapter[dag.HttpAdapter]
	foundAdapter := false
	for _, doc := range adapterDocs {
		ab, err := bson.Marshal(doc)
		if err != nil {
			return nil, nil, err
		}
		var m map[string]interface{}
		if err := bson.Unmarshal(ab, &m); err != nil {
			return nil, nil, err
		}
		jb, err := json.Marshal(m)
		if err != nil {
			return nil, nil, err
		}
		var candidate dag.Adapter[dag.HttpAdapter]
		if err := json.Unmarshal(jb, &candidate); err != nil {
			continue
		}
		if candidate.Type != dag.Adapter_Http {
			continue
		}
		adapter = candidate
		foundAdapter = true
		break
	}
	if !foundAdapter {
		err := fmt.Errorf("http adapter not found for graph %s version %d.%d", graphId, bestV, bestSV)
		log.Println(err.Error())
		return nil, nil, err
	}
	runner := dag.NewRunner(r.postgresdb, r.httpClient, &graph)
	return runner, &adapter, nil
}

func (r *RunnerService) RegisterFlowRoute(graphId string, router *mux.Router) error {
	executor, adapter, err := r.GetHTTPHandlerAdapter(graphId)
	if err != nil {
		log.Printf("failed to get http handler preference: %v", err)
		return err
	}

	var jwks jwk.Set
	if adapter.Meta.AuthType == dag.Auth_Bearer {
		// Prefer inline JWKS if provided
		if rawSet, ok := adapter.Meta.Auth["jwks"].(map[string]interface{}); ok {
			if b, err := json.Marshal(rawSet); err == nil {
				if set, err := jwk.Parse(b); err == nil {
					jwks = set
				} else {
					log.Printf("failed to parse inline JWKS: %v", err)
				}
			}
		} else if rawStr, ok := adapter.Meta.Auth["jwks"].(string); ok && rawStr != "" {
			if set, err := jwk.Parse([]byte(rawStr)); err == nil {
				jwks = set
			} else {
				log.Printf("failed to parse inline JWKS string: %v", err)
			}
		} else if url, ok := adapter.Meta.Auth["jwksUrl"].(string); ok && url != "" {
			// If jwksUrl looks like JSON, parse it directly; otherwise fetch from URL
			if strings.HasPrefix(strings.TrimSpace(url), "{") || strings.HasPrefix(strings.TrimSpace(url), "[") {
				if set, err := jwk.Parse([]byte(url)); err == nil {
					jwks = set
				} else {
					log.Printf("failed to parse JWKS from jwksUrl JSON: %v", err)
				}
			} else {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if set, err := jwk.Fetch(ctx, url); err == nil {
					jwks = set
				} else {
					log.Printf("failed to fetch JWKS: %v", err)
				}
			}
		}
	}

	selectKey := func(t *jwt.Token) (interface{}, error) {
		// If we don't have a JWKS, signal unverifiable so caller can relax if desired
		if jwks == nil {
			return nil, jwt.ErrTokenUnverifiable
		}
		if kid, _ := t.Header["kid"].(string); kid != "" {
			if key, found := jwks.LookupKeyID(kid); found {
				var pub interface{}
				if err := key.Raw(&pub); err != nil {
					return nil, err
				}
				return pub, nil
			}
		}
		// Fallback: try the first usable key
		it := jwks.Iterate(context.Background())
		for it.Next(context.Background()) {
			pair := it.Pair()
			if key, ok := pair.Value.(jwk.Key); ok {
				var pub interface{}
				if err := key.Raw(&pub); err == nil {
					return pub, nil
				}
			}
		}
		return nil, jwt.ErrTokenUnverifiable
	}

	isAuthorized := func(r *http.Request) (bool, string) {
		switch adapter.Meta.AuthType {
		case dag.Auth_None:
			return true, ""
		case dag.Auth_Basic:
			authHeader := r.Header.Get("Authorization")
			username, uok := adapter.Meta.Auth["username"].(string)
			password, pok := adapter.Meta.Auth["password"].(string)
			// If no configured creds, allow
			if !uok || !pok || username == "" || password == "" {
				return true, ""
			}
			if !strings.HasPrefix(authHeader, "Basic ") {
				return false, "missing Basic auth"
			}
			payload := strings.TrimPrefix(authHeader, "Basic ")
			decoded, err := base64.StdEncoding.DecodeString(payload)
			if err != nil {
				return false, "invalid Basic auth encoding"
			}
			parts := strings.SplitN(string(decoded), ":", 2)
			if len(parts) != 2 {
				return false, "invalid Basic auth payload"
			}
			if parts[0] != username || parts[1] != password {
				return false, "invalid credentials"
			}
			return true, ""
		case dag.Auth_Bearer:
			authHeader := r.Header.Get("Authorization")
			if !strings.HasPrefix(authHeader, "Bearer ") {
				return false, "missing Bearer token"
			}
			tokenStr := strings.TrimPrefix(authHeader, "Bearer ")

			// 1) If HMAC secret provided, verify with HS* first
			secret := ""
			if v, ok := adapter.Meta.Auth["secret"].(string); ok && v != "" {
				secret = v
			} else if v, ok := adapter.Meta.Auth["hmacSecret"].(string); ok && v != "" {
				secret = v
			} else if v, ok := adapter.Meta.Auth["sharedSecret"].(string); ok && v != "" {
				secret = v
			}
			if secret != "" {
				claims := jwt.MapClaims{}
				alg := ""
				if a, ok := adapter.Meta.Auth["alg"].(string); ok {
					alg = a
				}
				keyFunc := func(t *jwt.Token) (interface{}, error) {
					if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
						return nil, jwt.ErrTokenSignatureInvalid
					}
					if alg != "" && t.Method.Alg() != alg {
						return nil, jwt.ErrTokenSignatureInvalid
					}
					return []byte(secret), nil
				}
				tok, err := jwt.ParseWithClaims(tokenStr, claims, keyFunc)
				if err != nil || !tok.Valid {
					return false, "invalid token"
				}
				// Optional audience/issuer checks
				if aud, ok := adapter.Meta.Auth["audience"].(string); ok && aud != "" {
					if v, ok := claims["aud"].(string); ok {
						if v != aud {
							return false, "invalid audience"
						}
					} else if arr, ok := claims["aud"].([]interface{}); ok {
						match := false
						for _, a := range arr {
							if s, ok := a.(string); ok && s == aud {
								match = true
								break
							}
						}
						if !match {
							return false, "invalid audience"
						}
					}
				}
				if iss, ok := adapter.Meta.Auth["issuer"].(string); ok && iss != "" {
					if v, ok := claims["iss"].(string); !ok || v != iss {
						return false, "invalid issuer"
					}
				}
				return true, ""
			}

			// 2) Else, fall back to JWKS verification (if configured), or allow if not configured
			_, hasInline := adapter.Meta.Auth["jwks"]
			_, hasURL := adapter.Meta.Auth["jwksUrl"]
			if jwks == nil && !hasInline && !hasURL {
				return true, ""
			}
			if jwks == nil {
				return true, ""
			}
			claims := jwt.MapClaims{}
			tok, err := jwt.ParseWithClaims(tokenStr, claims, selectKey)
			if err != nil || !tok.Valid {
				return false, "invalid token"
			}
			if aud, ok := adapter.Meta.Auth["audience"].(string); ok && aud != "" {
				if v, ok := claims["aud"].(string); ok {
					if v != aud {
						return false, "invalid audience"
					}
				} else if arr, ok := claims["aud"].([]interface{}); ok {
					match := false
					for _, a := range arr {
						if s, ok := a.(string); ok && s == aud {
							match = true
							break
						}
					}
					if !match {
						return false, "invalid audience"
					}
				}
			}
			if iss, ok := adapter.Meta.Auth["issuer"].(string); ok && iss != "" {
				if v, ok := claims["iss"].(string); !ok || v != iss {
					return false, "invalid issuer"
				}
			}
			return true, ""
		case dag.Auth_ApiKey:
			// Only enforce if 'name' configured
			name, nameOK := adapter.Meta.Auth["name"].(string)
			if !nameOK || strings.TrimSpace(name) == "" {
				return true, ""
			}
			in := "header"
			if v, ok := adapter.Meta.Auth["in"].(string); ok && v != "" {
				in = v
			}
			expected := ""
			if v, ok := adapter.Meta.Auth["value"].(string); ok {
				expected = v
			} else if v, ok := adapter.Meta.Auth["key"].(string); ok {
				expected = v
			}
			var provided string
			switch strings.ToLower(in) {
			case "header":
				provided = r.Header.Get(name)
			case "query":
				provided = r.URL.Query().Get(name)
			case "cookie":
				if c, err := r.Cookie(name); err == nil {
					provided = c.Value
				}
			}
			if expected != "" {
				if provided == "" || provided != expected {
					return false, "invalid api key"
				}
				return true, ""
			}
			// No expected value configured: require presence only
			if strings.TrimSpace(provided) == "" {
				return false, "missing api key"
			}
			return true, ""
		default:
			return true, ""
		}
	}

	handler := func(w http.ResponseWriter, r *http.Request) {
		if ok, reason := isAuthorized(r); !ok {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]string{"error": reason})
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()
		var input map[string]interface{}
		if len(body) > 0 {
			err = json.Unmarshal(body, &input)
			if err != nil {
				http.Error(w, "invalid JSON body", http.StatusBadRequest)
				return
			}
		}
		resolvedInput := dag.ResolveValues(adapter.InputMap, map[string]interface{}{
			"headers": r.Header,
			"body":    input,
			"query":   r.URL.Query(),
			"path":    mux.Vars(r),
		}, &dag.Context{}).(map[string]interface{})

		w.Header().Set("Content-Type", "application/json")
		resultMap, err := executor.Execute(resolvedInput)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(&map[string]interface{}{
				"error": err.Error(),
			})
			return
		}
		result := dag.ResolveV2[interface{}](adapter.Meta.Response, resultMap, &dag.Context{Results: resultMap})
		if result != nil {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(result)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{})
	}

	routeKey := fmt.Sprintf("%s:%s", strings.ToUpper(string(adapter.Meta.Method)), adapter.Meta.Path)

	r.routeMu.Lock()
	defer r.routeMu.Unlock()

	// If graph already has a route registered, disable the old one before registering again.
	if existingKey, ok := r.graphIndex[graphId]; ok {
		if existingRoute, ok := r.routes[existingKey]; ok {
			if existingKey == routeKey {
				existingRoute.HandlerFunc(handler).Methods(string(adapter.Meta.Method))
				return nil
			}
			// Old path changed: neuter previous route so it no longer handles requests.
			existingRoute.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				http.NotFound(w, req)
			})
			delete(r.routes, existingKey)
		}
	}

	route := router.HandleFunc(adapter.Meta.Path, handler).Methods(string(adapter.Meta.Method))
	r.routes[routeKey] = route
	r.graphIndex[graphId] = routeKey

	return nil
}

func (r *RunnerService) RegisterAllPublishedFlowRoutes(router *mux.Router) error {
	graphDocs, err := r.mongodb.Retrieve(constants.GRAPH_COLLECTION, []string{}, map[string]interface{}{
		"status": string(dag.Status_Published),
	})
	if err != nil {
		return fmt.Errorf("failed to list published graphs: %w", err)
	}
	if len(graphDocs) == 0 {
		return nil
	}

	bestByGraph := make(map[string]map[string]interface{})
	for _, doc := range graphDocs {
		graphID, _ := doc["id"].(string)
		if strings.TrimSpace(graphID) == "" {
			continue
		}
		if existing, ok := bestByGraph[graphID]; ok {
			v, sv := extractVersion(doc)
			ev, esv := extractVersion(existing)
			if v > ev || (v == ev && sv > esv) {
				bestByGraph[graphID] = doc
			}
			// continue
		}
		bestByGraph[graphID] = doc
	}

	log.Printf("bestByGraph: %v", bestByGraph)
	var errs []string
	for id := range bestByGraph {
		if err := r.RegisterFlowRoute(id, router); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", id, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("failed to register published flows: %s", strings.Join(errs, "; "))
	}
	return nil
}

func (r *RunnerService) GetTableNames() ([]string, error) {
	return r.postgresdb.GetTableNames()
}

func (r *RunnerService) GetColumns(tableName string) (map[string]string, error) {
	return r.postgresdb.GetColumns(tableName)
}

func getIntValue(v interface{}) int {
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

func extractVersion(doc map[string]interface{}) (int, int) {
	if doc == nil {
		return 0, 0
	}
	return getIntValue(doc["version"]), getIntValue(doc["subversion"])
}
