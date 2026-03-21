package dag

import (
	"fmt"
	"regexp"
	"strings"
)

var resultsReferencePattern = regexp.MustCompile(`\bresults\.([A-Za-z0-9_-]+)\b`)

// ExtractResponseDependencyClosure returns the node IDs required to compute an adapter response.
func ExtractResponseDependencyClosure(
	response map[string]interface{},
	graph *Graph[*Action, any],
) (map[string]bool, error) {
	if graph == nil {
		return nil, fmt.Errorf("graph is required")
	}

	referenced := ExtractReferencedResultNodeIDs(response)
	if len(referenced) == 0 {
		return map[string]bool{}, nil
	}

	required := make(map[string]bool, len(referenced))
	visiting := make(map[string]bool, len(referenced))

	for nodeID := range referenced {
		if _, ok := graph.Nodes[nodeID]; !ok {
			return nil, fmt.Errorf("response references unknown node: %s", nodeID)
		}
		if err := collectDependencyClosure(nodeID, graph, required, visiting); err != nil {
			return nil, err
		}
	}

	return required, nil
}

// ExtractReferencedResultNodeIDs returns node IDs referenced through the `results` namespace.
func ExtractReferencedResultNodeIDs(input interface{}) map[string]bool {
	found := make(map[string]bool)
	walkResponseReferences(input, found)
	return found
}

func walkResponseReferences(input interface{}, found map[string]bool) {
	switch v := input.(type) {
	case map[string]interface{}:
		for _, value := range v {
			walkResponseReferences(value, found)
		}
	case []interface{}:
		for _, item := range v {
			walkResponseReferences(item, found)
		}
	case []map[string]interface{}:
		for _, item := range v {
			walkResponseReferences(item, found)
		}
	case string:
		extractNodeIDsFromExpression(v, found)
	}
}

func extractNodeIDsFromExpression(expr string, found map[string]bool) {
	if strings.TrimSpace(expr) == "" {
		return
	}

	normalized := strings.ReplaceAll(expr, "$results.", "results.")
	for _, match := range resultsReferencePattern.FindAllStringSubmatch(normalized, -1) {
		if len(match) < 2 {
			continue
		}
		nodeID := strings.TrimSpace(match[1])
		if nodeID != "" {
			found[nodeID] = true
		}
	}
}

func collectDependencyClosure(
	nodeID string,
	graph *Graph[*Action, any],
	required map[string]bool,
	visiting map[string]bool,
) error {
	if required[nodeID] {
		return nil
	}
	if visiting[nodeID] {
		return fmt.Errorf("cycle detected while collecting dependencies for node %s", nodeID)
	}

	node, ok := graph.Nodes[nodeID]
	if !ok {
		return fmt.Errorf("node not found in graph: %s", nodeID)
	}

	visiting[nodeID] = true
	required[nodeID] = true

	for _, depID := range node.Dependencies {
		if err := collectDependencyClosure(depID, graph, required, visiting); err != nil {
			return err
		}
	}

	delete(visiting, nodeID)
	return nil
}
