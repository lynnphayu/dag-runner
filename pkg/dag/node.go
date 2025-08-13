package dag

import (
	"fmt"

	"github.com/google/uuid"
)

type BackReferencable[T any] interface {
	SetBackRef(node *Node[T])
}

type Node[T any] struct {
	ID           string   `json:"id"                     bson:"id"`
	Name         string   `json:"name"                   bson:"name"`
	Data         T        `json:"data"                   bson:"data"`
	Dependencies []string `json:"dependencies,omitempty" bson:"dependencies,omitempty"`
	Dependents   []string `json:"dependents,omitempty"   bson:"dependents,omitempty"`
}

func NewNode[T BackReferencable[T]](name string, data T) *Node[T] {
	id := uuid.New().String()
	node := &Node[T]{
		ID:           id,
		Name:         name,
		Data:         data,
		Dependencies: make([]string, 0),
		Dependents:   make([]string, 0),
	}
	data.SetBackRef(node)
	return node
}

func (n *Node[T]) AddDependency(nodeID string) {
	if !n.HasDependency(nodeID) {
		n.Dependencies = append(n.Dependencies, nodeID)
	}
}

func (n *Node[T]) RemoveDependency(nodeID string) {
	for i, dep := range n.Dependencies {
		if dep == nodeID {
			n.Dependencies = append(n.Dependencies[:i], n.Dependencies[i+1:]...)
			break
		}
	}
}

func (n *Node[T]) HasDependency(nodeID string) bool {
	for _, dep := range n.Dependencies {
		if dep == nodeID {
			return true
		}
	}
	return false
}

func (n *Node[T]) AddDependent(nodeID string) {
	if !n.HasDependent(nodeID) {
		n.Dependents = append(n.Dependents, nodeID)
	}
}

func (n *Node[T]) RemoveDependent(nodeID string) {
	for i, dep := range n.Dependents {
		if dep == nodeID {
			n.Dependents = append(n.Dependents[:i], n.Dependents[i+1:]...)
			break
		}
	}
}

func (n *Node[T]) HasDependent(nodeID string) bool {
	for _, dep := range n.Dependents {
		if dep == nodeID {
			return true
		}
	}
	return false
}

func (n *Node[T]) IsLeaf() bool {
	return len(n.Dependents) == 0
}

func (n *Node[T]) IsRoot() bool {
	return len(n.Dependencies) == 0
}

type Graph[T any] struct {
	ID       string             `json:"id"       bson:"id"`
	Nodes    map[string]Node[T] `json:"nodes"    bson:"nodes"`
	adapters map[string]Adapter
}

func NewGraph[T any]() *Graph[T] {
	return &Graph[T]{
		Nodes: make(map[string]Node[T]),
	}
}

func (g *Graph[T]) AddNode(node Node[T]) error {
	if _, exists := g.Nodes[node.ID]; exists {
		return fmt.Errorf("node with ID %s already exists", node.ID)
	}
	g.Nodes[node.ID] = node
	return nil
}

func (g *Graph[T]) RemoveNode(nodeID string) error {
	node, exists := g.Nodes[nodeID]
	if !exists {
		return fmt.Errorf("node with ID %s does not exist", nodeID)
	}

	for _, depID := range node.Dependencies {
		if depNode, exists := g.Nodes[depID]; exists {
			depNode.RemoveDependent(nodeID)
		}
	}

	for _, depID := range node.Dependents {
		if depNode, exists := g.Nodes[depID]; exists {
			depNode.RemoveDependency(nodeID)
		}
	}

	delete(g.Nodes, nodeID)
	return nil
}

func (g *Graph[T]) GetNode(nodeID string) (*Node[T], error) {
	node, exists := g.Nodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("node with ID %s does not exist", nodeID)
	}
	return &node, nil
}

func (g *Graph[T]) AddEdge(fromID, toID string) error {
	fromNode, exists := g.Nodes[fromID]
	if !exists {
		return fmt.Errorf("source node %s does not exist", fromID)
	}

	toNode, exists := g.Nodes[toID]
	if !exists {
		return fmt.Errorf("target node %s does not exist", toID)
	}

	// Check if adding this edge would create a cycle
	if g.wouldCreateCycle(fromID, toID) {
		return fmt.Errorf("adding edge from %s to %s would create a cycle", fromID, toID)
	}

	// Add the dependency relationship
	toNode.AddDependency(fromID)
	fromNode.AddDependent(toID)

	return nil
}

// RemoveEdge removes a dependency relationship between two nodes
func (g *Graph[T]) RemoveEdge(fromID, toID string) error {
	fromNode, exists := g.Nodes[fromID]
	if !exists {
		return fmt.Errorf("source node %s does not exist", fromID)
	}

	toNode, exists := g.Nodes[toID]
	if !exists {
		return fmt.Errorf("target node %s does not exist", toID)
	}

	toNode.RemoveDependency(fromID)
	fromNode.RemoveDependent(toID)

	return nil
}

func (g *Graph[T]) wouldCreateCycle(fromID, toID string) bool {
	return g.hasPath(toID, fromID)
}

func (g *Graph[T]) hasPath(startID, endID string) bool {
	if startID == endID {
		return true
	}

	visited := make(map[string]bool)
	return g.dfsPath(startID, endID, visited)
}

func (g *Graph[T]) dfsPath(currentID, targetID string, visited map[string]bool) bool {
	if currentID == targetID {
		return true
	}

	visited[currentID] = true

	node, exists := g.Nodes[currentID]
	if !exists {
		return false
	}

	for _, depID := range node.Dependents {
		if !visited[depID] {
			if g.dfsPath(depID, targetID, visited) {
				return true
			}
		}
	}

	return false
}

func (g *Graph[T]) topologicalSort() ([]string, error) {
	// Kahn's algorithm
	inDegree := make(map[string]int)
	queue := make([]string, 0)
	result := make([]string, 0)

	// Calculate in-degrees for all nodes
	for nodeID := range g.Nodes {
		inDegree[nodeID] = len(g.Nodes[nodeID].Dependencies)
	}

	// Find all nodes with no incoming edges
	for nodeID, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, nodeID)
		}
	}

	// Process nodes
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		result = append(result, current)

		node := g.Nodes[current]
		for _, depID := range node.Dependents {
			inDegree[depID]--
			if inDegree[depID] == 0 {
				queue = append(queue, depID)
			}
		}
	}

	// Check if all nodes were processed (no cycles)
	if len(result) != len(g.Nodes) {
		return nil, fmt.Errorf("graph contains cycles")
	}

	return result, nil
}

// ValidateDAG validates that the graph is a proper DAG
func (g *Graph[T]) ValidateDAG() error {
	// Check for cycles using topological sort
	_, err := g.topologicalSort()
	if err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	// Check consistency of relationships
	for nodeID, node := range g.Nodes {
		// Verify that all dependencies exist
		for _, depID := range node.Dependencies {
			if _, exists := g.Nodes[depID]; !exists {
				return fmt.Errorf("node %s has dependency on non-existent node %s", nodeID, depID)
			}

			// Verify bidirectional relationship
			depNode := g.Nodes[depID]
			if !depNode.HasDependent(nodeID) {
				return fmt.Errorf(
					"inconsistent relationship: %s depends on %s but %s doesn't list %s as dependent",
					nodeID,
					depID,
					depID,
					nodeID,
				)
			}
		}

		// Verify that all dependents exist
		for _, depID := range node.Dependents {
			if _, exists := g.Nodes[depID]; !exists {
				return fmt.Errorf("node %s has dependent non-existent node %s", nodeID, depID)
			}

			// Verify bidirectional relationship
			depNode := g.Nodes[depID]
			if !depNode.HasDependency(nodeID) {
				return fmt.Errorf(
					"inconsistent relationship: %s lists %s as dependent but %s doesn't depend on %s",
					nodeID,
					depID,
					depID,
					nodeID,
				)
			}
		}
	}

	return nil
}

func (g *Graph[T]) GetNodesDict() map[string]*Node[T] {
	nodesMap := make(map[string]*Node[T])
	for _, node := range g.Nodes {
		nodesMap[node.ID] = &node
	}
	return nodesMap
}

// GetRootNodes returns all nodes with no dependencies
func (g *Graph[T]) GetRootNodes() []*Node[T] {
	roots := make([]*Node[T], 0)
	for _, node := range g.Nodes {
		if node.IsRoot() {
			roots = append(roots, &node)
		}
	}
	return roots
}

// GetLeafNodes returns all nodes with no dependents
func (g *Graph[T]) GetLeafNodes() []Node[T] {
	leaves := make([]Node[T], 0)
	for _, node := range g.Nodes {
		if node.IsLeaf() {
			leaves = append(leaves, node)
		}
	}
	return leaves
}

// Size returns the number of nodes in the graph
func (g *Graph[T]) GetSize() int {
	return len(g.Nodes)
}

// IsEmpty returns true if the graph has no nodes
func (g *Graph[T]) IsEmpty() bool {
	return len(g.Nodes) == 0
}

func (g *Graph[T]) GetFanOutEdges() map[string][]string {
	edges := make(map[string][]string)
	for nodeID, node := range g.Nodes {
		for _, depID := range node.Dependents {
			edges[depID] = append(edges[depID], nodeID)
		}
	}
	return edges
}

func (g *Graph[T]) GetFanInEdges() map[string][]string {
	edges := make(map[string][]string)
	for nodeID, node := range g.Nodes {
		for _, depID := range node.Dependencies {
			edges[nodeID] = append(edges[nodeID], depID)
		}
	}
	return edges
}
