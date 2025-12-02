package runner

import dag "github.com/lynnphayu/dag-runner/pkg/dag"

type ExecuteRequest struct {
	DAG   dag.Graph[dag.Action, any] `json:"dag"`
	Input map[string]interface{}     `json:"input"`
}
