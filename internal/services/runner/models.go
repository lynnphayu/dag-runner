package runner

import dag "github.com/lynnphayu/dag-runner/pkg/dag"

type ExecuteRequest struct {
	DAG   dag.DAG                `json:"dag"`
	Input map[string]interface{} `json:"input"`
}
