package main

import (
	"encoding/json"
	"log"
	"os"

	"github.com/lynnphayu/dag-runner/internal/services/runner"
	"github.com/lynnphayu/dag-runner/pkg/dag"
	"github.com/spf13/cobra"
)

func main() {

	// Initialize CLI commands
	rootCmd := &cobra.Command{
		Use:   "runner",
		Short: "DAG Runner CLI",
		Long:  "Command line interface for managing DAG workflows",
	}

	// Start server command
	startCmd := &cobra.Command{
		Use:   "start",
		Short: "Run DAG flow",
		Run: func(cmd *cobra.Command, args []string) {
			connStr, err := cmd.Flags().GetString("postgres")
			if err != nil {
				log.Fatalf("Failed to get connection string: %v", err)
			}
			if connStr == "" {
				log.Fatal("Connection string is required")
			}

			dagFile, err := cmd.Flags().GetString("file")
			if err != nil {
				log.Fatalf("Failed to get DAG file name: %v", err)
			}
			if dagFile == "" {
				log.Fatal("DAG file name is required")
			}

			input, err := cmd.Flags().GetString("input")
			if err != nil {
				log.Fatalf("Failed to get input: %v", err)
			}
			if input == "" {
				log.Fatal("Input is required")
			}

			var jsonData map[string]interface{}
			err = json.Unmarshal([]byte(input), &jsonData)
			if err != nil {
				log.Fatalf("Failed to parse input as JSON: %v", err)
			}
			dagContent, err := os.ReadFile(dagFile)
			if err != nil {
				log.Fatalf("Failed to read DAG file: %v", err)
			}

			var dag dag.DAG
			err = json.Unmarshal(dagContent, &dag)
			if err != nil {
				log.Fatalf("Failed to parse DAG file as JSON: %v", err)
			}

			runnerService := runner.NewRunnerService(connStr)

			log.Println(dag, jsonData)
			result, err := runnerService.Execute(&dag, jsonData)
			if err != nil {
				log.Fatalf("Failed to execute DAG: %v", err)
			}

			jsonResult, err := json.Marshal(result)
			if err != nil {
				log.Fatalf("Failed to marshal result to JSON: %v", err)
			}

			// Print the result to stdout
			log.Printf("Execution completed successfully. Result: %s", string(jsonResult))
		},
	}

	startCmd.Flags().StringP("file", "f", "", "DAG json file to execute")
	startCmd.Flags().StringP("postgres", "p", "", "Postgres connection string for db")
	startCmd.Flags().StringP("input", "i", "", "Input json according to dag provided")

	// Add commands to root
	rootCmd.AddCommand(startCmd)

	// Execute CLI
	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Failed to execute command: %v", err)
		os.Exit(1)
	}
}
