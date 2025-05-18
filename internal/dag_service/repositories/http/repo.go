package respositories

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/lynnphayu/dag-runner/pkg/dag"
)

// Postgres handles database operations for the DAG executor
type Http struct {
	client *http.Client
}

func NewHttp() (*Http, error) {
	client := http.DefaultClient
	return &Http{
		client,
	}, nil
}

func (r *Http) buildRequestURL(method string, path string, query map[string]interface{}) (*url.URL, error) {
	// validate url
	if path == "" {
		return nil, fmt.Errorf("url is empty")
	}
	// Check if URL is valid
	parsed, err := url.ParseRequestURI(path)
	if err != nil {
		return nil, fmt.Errorf("invalid URL format: %v", err)
	}
	for key, value := range query {
		parsed.Query().Set(key, fmt.Sprintf("%v", value))
	}
	return parsed, nil
}

func (r *Http) buildRequestBody(method string, body map[string]interface{}) ([]byte, error) {
	// If no body provided for methods that don't typically have a body, return nil
	if body == nil || len(body) == 0 {
		if method == http.MethodGet || method == http.MethodDelete {
			return nil, nil
		}
	}
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %v", err)
	}

	return jsonBody, nil
}

func (r *Http) buildHeaders(headers map[string]string) http.Header {
	reqHeaders := http.Header{}
	for key, value := range headers {
		reqHeaders.Set(key, value)
	}
	return reqHeaders
}

func (r *Http) Get(path string, query map[string]interface{}, headers map[string]string) (*dag.ParsedResponse, error) {
	return r.execute(http.MethodGet, path, query, nil, headers)
}

func (r *Http) Post(path string, query map[string]interface{}, body map[string]interface{}, headers map[string]string) (*dag.ParsedResponse, error) {
	return r.execute(http.MethodPost, path, query, body, headers)
}

func (r *Http) Put(path string, query map[string]interface{}, body map[string]interface{}, headers map[string]string) (*dag.ParsedResponse, error) {
	return r.execute(http.MethodPut, path, query, body, headers)
}

func (r *Http) Delete(path string, query map[string]interface{}, headers map[string]string) (*dag.ParsedResponse, error) {
	return r.execute(http.MethodDelete, path, query, nil, headers)
}

func (r *Http) Patch(path string, query map[string]interface{}, body map[string]interface{}, headers map[string]string) (*dag.ParsedResponse, error) {
	return r.execute(http.MethodPatch, path, query, body, headers)
}

func (r *Http) execute(method string, path string, query map[string]interface{}, body map[string]interface{}, headers map[string]string) (*dag.ParsedResponse, error) {
	// Build the request URL
	parsedURL, err := r.buildRequestURL(method, path, query)
	if err != nil {
		return nil, fmt.Errorf("failed to build request URL: %v", err)
	}
	// Build the request body
	jsonBody, err := r.buildRequestBody(method, body)
	if err != nil {
		return nil, fmt.Errorf("failed to build request body: %v", err)

	}
	reqHeaders := r.buildHeaders(headers)
	req, err := http.NewRequest(method, parsedURL.String(), bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}
	reqHeaders.Set("Content-Type", "application/json")
	req.Header = reqHeaders
	// Send the request
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	// Read the response body
	var respBody interface{}
	err = json.NewDecoder(resp.Body).Decode(&respBody)
	if err != nil {
		return nil, fmt.Errorf("failed to decode response body: %v", err)
	}

	return &dag.ParsedResponse{
		Data:       respBody,
		Raw:        resp,
		StatusCode: resp.StatusCode,
	}, nil
}
