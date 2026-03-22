package respositories

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/lynnphayu/dag-runner/pkg/dag"
)

const defaultHTTPTimeout = 30 * time.Second

type Http struct {
	client *http.Client
}

func NewHttp() (*Http, error) {
	return &Http{
		client: &http.Client{
			Timeout: defaultHTTPTimeout,
		},
	}, nil
}

func addQueryValue(values url.Values, key string, value interface{}) {
	if value == nil {
		return
	}

	switch typed := value.(type) {
	case []string:
		for _, item := range typed {
			values.Add(key, item)
		}
	case []interface{}:
		for _, item := range typed {
			if item == nil {
				continue
			}
			values.Add(key, fmt.Sprintf("%v", item))
		}
	default:
		values.Set(key, fmt.Sprintf("%v", value))
	}
}

func (r *Http) buildRequestURL(path string, query map[string]interface{}) (*url.URL, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("url is empty")
	}

	parsed, err := url.ParseRequestURI(path)
	if err != nil {
		return nil, fmt.Errorf("invalid URL format: %w", err)
	}

	if len(query) == 0 {
		return parsed, nil
	}

	values := parsed.Query()
	for key, value := range query {
		addQueryValue(values, key, value)
	}
	parsed.RawQuery = values.Encode()

	return parsed, nil
}

func (r *Http) buildRequestBody(body map[string]interface{}) ([]byte, error) {
	if len(body) == 0 {
		return nil, nil
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
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

func (r *Http) parseResponseBody(resp *http.Response) (interface{}, error) {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode == http.StatusNoContent || len(bytes.TrimSpace(body)) == 0 {
		return nil, nil
	}

	contentType := strings.ToLower(resp.Header.Get("Content-Type"))
	if strings.Contains(contentType, "application/json") || strings.Contains(contentType, "+json") || json.Valid(body) {
		var respBody interface{}
		if err := json.Unmarshal(body, &respBody); err != nil {
			return nil, fmt.Errorf("failed to decode JSON response body: %w", err)
		}
		return respBody, nil
	}

	return string(body), nil
}

func (r *Http) Get(ctx context.Context, path string, query map[string]interface{}, headers map[string]string) (*dag.ParsedResponse, error) {
	return r.execute(ctx, http.MethodGet, path, query, nil, headers)
}

func (r *Http) Post(ctx context.Context, path string, query map[string]interface{}, body map[string]interface{}, headers map[string]string) (*dag.ParsedResponse, error) {
	return r.execute(ctx, http.MethodPost, path, query, body, headers)
}

func (r *Http) Put(ctx context.Context, path string, query map[string]interface{}, body map[string]interface{}, headers map[string]string) (*dag.ParsedResponse, error) {
	return r.execute(ctx, http.MethodPut, path, query, body, headers)
}

func (r *Http) Delete(ctx context.Context, path string, query map[string]interface{}, headers map[string]string) (*dag.ParsedResponse, error) {
	return r.execute(ctx, http.MethodDelete, path, query, nil, headers)
}

func (r *Http) Patch(ctx context.Context, path string, query map[string]interface{}, body map[string]interface{}, headers map[string]string) (*dag.ParsedResponse, error) {
	return r.execute(ctx, http.MethodPatch, path, query, body, headers)
}

func (r *Http) execute(ctx context.Context, method string, path string, query map[string]interface{}, body map[string]interface{}, headers map[string]string) (*dag.ParsedResponse, error) {
	parsedURL, err := r.buildRequestURL(path, query)
	if err != nil {
		return nil, fmt.Errorf("failed to build request URL: %w", err)
	}

	jsonBody, err := r.buildRequestBody(body)
	if err != nil {
		return nil, fmt.Errorf("failed to build request body: %w", err)
	}

	reqHeaders := r.buildHeaders(headers)

	if ctx == nil {
		ctx = context.Background()
	}

	req, err := http.NewRequestWithContext(ctx, method, parsedURL.String(), bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	if len(jsonBody) > 0 && reqHeaders.Get("Content-Type") == "" {
		reqHeaders.Set("Content-Type", "application/json")
	}
	req.Header = reqHeaders

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := r.parseResponseBody(resp)
	if err != nil {
		return nil, err
	}

	return &dag.ParsedResponse{
		Data:       respBody,
		Raw:        resp,
		StatusCode: resp.StatusCode,
	}, nil
}
