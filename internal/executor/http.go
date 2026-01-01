package executor

import (
	"context"
	"fmt"
	"net/http"

	"resty.dev/v3"

	"github.com/mbeoliero/scheduler/domain/entity"
)

type HttpExecutor struct {
	client *resty.Client
}

func NewHttpExecutor() *HttpExecutor {
	return &HttpExecutor{
		client: resty.New(),
	}
}

func (e *HttpExecutor) Execute(ctx context.Context, config *entity.JobPayload) (*Result, error) {
	if config.Http == nil {
		return nil, fmt.Errorf("http payload is nil")
	}

	httpConfig := config.Http
	req := e.client.R().SetContext(ctx)

	if len(httpConfig.Headers) > 0 {
		req.SetHeaders(httpConfig.Headers)
	}

	if len(httpConfig.Queries) > 0 {
		req.SetQueryParams(httpConfig.Queries)
	}

	if httpConfig.Body != "" {
		req.SetBody(httpConfig.Body)
	}

	var resp *resty.Response
	var err error

	switch httpConfig.Method {
	case http.MethodGet:
		resp, err = req.Get(httpConfig.Url)
	case http.MethodPost:
		resp, err = req.Post(httpConfig.Url)
	case http.MethodPut:
		resp, err = req.Put(httpConfig.Url)
	case http.MethodDelete:
		resp, err = req.Delete(httpConfig.Url)
	case http.MethodPatch:
		resp, err = req.Patch(httpConfig.Url)
	default:
		return nil, fmt.Errorf("unsupported http method: %s", httpConfig.Method)
	}

	if err != nil {
		return &Result{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	success := resp.IsSuccess()
	result := &Result{
		Success: success,
		Output:  string(resp.Bytes()),
	}

	if !success {
		result.Error = fmt.Sprintf("http status: %s", resp.Status())
	}

	return result, nil
}
