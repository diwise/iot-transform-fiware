package contextbroker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild"
	ngsierrors "github.com/diwise/context-broker/pkg/ngsild/errors"
	"github.com/diwise/context-broker/pkg/ngsild/types"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/tracing"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/oauth2"
)

var tracer = otel.Tracer("iot-transform-fiware/contextbroker")

func endSpan(err error, span trace.Span) { tracing.RecordAnyErrorAndEndSpan(err, span) }

type HTTPError struct {
	Method string
	Status int
	Body   []byte
	target error
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("broker %s returned HTTP %d", e.Method, e.Status)
}
func (e *HTTPError) Unwrap() error { return e.target }

type PartialUpdateError struct {
	Result ngsild.UpdateEntityAttributesResult
	Body   []byte
}

func (e *PartialUpdateError) Error() string {
	return fmt.Sprintf("broker partially updated attributes: updated=%d failed=%d", len(e.Result.Updated), len(e.Result.NotUpdated))
}

type httpBrokerClient struct {
	baseURL, tenant, userAgent string
	http                       *http.Client
	tokens                     *tokenProvider
	budget                     time.Duration
}

const maxResponseBody = 1024 * 1024

func (c *httpBrokerClient) request(ctx context.Context, method, path, id string, body []byte, headers map[string][]string) (response *http.Response, content []byte, err error) {
	ctx, span := tracer.Start(ctx, "broker-request", trace.WithAttributes(attribute.String("http.request.method", method), attribute.String("tenant", c.tenant), attribute.String("entity_id", id)))
	defer func() { endSpan(err, span) }()
	ctx, cancel := context.WithTimeout(ctx, c.budget)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, nil, fmt.Errorf("invalid broker request URL")
	}
	for key, values := range headers {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	req.Header.Set("User-Agent", c.userAgent)
	if req.Header.Get("Content-Type") == "" && body != nil {
		req.Header.Set("Content-Type", "application/ld+json")
	}
	if c.tenant != entities.DefaultNGSITenant {
		req.Header.Set("NGSILD-Tenant", c.tenant)
	}
	var usedToken *oauth2.Token
	if c.tokens != nil {
		token, tokenErr := c.tokens.token(ctx)
		if tokenErr != nil {
			return nil, nil, fmt.Errorf("prepare broker authorization: %w", tokenErr)
		}
		token.SetAuthHeader(req)
		usedToken = token
	}
	response, err = c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		return nil, nil, fmt.Errorf("broker transport failed")
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusUnauthorized && c.tokens != nil {
		c.tokens.invalidate(ctx, usedToken)
	}
	span.SetAttributes(attribute.Int("http.response.status_code", response.StatusCode))
	content, err = io.ReadAll(io.LimitReader(response.Body, maxResponseBody+1))
	if err != nil {
		if ctx.Err() != nil {
			return response, nil, ctx.Err()
		}
		return response, nil, fmt.Errorf("read broker response failed")
	}
	if len(content) > maxResponseBody {
		return response, nil, fmt.Errorf("broker response exceeds size limit")
	}
	return response, content, nil
}

func statusError(method string, status int, body []byte) error {
	var target error
	switch status {
	case 404:
		target = ngsierrors.ErrNotFound
	case 409:
		target = ngsierrors.ErrAlreadyExists
	case 400, 422:
		target = ngsierrors.ErrBadRequest
	}
	return &HTTPError{Method: method, Status: status, Body: append([]byte(nil), body...), target: target}
}

func (c *httpBrokerClient) CreateEntity(ctx context.Context, entity types.Entity, headers map[string][]string) (result *ngsild.CreateEntityResult, err error) {
	ctx, span := tracer.Start(ctx, "create-entity")
	defer func() { endSpan(err, span) }()
	body, err := entity.MarshalJSON()
	if err != nil {
		return nil, err
	}
	response, content, err := c.request(ctx, http.MethodPost, "/ngsi-ld/v1/entities", entity.ID(), body, headers)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusCreated {
		return nil, statusError(http.MethodPost, response.StatusCode, content)
	}
	location := response.Header.Get("Location")
	if location == "" {
		location = "/ngsi-ld/v1/entities/" + url.PathEscape(entity.ID())
	}
	return ngsild.NewCreateEntityResult(location), nil
}

func (c *httpBrokerClient) MergeEntity(ctx context.Context, id string, fragment types.EntityFragment, headers map[string][]string) (result *ngsild.MergeEntityResult, err error) {
	ctx, span := tracer.Start(ctx, "merge-entity")
	defer func() { endSpan(err, span) }()
	body, err := fragment.MarshalJSON()
	if err != nil {
		return nil, err
	}
	response, content, err := c.request(ctx, http.MethodPatch, "/ngsi-ld/v1/entities/"+url.PathEscape(id), id, body, headers)
	if err != nil {
		return nil, err
	}
	if response.StatusCode == http.StatusNoContent {
		return &ngsild.MergeEntityResult{}, nil
	}
	if response.StatusCode != http.StatusMultiStatus {
		return nil, statusError(http.MethodPatch, response.StatusCode, content)
	}
	var report ngsild.UpdateEntityAttributesResult
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&report); err != nil || len(report.Updated)+len(report.NotUpdated) == 0 {
		return nil, fmt.Errorf("broker returned unverifiable HTTP 207 update report")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("broker returned multiple HTTP 207 documents")
	}
	seen := map[string]bool{}
	for _, name := range report.Updated {
		if name == "" || seen[name] {
			return nil, fmt.Errorf("broker returned malformed HTTP 207 updated list")
		}
		seen[name] = true
	}
	for _, failed := range report.NotUpdated {
		if failed.AttributeName == "" {
			return nil, fmt.Errorf("broker returned malformed HTTP 207 attribute failure")
		}
	}
	if len(report.NotUpdated) > 0 {
		return nil, &PartialUpdateError{Result: report, Body: append([]byte(nil), content...)}
	}
	var requested map[string]any
	_ = json.Unmarshal(body, &requested)
	delete(requested, "@context")
	if len(report.Updated) != len(requested) {
		return nil, fmt.Errorf("broker HTTP 207 does not confirm all requested attributes")
	}
	return &ngsild.MergeEntityResult{}, nil
}

func (c *httpBrokerClient) DeleteEntity(ctx context.Context, id string) (result *ngsild.DeleteEntityResult, err error) {
	ctx, span := tracer.Start(ctx, "delete-entity")
	defer func() { endSpan(err, span) }()
	response, content, err := c.request(ctx, http.MethodDelete, "/ngsi-ld/v1/entities/"+url.PathEscape(id), id, nil, nil)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusNoContent {
		return nil, statusError(http.MethodDelete, response.StatusCode, content)
	}
	return ngsild.NewDeleteEntityResult(), nil
}

func PermanentBrokerError(err error) bool {
	if response, ok := errors.AsType[*HTTPError](err); ok {
		if response.Status == 400 || response.Status == 422 || response.Status == 413 {
			var report struct {
				Type string `json:"type"`
			}
			_ = json.Unmarshal(response.Body, &report)
			return report.Type != "https://uri.etsi.org/ngsi-ld/errors/NonexistentTenant"
		}
	}
	return false
}
