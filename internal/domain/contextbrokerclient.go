package domain

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/tracing"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("context-broker-client")

type ContextBrokerClient interface {
	Post(ctx context.Context, entity interface{}) error
}

type contextBrokerClient struct {
	baseUrl string
	log     zerolog.Logger
}

func (c *contextBrokerClient) Post(ctx context.Context, entity interface{}) error {
	var err error
	ctx, span := tracer.Start(ctx, "create-entity")
	defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()

	httpClient := http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}

	parsedUrl, err := url.Parse(c.baseUrl + "/ngsi-ld/v1/entities")
	if err != nil {
		c.log.Err(err).Msg("unable to parse URL to context broker")
		return err
	}

	body, err := json.Marshal(entity)
	if err != nil {
		c.log.Err(err).Msg("unable to marshal entity to json")
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, parsedUrl.String(), bytes.NewReader(body))
	if err != nil {
		c.log.Error().Err(err).Msg("failed to create http request")
		return err
	}

	req.Header.Add("Content-Type", "application/ld+json")

	dump, err := httputil.DumpRequest(req, true)
	if err != nil {
		c.log.Debug().Msg(string(dump))
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		c.log.Error().Msgf("unable to store entity: %s", err.Error())
		return err
	}

	if resp.StatusCode != http.StatusCreated {
		c.log.Error().Msgf("request failed with status code %d, expected 201 (created)", resp.StatusCode)
		return fmt.Errorf("request failed, unable to store entity")
	}

	return nil
}

func NewContextBrokerClient(baseUrl string, log zerolog.Logger) ContextBrokerClient {
	return &contextBrokerClient{
		baseUrl: baseUrl,
		log:     log,
	}
}
