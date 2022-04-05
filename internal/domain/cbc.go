package domain

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

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
	ctx, span := tracer.Start(ctx, "upload-entity")
	defer func() {
		if err != nil {
			span.RecordError(err)
		}
		span.End()
	}()

	httpClient := http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}

	url := c.baseUrl + "/ngsi-ld/v1/entities/"

	body, err := json.Marshal(entity)
	if err != nil {
		c.log.Err(err).Msg("unable to marshal entity to json")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		c.log.Error().Err(err).Msg("failed to create http request")
		return err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		c.log.Error().Msgf("unable to store entity: %s", err.Error())
		return err
	}

	if resp.StatusCode != http.StatusCreated {
		c.log.Error().Msgf("request failed with status code %d", resp.StatusCode)
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
