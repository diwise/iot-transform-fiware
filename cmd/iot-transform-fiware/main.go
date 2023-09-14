package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/iot-transform-fiware/internal/pkg/infrastructure/router"
	transformfiware "github.com/diwise/iot-transform-fiware/pkg/application"

	"github.com/diwise/messaging-golang/pkg/messaging"

	"github.com/diwise/service-chassis/pkg/infrastructure/buildinfo"
	"github.com/diwise/service-chassis/pkg/infrastructure/env"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	infra "github.com/diwise/service-chassis/pkg/infrastructure/router"

	"github.com/rs/zerolog"
)

const serviceName string = "iot-transform-fiware"

func main() {
	serviceVersion := buildinfo.SourceVersion()

	ctx, logger, cleanup := o11y.Init(context.Background(), serviceName, serviceVersion)
	defer cleanup()

	contextBrokerUrl := env.GetVariableOrDie(logger, "NGSI_CB_URL", "URL to ngsi-ld context broker")
	messenger := createMessagingContextOrDie(ctx, logger)
	r := createRouterAndRegisterHealthEndpoint()

	factory := newContextBrokerClientFactory(contextBrokerUrl, serviceName, serviceVersion)

	tfw := transformfiware.New(ctx, r, messenger, factory)
	tfw.Start()

	servicePort := env.GetVariableOrDefault(logger, "SERVICE_PORT", "8080")
	err := http.ListenAndServe(":"+servicePort, r.Router())
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to start request router")
	}
}

func createMessagingContextOrDie(ctx context.Context, logger zerolog.Logger) messaging.MsgContext {
	config := messaging.LoadConfiguration(serviceName, logger)
	messenger, err := messaging.Initialize(config)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to init messaging")
	}

	return messenger
}

func createRouterAndRegisterHealthEndpoint() infra.Router {
	r := router.New(serviceName)

	r.Router().Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	return r
}

type ContextBrokerClientFactoryFunc func(string) client.ContextBrokerClient

func newContextBrokerClientFactory(contextBrokerUrl, serviceName, serviceVersion string) ContextBrokerClientFactoryFunc {

	type request struct {
		tenant string
		result chan client.ContextBrokerClient
	}

	requestQueue := make(chan request)

	go func() {
		clients := map[string]client.ContextBrokerClient{}

		for r := range requestQueue {
			c, ok := clients[r.tenant]

			if !ok {
				c = client.NewContextBrokerClient(
					contextBrokerUrl,
					client.Tenant(r.tenant),
					client.UserAgent(fmt.Sprintf("%s/%s", serviceName, serviceVersion)),
				)
				clients[r.tenant] = c
			}

			r.result <- c
		}
	}()

	return func(tenant string) client.ContextBrokerClient {
		r := request{tenant: tenant, result: make(chan client.ContextBrokerClient)}
		requestQueue <- r
		return <-r.result
	}
}
