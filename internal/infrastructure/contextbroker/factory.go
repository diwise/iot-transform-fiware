package contextbroker

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

type ContextBrokerClientFactoryFunc func(string) client.ContextBrokerClient

func NewContextBrokerClientFactory(ctx context.Context, contextBrokerUrl, serviceName, serviceVersion, oauth2ClientId, oauth2ClientSecret, oauth2TokenUrl string, oauthInsecureURL bool) ContextBrokerClientFactoryFunc {
	log := logging.GetFromContext(ctx)

	var tokenSource oauth2.TokenSource

	if oauth2ClientId != "" && oauth2ClientSecret != "" && oauth2TokenUrl != "" {
		oauthConfig := &clientcredentials.Config{
			ClientID:     oauth2ClientId,
			ClientSecret: oauth2ClientSecret,
			TokenURL:     oauth2TokenUrl,
		}

		httpTransport := http.DefaultTransport
		if trans, ok := httpTransport.(*http.Transport); ok {
			cloned := trans.Clone()
			if oauthInsecureURL {
				if cloned.TLSClientConfig == nil {
					cloned.TLSClientConfig = &tls.Config{}
				}
				cloned.TLSClientConfig.InsecureSkipVerify = true
			}
			httpTransport = cloned
		}

		httpClient := &http.Client{
			Transport: otelhttp.NewTransport(httpTransport),
		}

		ctx = context.WithValue(ctx, oauth2.HTTPClient, httpClient)

		tokenSource = oauthConfig.TokenSource(ctx)
	}

	return func(tenant string) client.ContextBrokerClient {
		if tokenSource != nil {
			token, err := tokenSource.Token()
			if err != nil {
				log.Error("failed to retrieve oauth2 token, continuing without authorization header", "err", err.Error())
			} else {
				return client.NewContextBrokerClient(
					contextBrokerUrl,
					client.Tenant(tenant),
					client.UserAgent(fmt.Sprintf("%s/%s", serviceName, serviceVersion)),
					client.RequestHeader("Authorization", []string{fmt.Sprintf("%s %s", token.TokenType, token.AccessToken)}),
				)
			}
		}

		return client.NewContextBrokerClient(
			contextBrokerUrl,
			client.Tenant(tenant),
			client.UserAgent(fmt.Sprintf("%s/%s", serviceName, serviceVersion)),
		)
	}
}
