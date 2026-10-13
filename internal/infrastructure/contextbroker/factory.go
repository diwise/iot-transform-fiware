package contextbroker

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild"
	"github.com/diwise/context-broker/pkg/ngsild/types"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

type BrokerClient interface {
	CreateEntity(context.Context, types.Entity, map[string][]string) (*ngsild.CreateEntityResult, error)
	MergeEntity(context.Context, string, types.EntityFragment, map[string][]string) (*ngsild.MergeEntityResult, error)
	DeleteEntity(context.Context, string) (*ngsild.DeleteEntityResult, error)
}

type ContextBrokerClientFactoryFunc func(string) (BrokerClient, error)

const DefaultBrokerTimeout = 10 * time.Second
const DefaultTokenTimeout = 5 * time.Second

type FactoryOption func(*factorySettings)
type factorySettings struct{ brokerTimeout, tokenTimeout time.Duration }

func WithTimeouts(broker, token time.Duration) FactoryOption {
	return func(s *factorySettings) { s.brokerTimeout, s.tokenTimeout = broker, token }
}

// Construction performs no I/O. Token refresh uses each write's bounded
// request context, not the lifecycle context passed to the constructor.
func NewContextBrokerClientFactory(_ context.Context, brokerURL, serviceName, serviceVersion, clientID, secret, tokenURL string, insecureOAuth bool, options ...FactoryOption) ContextBrokerClientFactoryFunc {
	s := factorySettings{DefaultBrokerTimeout, DefaultTokenTimeout}
	for _, option := range options {
		option(&s)
	}
	brokerHTTP := &http.Client{Timeout: s.brokerTimeout, Transport: otelhttp.NewTransport(http.DefaultTransport), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	var tokens *tokenProvider
	if clientID != "" && secret != "" && tokenURL != "" {
		transport := http.DefaultTransport
		if base, ok := transport.(*http.Transport); ok {
			cloned := base.Clone()
			if insecureOAuth {
				if cloned.TLSClientConfig == nil {
					cloned.TLSClientConfig = &tls.Config{}
				}
				cloned.TLSClientConfig.InsecureSkipVerify = true
			}
			transport = cloned
		}
		tokens = &tokenProvider{config: clientcredentials.Config{ClientID: clientID, ClientSecret: secret, TokenURL: tokenURL}, http: &http.Client{Timeout: s.tokenTimeout, Transport: otelhttp.NewTransport(transport), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, budget: s.tokenTimeout, gate: make(chan struct{}, 1)}
	}
	return func(tenant string) (BrokerClient, error) {
		if s.brokerTimeout <= 0 || s.tokenTimeout <= 0 {
			return nil, fmt.Errorf("broker/token timeouts must be positive")
		}
		return &httpBrokerClient{baseURL: strings.TrimRight(brokerURL, "/"), tenant: tenant, userAgent: serviceName + "/" + serviceVersion, http: brokerHTTP, tokens: tokens, budget: s.brokerTimeout}, nil
	}
}

type tokenProvider struct {
	config clientcredentials.Config
	http   *http.Client
	budget time.Duration
	gate   chan struct{}
	cached *oauth2.Token
}

func (p *tokenProvider) token(ctx context.Context) (token *oauth2.Token, err error) {
	ctx, span := tracer.Start(ctx, "get-token")
	defer func() { endSpan(err, span) }()
	ctx, cancel := context.WithTimeout(ctx, p.budget)
	defer cancel()
	select {
	case p.gate <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-p.gate }()
	if p.cached.Valid() {
		return p.cached, nil
	}
	ctx = context.WithValue(ctx, oauth2.HTTPClient, p.http)
	token, err = p.config.Token(ctx)
	if err != nil {
		// RetrieveError may contain token response bodies/credentials; retain
		// only cancellation cause or a sanitized dependency error.
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("oauth2 token retrieval failed")
	}
	p.cached = token
	return token, nil
}

func (p *tokenProvider) invalidate(ctx context.Context, used *oauth2.Token) {
	select {
	case p.gate <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-p.gate }()
	if p.cached != nil && used != nil && p.cached.AccessToken == used.AccessToken {
		p.cached = nil
	}
}
