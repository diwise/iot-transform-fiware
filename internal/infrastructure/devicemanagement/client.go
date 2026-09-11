// Package devicemanagement slår upp en enhets sensortyp via
// iot-device-management och cachar resultatet med en TTL.
package devicemanagement

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// DefaultTTL är hur länge en uppslagen sensortyp cachas.
const DefaultTTL = 5 * time.Minute

// ErrNotFound returnerar när enheten inte finns i device-management.
var ErrNotFound = errors.New("device not found")

// SensorTypeResolver slår upp en enhets sensortyp.
type SensorTypeResolver interface {
	SensorType(ctx context.Context, deviceID string) (string, error)
}

type cacheEntry struct {
	sensorType string
	expiresAt  time.Time
}

// Client är en HTTP-klient mot iot-device-management med TTL-cache.
type Client struct {
	baseURL string
	http    *http.Client
	ttl     time.Duration
	now     func() time.Time

	mu    sync.Mutex
	cache map[string]cacheEntry
}

// New skapar en klient. ttl <= 0 använder DefaultTTL.
func New(baseURL string, httpClient *http.Client, ttl time.Duration) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	return &Client{
		baseURL: baseURL,
		http:    httpClient,
		ttl:     ttl,
		now:     time.Now,
		cache:   map[string]cacheEntry{},
	}
}

// NewOAuth2Client returnerar en http-klient som autentiserar med
// client_credentials mot tokenURL. Tomma uppgifter ger nil, så att anroparen
// kan falla tillbaka på en oautentiserad klient.
func NewOAuth2Client(ctx context.Context, tokenURL, clientID, clientSecret string, insecureURL bool) *http.Client {
	if tokenURL == "" || clientID == "" || clientSecret == "" {
		return nil
	}

	httpTransport := http.DefaultTransport
	if trans, ok := httpTransport.(*http.Transport); ok {
		cloned := trans.Clone()
		if insecureURL {
			if cloned.TLSClientConfig == nil {
				cloned.TLSClientConfig = &tls.Config{}
			}
			cloned.TLSClientConfig.InsecureSkipVerify = true
		}
		httpTransport = cloned
	}

	tokenClient := &http.Client{
		Transport: otelhttp.NewTransport(httpTransport),
		Timeout:   10 * time.Second,
	}

	ctx = context.WithValue(ctx, oauth2.HTTPClient, tokenClient)

	oauthConfig := &clientcredentials.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		TokenURL:     tokenURL,
	}

	return oauthConfig.Client(ctx)
}

// SensorType returnerar enhetens sensortyp. Uppslag cachas i ttl. Vid
// device-management-fel cachas inget, så nästa anrop försöker igen.
func (c *Client) SensorType(ctx context.Context, deviceID string) (string, error) {
	if st, ok := c.lookup(deviceID); ok {
		return st, nil
	}

	st, err := c.fetch(ctx, deviceID)
	if err != nil {
		return "", err
	}

	c.store(deviceID, st)
	return st, nil
}

func (c *Client) lookup(deviceID string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.cache[deviceID]
	if !ok || !c.now().Before(entry.expiresAt) {
		return "", false
	}
	return entry.sensorType, true
}

func (c *Client) store(deviceID, sensorType string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache[deviceID] = cacheEntry{
		sensorType: sensorType,
		expiresAt:  c.now().Add(c.ttl),
	}
}

func (c *Client) fetch(ctx context.Context, deviceID string) (string, error) {
	deviceURL := c.baseURL + "/api/v0/devices/" + url.PathEscape(deviceID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, deviceURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create device request: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to retrieve device %s: %w", deviceID, err)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusNotFound:
		return "", fmt.Errorf("%w: %s", ErrNotFound, deviceID)
	case resp.StatusCode != http.StatusOK:
		return "", fmt.Errorf("device management returned status %d for %s", resp.StatusCode, deviceID)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read device response: %w", err)
	}

	payload := struct {
		Data struct {
			SensorProfile *struct {
				Decoder string `json:"decoder"`
			} `json:"sensorProfile"`
		} `json:"data"`
	}{}

	if err := json.Unmarshal(body, &payload); err != nil {
		return "", fmt.Errorf("failed to decode device response: %w", err)
	}

	if payload.Data.SensorProfile == nil {
		return "", nil
	}

	return payload.Data.SensorProfile.Decoder, nil
}
