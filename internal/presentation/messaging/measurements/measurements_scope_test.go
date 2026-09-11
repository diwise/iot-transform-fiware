package measurements

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/diwise/context-broker/pkg/ngsild"
	"github.com/diwise/context-broker/pkg/ngsild/client"
	ngsierrors "github.com/diwise/context-broker/pkg/ngsild/errors"
	"github.com/diwise/context-broker/pkg/ngsild/types"
	clienttest "github.com/diwise/context-broker/pkg/test"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/matryer/is"
)

// Fas D: ett pack med två observationer (luftkvalitet + ljudnivå) ska ge en
// transformering per observation – resurser får aldrig läsas från fel objekt.
func TestMultiObservationTransformsEach(t *testing.T) {
	is := is.New(t)

	cbClient := &clienttest.ContextBrokerClientMock{
		CreateEntityFunc: func(ctx context.Context, entity types.Entity, headers map[string][]string) (*ngsild.CreateEntityResult, error) {
			return ngsild.NewCreateEntityResult("ignored"), nil
		},
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			return &ngsild.MergeEntityResult{}, ngsierrors.ErrNotFound
		},
		RetrieveEntityFunc: func(ctx context.Context, entityID string, headers map[string][]string) (types.Entity, error) {
			return nil, ngsierrors.ErrNotFound
		},
	}

	var tenantSeen string
	factory := func(tenant string) (client.ContextBrokerClient, error) {
		tenantSeen = tenant
		return cbClient, nil
	}

	body := `{"pack":[
		{"bn":"dev1/3428/","bt":1720000000,"n":"0","vs":"urn:oma:lwm2m:ext:3428"},
		{"n":"5700","u":"Cel","v":22.2},
		{"n":"17","u":"ppm","v":800},
		{"bn":"dev1/3324/","bt":1720000000,"n":"0","vs":"urn:oma:lwm2m:ext:3324"},
		{"n":"5700","u":"dB","v":55},
		{"bn":"dev1/","n":"tenant","vs":"acme"}
	],"timestamp":"2024-07-03T09:46:40Z"}`

	msg := &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return []byte(body) },
		TopicNameFunc:   func() string { return "message.accepted" },
		ContentTypeFunc: func() string { return "application/vnd.oma.lwm2m+json" },
	}

	handler := NewMeasurementTopicMessageHandler(factory)
	is.NoErr(handler(context.Background(), msg, slog.Default()))
	is.Equal(tenantSeen, "acme")

	if len(cbClient.CreateEntityCalls())+len(cbClient.MergeEntityCalls()) < 2 {
		t.Fatalf("expected at least two entity writes, got %+v",
			len(cbClient.CreateEntityCalls())+len(cbClient.MergeEntityCalls()))
	}

	var ids []string
	for _, c := range cbClient.CreateEntityCalls() {
		b, _ := json.Marshal(c.Entity)
		ids = append(ids, string(b))
	}
	joined := strings.Join(ids, "\n")
	if !strings.Contains(joined, "AirQualityObserved") {
		t.Fatalf("no AirQualityObserved entity created:\n%s", joined)
	}
	if !strings.Contains(joined, "NoiseLevelObserved") {
		t.Fatalf("no NoiseLevelObserved entity created:\n%s", joined)
	}
	if !strings.Contains(joined, `"CO2":{"type":"Property","value":800`) {
		t.Fatalf("CO2 value missing from air quality entity:\n%s", joined)
	}
	if !strings.Contains(joined, `"noiseLevel":{"type":"Property","value":55`) {
		t.Fatalf("noise value missing from noise entity:\n%s", joined)
	}
	// 3303-observationens temperatur får inte läcka in i 3428-entiteten.
	if strings.Contains(joined, "22.2") {
		t.Fatalf("temperature leaked across observations:\n%s", joined)
	}
}

// Fas D: strukturskada är permanent.
func TestScopeRejectsMalformedPermanently(t *testing.T) {
	is := is.New(t)

	factory := func(tenant string) (client.ContextBrokerClient, error) {
		t.Fatal("factory must not be called")
		return nil, nil
	}

	msg := &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return []byte(`{"pack":[{"n":"5700","v":1}],"timestamp":"2024-07-03T09:46:40Z"}`) },
		TopicNameFunc:   func() string { return "message.accepted" },
		ContentTypeFunc: func() string { return "application/vnd.oma.lwm2m+json" },
	}

	handler := NewMeasurementTopicMessageHandler(factory)
	err := handler(context.Background(), msg, slog.Default())
	is.True(err != nil)
	is.True(messaging.IsPermanent(err))
}

// Brokerfel ska propageras (retry), inte sväljas.
func TestBrokerErrorIsPropagated(t *testing.T) {
	is := is.New(t)

	cbClient := &clienttest.ContextBrokerClientMock{
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			return nil, errors.New("context broker unavailable")
		},
	}

	factory := func(tenant string) (client.ContextBrokerClient, error) { return cbClient, nil }

	body := `{"pack":[
		{"bn":"dev1/3303/","bt":1720000000,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},
		{"n":"5700","u":"Cel","v":21.5},
		{"bn":"dev1/","n":"env","vs":"air"},
		{"bn":"dev1/","n":"tenant","vs":"acme"}
	],"timestamp":"2024-07-03T09:46:40Z"}`

	msg := &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return []byte(body) },
		TopicNameFunc:   func() string { return "message.accepted" },
		ContentTypeFunc: func() string { return "application/vnd.oma.lwm2m+json" },
	}

	handler := NewMeasurementTopicMessageHandler(factory)
	is.True(handler(context.Background(), msg, slog.Default()) != nil)
}
