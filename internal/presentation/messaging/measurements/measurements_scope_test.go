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
	appmeasurements "github.com/diwise/iot-transform-fiware/internal/application/measurements"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/matryer/is"
)

// dynamicHandler registrerar de modeller som testerna behöver.
func dynamicHandler(cbClientFn contextbroker.ContextBrokerClientFactoryFunc) messaging.TopicMessageHandler {
	locationRequired := transform.Property{Target: "location", Type: transform.TypeGeo, Required: true, Source: transform.Source{LatLon: true}}
	dateObserved := func(target string) transform.Property {
		return transform.Property{Target: target, Type: transform.TypeDateTime, Required: true, Source: transform.Source{Timestamp: true}}
	}

	rules := []transform.Rule{
		{
			Match: transform.Match{Kind: transform.KindMeasurement, Object: "urn:oma:lwm2m:ext:3428"},
			Entities: []transform.Entity{{
				ID: "urn:ngsi-ld:AirQualityObserved:{{deviceID}}", Type: "AirQualityObserved",
				Properties: []transform.Property{
					{Target: "co2", Type: transform.TypeNumber, Source: transform.Source{Resource: "17"}, ObservedAt: transform.ObservedAtHeader},
					locationRequired,
					dateObserved("dateObserved"),
				},
			}},
		},
		{
			Match: transform.Match{Kind: transform.KindMeasurement, Object: "urn:oma:lwm2m:ext:3324"},
			Entities: []transform.Entity{{
				ID: "urn:ngsi-ld:NoiseLevelObserved:{{deviceID}}", Type: "NoiseLevelObserved",
				Properties: []transform.Property{
					{Target: "LAeq", Type: transform.TypeNumber, Source: transform.Source{Resource: "5700"}, ObservedAt: transform.ObservedAtHeader},
					locationRequired,
					dateObserved("dateObservedFrom"),
					dateObserved("dateObservedTo"),
				},
			}},
		},
		{
			Match: transform.Match{Kind: transform.KindMeasurement, Object: "urn:oma:lwm2m:ext:3303", Env: "air"},
			Entities: []transform.Entity{{
				ID: "urn:ngsi-ld:WeatherObserved:{{deviceID}}", Type: "WeatherObserved",
				Properties: []transform.Property{
					{Target: "temperature", Type: transform.TypeNumber, Source: transform.Source{Resource: "5700"}, ObservedAt: transform.ObservedAtHeader},
					locationRequired,
					dateObserved("dateObserved"),
				},
			}},
		},
	}

	return NewDynamicMeasurementTopicMessageHandler(cbClientFn, staticMeasurementsProvider{appmeasurements.NewDynamicTransformer(rules, nil)}, "")
}

// staticMeasurementsProvider levererar en fast motor till handler-tester.
type staticMeasurementsProvider struct {
	t *appmeasurements.DynamicTransformer
}

func (s staticMeasurementsProvider) MeasurementsTransformer() *appmeasurements.DynamicTransformer {
	return s.t
}

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
		{"bn":"dev1/","n":"lat","u":"lat","v":62.0},
		{"bn":"dev1/","n":"lon","u":"lon","v":17.0},
		{"bn":"dev1/","n":"tenant","vs":"acme"}
	],"timestamp":"2024-07-03T09:46:40Z"}`

	msg := &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return []byte(body) },
		TopicNameFunc:   func() string { return "message.accepted" },
		ContentTypeFunc: func() string { return "application/vnd.oma.lwm2m+json" },
	}

	handler := dynamicHandler(factory)
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
	if !strings.Contains(joined, `"co2":{"type":"Property","value":800`) {
		t.Fatalf("co2 value missing from air quality entity:\n%s", joined)
	}
	if !strings.Contains(joined, `"LAeq":{"type":"Property","value":55`) {
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

	handler := dynamicHandler(factory)
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
		{"bn":"dev1/","n":"lat","u":"lat","v":62.0},
		{"bn":"dev1/","n":"lon","u":"lon","v":17.0},
		{"bn":"dev1/","n":"tenant","vs":"acme"}
	],"timestamp":"2024-07-03T09:46:40Z"}`

	msg := &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return []byte(body) },
		TopicNameFunc:   func() string { return "message.accepted" },
		ContentTypeFunc: func() string { return "application/vnd.oma.lwm2m+json" },
	}

	handler := dynamicHandler(factory)
	is.True(handler(context.Background(), msg, slog.Default()) != nil)
}

func TestMeasurementHandlerKeepsPhysicalPortsSeparate(t *testing.T) {
	makeRule := func(port, id string) transform.Rule {
		return transform.Rule{Match: transform.Match{Kind: transform.KindMeasurement, Object: "urn:oma:lwm2m:ext:3303", Port: port},
			Entities: []transform.Entity{{ID: id, Type: "Observation", Properties: []transform.Property{{Target: "reading", Type: transform.TypeNumber, Source: transform.Source{Resource: "5700"}, ObservedAt: transform.ObservedAtHeader}}}}}
	}
	rules := []transform.Rule{
		makeRule("", "urn:example:Observation:{{id}}"),
		makeRule("0", "urn:example:Observation:{{id}}:{{port}}"),
		makeRule("1", "urn:example:Observation:{{id}}:{{port}}"),
		makeRule("2", "urn:example:Observation:{{id}}:{{port}}"),
	}
	if err := transform.Validate(rules); err != nil {
		t.Fatal(err)
	}
	seen := map[string]map[string]any{}
	broker := &clienttest.ContextBrokerClientMock{MergeEntityFunc: func(ctx context.Context, id string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
		raw, err := fragment.MarshalJSON()
		if err != nil {
			return nil, err
		}
		var body map[string]any
		if err := json.Unmarshal(raw, &body); err != nil {
			return nil, err
		}
		seen[id] = body
		return &ngsild.MergeEntityResult{}, nil
	}}
	handler := NewDynamicMeasurementTopicMessageHandler(func(tenant string) (client.ContextBrokerClient, error) {
		if tenant != "test" {
			t.Fatalf("wrong tenant: %q", tenant)
		}
		return broker, nil
	}, staticMeasurementsProvider{appmeasurements.NewDynamicTransformer(rules, nil)}, "")
	body := `{"pack":[
	 {"bn":"123-456-789/3303/","bt":1720000000,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},{"n":"5700","v":11},
	 {"bn":"123-456-789/0/3303/","bt":1720000001,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},{"n":"5700","v":22},
	 {"bn":"123-456-789/1/3303/","bt":1720000002,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},{"n":"5700","v":33},
	 {"bn":"123-456-789/2/3303/","bt":1720000003,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},{"n":"5700","v":44},
	 {"bn":"123-456-789/","n":"tenant","vs":"test"}],"timestamp":"2024-07-03T09:46:45Z"}`
	message := &messaging.IncomingTopicMessageMock{BodyFunc: func() []byte { return []byte(body) }, ContentTypeFunc: func() string { return "application/vnd.oma.lwm2m+json" }}
	if err := handler(t.Context(), message, slog.Default()); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 4 {
		t.Fatalf("ports merged together: %+v", seen)
	}
	for id, want := range map[string]float64{"urn:example:Observation:123-456-789": 11, "urn:example:Observation:123-456-789:0": 22, "urn:example:Observation:123-456-789:1": 33, "urn:example:Observation:123-456-789:2": 44} {
		fragment, ok := seen[id]
		if !ok {
			t.Fatalf("missing target for port: %s", id)
		}
		if fragment["reading"].(map[string]any)["value"] != want {
			t.Fatalf("wrong port value in %s: %+v", id, fragment)
		}
	}
}
