package things

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild"
	"github.com/diwise/context-broker/pkg/ngsild/types"
	clienttest "github.com/diwise/context-broker/pkg/test"
	"github.com/diwise/iot-transform-fiware/internal/application/relations"
	appthings "github.com/diwise/iot-transform-fiware/internal/application/things"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/config"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/projection"
)

func TestAggregatesThroughRealHandlersAndPersistentQueue(t *testing.T) {
	store, err := projection.NewPostgres(t.Context(), c2ProjectionConfig(t))
	if err != nil {
		t.Skipf("no test database: %v", err)
	}
	defer store.Close()
	c2CleanProjection(t, store)
	rules, err := config.LoadModelsDir("../../../../deployments/examples/tank-greenhouse")
	if err != nil {
		t.Fatal(err)
	}
	provider := staticThingsProvider{appthings.NewDynamicThingTransformer(rules)}
	last := map[string]map[string]any{}
	var deleted []string
	fail := false
	factory := func(tenant string) (contextbroker.BrokerClient, error) {
		return &clienttest.ContextBrokerClientMock{
			MergeEntityFunc: func(ctx context.Context, id string, fragment types.EntityFragment, _ map[string][]string) (*ngsild.MergeEntityResult, error) {
				if fail {
					return nil, errors.New("broker 503")
				}
				raw, err := json.Marshal(fragment)
				if err != nil {
					return nil, err
				}
				var doc map[string]any
				if err := json.Unmarshal(raw, &doc); err != nil {
					return nil, err
				}
				last[tenant+"/"+id] = doc
				return &ngsild.MergeEntityResult{}, nil
			},
			DeleteEntityFunc: func(_ context.Context, id string) (*ngsild.DeleteEntityResult, error) {
				deleted = append(deleted, tenant+"/"+id)
				return &ngsild.DeleteEntityResult{}, nil
			},
		}, nil
	}
	handler := NewThingRelationsTopicMessageHandlerWithStore(factory, provider, "", store)
	msg := func(from, to string, rev int64, removed bool) string {
		raw, _ := json.Marshal(map[string]any{"schema": "v1", "eventId": fmt.Sprintf("event-%s-%d", from, rev),
			"relationId": relations.BuildRelationID("example", from, "partOf", to, rev, removed),
			"type":       "partOf", "tenant": "example", "fromId": from, "toId": to, "removed": removed, "revision": rev,
			"fromThingType": "tank", "toThingType": "greenhouse"})
		return string(raw)
	}
	for i := 1; i <= 4; i++ {
		if err := handler(t.Context(), c2Incoming(msg(fmt.Sprintf("tank-%d", i), "greenhouse-1", 1, false)), slog.Default()); err != nil {
			t.Fatal(err)
		}
	}
	owner := "example/urn:ngsi-ld:AgriGreenhouse:greenhouse-1"
	objects := func() []any {
		rel, _ := last[owner]["hasWaterQualityObserved"].(map[string]any)
		list, _ := rel["object"].([]any)
		return list
	}
	want := []any{"urn:ngsi-ld:WaterQualityObserved:tank-1", "urn:ngsi-ld:WaterQualityObserved:tank-2", "urn:ngsi-ld:WaterQualityObserved:tank-3", "urn:ngsi-ld:WaterQualityObserved:tank-4"}
	if !reflect.DeepEqual(objects(), want) {
		t.Fatal(last)
	}
	if err := handler(t.Context(), c2Incoming(msg("tank-2", "greenhouse-1", 2, true)), slog.Default()); err != nil {
		t.Fatal(err)
	}
	if got := objects(); len(got) != 3 || got[1] != "urn:ngsi-ld:WaterQualityObserved:tank-3" {
		t.Fatal(got)
	}
	// Handler sees a failed broker, but durable work survives its exact replay.
	fail = true
	if err := handler(t.Context(), c2Incoming(msg("tank-3", "greenhouse-1", 2, true)), slog.Default()); err == nil {
		t.Fatal("broker error swallowed")
	}
	fail = false
	if err := handler(t.Context(), c2Incoming(msg("tank-3", "greenhouse-1", 2, true)), slog.Default()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.DrainOnce(t.Context(), "example", 100, time.Minute, WorkerBrokerFunc(factory)); err != nil {
		t.Fatal(err)
	}
	if len(objects()) != 2 {
		t.Fatal(objects())
	}
	lifecycle := NewThingLifecycleTopicMessageHandlerWithStore(factory, provider, "", store)
	if err := lifecycle(t.Context(), c2Incoming(`{"schema":"v1","eventId":"deleted-tank-1","tenant":"example","thingId":"tank-1","thingType":"tank","kind":"deleted","configRevision":2}`), slog.Default()); err != nil {
		t.Fatal(err)
	}
	if got := objects(); !reflect.DeepEqual(got, []any{"urn:ngsi-ld:WaterQualityObserved:tank-4"}) {
		t.Fatal(got)
	}
	if len(deleted) != 1 || deleted[0] != "example/urn:ngsi-ld:WaterQualityObserved:tank-1" {
		t.Fatal(deleted)
	}
	if err := handler(t.Context(), c2Incoming(msg("tank-4", "greenhouse-1", 2, true)), slog.Default()); err != nil {
		t.Fatal(err)
	}
	if v, ok := last[owner]["hasWaterQualityObserved"]; !ok || v != nil {
		t.Fatal("last detach did not send PATCH-null", last[owner])
	}
}

func TestAggregateNoMatchRemoveStillUsesProjection(t *testing.T) {
	store, err := projection.NewPostgres(t.Context(), c2ProjectionConfig(t))
	if err != nil {
		t.Skipf("no test database: %v", err)
	}
	defer store.Close()
	c2CleanProjection(t, store)
	rules, err := config.LoadModelsDir("../../../../deployments/examples/tank-greenhouse")
	if err != nil {
		t.Fatal(err)
	}
	// No stateless writes or removal rule; aggregation is still active.
	r := rules[0]
	r.Entities = r.Entities[1:]
	provider := staticThingsProvider{appthings.NewDynamicThingTransformer([]transform.Rule{r})}
	var payloads []map[string]any
	factory := func(string) (contextbroker.BrokerClient, error) {
		return &clienttest.ContextBrokerClientMock{MergeEntityFunc: func(_ context.Context, _ string, f types.EntityFragment, _ map[string][]string) (*ngsild.MergeEntityResult, error) {
			raw, _ := json.Marshal(f)
			var doc map[string]any
			if err := json.Unmarshal(raw, &doc); err != nil {
				return nil, err
			}
			payloads = append(payloads, doc)
			return &ngsild.MergeEntityResult{}, nil
		}}, nil
	}
	h := NewThingRelationsTopicMessageHandlerWithStore(factory, provider, "", store)
	for _, removed := range []bool{false, true} {
		raw, _ := json.Marshal(map[string]any{"tenant": "example", "type": "partOf", "fromId": "one", "toId": "parent", "fromThingType": "tank", "toThingType": "greenhouse", "revision": 2, "removed": removed,
			"relationId": relations.BuildRelationID("example", "one", "partOf", "parent", 2, removed)})
		if err := h(t.Context(), c2Incoming(string(raw)), slog.Default()); err != nil {
			t.Fatal(err)
		}
	}
	if len(payloads) != 2 || payloads[1]["hasWaterQualityObserved"] != nil {
		t.Fatal(payloads)
	}
}
