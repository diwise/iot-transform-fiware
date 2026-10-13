package contextbroker

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/diwise/context-broker/pkg/ngsild"
	ngsilderrors "github.com/diwise/context-broker/pkg/ngsild/errors"
	"github.com/diwise/context-broker/pkg/ngsild/types"
	clienttest "github.com/diwise/context-broker/pkg/test"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
)

func recordingMock() (*clienttest.ContextBrokerClientMock, *[]string, *[][]byte) {
	var calls []string
	var bodies [][]byte
	mock := &clienttest.ContextBrokerClientMock{
		CreateEntityFunc: func(ctx context.Context, entity types.Entity, headers map[string][]string) (*ngsild.CreateEntityResult, error) {
			calls = append(calls, "create:"+entity.ID())
			raw, _ := json.Marshal(entity)
			bodies = append(bodies, raw)
			return &ngsild.CreateEntityResult{}, nil
		},
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			calls = append(calls, "merge:"+entityID)
			raw, _ := fragment.MarshalJSON()
			bodies = append(bodies, raw)
			return &ngsild.MergeEntityResult{}, nil
		},
		DeleteEntityFunc: func(ctx context.Context, entityID string) (*ngsild.DeleteEntityResult, error) {
			calls = append(calls, "delete:"+entityID)
			return &ngsild.DeleteEntityResult{}, nil
		},
	}
	return mock, &calls, &bodies
}

// removeAttributes skickar PATCH-null med context och tolererar 404.
func TestApplyEntityWritesRemoveAttributes(t *testing.T) {
	mock, calls, bodies := recordingMock()
	writes := []transform.EntityWrite{{
		EntityID:         "urn:ngsi-ld:AgriGreenhouse:gh",
		TypeName:         "AgriGreenhouse",
		Context:          "https://example.com/ctx.jsonld",
		RemoveAttributes: []string{"hasWaterQualityObserved"},
	}}
	if err := ApplyEntityWrites(context.Background(), mock, writes, "https://default.example/ctx"); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(*calls) != 1 || (*calls)[0] != "merge:urn:ngsi-ld:AgriGreenhouse:gh" {
		t.Fatalf("calls=%v, want single merge", *calls)
	}
	var frag map[string]any
	if err := json.Unmarshal((*bodies)[0], &frag); err != nil {
		t.Fatalf("fragment: %v", err)
	}
	v, ok := frag["hasWaterQualityObserved"]
	if !ok || v != nil {
		t.Fatalf("fragment=%s, want hasWaterQualityObserved:null", string((*bodies)[0]))
	}
	if frag["@context"] == nil || !strings.Contains(string((*bodies)[0]), "https://example.com/ctx.jsonld") {
		t.Fatalf("fragment=%s, want write context", string((*bodies)[0]))
	}

	// Saknad entitet (404) är framgång — inget att rensa.
	mock.MergeEntityFunc = func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
		return nil, ngsilderrors.ErrNotFound
	}
	if err := ApplyEntityWrites(context.Background(), mock, writes, ""); err != nil {
		t.Fatalf("404 should succeed: %v", err)
	}
}

// Andra brokerfel propageras för retry.
func TestApplyEntityWritesPropagatesBrokerError(t *testing.T) {
	mock, _, _ := recordingMock()
	boom := errors.New("broker down")
	mock.MergeEntityFunc = func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
		return nil, boom
	}
	writes := []transform.EntityWrite{{EntityID: "urn:ngsi-ld:X:x", TypeName: "X"}}
	if err := ApplyEntityWrites(context.Background(), mock, writes, ""); err == nil {
		t.Fatal("expected broker error")
	}
}

// delete raderar via DeleteEntity och tolererar 404 (idempotent retry).
func TestApplyEntityWritesDelete(t *testing.T) {
	mock, calls, _ := recordingMock()
	writes := []transform.EntityWrite{{
		EntityID: "urn:ngsi-ld:IndoorEnvironmentObserved:room:Room:A",
		TypeName: "IndoorEnvironmentObserved",
		Delete:   true,
	}}
	if err := ApplyEntityWrites(context.Background(), mock, writes, "https://default.example/ctx"); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(*calls) != 1 || (*calls)[0] != "delete:urn:ngsi-ld:IndoorEnvironmentObserved:room:Room:A" {
		t.Fatalf("calls=%v, want single delete", *calls)
	}

	// Saknad entitet (404) är framgång — inget att radera.
	mock.DeleteEntityFunc = func(ctx context.Context, entityID string) (*ngsild.DeleteEntityResult, error) {
		return nil, ngsilderrors.ErrNotFound
	}
	if err := ApplyEntityWrites(context.Background(), mock, writes, ""); err != nil {
		t.Fatalf("404 should succeed: %v", err)
	}

	// Andra fel propageras för retry.
	boom := errors.New("broker down")
	mock.DeleteEntityFunc = func(ctx context.Context, entityID string) (*ngsild.DeleteEntityResult, error) {
		return nil, boom
	}
	if err := ApplyEntityWrites(context.Background(), mock, writes, ""); err == nil {
		t.Fatal("expected broker error")
	}
}
