package projection

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/diwise/iot-transform-fiware/internal/application/relations"
)

func bootstrapLink(tenant, from, to string, rev int64) relations.Link {
	return relations.Link{
		Tenant: tenant, Relation: "contains", FromID: from, ToID: to, Revision: rev,
		ChangedAt: time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC),
		FromKind:  "kind-a", FromKindVersion: "1", ToKind: "kind-b", ToKindVersion: "2",
	}
}

func TestProjectionBootstrapUpsertAndPrune(t *testing.T) {
	s := pgTestStore(t)

	// Gammal rad (före watermark) som saknas i snapshot beskärs; färska
	// snapshotrader och samtidiga live-skrivningar överlever.
	if _, _, err := s.BootstrapUpsert(t.Context(), []relations.Link{
		bootstrapLink("tenant-a", "from-2", "to-3", 1),
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	start := time.Now().UTC()
	inserted, updated, err := s.BootstrapUpsert(t.Context(), []relations.Link{
		bootstrapLink("tenant-a", "from-1", "to-1", 1),
		bootstrapLink("tenant-a", "from-1", "to-2", 1),
	})
	if err != nil || inserted != 2 || updated != 0 {
		t.Fatalf("upsert=%d/%d err=%v", inserted, updated, err)
	}
	// Idempotent återinläsning av samma underlag.
	inserted, updated, err = s.BootstrapUpsert(t.Context(), []relations.Link{
		bootstrapLink("tenant-a", "from-1", "to-1", 1),
	})
	if err != nil || inserted != 0 || updated != 1 {
		t.Fatalf("re-upsert=%d/%d err=%v", inserted, updated, err)
	}
	// Prune tar bort rader som varken finns i snapshot eller rört sig sedan start.
	pruned, err := s.BootstrapPrune(t.Context(), "tenant-a", []relations.LinkKey{
		{Relation: "contains", FromID: "from-1", ToID: "to-1"},
		{Relation: "contains", FromID: "from-1", ToID: "to-2"},
	}, start)
	if err != nil || pruned != 1 {
		t.Fatalf("pruned=%d err=%v, want 1 (from-2/to-3)", pruned, err)
	}
	exported, err := s.ExportTenant(t.Context(), "tenant-a")
	if err != nil || len(exported) != 2 {
		t.Fatalf("exported=%+v err=%v", exported, err)
	}
}

func TestProjectionBootstrapConcurrentLiveChange(t *testing.T) {
	s := pgTestStore(t)

	start := time.Now().UTC()
	if _, _, err := s.BootstrapUpsert(t.Context(), []relations.Link{
		bootstrapLink("tenant-a", "from-1", "to-1", 1),
	}); err != nil {
		t.Fatal(err)
	}
	// Samtidig live-ändring under bootstrap: ny länk som snapshot inte känner.
	var wg sync.WaitGroup
	wg.Go(func() {
		planner := &recordingPlanner{}
		if _, err := s.ApplyChange(t.Context(), setChange("tenant-a", "from-9", "to-9", 1), nil, planner.plan()); err != nil {
			t.Errorf("live apply: %v", err)
		}
	})
	wg.Wait()
	// Prune med snapshot utan live-länken: den överlever via watermark.
	pruned, err := s.BootstrapPrune(t.Context(), "tenant-a", []relations.LinkKey{
		{Relation: "contains", FromID: "from-1", ToID: "to-1"},
	}, start)
	if err != nil {
		t.Fatal(err)
	}
	if pruned != 0 {
		t.Fatalf("pruned=%d, concurrent live link must survive", pruned)
	}
	if got := outgoingToIDs(t, s, "tenant-a", "contains", "from-9"); len(got) != 1 {
		t.Fatalf("live link lost: %v", got)
	}
}

func TestProjectionRebuildAfterRuleChange(t *testing.T) {
	s := pgTestStore(t)

	if _, _, err := s.BootstrapUpsert(t.Context(), []relations.Link{
		bootstrapLink("tenant-a", "from-1", "to-1", 4),
		bootstrapLink("tenant-a", "from-1", "to-2", 4),
		bootstrapLink("tenant-a", "from-2", "to-3", 2),
	}); err != nil {
		t.Fatal(err)
	}
	// Regeländring: planera om allt brokerarbete från projektionen utan
	// nya mätningar. Nya idempotensnycklar (regelrevision i nyckeln).
	planner := &recordingPlanner{writes: func(snap relations.Snapshot) []relations.PendingWrite {
		var out []relations.PendingWrite
		for _, l := range snap.Outgoing {
			out = append(out, relations.PendingWrite{
				IdempotencyKey: "rule-rev-9:" + l.FromID + ":" + l.ToID,
				Operation:      relations.OpMerge,
				EntityID:       "entity:" + l.FromID,
				Payload:        []byte(`{"rev":9}`),
				RuleID:         "rule-1",
				RuleRevision:   9,
			})
		}
		return out
	}}
	res, err := s.Rebuild(t.Context(), "tenant-a", "contains", nil, planner.plan(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if res.Groups != 2 || res.Pending != 3 {
		t.Fatalf("rebuild=%+v, want 2 groups 3 pending", res)
	}
	// Återuppbyggnad är idempotent: samma nycklar ger inget nytt arbete.
	res, err = s.Rebuild(t.Context(), "tenant-a", "contains", nil, planner.plan(), 0)
	if err != nil || res.Pending != 0 {
		t.Fatalf("second rebuild=%+v err=%v, want 0 new", res, err)
	}
	count, _ := s.PendingCount(t.Context(), "tenant-a")
	if count != 3 {
		t.Fatalf("pending=%d, want 3", count)
	}
	// Projektionen är orörd av rebuild (endast arbete planerades).
	if got := outgoingToIDs(t, s, "tenant-a", "contains", "from-1"); len(got) != 2 {
		t.Fatalf("rebuild mutated projection: %v", got)
	}
}

func TestProjectionSpecRoundtrip(t *testing.T) {
	s := pgTestStore(t)
	spec := relations.Spec{Name: "contains", AllowedTargets: []string{"kind-b", "kind-c"}, MinLinks: 1, MaxLinks: 3}
	if err := s.SetSpec(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetSpec(t.Context(), "contains")
	if err != nil {
		t.Fatal(err)
	}
	if got.MaxLinks != 3 || got.MinLinks != 1 || len(got.AllowedTargets) != 2 {
		t.Fatalf("spec=%+v", got)
	}
	if _, err := s.GetSpec(t.Context(), "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing spec must be not found, got %v", err)
	}
}
