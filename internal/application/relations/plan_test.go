package relations

import (
	"strings"
	"testing"
	"time"
)

// Generella namn i tester: aldrig standardmodellnamn.

func testSnapshot() Snapshot {
	return Snapshot{
		Tenant:   "tenant-a",
		Relation: "contains",
		FromID:   "from-1",
		Outgoing: []Link{
			{Tenant: "tenant-a", Relation: "contains", FromID: "from-1", ToID: "to-1"},
			{Tenant: "tenant-a", Relation: "contains", FromID: "from-1", ToID: "to-2"},
		},
		ReverseNew: map[string][]Link{
			"to-1": {{Tenant: "tenant-a", Relation: "contains", FromID: "from-1", ToID: "to-1"}},
			"to-2": {{Tenant: "tenant-a", Relation: "contains", FromID: "from-1", ToID: "to-2"}},
		},
		ReversePrevious: map[string][]Link{},
	}
}

func TestIdempotencyKeyStability(t *testing.T) {
	rule := RuleRef{ID: "rule-1", Revision: 9}
	a := IdempotencyKey(rule, OpMerge, "contains", "from-1", "to-1")
	b := IdempotencyKey(rule, OpMerge, "contains", "from-1", "to-1")
	if a != b || a == "" {
		t.Fatalf("unstable key: %q vs %q", a, b)
	}
	// Ny regelrevision ger ny nyckel (nytt arbete vid regeländring).
	other := IdempotencyKey(RuleRef{ID: "rule-1", Revision: 10}, OpMerge, "contains", "from-1", "to-1")
	if other == a {
		t.Fatalf("rule revision must change key: %q", a)
	}
	// Olika operationer/länkar ger olika nycklar.
	if IdempotencyKey(rule, OpRemoveAttributes, "contains", "from-1", "to-1") == a {
		t.Fatal("operation must separate keys")
	}
	if IdempotencyKeyForClear(rule, "contains", "from-1") == a {
		t.Fatal("clear must separate keys")
	}
	if IdempotencyKeyForEndpointDelete(rule, "from-1") == a {
		t.Fatal("delete must separate keys")
	}
	// Injektivitet: ":" i ID:n får inte kollidera.
	collideA := IdempotencyKey(rule, OpMerge, "contains", "a:b", "c")
	collideB := IdempotencyKey(rule, OpMerge, "contains", "a", "b:c")
	if collideA == collideB {
		t.Fatalf("key collision: %q", collideA)
	}
	// Inga modellnamn i nycklar (generiska begrepp endast).
	for _, k := range []string{a, other, collideA} {
		for _, banned := range []string{"Tank", "Greenhouse", "Sewer", "Room", "AirQuality"} {
			if strings.Contains(k, banned) {
				t.Fatalf("key %q contains model name %q", k, banned)
			}
		}
	}
}

func TestReferencePlannerDeterministic(t *testing.T) {
	rule := RuleRef{ID: "rule-1", Revision: 9}
	planner := ReferencePlanner(rule)
	snap := testSnapshot()

	first, err := planner(snap)
	if err != nil {
		t.Fatal(err)
	}
	second, err := planner(snap)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 {
		t.Fatalf("want 1 merge per outgoing link, got %d", len(first))
	}
	// Samma Change + samma regel → samma writes/idempotensnycklar (byte-identiskt).
	for i := range first {
		if first[i].IdempotencyKey != second[i].IdempotencyKey ||
			first[i].Operation != second[i].Operation ||
			first[i].EntityID != second[i].EntityID ||
			string(first[i].Payload) != string(second[i].Payload) {
			t.Fatalf("non-deterministic plan:\n%+v\n%+v", first[i], second[i])
		}
	}
	// Nycklarna bär regelidentiteten; payload bär inga modellnamn.
	for _, w := range first {
		if !strings.Contains(w.IdempotencyKey, "rule-1:9") {
			t.Fatalf("key %q lacks rule identity", w.IdempotencyKey)
		}
		if w.RuleID != "rule-1" || w.RuleRevision != 9 {
			t.Fatalf("rule ref not carried: %+v", w)
		}
		for _, banned := range []string{"Tank", "Greenhouse", "Sewer", "Room"} {
			if strings.Contains(string(w.Payload), banned) || strings.Contains(w.EntityID, banned) {
				t.Fatalf("write %+v contains model name", w)
			}
		}
	}
	// Sorterad ordning för stabil persistens.
	if first[0].IdempotencyKey > first[1].IdempotencyKey {
		t.Fatalf("writes not sorted: %q > %q", first[0].IdempotencyKey, first[1].IdempotencyKey)
	}
}

func TestReferencePlannerRemovedLastLinkDelete(t *testing.T) {
	rule := RuleRef{ID: "rule-1", Revision: 9}
	planner := ReferencePlanner(rule)

	// removed:true-skrivning per borttagen länk.
	snap := testSnapshot()
	snap.Removed = []Link{{Tenant: "tenant-a", Relation: "contains", FromID: "from-1", ToID: "to-0"}}
	snap.ReversePrevious = map[string][]Link{"to-0": {}}
	writes, err := planner(snap)
	if err != nil {
		t.Fatal(err)
	}
	foundRemove := false
	for _, w := range writes {
		if w.Operation == OpRemoveAttributes && strings.Contains(w.IdempotencyKey, "to-0") {
			foundRemove = true
		}
	}
	if !foundRemove {
		t.Fatalf("removed:true write missing: %+v", writes)
	}

	// Sista-länk-rensning vid tom utgående mängd.
	last := Snapshot{Tenant: "tenant-a", Relation: "contains", FromID: "from-1",
		Outgoing: []Link{}, ReverseNew: map[string][]Link{}, ReversePrevious: map[string][]Link{},
		Removed:    []Link{{Tenant: "tenant-a", Relation: "contains", FromID: "from-1", ToID: "to-1"}},
		IsLastLink: true}
	writes, err = planner(last)
	if err != nil {
		t.Fatal(err)
	}
	foundClear := false
	for _, w := range writes {
		if w.IdempotencyKey == IdempotencyKeyForClear(rule, "contains", "from-1") {
			foundClear = true
		}
	}
	if !foundClear {
		t.Fatalf("last-link clear missing: %+v", writes)
	}

	// Lifecycle-delete: delete av målet självt utöver per-länk-rensningar.
	del := Snapshot{Tenant: "tenant-a", FromID: "mid",
		Outgoing: []Link{}, ReverseNew: map[string][]Link{}, ReversePrevious: map[string][]Link{},
		Removed:    []Link{{Tenant: "tenant-a", Relation: "contains", FromID: "root", ToID: "mid"}},
		IsLastLink: true, Detached: []Link{{Tenant: "tenant-a", Relation: "contains", FromID: "root", ToID: "mid"}},
		EndpointDeleted: "mid"}
	writes, err = planner(del)
	if err != nil {
		t.Fatal(err)
	}
	foundDelete := false
	for _, w := range writes {
		if w.Operation == OpDelete && w.IdempotencyKey == IdempotencyKeyForEndpointDelete(rule, "mid") {
			foundDelete = true
		}
	}
	if !foundDelete {
		t.Fatalf("endpoint delete missing: %+v", writes)
	}
}

func TestSnapshotsForRebuildGrouping(t *testing.T) {
	links := []Link{
		{Tenant: "tenant-a", Relation: "contains", FromID: "from-2", ToID: "to-3"},
		{Tenant: "tenant-a", Relation: "contains", FromID: "from-1", ToID: "to-2"},
		{Tenant: "tenant-a", Relation: "contains", FromID: "from-1", ToID: "to-1"},
		{Tenant: "tenant-a", Relation: "contains", FromID: "from-1", ToID: "to-1", Revision: 5}, // dubblett normaliseras ej här
		{Tenant: "tenant-b", Relation: "contains", FromID: "from-1", ToID: "to-9"},
		{Tenant: "tenant-a", Relation: "other", FromID: "from-1", ToID: "to-1"},
	}
	snaps := SnapshotsForRebuild(links, "tenant-a", "contains")
	if len(snaps) != 2 {
		t.Fatalf("groups=%d, want 2 (from-1, from-2)", len(snaps))
	}
	if snaps[0].FromID != "from-1" || snaps[1].FromID != "from-2" {
		t.Fatalf("groups not sorted: %+v", snaps)
	}
	if len(snaps[0].Outgoing) != 3 {
		t.Fatalf("from-1 outgoing=%d, want 3", len(snaps[0].Outgoing))
	}
	// Komplett omvänd mängd per mål (tenant-avgränsad: tenant-b läcker aldrig).
	for to, rev := range snaps[0].ReverseNew {
		for _, l := range rev {
			if l.Tenant != "tenant-a" {
				t.Fatalf("reverse %s leaks tenant: %+v", to, l)
			}
		}
	}
	// Relationsfilter.
	if got := SnapshotsForRebuild(links, "tenant-a", "other"); len(got) != 1 {
		t.Fatalf("relation filter groups=%d, want 1", len(got))
	}
	if got := SnapshotsForRebuild(links, "tenant-b", "contains"); len(got) != 1 {
		t.Fatalf("tenant-b groups=%d, want 1", len(got))
	}
	if got := SnapshotsForRebuild(nil, "tenant-a", ""); len(got) != 0 {
		t.Fatalf("empty export must give no groups: %+v", got)
	}
}

func TestPlanRebuildFromExportDeterministic(t *testing.T) {
	links := []Link{
		{Tenant: "tenant-a", Relation: "contains", FromID: "from-1", ToID: "to-1", Revision: 4,
			ChangedAt: time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC), FromKind: "kind-a", ToKind: "kind-b"},
		{Tenant: "tenant-a", Relation: "contains", FromID: "from-1", ToID: "to-2", Revision: 4, FromKind: "kind-a", ToKind: "kind-b"},
		{Tenant: "tenant-a", Relation: "contains", FromID: "from-2", ToID: "to-3", Revision: 2, FromKind: "kind-a", ToKind: "kind-b"},
	}
	rule := RuleRef{ID: "rule-1", Revision: 9}
	first, err := PlanRebuildFromExport(links, "tenant-a", "contains", ReferencePlanner(rule))
	if err != nil {
		t.Fatal(err)
	}
	second, err := PlanRebuildFromExport(links, "tenant-a", "contains", ReferencePlanner(rule))
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 3 {
		t.Fatalf("writes=%d, want 3 (en per länk)", len(first))
	}
	for i := range first {
		if first[i].IdempotencyKey != second[i].IdempotencyKey {
			t.Fatalf("non-deterministic rebuild:\n%+v\n%+v", first, second)
		}
	}
	// Tenantfrämmande länkar planeras aldrig.
	links = append(links, Link{Tenant: "tenant-b", Relation: "contains", FromID: "from-1", ToID: "to-9"})
	third, err := PlanRebuildFromExport(links, "tenant-a", "contains", ReferencePlanner(rule))
	if err != nil {
		t.Fatal(err)
	}
	if len(third) != 3 {
		t.Fatalf("cross-tenant leak: writes=%d, want 3", len(third))
	}
	// Fel i en grupp avbryter utan delsvar.
	broken := PlanFunc(func(Snapshot) ([]PendingWrite, error) { return nil, assertErrBoom })
	if _, err := PlanRebuildFromExport(links, "tenant-a", "contains", broken); err == nil {
		t.Fatal("planner error must propagate")
	}
	if _, err := PlanRebuildFromExport(links, "", "contains", ReferencePlanner(rule)); err == nil {
		t.Fatal("empty tenant must fail")
	}
	if _, err := PlanRebuildFromExport(links, "tenant-a", "contains", nil); err == nil {
		t.Fatal("nil planner must fail")
	}
}

var assertErrBoom = errBoom{}

type errBoom struct{}

func (errBoom) Error() string { return "boom" }
