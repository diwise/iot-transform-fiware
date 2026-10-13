package relations

import (
	"errors"
	"testing"
	"time"
)

// Generella namn i tester: aldrig standardmodellnamn.

func testChange() Change {
	return Change{
		RelationID: BuildRelationID("tenant-a", "from-1", "contains", "to-1", 3, false),
		Tenant:     "tenant-a", Relation: "contains", FromID: "from-1", ToID: "to-1",
		Revision: 3, ChangedAt: time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC),
		FromKind: "kind-a", FromKindVersion: "1", ToKind: "kind-b", ToKindVersion: "2",
	}
}

func TestRelationIDRoundtrip(t *testing.T) {
	id := BuildRelationID("tenant-a", "from-1", "contains", "to-1", 7, false)
	parsed, err := ParseRelationID(id)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Tenant != "tenant-a" || parsed.FromID != "from-1" || parsed.Relation != "contains" || parsed.ToID != "to-1" || parsed.Revision != 7 || parsed.Removed {
		t.Fatalf("parsed=%+v", parsed)
	}
	removed := BuildRelationID("tenant-a", "from-1", "contains", "to-1", 8, true)
	parsed, err = ParseRelationID(removed)
	if err != nil || !parsed.Removed || parsed.Revision != 8 || parsed.ToID != "to-1" {
		t.Fatalf("removed parsed=%+v err=%v", parsed, err)
	}
	// Legacy v1 utan To (sju segment) förblir läsbar med tom ToID.
	legacy, err := ParseRelationID("v1/tenant-a/from-1/relations/contains/7/set")
	if err != nil || legacy.ToID != "" || legacy.Revision != 7 {
		t.Fatalf("legacy parsed=%+v err=%v", legacy, err)
	}
	for _, bad := range []string{"", "v1/a/b", "v2/a/b/relations/c/1/set", "v1/a/b/links/c/1/set", "v1/a/b/relations/c/x/set", "v1/a/b/relations/c/1/other", "v1//b/relations/c/1/set"} {
		if _, err := ParseRelationID(bad); err == nil {
			t.Fatalf("bad id %q accepted", bad)
		}
	}
}

func TestCheckRelationIDMismatch(t *testing.T) {
	c := testChange()
	if err := CheckRelationID(c); err != nil {
		t.Fatalf("matching id rejected: %v", err)
	}
	c.Revision = 4
	if err := CheckRelationID(c); !errors.Is(err, ErrInvalidChange) {
		t.Fatalf("revision mismatch must fail, got %v", err)
	}
	c = testChange()
	c.Removed = true
	if err := CheckRelationID(c); !errors.Is(err, ErrInvalidChange) {
		t.Fatalf("removed mismatch must fail, got %v", err)
	}
	c = testChange()
	c.RelationID = ""
	if err := CheckRelationID(c); err != nil {
		t.Fatalf("empty id must be tolerated, got %v", err)
	}
	c = testChange()
	c.ToID = "to-2"
	if err := CheckRelationID(c); !errors.Is(err, ErrInvalidChange) {
		t.Fatalf("to mismatch must fail, got %v", err)
	}
	// Legacy v1 utan To hoppar To-kontrollen men validerar övrigt.
	c = testChange()
	c.RelationID = "v1/tenant-a/from-1/relations/contains/3/set"
	if err := CheckRelationID(c); err != nil {
		t.Fatalf("legacy id must be tolerated, got %v", err)
	}
}

func TestValidateChange(t *testing.T) {
	if err := ValidateChange(testChange()); err != nil {
		t.Fatal(err)
	}
	c := testChange()
	c.Tenant = ""
	if err := ValidateChange(c); !errors.Is(err, ErrInvalidChange) {
		t.Fatalf("empty tenant: %v", err)
	}
	c = testChange()
	c.ToID = ""
	if err := ValidateChange(c); !errors.Is(err, ErrInvalidChange) {
		t.Fatalf("empty to: %v", err)
	}
	c = testChange()
	c.ToTenant = "tenant-b"
	if err := ValidateChange(c); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("cross tenant: %v", err)
	}
	move := testChange()
	move.PreviousToID = "to-0"
	if !move.IsMove() {
		t.Fatal("move not detected")
	}
	if err := ValidateChange(move); err != nil {
		t.Fatalf("valid move: %v", err)
	}
	move.ToID = move.PreviousToID
	if err := ValidateChange(move); !errors.Is(err, ErrInvalidChange) {
		t.Fatalf("same-target move: %v", err)
	}
	move = testChange()
	move.PreviousToID = "to-0"
	move.Removed = true
	if err := ValidateChange(move); !errors.Is(err, ErrInvalidChange) {
		t.Fatalf("removed move: %v", err)
	}
}

func TestOutgoingAndReverseSets(t *testing.T) {
	all := []Link{
		{Tenant: "tenant-a", Relation: "contains", FromID: "from-1", ToID: "to-2"},
		{Tenant: "tenant-a", Relation: "contains", FromID: "from-1", ToID: "to-1"},
		{Tenant: "tenant-a", Relation: "contains", FromID: "from-2", ToID: "to-1"},
		{Tenant: "tenant-b", Relation: "contains", FromID: "from-1", ToID: "to-9"},
		{Tenant: "tenant-a", Relation: "other", FromID: "from-1", ToID: "to-1"},
	}
	out := Outgoing(all, "tenant-a", "contains", "from-1")
	if len(out) != 2 || out[0].ToID != "to-1" || out[1].ToID != "to-2" {
		t.Fatalf("outgoing=%+v", out)
	}
	rev := Reverse(all, "tenant-a", "contains", "to-1")
	if len(rev) != 2 || rev[0].FromID != "from-1" || rev[1].FromID != "from-2" {
		t.Fatalf("reverse=%+v", rev)
	}
	if got := Outgoing(all, "tenant-a", "contains", "missing"); len(got) != 0 {
		t.Fatalf("missing from must be empty: %+v", got)
	}
}

func TestDetachForEndpointBothSides(t *testing.T) {
	all := []Link{
		{Tenant: "tenant-a", Relation: "contains", FromID: "mid", ToID: "leaf"},
		{Tenant: "tenant-a", Relation: "contains", FromID: "root", ToID: "mid"},
		{Tenant: "tenant-a", Relation: "contains", FromID: "root", ToID: "other"},
		{Tenant: "tenant-b", Relation: "contains", FromID: "root", ToID: "mid"},
	}
	detached := DetachForEndpoint(all, "tenant-a", "mid")
	if len(detached) != 2 {
		t.Fatalf("detached=%+v, want both directions", detached)
	}
}

func TestCheckSetLimits(t *testing.T) {
	spec := &Spec{Name: "contains", AllowedTargets: []string{"kind-b"}, MinLinks: 0, MaxLinks: 2}
	if err := CheckSet(spec, "contains", "kind-b", 1, false); err != nil {
		t.Fatalf("within max: %v", err)
	}
	if err := CheckSet(spec, "contains", "kind-c", 1, false); !errors.Is(err, ErrAllowedTarget) {
		t.Fatalf("disallowed target: %v", err)
	}
	if err := CheckSet(spec, "contains", "kind-b", 2, false); !errors.Is(err, ErrCardinality) {
		t.Fatalf("max exceeded: %v", err)
	}
	// Duplikat är idempotent upsert oavsett max.
	if err := CheckSet(spec, "contains", "kind-b", 2, true); err != nil {
		t.Fatalf("duplicate must pass: %v", err)
	}
	// Okänd målkind är fail-open.
	if err := CheckSet(spec, "contains", "", 1, false); err != nil {
		t.Fatalf("unknown kind must pass: %v", err)
	}
	// Hierarki: max-en även utan spec.
	if err := CheckSet(nil, HierarchicalRelation, "kind-b", 1, false); !errors.Is(err, ErrCardinality) {
		t.Fatalf("hierarchical second target: %v", err)
	}
	if err := CheckSet(nil, HierarchicalRelation, "kind-b", 0, false); err != nil {
		t.Fatalf("hierarchical first target: %v", err)
	}
	// Generellt namn utan spec är permissivt.
	if err := CheckSet(nil, "contains", "kind-b", 99, false); err != nil {
		t.Fatalf("unspecified generic: %v", err)
	}
}

func TestCheckRemoveMin(t *testing.T) {
	spec := &Spec{Name: "contains", MinLinks: 1}
	if err := CheckRemove(spec, "contains", 2); err != nil {
		t.Fatalf("above min: %v", err)
	}
	if err := CheckRemove(spec, "contains", 1); !errors.Is(err, ErrCardinality) {
		t.Fatalf("below min: %v", err)
	}
	if err := CheckRemove(nil, "contains", 1); err != nil {
		t.Fatalf("no spec: %v", err)
	}
	other := &Spec{Name: "other", MinLinks: 5}
	if err := CheckRemove(other, "contains", 1); err != nil {
		t.Fatalf("foreign spec must not apply: %v", err)
	}
}

func TestCreatesCycle(t *testing.T) {
	parents := map[string]string{"b": "a", "c": "b"}
	if !CreatesCycle(parents, "a", "c") {
		t.Fatal("c under a must be cycle for child a")
	}
	if CreatesCycle(parents, "a", "d") {
		t.Fatal("unrelated d must not cycle")
	}
	if !CreatesCycle(parents, "a", "a") {
		t.Fatal("self parent must cycle")
	}
	if CreatesCycle(map[string]string{}, "a", "b") {
		t.Fatal("empty chain must not cycle")
	}
}
