package relations

import (
	"errors"
	"testing"
	"time"
)

// Generella namn i tester: aldrig standardmodellnamn.

func TestChangeFromJSONSet(t *testing.T) {
	raw := []byte(`{
	  "relationId":"v1/tenant-a/from-1/relations/contains/to-1/3/set",
	  "type":"contains","fromId":"from-1","toId":"to-1",
	  "tenant":"tenant-a","revision":3,
	  "changedAt":"2026-10-05T12:00:00Z",
	  "fromThingType":"kind-a","fromThingTypeVersion":"1",
	  "from":{"thingId":"from-1"},
	  "toThingType":"kind-b","toThingTypeVersion":"2",
	  "to":{"thingId":"to-1"}}`)
	c, err := ChangeFromJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	if c.Tenant != "tenant-a" || c.Relation != "contains" || c.FromID != "from-1" || c.ToID != "to-1" {
		t.Fatalf("change=%+v", c)
	}
	if c.PreviousToID != "" || c.Removed {
		t.Fatalf("set must not be move/removed: %+v", c)
	}
	if c.FromKind != "kind-a" || c.FromKindVersion != "1" || c.ToKind != "kind-b" || c.ToKindVersion != "2" {
		t.Fatalf("frozen descriptors lost: %+v", c)
	}
	if c.Revision != 3 || c.RelationID == "" {
		t.Fatalf("identity lost: %+v", c)
	}
	want := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	if !c.ChangedAt.Equal(want) {
		t.Fatalf("changedAt=%v, want %v", c.ChangedAt, want)
	}
}

func TestChangeFromJSONMoveAndRemoved(t *testing.T) {
	move := []byte(`{
	  "relationId":"v1/tenant-a/from-1/relations/contains/to-2/5/set",
	  "type":"contains","fromId":"from-1","toId":"to-2","previousToId":"to-0",
	  "tenant":"tenant-a","revision":5,
	  "fromThingType":"kind-a","fromThingTypeVersion":"2",
	  "from":{"thingId":"from-1"},
	  "toThingType":"kind-b","toThingTypeVersion":"1",
	  "to":{"thingId":"to-2"}}`)
	c, err := ChangeFromJSON(move)
	if err != nil {
		t.Fatal(err)
	}
	if !c.IsMove() || c.PreviousToID != "to-0" || c.ToID != "to-2" {
		t.Fatalf("move=%+v", c)
	}
	if c.FromKindVersion != "2" || c.ToKindVersion != "1" {
		t.Fatalf("move versions lost: %+v", c)
	}

	removed := []byte(`{
	  "relationId":"v1/tenant-a/from-1/relations/contains/to-2/6/removed",
	  "type":"contains","fromId":"from-1","toId":"to-2","removed":true,
	  "tenant":"tenant-a","revision":6}`)
	c, err = ChangeFromJSON(removed)
	if err != nil {
		t.Fatal(err)
	}
	if !c.Removed || c.ToID != "to-2" {
		t.Fatalf("removed=%+v", c)
	}
}

func TestChangeFromJSONBWireShape(t *testing.T) {
	// B:s verkliga trådform (neutrala källtyper, additiva fält, frysta
	// descriptors): samma JSON som C1 läser ska ge samma Change här.
	raw := []byte(`{"relationId":"relation-1","type":"source-relation",` +
		`"fromId":"object-1","toId":"object-2","tenant":"tenant-a","revision":4,` +
		`"changedAt":"2026-10-01T12:00:00Z",` +
		`"fromThingType":"source-template","from":{"thingId":"object-1","name":"Raw name"},` +
		`"toThingType":"other-template","to":{"thingId":"object-2"},"unknownTop":1}`)
	// relationId "relation-1" är inte v1-form: CheckRelationID nekar (strikt
	// identitet för projektionens seen-tabell). Formatet i sig parsas.
	if _, err := ChangeFromJSON(raw); err == nil {
		t.Fatal("opaque legacy relationId must be rejected by strict identity check")
	}
	// Samma kropp med v1-identitet mappar.
	versioned := []byte(`{"relationId":"v1/tenant-a/object-1/relations/source-relation/object-2/4/set",` +
		`"type":"source-relation",` +
		`"fromId":"object-1","toId":"object-2","tenant":"tenant-a","revision":4,` +
		`"changedAt":"2026-10-01T12:00:00Z",` +
		`"fromThingType":"source-template","fromThingTypeVersion":"2",` +
		`"from":{"thingId":"object-1","name":"Raw name"},` +
		`"toThingType":"other-template","toThingTypeVersion":"1",` +
		`"to":{"thingId":"object-2"},"unknownTop":1}`)
	c, err := ChangeFromJSON(versioned)
	if err != nil {
		t.Fatalf("B wire shape rejected: %v", err)
	}
	if c.FromID != "object-1" || c.ToID != "object-2" || c.FromKindVersion != "2" || c.ToKindVersion != "1" {
		t.Fatalf("B wire change=%+v", c)
	}
}

func TestChangeFromJSONRejects(t *testing.T) {
	for name, body := range map[string]string{
		"not-json":          `{"type":`,
		"missing-from":      `{"type":"contains","tenant":"tenant-a"}`,
		"missing-to":        `{"type":"contains","fromId":"from-1","tenant":"tenant-a"}`,
		"conflict-from":     `{"type":"contains","fromId":"a","toId":"b","tenant":"t","from":{"thingId":"other"}}`,
		"conflict-to":       `{"type":"contains","fromId":"a","toId":"b","tenant":"t","to":{"thingId":"other"}}`,
		"same-target":       `{"relationId":"v1/t/a/relations/contains/b/1/set","type":"contains","fromId":"a","toId":"b","previousToId":"b","tenant":"t","revision":1}`,
		"bad-identity":      `{"relationId":"nope","type":"contains","fromId":"a","toId":"b","tenant":"t","revision":1}`,
		"identity-mismatch": `{"relationId":"v1/t/a/relations/contains/b/2/set","type":"contains","fromId":"a","toId":"b","tenant":"t","revision":1}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ChangeFromJSON([]byte(body)); !errors.Is(err, ErrInvalidChange) {
				t.Fatalf("must be invalid change, got %v", err)
			}
		})
	}
	// Tom tenant är inget strukturfel här (presentationen skippar) — men
	// lagrets ValidateChange nekar inte heller tom tenant? Nej: tom tenant
	// ÄR strukturfel i ValidateChange. Mappern följer samma grind.
	if _, err := ChangeFromJSON([]byte(`{"type":"contains","fromId":"a","toId":"b"}`)); err != nil {
		t.Fatalf("empty tenant must be tolerated (skip, not Permanent), got %v", err)
	} else {
		c, _ := ChangeFromJSON([]byte(`{"type":"contains","fromId":"a","toId":"b"}`))
		if c.Tenant != "" || c.FromID != "a" {
			t.Fatalf("empty-tenant change=%+v", c)
		}
	}
	// Tom relation förblir strukturfel även vid tom tenant.
	if _, err := ChangeFromJSON([]byte(`{"fromId":"a","toId":"b"}`)); !errors.Is(err, ErrInvalidChange) {
		t.Fatalf("empty relation must fail: %v", err)
	}
	// Saknad to-sida tolereras endast när ToID är satt via kuvertet; helt
	// utan toId för set är strukturfel (samma som C1:s fromId-grind).
	if _, err := ChangeFromJSON([]byte(`{"relationId":"v1/t/a/relations/contains/b/1/set","type":"contains","fromId":"a","tenant":"t","revision":1}`)); !errors.Is(err, ErrInvalidChange) {
		t.Fatalf("missing to for set: accepted")
	}
	// To-mismatch mot identitet är strukturfel (unicitet vid flera mål).
	if _, err := ChangeFromJSON([]byte(`{"relationId":"v1/t/a/relations/contains/other/1/set","type":"contains","fromId":"a","toId":"b","tenant":"t","revision":1}`)); !errors.Is(err, ErrInvalidChange) {
		t.Fatalf("to mismatch: accepted")
	}
	// Legacy v1 utan To förblir läsbar (hoppar To-kontroll, validerar övrigt).
	if _, err := ChangeFromJSON([]byte(`{"relationId":"v1/t/a/relations/contains/1/set","type":"contains","fromId":"a","toId":"b","tenant":"t","revision":1}`)); err != nil {
		t.Fatalf("legacy id rejected: %v", err)
	}
}
