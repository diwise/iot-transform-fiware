package relations

import (
	"errors"
	"testing"
	"time"
)

// Generella namn i tester: aldrig standardmodellnamn.

func TestLinksJSONRoundtrip(t *testing.T) {
	links := []Link{
		{Tenant: "tenant-a", Relation: "contains", FromID: "from-1", ToID: "to-2", Revision: 4,
			ChangedAt: time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC),
			FromKind:  "kind-a", FromKindVersion: "1", ToKind: "kind-b", ToKindVersion: "2"},
		{Tenant: "tenant-a", Relation: "contains", FromID: "from-1", ToID: "to-1", Revision: 4,
			FromKind: "kind-a", ToKind: "kind-b"},
	}
	raw, err := LinksToJSON(links)
	if err != nil {
		t.Fatal(err)
	}
	back, err := LinksFromJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(back) != 2 {
		t.Fatalf("roundtrip=%+v", back)
	}
	// Deterministisk sortering: to-1 före to-2 oavsett inordning.
	if back[0].ToID != "to-1" || back[1].ToID != "to-2" {
		t.Fatalf("not sorted: %+v", back)
	}
	if back[0].FromKind != "kind-a" || back[0].FromKindVersion != "" || back[0].ToKindVersion != "" {
		// to-1-posten saknade versioner: omitempty bevarar tomhet.
		t.Fatalf("kinds=%+v", back)
	}
	if back[1].FromKindVersion != "1" || back[1].ToKindVersion != "2" {
		t.Fatalf("versions lost: %+v", back[1])
	}
	if !back[0].ChangedAt.IsZero() {
		t.Fatalf("zero time must stay zero: %+v", back[0])
	}
	if back[1].ChangedAt.IsZero() {
		// Andra posten (to-2) bar tid — men sorteringen lade to-1 först.
		// Kontrollera att tiden överlevde på rätt länk.
		t.Fatalf("changedAt lost: %+v", back)
	}
	// Tom indata ger tom lista, aldrig nil.
	empty, err := LinksFromJSON(nil)
	if err != nil || empty == nil || len(empty) != 0 {
		t.Fatalf("empty=%+v err=%v", empty, err)
	}
	emptyRaw, err := LinksToJSON(nil)
	if err != nil || string(emptyRaw) != "[]" {
		t.Fatalf("empty marshal=%s err=%v", emptyRaw, err)
	}
}

func TestLinksFromJSONRejects(t *testing.T) {
	for _, body := range []string{
		`[{"relation":"contains","fromId":"a","toId":"b"}]`,
		`[{"tenant":"t","fromId":"a","toId":"b"}]`,
		`[{"tenant":"t","relation":"contains","toId":"b"}]`,
		`[{"tenant":"t","relation":"contains","fromId":"a"}]`,
		`not-json`,
	} {
		if _, err := LinksFromJSON([]byte(body)); !errors.Is(err, ErrInvalidChange) {
			t.Fatalf("body %s must fail, got nil", body)
		}
	}
	// Additiva fält tolereras.
	back, err := LinksFromJSON([]byte(`[{"tenant":"t","relation":"contains","fromId":"a","toId":"b","futureField":1}]`))
	if err != nil || len(back) != 1 {
		t.Fatalf("additive=%+v err=%v", back, err)
	}
}

func TestKeysForPrune(t *testing.T) {
	links := []Link{
		{Tenant: "tenant-a", Relation: "contains", FromID: "from-1", ToID: "to-1"},
		{Tenant: "tenant-a", Relation: "contains", FromID: "from-1", ToID: "to-2"},
	}
	keys := KeysForPrune(links)
	if len(keys) != 2 || keys[0].ToID != "to-1" || keys[1].Relation != "contains" {
		t.Fatalf("keys=%+v", keys)
	}
	if len(KeysForPrune(nil)) != 0 {
		t.Fatal("nil must give empty keys")
	}
}
