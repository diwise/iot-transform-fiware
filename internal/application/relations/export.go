// Package relations — bootstrap-codec (Våg 2).
//
// Link bär JSON-taggar och är därmed det neutrala bootstrap-/återsynkformatet
// mot B: anroparen aggregerar GET /things/export (sakunderlag) +
// GET /things/{id}/relations-underlag (neutralt Link-format, frysta
// källtyper, aldrig FIWARE-ID:n) till []Link och matar Store.BootstrapUpsert
// idempotent; därefter Store.BootstrapPrune med snapshotnycklar + watermark.
//
// Flödet (A äger slutlig wiring; F äger format + semantik):
//
//	exported, _ := relations.LinksFromJSON(aggregateFromB)
//	watermark := time.Now().UTC()          // före upsert
//	store.BootstrapUpsert(ctx, exported)
//	store.BootstrapPrune(ctx, tenant, relations.KeysForPrune(exported), watermark)
//
// Watermark-regeln: prune tar endast bort rader med updated_at < watermark
// som saknas i snapshot — samtidiga live-skrivningar (updated_at >=
// watermark) överlever även när snapshot inte känner dem. Se
// Store.BootstrapUpsert/BootstrapPrune.
package relations

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// linkWire är den explicita trådformen: ChangedAt som pekare så att nolltid
// utelämnas (omitempty fungerar inte på struct-värden) och roundtrippar som
// noll (NULL i DB via nullableTime) i stället för år-1-tid.
type linkWire struct {
	Tenant          string     `json:"tenant"`
	Relation        string     `json:"relation"`
	FromID          string     `json:"fromId"`
	ToID            string     `json:"toId"`
	Revision        int64      `json:"revision"`
	ChangedAt       *time.Time `json:"changedAt,omitempty"`
	FromKind        string     `json:"fromKind,omitempty"`
	FromKindVersion string     `json:"fromKindVersion,omitempty"`
	ToKind          string     `json:"toKind,omitempty"`
	ToKindVersion   string     `json:"toKindVersion,omitempty"`
}

// MarshalJSON utelämnar noll-ChangedAt (bevarar NULL-semantik).
func (l Link) MarshalJSON() ([]byte, error) {
	w := linkWire{
		Tenant: l.Tenant, Relation: l.Relation, FromID: l.FromID, ToID: l.ToID,
		Revision: l.Revision,
		FromKind: l.FromKind, FromKindVersion: l.FromKindVersion,
		ToKind: l.ToKind, ToKindVersion: l.ToKindVersion,
	}
	if !l.ChangedAt.IsZero() {
		t := l.ChangedAt.UTC()
		w.ChangedAt = &t
	}
	return json.Marshal(w)
}

// UnmarshalJSON återställer saknad ChangedAt som nolltid.
func (l *Link) UnmarshalJSON(data []byte) error {
	var w linkWire
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	l.Tenant, l.Relation, l.FromID, l.ToID = w.Tenant, w.Relation, w.FromID, w.ToID
	l.Revision = w.Revision
	if w.ChangedAt != nil {
		l.ChangedAt = *w.ChangedAt
	} else {
		l.ChangedAt = time.Time{}
	}
	l.FromKind, l.FromKindVersion = w.FromKind, w.FromKindVersion
	l.ToKind, l.ToKindVersion = w.ToKind, w.ToKindVersion
	return nil
}

// LinksFromJSON tolkar ett bootstrap-underlag (JSON-array av Link).
// Okända/additiva fält tolereras; varje post kräver
// tenant/relation/fromId/toId — annars strukturfel (ingen tyst skip).
// Tom indata ger tom (icke-nil) lista.
func LinksFromJSON(data []byte) ([]Link, error) {
	if len(data) == 0 {
		return []Link{}, nil
	}
	var links []Link
	if err := json.Unmarshal(data, &links); err != nil {
		return nil, fmt.Errorf("%w: bootstrap links are not decodable: %v", ErrInvalidChange, err)
	}
	for i, l := range links {
		if l.Tenant == "" || l.Relation == "" || l.FromID == "" || l.ToID == "" {
			return nil, fmt.Errorf("%w: bootstrap link %d lacks tenant/relation/from/to", ErrInvalidChange, i)
		}
	}
	if links == nil {
		links = []Link{}
	}
	return links, nil
}

// LinksToJSON serialiserar ett underlag deterministiskt (sorterat på
// tenant/relation/from/to) för golden-filer och B-paritetstester.
func LinksToJSON(links []Link) ([]byte, error) {
	sorted := append([]Link(nil), links...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Tenant != sorted[j].Tenant {
			return sorted[i].Tenant < sorted[j].Tenant
		}
		if sorted[i].Relation != sorted[j].Relation {
			return sorted[i].Relation < sorted[j].Relation
		}
		if sorted[i].FromID != sorted[j].FromID {
			return sorted[i].FromID < sorted[j].FromID
		}
		return sorted[i].ToID < sorted[j].ToID
	})
	if sorted == nil {
		sorted = []Link{}
	}
	raw, err := json.Marshal(sorted)
	if err != nil {
		return nil, fmt.Errorf("relations: links are not encodable: %w", err)
	}
	return raw, nil
}

// KeysForPrune projicerar ett snapshot-underlag till prune-nycklar
// (relation/from/to per länk) för Store.BootstrapPrune.
func KeysForPrune(links []Link) []LinkKey {
	keys := make([]LinkKey, 0, len(links))
	for _, l := range links {
		keys = append(keys, l.Key())
	}
	return keys
}
