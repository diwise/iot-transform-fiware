// Package relations — C2-gränssnitt (Våg 2).
//
// ChangeFromJSON är det minimala C2-gränssnittet: den mappar
// ThingRelationsFromJSON-trådformen (things.v1.relations, samma JSON som
// C1:s adapter läser) till relations.Change — den neutrala
// projektionsindatan. Ren hjälpare (ingen I/O, ingen context): C2 anropar
// den i handlern, fångar trace via projection.CaptureTrace(transport-ctx)
// och kallar Store.ApplyChange(change, trace, planner). C äger
// inkopplingen i Våg 3; denna fil ändrar inga handlerfiler.
//
// Fältmappningen:
//   - relationId → RelationID (stabil identitet, se BuildRelationID),
//   - tenant → Tenant, type → Relation, fromId → FromID, toId → ToID,
//   - previousToId → PreviousToID (flytt: atomärt remove+set),
//   - removed → Removed, revision → Revision, changedAt → ChangedAt,
//   - fromThingType → FromKind (fryst källtyp), fromThingTypeVersion →
//     FromKindVersion, toThingType → ToKind, toThingTypeVersion →
//     ToKindVersion,
//   - ToTenant lämnas tom (B:s payload är strikt intra-tenant; avvikelse
//     nekas av ValidateChange om den sätts av annan väg).
//
// Paritet med C1 (internal/application/things): saknat fromId är strukturfel,
// motstridiga kuvert-/deskriptoridentiteter (from.thingId vs fromId,
// to.thingId vs toId) är strukturfel utan tyst last-write-wins, okända/
// additiva fält tolereras (ingen DisallowUnknownFields), saknad to-sida
// tolereras (äldre producent/okänt mål). Tom tenant är inget strukturfel
// här — presentationen skippar utan brokerkontakt (samma grind som
// thing.updated); lagret nekar tom tenant vid behov.
package relations

import (
	"encoding/json"
	"fmt"
	"time"
)

// changeDescriptorRef speglar producentens frysta ändpunktsdeskriptor så
// långt F behöver den: endast identiteten kontrolleras här (konflikt mot
// kuvertet nekas); övriga deskriptorfält stannar hos C1/E.
type changeDescriptorRef struct {
	ThingID string `json:"thingId"`
}

// changeEnvelope är trådformen för things.v1.relations — samma JSON som
// C1:s ThingRelationsFromJSON läser (egen typ, ingen import av
// producentpaketet; tråd, inte Go-moduler, är kopplingen).
type changeEnvelope struct {
	Schema               string               `json:"schema"`
	EventID              string               `json:"eventId"`
	RelationID           string               `json:"relationId"`
	Type                 string               `json:"type"`
	FromID               string               `json:"fromId"`
	ToID                 string               `json:"toId"`
	PreviousToID         string               `json:"previousToId"`
	Removed              bool                 `json:"removed"`
	Tenant               string               `json:"tenant"`
	Revision             int64                `json:"revision"`
	ChangedAt            time.Time            `json:"changedAt"`
	FromThingType        string               `json:"fromThingType"`
	FromThingTypeVersion string               `json:"fromThingTypeVersion"`
	From                 *changeDescriptorRef `json:"from"`
	ToThingType          string               `json:"toThingType"`
	ToThingTypeVersion   string               `json:"toThingTypeVersion"`
	To                   *changeDescriptorRef `json:"to"`
}

// ChangeFromJSON mappar en versionerad relationshändelse till Change.
// Exempel (generiskt, inga modellnamn):
//
//	change, err := relations.ChangeFromJSON([]byte(`{
//	  "relationId":"v1/tenant-a/from-1/relations/contains/to-1/3/set",
//	  "type":"contains","fromId":"from-1","toId":"to-1",
//	  "tenant":"tenant-a","revision":3,
//	  "changedAt":"2026-10-05T12:00:00Z",
//	  "fromThingType":"kind-a","fromThingTypeVersion":"1",
//	  "toThingType":"kind-b","toThingTypeVersion":"2"}`))
//	// trace := projection.CaptureTrace(ctx)  // C2: separat från JSON
//	// res, err := store.ApplyChange(ctx, change, trace, planner)
func ChangeFromJSON(body []byte) (Change, error) {
	var env changeEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return Change{}, fmt.Errorf("%w: relations change is not decodable: %v", ErrInvalidChange, err)
	}
	if env.FromID == "" {
		return Change{}, fmt.Errorf("%w: fromId is required", ErrInvalidChange)
	}
	if env.From != nil && env.From.ThingID != "" && env.From.ThingID != env.FromID {
		return Change{}, fmt.Errorf("%w: from thingId %q conflicts with fromId %q", ErrInvalidChange, env.From.ThingID, env.FromID)
	}
	if env.To != nil && env.To.ThingID != "" && env.ToID != "" && env.To.ThingID != env.ToID {
		return Change{}, fmt.Errorf("%w: to thingId %q conflicts with toId %q", ErrInvalidChange, env.To.ThingID, env.ToID)
	}
	c := Change{
		RelationID:      env.RelationID,
		Tenant:          env.Tenant,
		Relation:        env.Type,
		FromID:          env.FromID,
		ToID:            env.ToID,
		PreviousToID:    env.PreviousToID,
		Removed:         env.Removed,
		Revision:        env.Revision,
		ChangedAt:       env.ChangedAt.UTC(),
		FromKind:        env.FromThingType,
		FromKindVersion: env.FromThingTypeVersion,
		ToKind:          env.ToThingType,
		ToKindVersion:   env.ToThingTypeVersion,
	}
	// Tom tenant är inget strukturfel här — paritet med C1
	// (ThingRelationsFromJSON): presentationen skippar utan brokerkontakt
	// (samma grind som thing.updated). C2 kontrollerar `change.Tenant == ""`
	// före ApplyChange; lagret nekar tom tenant vid behov.
	if c.Tenant == "" {
		probe := c
		probe.Tenant = "probe-tenant"
		probe.RelationID = ""
		if err := ValidateChange(probe); err != nil {
			return Change{}, err
		}
		return c, nil
	}
	if err := ValidateChange(c); err != nil {
		return Change{}, err
	}
	if err := CheckRelationID(c); err != nil {
		return Change{}, err
	}
	return c, nil
}
