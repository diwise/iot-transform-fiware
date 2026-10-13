package things

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/diwise/iot-transform-fiware/internal/application/transform"
)

// thingRelationEnvelope är den lokala trådformen för versionerade
// relationshändelser (things.v1.relations). Samma princip som values.go:
// egen typ, ingen import av producentpaketet. JSON avkodas utan
// DisallowUnknownFields: okända/additiva fält tolereras.
//
// Additiv v1-baslinje (PLAN1050 A): stabil relationsidentitet med
// relationId/previousToId/revision, frysta ändpunktsdescriptors och
// målversioner (fromThingTypeVersion/toThingTypeVersion). Äldre payloads
// utan previousToId/versioner tolereras. previousToId bär föregående mål
// vid flytt (set med nytt toId); removed=true bär sista kända målet i toId.
type thingRelationEnvelope struct {
	Schema               string           `json:"schema"`
	EventID              string           `json:"eventId"`
	RelationID           string           `json:"relationId"`
	Type                 string           `json:"type"`
	FromID               string           `json:"fromId"`
	ToID                 string           `json:"toId"`
	PreviousToID         string           `json:"previousToId"`
	Removed              bool             `json:"removed"`
	Tenant               string           `json:"tenant"`
	Revision             int64            `json:"revision"`
	ChangedAt            time.Time        `json:"changedAt"`
	FromThingType        string           `json:"fromThingType"`
	FromThingTypeVersion string           `json:"fromThingTypeVersion"`
	From                 *thingDescriptor `json:"from"`
	ToThingType          string           `json:"toThingType"`
	ToThingTypeVersion   string           `json:"toThingTypeVersion"`
	To                   *thingDescriptor `json:"to"`
}

// ThingRelationsFromJSON tolkar en versionerad relationshändelse och bygger
// motorns indata. Delad normalisering med preview (paritet
// produktion/preview): preview anropar samma funktion. Saknat fromId är
// strukturfel (identitet krävs, Permanent); saknad typ matchar därefter
// ingen typad regel utan att felet eskaleras — tom tenant är inget
// strukturfel (presentationen skippar utan broker-kontakt, samma grind som
// thing.updated). Schema/content-type tolereras additivt (loggas av
// presentationen).
//
// From-sidan blir motorns sak med rått ID, typ och namn. Båda sidor
// exponerar generella descriptors; mål-ID och modell väljs enbart av regeln.
// Alla dokumenterade nycklar sätts alltid (tom sträng vid saknad) så att
// firstPresent beter sig deterministiskt. Rå GeoJSON exponeras konsekvent
// (Point-genväg döljer aldrig Polygon/3D); inga modellnamn sätts här.
// Ingen recency-/ordningsfiltrering sker: sena event är en dokumenterad
// avgränsning, revisioner bär metadata/konflikter/diagnostik.
func ThingRelationsFromJSON(body []byte) (transform.Thing, error) {
	var env thingRelationEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return transform.Thing{}, err
	}
	if env.FromID == "" {
		return transform.Thing{}, fmt.Errorf("things.v1.relations: missing fromId")
	}
	// Tom tenant är inget strukturfel: presentationen skippar utan
	// broker-kontakt (samma grind som thing.updated).

	fields, err := validatedRelationFields(env)
	if err != nil {
		return transform.Thing{}, err
	}

	input := descriptorInput(env.FromID, env.FromThingType, env.Tenant, env.From)
	input.Event, input.Relation, input.RelationRemoved, input.ObservedAt = transform.EventThingsV1Relations, env.Type, env.Removed, env.ChangedAt
	input.Fields["relation"] = fields
	// relations[] (en-element) exponeras parallellt med relation (singular)
	// så att E/F-regler kan läsa enhetligt mot values-events relations[].
	input.Fields["relations"] = []any{fields}
	input.Fields["event"] = fields
	return input, nil
}

// validatedRelationFields validerar motstridiga ändpunktsidentiteter utan
// tysta last-write-wins: deskriptorns thingId får inte strida mot
// kuvertets fromId/toId när båda är satta. Saknad to-sida tolereras
// (äldre producent / okänt mål).
func validatedRelationFields(env thingRelationEnvelope) (map[string]any, error) {
	if env.From != nil && env.From.ThingID != "" && env.From.ThingID != env.FromID {
		return nil, fmt.Errorf("things.v1.relations: from thingId %q conflicts with fromId %q", env.From.ThingID, env.FromID)
	}
	if env.To != nil && env.To.ThingID != "" && env.ToID != "" && env.To.ThingID != env.ToID {
		return nil, fmt.Errorf("things.v1.relations: to thingId %q conflicts with toId %q", env.To.ThingID, env.ToID)
	}
	return relationFields(env), nil
}

func relationFields(env thingRelationEnvelope) map[string]any {
	fields := map[string]any{
		"relationId":    env.RelationID,
		"type":          env.Type,
		"fromId":        env.FromID,
		"toId":          env.ToID,
		"previousToId":  env.PreviousToID,
		"removed":       env.Removed,
		"revision":      env.Revision,
		"schema":        env.Schema,
		"eventId":       env.EventID,
		"tenant":        env.Tenant,
		"fromThingType": env.FromThingType, "fromThingTypeVersion": env.FromThingTypeVersion,
		"toThingType": env.ToThingType, "toThingTypeVersion": env.ToThingTypeVersion,
		"to":   descriptorFields(env.ToID, env.ToThingType, env.To),
		"from": descriptorFields(env.FromID, env.FromThingType, env.From),
	}
	addSourceTime(fields, "changedAt", env.ChangedAt)
	return fields
}
