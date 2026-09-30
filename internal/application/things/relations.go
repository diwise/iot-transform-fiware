package things

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/diwise/iot-transform-fiware/internal/application/transform"
)

// thingRelationEnvelope är den lokala trådformen för versionerade
// relationshändelser (things.v1.relations). Samma princip som values.go:
// egen typ, ingen import av producentpaketet.
type thingRelationEnvelope struct {
	RelationID    string           `json:"relationId"`
	Type          string           `json:"type"`
	FromID        string           `json:"fromId"`
	ToID          string           `json:"toId"`
	Removed       bool             `json:"removed"`
	Tenant        string           `json:"tenant"`
	Revision      int64            `json:"revision"`
	ChangedAt     time.Time        `json:"changedAt"`
	FromThingType string           `json:"fromThingType"`
	From          *thingDescriptor `json:"from"`
	ToThingType   string           `json:"toThingType"`
	To            *thingDescriptor `json:"to"`
}

// ThingRelationsFromJSON tolkar en versionerad relationshändelse och bygger
// motorns indata. Saknat fromId är strukturfel (identitet krävs); saknad
// typ matchar därefter ingen typad regel utan att felet eskaleras —
// tom tenant är inget strukturfel (presentationen skippar utan
// broker-kontakt, samma grind som thing.updated).
//
// From-sidan blir motorns sak med rått ID, typ och namn. Båda sidor
// exponerar generella descriptors; mål-ID och modell väljs enbart av regeln.
// Alla dokumenterade nycklar sätts alltid (tom sträng vid saknad) så att
// firstPresent beter sig deterministiskt.
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

	meta := Thing{ID: env.FromID, Type: env.FromThingType, Tenant: env.Tenant}
	if env.From != nil {
		meta.Name = env.From.Name
		meta.AlternativeName = env.From.AlternativeName
		if env.From.SubType != "" {
			sub := env.From.SubType
			meta.SubType = &sub
		}
	}

	toFields := descriptorFields(env.ToID, env.ToThingType, env.To)

	fields := map[string]any{
		"relation": map[string]any{
			"relationId": env.RelationID,
			"type":       env.Type,
			"fromId":     env.FromID,
			"toId":       env.ToID,
			"removed":    env.Removed,
			"revision":   env.Revision,
			"changedAt":  env.ChangedAt.UTC().Format(time.RFC3339),
			"to":         toFields,
			"from":       descriptorFields(env.FromID, env.FromThingType, env.From),
		},
	}

	return transform.Thing{
		ID:              meta.ID,
		Type:            meta.Type,
		SubType:         stringOrEmpty(meta.SubType),
		Name:            meta.Name,
		AlternativeName: meta.AlternativeName,
		Tenant:          meta.Tenant,
		TypeName:        meta.TypeName(),
		Event:           transform.EventThingsV1Relations,
		Relation:        env.Type,
		RelationRemoved: env.Removed,
		ObservedAt:      env.ChangedAt,
		Fields:          fields,
	}, nil
}
