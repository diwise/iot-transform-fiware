package things

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/diwise/iot-transform-fiware/internal/application/transform"
)

// thingRelationDescriptor är sakdeskriptorn i en relationshändelse
// (samma form som värdeseventens thing-descriptor).
type thingRelationDescriptor struct {
	ThingID         string `json:"thingId"`
	Name            string `json:"name"`
	AlternativeName string `json:"alternativeName"`
	SubType         string `json:"subType"`
	Category        string `json:"category"`
}

// thingRelationEnvelope är den lokala trådformen för versionerade
// relationshändelser (things.v1.relations). Samma princip som values.go:
// egen typ, ingen import av producentpaketet.
type thingRelationEnvelope struct {
	RelationID    string                   `json:"relationId"`
	Type          string                   `json:"type"`
	FromID        string                   `json:"fromId"`
	ToID          string                   `json:"toId"`
	Removed       bool                     `json:"removed"`
	Tenant        string                   `json:"tenant"`
	Revision      int64                    `json:"revision"`
	ChangedAt     time.Time                `json:"changedAt"`
	FromThingType string                   `json:"fromThingType"`
	From          *thingRelationDescriptor `json:"from"`
	ToThingType   string                   `json:"toThingType"`
	To            *thingRelationDescriptor `json:"to"`
}

// ThingRelationsFromJSON tolkar en versionerad relationshändelse och bygger
// motorns indata. Saknat fromId är strukturfel (identitet krävs); saknad
// typ matchar därefter ingen typad regel utan att felet eskaleras —
// tom tenant är inget strukturfel (presentationen skippar utan
// broker-kontakt, samma grind som thing.updated).
//
// From-sidan blir motorns sak (ID/type/subType/namn med samma konvention
// som sakens egna regler, så {{nameOrID}}/{{type}} renderar identiskt).
// relation.to.* bär förälderns förberäknade typeName/nameOrID plus råa
// fält; relation.from.waterQualityObserved är den konventionella
// observations-URN:en för from-saken som en-element-array (speglar
// tank-vattenregelns id-mall; regler avgränsar via match.type=tank).
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

	toFields := map[string]any{
		"typeName": "", "nameOrID": "", "thingId": "",
		"subType": "", "category": "", "name": "", "alternativeName": "",
	}
	if env.To != nil || env.ToThingType != "" || env.ToID != "" {
		toID := env.ToID
		var toName, toAlt, toSub, toCat string
		if env.To != nil {
			if env.To.ThingID != "" {
				toID = env.To.ThingID
			}
			toName, toAlt = env.To.Name, env.To.AlternativeName
			toSub, toCat = env.To.SubType, env.To.Category
		}
		toMeta := Thing{ID: toID, Type: env.ToThingType, Name: toName, AlternativeName: toAlt}
		if toSub != "" {
			sub := toSub
			toMeta.SubType = &sub
		}
		toFields["typeName"] = toMeta.TypeName()
		toFields["nameOrID"] = toMeta.AlternativeNameOrNameOrID()
		toFields["thingId"] = toID
		toFields["subType"] = toSub
		toFields["category"] = toCat
		toFields["name"] = toName
		toFields["alternativeName"] = toAlt
	}

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
			"from": map[string]any{
				// En-element-array: hasWaterQualityObserved är en
				// URI-array i modellen; fältkälla med required ger skip
				// (aldrig retry) om den mot förmodan saknas.
				"waterQualityObserved": []any{
					"urn:ngsi-ld:WaterQualityObserved:" + meta.AlternativeNameOrNameOrID(),
				},
			},
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
		NameOrID:        meta.AlternativeNameOrNameOrID(),
		Event:           transform.EventThingsV1Relations,
		Relation:        env.Type,
		RelationRemoved: env.Removed,
		ObservedAt:      env.ChangedAt,
		Fields:          fields,
	}, nil
}
