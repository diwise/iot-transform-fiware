package things

import (
	"encoding/json"
	"fmt"
	"maps"
	"time"

	"github.com/diwise/iot-transform-fiware/internal/application/transform"
)

// thingValuesEnvelope är den lokala trådformen för versionerade
// värdesevent (things.v1.values). Den speglar producentens låsta K1-kontrakt
// fält för fält men är avsiktligt en egen typ: kopplingen mellan tjänsterna
// är tråd (topics/JSON), inte Go-moduler, så ingen import av producentens
// paket sker här.
type thingValuesEnvelope struct {
	Schema           string                  `json:"schema"`
	EventID          string                  `json:"eventId"`
	Tenant           string                  `json:"tenant"`
	ThingID          string                  `json:"thingId"`
	ThingType        string                  `json:"thingType"`
	ThingTypeVersion string                  `json:"thingTypeVersion"`
	ConfigRevision   int64                   `json:"configRevision"`
	PublishedAt      time.Time               `json:"publishedAt"`
	Thing            *thingDescriptor        `json:"thing"`
	Values           []thingValue            `json:"values"`
	Relations        []thingRelationEnvelope `json:"relations"`
}

type thingValue struct {
	Schema         string    `json:"schema"`
	EventID        string    `json:"eventId"`
	ThingID        string    `json:"thingId"`
	Tenant         string    `json:"tenant"`
	PropertyID     string    `json:"propertyId"`
	Value          *float64  `json:"value"`
	Quality        string    `json:"quality"`
	Unit           string    `json:"unit"`
	Source         string    `json:"source"`
	MessageID      string    `json:"messageId"`
	ObservedAt     time.Time `json:"observedAt"`
	ReceivedAt     time.Time `json:"receivedAt"`
	ComputedAt     time.Time `json:"computedAt"`
	PublishedAt    time.Time `json:"publishedAt"`
	ConfigRevision int64     `json:"configRevision"`
}

// ThingValuesFromJSON tolkar ett versionerat värdesevent och bygger motorns
// indata. Kontraktsregler (K1): saknad/ogiltig kvalitet ersätter aldrig
// aktuellt — därför tas endast ok/uncertain-värden med i Fields, så att
// required:true hoppar entiteten när en krävd källa saknas eller är ogiltig.
// Konsumenttid är max(values[].observedAt) (mättid, aldrig transporttid).
func ThingValuesFromJSON(body []byte) (transform.Thing, error) {
	var env thingValuesEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return transform.Thing{}, err
	}
	if env.ThingID == "" {
		return transform.Thing{}, fmt.Errorf("things.v1.values: missing thingId")
	}
	// Tom tenant är inget strukturfel: presentationen skippar utan
	// broker-kontakt (samma grind som thing.updated).

	input := descriptorInput(env.ThingID, env.ThingType, env.Tenant, env.Thing)
	input.Event = transform.EventThingsV1Values
	observedAt, fields := valuesFields(env.Values)
	input.ObservedAt = observedAt
	maps.Copy(input.Fields, fields)
	event := map[string]any{
		"schema": env.Schema, "eventId": env.EventID, "tenant": env.Tenant, "thingId": env.ThingID,
		"thingType": env.ThingType, "thingTypeVersion": env.ThingTypeVersion, "configRevision": env.ConfigRevision,
	}
	addSourceTime(event, "publishedAt", env.PublishedAt)
	input.Fields["event"] = event
	if len(env.Relations) > 0 {
		relations := make([]any, len(env.Relations))
		for i, relation := range env.Relations {
			relations[i] = relationFields(relation)
		}
		input.Fields["relations"] = relations
	}
	return input, nil
}

// valuesFields mappar värden till values.<propertyId>.{value,quality,unit,
// source,observedAt} och returnerar senaste mättid. Endast ok/uncertain tas
// med; invalid/okänt utelämnas så att konsumentregeln gäller (hoppa property,
// required hoppar entitet).
func valuesFields(values []thingValue) (time.Time, map[string]any) {
	var latest time.Time
	byProperty := make(map[string]any, len(values))
	observations := make([]any, 0, len(values))
	for _, v := range values {
		if v.ObservedAt.After(latest) {
			latest = v.ObservedAt
		}
		provenance := valueProvenance(v)
		provenance["valuePresent"] = v.Value != nil
		observations = append(observations, provenance)
		if v.Quality != "ok" && v.Quality != "uncertain" {
			continue
		}
		if v.Value == nil {
			// Värdelös observation (nullable i kontraktet): behandla som
			// saknad källa så att required hoppar entiteten i stället för
			// att typfel ger omsändningsloop.
			continue
		}
		entry := valueProvenance(v)
		entry["value"] = *v.Value
		byProperty[v.PropertyID] = entry
	}
	return latest, map[string]any{"values": byProperty, "observations": observations}
}

func valueProvenance(v thingValue) map[string]any {
	fields := map[string]any{
		"schema": v.Schema, "eventId": v.EventID, "thingId": v.ThingID, "tenant": v.Tenant,
		"propertyId": v.PropertyID, "quality": v.Quality, "source": v.Source,
		"messageId": v.MessageID, "configRevision": v.ConfigRevision,
	}
	if v.Unit != "" {
		fields["unit"] = v.Unit
	}
	addSourceTime(fields, "observedAt", v.ObservedAt)
	addSourceTime(fields, "receivedAt", v.ReceivedAt)
	addSourceTime(fields, "computedAt", v.ComputedAt)
	addSourceTime(fields, "publishedAt", v.PublishedAt)
	return fields
}
