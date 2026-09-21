package things

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/diwise/iot-transform-fiware/internal/application/transform"
)

// thingValuesEnvelope är den lokala trådformen för versionerade
// värdesevent (things.v1.values). Den speglar producentens låsta K1-kontrakt
// fält för fält men är avsiktligt en egen typ: kopplingen mellan tjänsterna
// är tråd (topics/JSON), inte Go-moduler, så ingen import av producentens
// paket sker här.
type thingValuesEnvelope struct {
	Schema           string                 `json:"schema"`
	EventID          string                 `json:"eventId"`
	Tenant           string                 `json:"tenant"`
	ThingID          string                 `json:"thingId"`
	ThingType        string                 `json:"thingType"`
	ThingTypeVersion string                 `json:"thingTypeVersion"`
	ConfigRevision   int64                  `json:"configRevision"`
	PublishedAt      time.Time              `json:"publishedAt"`
	Thing            *thingValuesDescriptor `json:"thing"`
	Values           []thingValue           `json:"values"`
}

type thingValuesDescriptor struct {
	ThingID         string            `json:"thingId"`
	Name            string            `json:"name"`
	AlternativeName string            `json:"alternativeName"`
	SubType         string            `json:"subType"`
	Category        string            `json:"category"`
	Location        json.RawMessage   `json:"location"`
	Metadata        map[string]string `json:"metadata"`
	VariantID       string            `json:"variantId"`
	VariantVersion  string            `json:"variantVersion"`
}

type thingValue struct {
	PropertyID string    `json:"propertyId"`
	Value      *float64  `json:"value"`
	Quality    string    `json:"quality"`
	Unit       string    `json:"unit"`
	Source     string    `json:"source"`
	MessageID  string    `json:"messageId"`
	ObservedAt time.Time `json:"observedAt"`
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

	meta := Thing{
		ID:     env.ThingID,
		Type:   env.ThingType,
		Name:   env.ThingID,
		Tenant: env.Tenant,
	}
	var rawLocation json.RawMessage
	if env.Thing != nil {
		meta.Name = env.Thing.Name
		meta.AlternativeName = env.Thing.AlternativeName
		if sub := env.Thing.SubType; sub != "" {
			meta.SubType = &sub
		}
		rawLocation = env.Thing.Location
	}

	observedAt, fields := valuesFields(env.Values)
	if desc := env.Thing; desc != nil {
		if desc.Name != "" {
			fields["name"] = desc.Name
		}
		if desc.AlternativeName != "" {
			fields["alternativeName"] = desc.AlternativeName
		}
		if desc.Category != "" {
			fields["category"] = desc.Category
		}
		if len(rawLocation) > 0 {
			if ll, ok := geoPointToLatLon(rawLocation); ok {
				fields["location"] = ll
			}
		}
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
		Event:           transform.EventThingsV1Values,
		ObservedAt:      observedAt,
		Fields:          fields,
	}, nil
}

// valuesFields mappar värden till values.<propertyId>.{value,quality,unit,
// source,observedAt} och returnerar senaste mättid. Endast ok/uncertain tas
// med; invalid/okänt utelämnas så att konsumentregeln gäller (hoppa property,
// required hoppar entitet).
func valuesFields(values []thingValue) (time.Time, map[string]any) {
	var latest time.Time
	byProperty := make(map[string]any, len(values))
	for _, v := range values {
		if v.ObservedAt.After(latest) {
			latest = v.ObservedAt
		}
		if v.Quality != "ok" && v.Quality != "uncertain" {
			continue
		}
		if v.Value == nil {
			// Värdelös observation (nullable i kontraktet): behandla som
			// saknad källa så att required hoppar entiteten i stället för
			// att typfel ger omsändningsloop.
			continue
		}
		entry := map[string]any{
			"quality":    v.Quality,
			"source":     v.Source,
			"observedAt": v.ObservedAt.UTC().Format(time.RFC3339),
		}
		if v.Value != nil {
			entry["value"] = *v.Value
		}
		if v.Unit != "" {
			entry["unit"] = v.Unit
		}
		byProperty[v.PropertyID] = entry
	}
	return latest, map[string]any{"values": byProperty}
}

// geoPointToLatLon översätter en GeoJSON-Point ({type,coordinates:[lon,lat]})
// till motorns platsform ({latitude,longitude}). Icke-punkter ger false och
// utelämnas ur Fields, så att required:true hoppar entiteten i stället för
// att felaktig geometri ger omsändningsfel.
func geoPointToLatLon(raw json.RawMessage) (map[string]any, bool) {
	var geo struct {
		Type        string    `json:"type"`
		Coordinates []float64 `json:"coordinates"`
	}
	if err := json.Unmarshal(raw, &geo); err != nil {
		return nil, false
	}
	if geo.Type != "Point" || len(geo.Coordinates) < 2 {
		return nil, false
	}
	return map[string]any{
		"latitude":  geo.Coordinates[1],
		"longitude": geo.Coordinates[0],
	}, true
}
