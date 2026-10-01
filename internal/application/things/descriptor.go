package things

import (
	"encoding/json"
	"time"

	"github.com/diwise/iot-transform-fiware/internal/application/transform"
)

// thingDescriptor speglar producentens ThingDescriptorV1 för samtliga event.
type thingDescriptor struct {
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

func descriptorFields(id, sourceType string, descriptor *thingDescriptor) map[string]any {
	fields := map[string]any{"thingId": id, "type": sourceType, "typeName": sourceType,
		"name": "", "alternativeName": "", "subType": "", "category": "", "variantId": "", "variantVersion": ""}
	if descriptor == nil {
		return fields
	}
	if descriptor.ThingID != "" {
		fields["thingId"] = descriptor.ThingID
	}
	fields["name"], fields["alternativeName"] = descriptor.Name, descriptor.AlternativeName
	fields["subType"], fields["category"] = descriptor.SubType, descriptor.Category
	fields["variantId"], fields["variantVersion"] = descriptor.VariantID, descriptor.VariantVersion
	if descriptor.SubType != "" {
		fields["typeName"] = descriptor.SubType
	}
	if len(descriptor.Metadata) > 0 {
		metadata := make(map[string]any, len(descriptor.Metadata))
		for key, value := range descriptor.Metadata {
			metadata[key] = value
		}
		fields["metadata"] = metadata
	}
	if len(descriptor.Location) > 0 {
		var location map[string]any
		if json.Unmarshal(descriptor.Location, &location) == nil && location != nil {
			fields["location"] = location
		}
	}
	return fields
}

// descriptorInput keeps the envelope's identity authoritative while exposing
// the descriptor, raw GeoJSON and optional Point shortcut for every source.
func descriptorInput(id, sourceType, tenant string, descriptor *thingDescriptor) transform.Thing {
	desc := descriptorFields(id, sourceType, descriptor)
	fields := make(map[string]any, len(desc)+1)
	for key, value := range desc {
		if key != "location" {
			fields[key] = value
		}
	}
	fields["thingId"], fields["tenant"], fields["thing"] = id, tenant, desc
	if descriptor != nil {
		if geometry, ok := desc["location"]; ok {
			fields["geometry"] = geometry
		}
		if point, ok := geoPointToLatLon(descriptor.Location); ok {
			fields["location"] = point
		}
	}
	return transform.Thing{
		ID: id, Type: sourceType, Tenant: tenant, Fields: fields,
		Name: desc["name"].(string), AlternativeName: desc["alternativeName"].(string),
		SubType: desc["subType"].(string), TypeName: desc["typeName"].(string),
	}
}

func addSourceTime(fields map[string]any, key string, value time.Time) {
	if !value.IsZero() {
		fields[key] = value.UTC().Format(time.RFC3339Nano)
	}
}

// Point shortcuts complement raw geometry; they never replace it or discard
// extra coordinates from the descriptor's GeoJSON view.
func geoPointToLatLon(raw json.RawMessage) (map[string]any, bool) {
	var geo struct {
		Type        string    `json:"type"`
		Coordinates []float64 `json:"coordinates"`
	}
	if json.Unmarshal(raw, &geo) != nil || geo.Type != "Point" || len(geo.Coordinates) < 2 {
		return nil, false
	}
	return map[string]any{"latitude": geo.Coordinates[1], "longitude": geo.Coordinates[0]}, true
}
