package things

import "encoding/json"

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
