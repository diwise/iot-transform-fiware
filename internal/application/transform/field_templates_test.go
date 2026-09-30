package transform

import (
	"errors"
	"testing"
)

func TestTemplatesResolveArbitrarySourceFields(t *testing.T) {
	in := Thing{ID: "f7cf7057-d422-4e46-af2b-a8d40c6e739", Event: EventThingsV1Values,
		Fields: map[string]any{
			"custom": "a value", "metadata": map[string]any{"description": "Raw description"},
			"values": map[string]any{"temperature.average": map[string]any{"value": 21.5}},
			"labels": []any{"first", "second"}, "enabled": false,
			"geometry": map[string]any{"type": "Point", "coordinates": []any{17.5, 62.3}},
		}}
	for template, want := range map[string]string{
		"{{id}}": in.ID, "{{custom}}": "a value", "{{metadata.description}}": "Raw description",
		`{{values."temperature.average".value}}`: "21.5", "{{labels[1]}}": "second", "{{enabled}}": "false",
		"{{safeURI(custom)}}": "a%20value", "{{firstPresent(missing,metadata.description)}}": "Raw description",
		"{{geometry}}": `{"coordinates":[17.5,62.3],"type":"Point"}`,
	} {
		t.Run(template, func(t *testing.T) {
			rule := expressionRule()
			rule.Entities[0].Properties[0].Value = template
			if err := Validate([]Rule{rule}); err != nil {
				t.Fatalf("valid field template: %v", err)
			}
			got, err := renderTemplate(template, in, nil)
			if err != nil || got != want {
				t.Fatalf("got %q, want %q: %v", got, want, err)
			}
		})
	}
	if _, err := renderTemplate("{{missing}}", in, nil); !errors.Is(err, errExprNotFound) {
		t.Fatalf("missing source classification: %v", err)
	}
}

func TestMeasurementDefaultIDAndDynamicTemplateFields(t *testing.T) {
	value := 7.0
	in := Measurement{DeviceID: "7dc1d1e9-2b73-4324-b64f-bc35cb8b0dc3", Records: []Record{{Name: "custom", Value: &value}}}
	rule := validRule()
	rule.Entities[0].ID = "urn:example:{{id}}"
	rule.Entities[0].Properties[0].Source = Source{Value: "{{custom}}"}
	if err := Validate([]Rule{rule}); err != nil {
		t.Fatalf("dynamic measurement field: %v", err)
	}
	for template, want := range map[string]string{"{{id}}": in.DeviceID, "{{custom}}": "7", "{{field:custom}}": "7"} {
		got, err := renderTemplate(template, in, nil)
		if err != nil || got != want {
			t.Fatalf("%s: got %q want %q: %v", template, got, want, err)
		}
	}
}
