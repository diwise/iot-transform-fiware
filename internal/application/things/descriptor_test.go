package things

import (
	"encoding/json"
	"testing"
)

func TestDescriptorsExposeRawFieldsForEveryEvent(t *testing.T) {
	const id = "879db55a-6102-4c44-9f6f-fd74a8c4eea9"
	descriptor := map[string]any{"thingId": id, "name": "Raw åäö name", "alternativeName": "Raw alias", "subType": "Raw subtype", "category": "source category",
		"metadata": map[string]any{"description": "source description"}, "variantId": "variant-1", "variantVersion": "2", "location": map[string]any{"type": "Point", "coordinates": []any{17.5, 62.3}}}
	for _, event := range []string{"values", "lifecycle", "relations"} {
		t.Run(event, func(t *testing.T) {
			body := map[string]any{"thingId": id, "thingType": "Raw type", "tenant": "test", "thing": descriptor, "values": []any{}, "kind": "updated", "changedAt": "2026-10-01T12:00:00Z",
				"fromId": id, "toId": id, "fromThingType": "Raw type", "toThingType": "Raw type", "from": descriptor, "to": descriptor, "type": "source-relation"}
			raw, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			input, err := ThingValuesFromJSON(raw)
			paths := []string{"thing"}
			if event == "lifecycle" {
				input, err = ThingLifecycleFromJSON(raw)
			}
			if event == "relations" {
				input, err = ThingRelationsFromJSON(raw)
				paths = []string{"relation.from", "relation.to"}
			}
			if err != nil {
				t.Fatal(err)
			}
			if input.ID != id || input.Name != "Raw åäö name" || input.Variables()["type"] != "Raw type" || input.Variables()["typeName"] != "Raw subtype" {
				t.Fatalf("raw identity/name/type changed: %+v", input)
			}
			for _, path := range paths {
				for key, want := range map[string]string{"thingId": id, "name": "Raw åäö name", "alternativeName": "Raw alias", "type": "Raw type", "typeName": "Raw subtype", "category": "source category", "variantId": "variant-1", "variantVersion": "2", "metadata.description": "source description", "location.type": "Point"} {
					got, ok := input.ResolvePath(path + "." + key)
					if !ok || got != want {
						t.Fatalf("%s.%s = %#v, want %q", path, key, got, want)
					}
				}
				if _, provided := input.ResolvePath(path + ".nameOrID"); provided {
					t.Fatalf("removed helper provided under %s", path)
				}
			}
		})
	}
}

func TestMissingNamesDoNotFallBackToID(t *testing.T) {
	for _, parse := range []struct {
		name string
		body string
	}{
		{"values", `{"thingId":"stable-id","thingType":"source-type","tenant":"test","values":[]}`},
		{"lifecycle", `{"thingId":"stable-id","thingType":"source-type","tenant":"test","kind":"deleted"}`},
	} {
		t.Run(parse.name, func(t *testing.T) {
			input, err := ThingValuesFromJSON([]byte(parse.body))
			if parse.name == "lifecycle" {
				input, err = ThingLifecycleFromJSON([]byte(parse.body))
			}
			if err != nil {
				t.Fatal(err)
			}
			if input.Name != "" || input.AlternativeName != "" || input.ID != "stable-id" {
				t.Fatalf("missing names invented from ID: %+v", input)
			}
		})
	}
}
