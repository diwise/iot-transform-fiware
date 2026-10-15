package config

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/iot-transform-fiware/internal/application/things"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
)

const sourceGUID = "a7fe38f7-7a2f-44f7-9a95-9d54b4ba5bf2"
const parentGUID = "8b5c9f72-ef60-4e86-92ef-a1572644fa39"

func valuesPayload(id, sourceType, name, alias, subType string) map[string]any {
	values := []any{}
	for _, property := range []string{"temperature.average", "temperature", "humidity", "co2", "fillRate", "presence", "run", "overflow", "level", "ph"} {
		values = append(values, map[string]any{"propertyId": property, "value": 21.5, "quality": "ok", "source": "neutral-source", "observedAt": "2026-10-01T10:00:00Z"})
	}
	return map[string]any{"schema": "v1", "eventId": "neutral-event", "configRevision": 1, "publishedAt": "2026-10-01T10:00:01Z", "thingId": id, "thingType": sourceType, "tenant": "test", "values": values,
		"thing": map[string]any{"thingId": id, "name": name, "alternativeName": alias, "subType": subType,
			"metadata": map[string]any{"description": "Source description"},
			"location": map[string]any{"type": "Point", "coordinates": []any{17.5, 62.3}}}}
}

func marshalPayload(t *testing.T, payload any) []byte {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func writeJSON(t *testing.T, write transform.EntityWrite) map[string]any {
	t.Helper()
	entity, err := entities.New(write.EntityID, write.TypeName, write.Props...)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal(marshalPayload(t, entity), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestShippedRulesKeepGUIDAcrossNamesAndLifecycle(t *testing.T) {
	rules, err := LoadModelsDir("../../../deployments/models")
	if err != nil {
		t.Fatal(err)
	}
	engine := things.NewDynamicThingTransformer(rules)
	for sourceType, modelType := range map[string]string{
		"room": "IndoorEnvironmentObserved", "wastebin": "WasteContainer",
		"lifebuoy": "Lifebuoy", "passage": "Device", "pumpingstation": "SewagePumpingStation", "sewer": "Sewer", "pointofinterest": "PointOfInterest",
		"tank": "WaterQualityObserved", "greenhouse": "AgriGreenhouse",
	} {
		t.Run(sourceType, func(t *testing.T) {
			wantedID := "urn:ngsi-ld:" + modelType + ":" + sourceGUID
			for _, names := range [][3]string{{"Original name", "Original alias", "Original subtype"}, {"Renamed åäö", "Renamed alias", "New subtype"}} {
				payload := valuesPayload(sourceGUID, sourceType, names[0], names[1], names[2])
				input, err := things.ThingValuesFromJSON(marshalPayload(t, payload))
				if err != nil {
					t.Fatal(err)
				}
				writes, err := engine.Transform(input)
				if sourceType == "pointofinterest" {
					if err != nil || len(writes) != 2 {
						t.Fatalf("values: %+v %v", writes, err)
					}
					if writes[0].EntityID != wantedID {
						t.Fatalf("name/subtype affected identity: %s", writes[0].EntityID)
					}
					if writes[1].EntityID != "urn:ngsi-ld:WeatherObserved:"+sourceGUID || writes[1].TypeName != "WeatherObserved" {
						t.Fatalf("poi weather identity lost: %+v", writes[1])
					}
				} else if sourceType == "greenhouse" {
					if err != nil || len(writes) != 2 {
						t.Fatalf("values: %+v %v", writes, err)
					}
					if writes[0].EntityID != wantedID || writes[0].TypeName != "AgriGreenhouse" {
						t.Fatalf("greenhouse agri identity lost: %+v", writes[0])
					}
					if writes[1].EntityID != "urn:ngsi-ld:IndoorEnvironmentObserved:"+sourceGUID || writes[1].TypeName != "IndoorEnvironmentObserved" {
						t.Fatalf("greenhouse indoor identity lost: %+v", writes[1])
					}
				} else if err != nil || len(writes) != 1 {
					t.Fatalf("values: %+v %v", writes, err)
				}
				if writes[0].EntityID != wantedID {
					t.Fatalf("name/subtype affected identity: %s", writes[0].EntityID)
				}
				entity := writeJSON(t, writes[0])
				if entity["name"].(map[string]any)["value"] != names[0] || entity["alternateName"].(map[string]any)["value"] != names[1] {
					t.Fatalf("names not mapped as attributes: %+v", entity)
				}
				if sourceType == "pointofinterest" && entity["description"].(map[string]any)["value"] != "Source description" {
					t.Fatal("POI description lost")
				}
				lifecycleBody := map[string]any{"schema": "v1", "eventId": "neutral-delete", "thingId": sourceGUID, "thingType": sourceType, "tenant": "test", "configRevision": 2, "kind": "deleted", "changedAt": "2026-10-01T11:00:00Z", "thing": payload["thing"]}
				lifecycle, err := things.ThingLifecycleFromJSON(marshalPayload(t, lifecycleBody))
				if err != nil {
					t.Fatal(err)
				}
				deleted, err := engine.Transform(lifecycle)
				if sourceType == "pointofinterest" {
					if err != nil || len(deleted) != 2 || !deleted[0].Delete || !deleted[1].Delete {
						t.Fatalf("delete identity differs from values: %+v %v", deleted, err)
					}
					if deleted[0].EntityID != wantedID || deleted[1].EntityID != "urn:ngsi-ld:WeatherObserved:"+sourceGUID {
						t.Fatalf("poi delete identities lost: %+v", deleted)
					}
				} else if sourceType == "greenhouse" {
					if err != nil || len(deleted) != 2 || !deleted[0].Delete || !deleted[1].Delete {
						t.Fatalf("delete identity differs from values: %+v %v", deleted, err)
					}
					if deleted[0].EntityID != wantedID || deleted[1].EntityID != "urn:ngsi-ld:IndoorEnvironmentObserved:"+sourceGUID {
						t.Fatalf("greenhouse delete identities lost: %+v", deleted)
					}
				} else if err != nil || len(deleted) != 1 || !deleted[0].Delete || deleted[0].EntityID != wantedID {
					t.Fatalf("delete identity differs from values: %+v %v", deleted, err)
				}
			}
		})
	}
}

func TestShippedRelationIDsMatchValuesDespiteRenames(t *testing.T) {
	rules, err := LoadModelsDir("../../../deployments/models")
	if err != nil {
		t.Fatal(err)
	}
	engine := things.NewDynamicThingTransformer(rules)
	for _, names := range [][2]string{{"Room original", "Building original"}, {"Room renamed", "Building renamed"}} {
		body := map[string]any{"relationId": "neutral-link", "type": "partOf", "fromId": sourceGUID, "toId": parentGUID, "tenant": "test", "changedAt": "2026-10-01T12:00:00Z",
			"fromThingType": "room", "toThingType": "building", "from": map[string]any{"thingId": sourceGUID, "name": names[0]}, "to": map[string]any{"thingId": parentGUID, "name": names[1]}}
		input, err := things.ThingRelationsFromJSON(marshalPayload(t, body))
		if err != nil {
			t.Fatal(err)
		}
		writes, err := engine.Transform(input)
		if err != nil || len(writes) != 1 {
			t.Fatalf("relation: %+v %v", writes, err)
		}
		if writes[0].EntityID != "urn:ngsi-ld:IndoorEnvironmentObserved:"+sourceGUID {
			t.Fatalf("target ID derived from name: %s", writes[0].EntityID)
		}
		ref := writeJSON(t, writes[0])["refParent"].(map[string]any)["object"]
		if ref != "urn:ngsi-ld:Building:"+parentGUID {
			t.Fatalf("reference not stable across renames: %+v", ref)
		}
		body["removed"] = true
		removed, err := things.ThingRelationsFromJSON(marshalPayload(t, body))
		if err != nil {
			t.Fatal(err)
		}
		cleared, err := engine.Transform(removed)
		if err != nil || len(cleared) != 1 || cleared[0].EntityID != writes[0].EntityID || strings.Join(cleared[0].RemoveAttributes, ",") != "refParent" {
			t.Fatalf("remove ID differs: %+v %v", cleared, err)
		}
	}
}
