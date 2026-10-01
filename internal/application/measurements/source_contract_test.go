package measurements

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/diwise/iot-core/pkg/messaging/events"
)

// Repeated prefixes are independent observations. Pack defaults are deliberately
// placed after local metadata so choosing the first matching record is wrong.
const sourceMeasurements = `{"timestamp":"2026-10-01T12:04:00.123456789Z","pack":[
 {"bn":"sensor/2/3303/","bt":1790856000.25,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},
 {"n":"5700","v":0,"u":"Cel","t":0.5},
 {"n":"env","vs":"first"},{"n":"source","vs":"first-source"},{"n":"lat","v":0,"u":"lat"},{"n":"lon","v":17,"u":"lon"},
 {"bn":"sensor/2/3303/","bt":1790856001.25,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},
 {"n":"5700","vb":false,"u":"1","t":0.75},
 {"n":"env","vs":"second"},{"n":"source","vs":"second-source"},{"n":"lat","v":62,"u":"lat"},{"n":"lon","v":18,"u":"lon"},
 {"bn":"sensor/","n":"tenant","vs":"tenant-a"},{"n":"env","vs":"default"},{"n":"source","vs":"default-source"}
]}`

func TestSourceContractMeasurementsPreserveIdentityAndPartitionMetadata(t *testing.T) {
	var msg events.MessageAccepted
	if err := json.Unmarshal([]byte(sourceMeasurements), &msg); err != nil {
		t.Fatal(err)
	}
	observations, err := PartitionMessage(msg, msg.Timestamp)
	if err != nil || len(observations) != 2 {
		t.Fatalf("%v %v", observations, err)
	}
	for i, observation := range observations {
		input := measurementFromMessage(observation, "raw-sensor-type")
		wantEnv, wantSource := []string{"first", "second"}[i], []string{"first-source", "second-source"}[i]
		if input.Env != wantEnv {
			t.Errorf("observation %d env=%q want %q", i, input.Env, wantEnv)
		}
		for path, want := range map[string]any{
			"source": wantSource, "records[0].fullName": "sensor/2/3303/5700", "records[0].object": "urn:oma:lwm2m:ext:3303",
			"records[0].name": "5700", "records[0].channel": "2/3303", "records[0].unit": []string{"Cel", "1"}[i],
			"records[0].value": []any{float64(0), false}[i],
		} {
			got, ok := input.ResolvePath(path)
			if !ok || got != want {
				t.Errorf("observation %d %s=%v (found=%t), want %v", i, path, got, ok, want)
			}
		}
		wantTime := time.Unix(1790856000+int64(i), []int64{750000000, 1000000000}[i]).UTC()
		if got, ok := input.ResolveTime("records[0].observedAt"); !ok || !got.Equal(wantTime) {
			t.Errorf("resource time: %v %t want %v", got, ok, wantTime)
		}
		lat, lon, ok := input.LatLon()
		if !ok || lat != []float64{0, 62}[i] || lon != []float64{17, 18}[i] {
			t.Errorf("coordinates leaked: %v %v %t", lat, lon, ok)
		}
		if input.Port != "2" || input.Tenant != "tenant-a" || !input.Timestamp.Equal(msg.Timestamp) {
			t.Fatalf("identity/transport time changed: %+v", input)
		}
	}
}

func TestSourceContractMetadataDefaultsAndRelativeTimes(t *testing.T) {
	const raw = `{"pack":[
 {"bn":"sensor/","n":"tenant","vs":"tenant-a"},{"n":"env","vs":"default"},{"n":"source","vs":"default-source"},{"n":"lat","v":62,"u":"lat"},{"n":"lon","v":17,"u":"lon"},
 {"bn":"sensor/1/3303/","bt":-2.5,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},{"n":"5700","v":1,"u":"Cel","t":0.25},{"n":"env","vs":"local"},{"n":"lat","v":0,"u":"lat"},
 {"bn":"sensor/2/3303/","bt":-1.5,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},{"n":"5700","vs":"text","u":"1","t":0.5}
 ],"timestamp":"2026-10-01T12:04:00Z"}`
	var message events.MessageAccepted
	if err := json.Unmarshal([]byte(raw), &message); err != nil {
		t.Fatal(err)
	}
	reference := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	observations, err := PartitionMessage(message, reference)
	if err != nil || len(observations) != 2 {
		t.Fatalf("%v %v", observations, err)
	}
	for i, observation := range observations {
		input := measurementFromMessage(observation, "")
		if input.Env != []string{"local", "default"}[i] || input.Source != "default-source" {
			t.Fatalf("metadata overlay wrong: %+v", input)
		}
		lat, lon, ok := input.LatLon()
		if !ok || lat != []float64{0, 62}[i] || lon != 17 {
			t.Fatalf("defaults/zero override wrong: %v %v %v", lat, lon, ok)
		}
		want := reference.Add([]time.Duration{-2250 * time.Millisecond, -time.Second}[i])
		if got, ok := input.ResolveTime("records[0].observedAt"); !ok || !got.Equal(want) {
			t.Fatalf("relative time wrong: %v %v want %v", got, ok, want)
		}
		if value, ok := input.ResolvePath("sensor/" + []string{"1", "2"}[i] + "/3303/5700"); !ok || value != []any{float64(1), "text"}[i] {
			t.Fatalf("full source lookup: %v %v", value, ok)
		}
	}
}
