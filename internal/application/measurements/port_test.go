package measurements

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/senml"
)

// Kompakt producentformat: samma objekt/resurs på samma enhet, utan port
// samt med explicit port 0, 1 och 2. Inga modell-ID:n ingår i källpayloaden.
const multiPortPayload = `{"pack":[
 {"bn":"123-456-789/3303/","bt":1720000000,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},
 {"n":"5700","u":"Cel","v":11},
 {"bn":"123-456-789/0/3303/","bt":1720000001,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},
 {"n":"5700","u":"Cel","v":22},
 {"bn":"123-456-789/1/3303/","bt":1720000002,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},
 {"n":"5700","u":"Cel","v":33},
 {"bn":"123-456-789/2/3303/","bt":1720000003,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},
 {"n":"5700","u":"Cel","v":44},
 {"bn":"123-456-789/","n":"tenant","vs":"test"}
],"timestamp":"2024-07-03T09:46:45Z"}`

func portRule(port string) transform.Rule {
	return transform.Rule{Match: transform.Match{Kind: transform.KindMeasurement, Object: "urn:oma:lwm2m:ext:3303", Port: port},
		Entities: []transform.Entity{{ID: "urn:example:Observation:{{id}}:{{port}}", Type: "Observation", Properties: []transform.Property{
			{Target: "reading", Type: transform.TypeNumber, Source: transform.Source{Resource: "5700"}, Required: true},
		}}}}
}

func portObservations(t *testing.T) []events.MessageAccepted {
	t.Helper()
	var message events.MessageAccepted
	if err := json.Unmarshal([]byte(multiPortPayload), &message); err != nil {
		t.Fatal(err)
	}
	observations, err := PartitionMessage(message, time.Date(2024, 7, 3, 9, 46, 45, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(observations) != 4 {
		t.Fatalf("observations=%d, want 4", len(observations))
	}
	return observations
}

func TestPortFromPartitionedObservation(t *testing.T) {
	for i, observation := range portObservations(t) {
		wantPort := []string{"", "0", "1", "2"}[i]
		input := measurementFromMessage(observation, "source-sensor")
		if input.Port != wantPort || input.MatchInfo().Port != wantPort || input.Variables()["port"] != wantPort {
			t.Fatalf("observation %d: port=%q, want %q", i, input.Port, wantPort)
		}
		if input.DeviceID != "123-456-789" || input.Object != "urn:oma:lwm2m:ext:3303" || input.Tenant != "test" {
			t.Fatalf("identity changed: %+v", input)
		}
		if len(input.Records) == 0 || input.Records[0].Value == nil || *input.Records[0].Value != float64((i+1)*11) {
			t.Fatalf("value leaked between ports: %+v", input.Records)
		}
		if input.HeaderTime.Unix() != 1720000000+int64(i) {
			t.Fatalf("time leaked between ports: %v", input.HeaderTime)
		}
	}
}

func TestPortRulesDoNotCrossActivate(t *testing.T) {
	for _, selectedPort := range []string{"0", "1", "2"} {
		rule := portRule(selectedPort)
		if err := transform.Validate([]transform.Rule{rule}); err != nil {
			t.Fatalf("valid regression rule: %v", err)
		}
		transformer := NewDynamicTransformer([]transform.Rule{rule}, nil)
		for i, observation := range portObservations(t) {
			wantPort := []string{"", "0", "1", "2"}[i]
			writes, err := transformer.Transform(t.Context(), observation)
			if selectedPort != wantPort {
				if !errors.Is(err, transform.ErrNoMatchingRule) {
					t.Fatalf("port %s matched observation %q: %+v %v", selectedPort, wantPort, writes, err)
				}
				continue
			}
			if err != nil || len(writes) != 1 || writes[0].EntityID != "urn:example:Observation:123-456-789:"+wantPort {
				t.Fatalf("matching port %s: %+v %v", selectedPort, writes, err)
			}
			entity := marshalEntity(t, writes[0].EntityID, writes[0].TypeName, writes[0].Props)
			var output map[string]any
			if err := json.Unmarshal([]byte(entity), &output); err != nil {
				t.Fatal(err)
			}
			if output["reading"].(map[string]any)["value"] != float64((i+1)*11) {
				t.Fatalf("wrong port value: %s", entity)
			}
		}
	}
}

func TestSpecificPortRuleWinsOverGeneric(t *testing.T) {
	generic := portRule("")
	generic.Entities[0].ID = "urn:example:generic:{{id}}"
	specific := portRule("1")
	if err := transform.Validate([]transform.Rule{generic, specific}); err != nil {
		t.Fatal(err)
	}
	transformer := NewDynamicTransformer([]transform.Rule{generic, specific}, nil)
	writes, err := transformer.Transform(t.Context(), portObservations(t)[2])
	if err != nil || len(writes) != 1 || writes[0].EntityID != "urn:example:Observation:123-456-789:1" {
		t.Fatalf("generic rule shadowed port rule: %+v %v", writes, err)
	}
}

func TestPortHeaderNames(t *testing.T) {
	for _, test := range []struct {
		header senml.Record
		want   string
	}{
		{senml.Record{Name: "device/3303/0", StringValue: "urn:oma:lwm2m:ext:3303"}, ""},
		{senml.Record{Name: "device/0/3303/0", StringValue: "urn:oma:lwm2m:ext:3303"}, "0"},
		{senml.Record{Name: "device/01/3303/0", StringValue: "urn:oma:lwm2m:ext:3303"}, "01"},
		{senml.Record{BaseName: "device/2/3303/", Name: "0", StringValue: "urn:oma:lwm2m:ext:3303"}, "2"},
		{senml.Record{Name: "device/output-a/3303/0", StringValue: "urn:oma:lwm2m:ext:3303"}, "output-a"},
		{senml.Record{Name: "device/branch/channel/3303/0", StringValue: "urn:oma:lwm2m:ext:3303"}, ""},
		{senml.Record{Name: "device/1/3304/0", StringValue: "urn:oma:lwm2m:ext:3303"}, ""},
	} {
		if got := portFromHeader(test.header); got != test.want {
			t.Fatalf("header %+v: port=%q, want %q", test.header, got, test.want)
		}
	}
}
