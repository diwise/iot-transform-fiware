package transform

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/matryer/is"
)

func strptr(s string) *string { return &s }

func marshalWrite(t *testing.T, w EntityWrite) string {
	t.Helper()

	e, err := entities.New(w.EntityID, w.TypeName, w.Props...)
	if err != nil {
		t.Fatalf("entities.New: %v", err)
	}

	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	return string(b)
}

func TestBuildNumberFromResource(t *testing.T) {
	is := is.New(t)

	m := testMeasurement()
	m.Records = []Record{{Name: "5700", Value: ptr(22.5)}}

	r := Rule{Match: Match{Kind: KindMeasurement}, Entities: []Entity{{
		ID: "urn:ngsi-ld:TemperatureObserved:x", Type: "TemperatureObserved",
		Properties: []Property{{Target: "temperature", Type: TypeNumber, Source: Source{Resource: "5700"}, Unit: "Cel"}},
	}}}

	writes, err := NewEngine([]Rule{r}).Transform(m)
	is.NoErr(err)
	is.Equal(len(writes), 1)

	b := marshalWrite(t, writes[0])
	is.True(strings.Contains(b, `"value":22.5`))
	is.True(strings.Contains(b, `"unitCode":"Cel"`))
}

func TestUnitBecomesUnitCode(t *testing.T) {
	is := is.New(t)

	m := testMeasurement()
	m.Records = []Record{{Name: "5700", Value: ptr(7)}}

	withUnit := Rule{Match: Match{Kind: KindMeasurement}, Entities: []Entity{{
		ID: "urn:ngsi-ld:T:x", Type: "T",
		Properties: []Property{{Target: "v", Type: TypeNumber, Source: Source{Resource: "5700"}, Unit: "KPA"}},
	}}}
	writes, err := NewEngine([]Rule{withUnit}).Transform(m)
	is.NoErr(err)
	is.True(strings.Contains(marshalWrite(t, writes[0]), `"unitCode":"KPA"`))

	withoutUnit := Rule{Match: Match{Kind: KindMeasurement}, Entities: []Entity{{
		ID: "urn:ngsi-ld:T:x", Type: "T",
		Properties: []Property{{Target: "v", Type: TypeNumber, Source: Source{Resource: "5700"}}},
	}}}
	writes, err = NewEngine([]Rule{withoutUnit}).Transform(m)
	is.NoErr(err)
	is.True(!strings.Contains(marshalWrite(t, writes[0]), `unitCode`))
}

func TestBuildTextFromMetadata(t *testing.T) {
	is := is.New(t)

	m := testMeasurement()
	m.Records = []Record{{Name: "source", String: strptr("outdoor")}}

	r := Rule{Match: Match{Kind: KindMeasurement}, Entities: []Entity{{
		ID: "urn:ngsi-ld:WeatherObserved:x", Type: "WeatherObserved",
		Properties: []Property{{Target: "source", Type: TypeText, Source: Source{Name: "source"}}},
	}}}

	writes, err := NewEngine([]Rule{r}).Transform(m)
	is.NoErr(err)
	is.Equal(len(writes), 1)
	is.True(strings.Contains(marshalWrite(t, writes[0]), `"source":{"type":"Property","value":"outdoor"`))
}

func TestBuildUsesObjectSelector(t *testing.T) {
	is := is.New(t)

	m := testMeasurement()
	m.Records = []Record{
		{Object: "urn:oma:lwm2m:ext:3428", Name: "5700", Value: ptr(1)},
		{Object: "urn:oma:lwm2m:ext:3303", Name: "5700", Value: ptr(2)},
	}

	r := Rule{Match: Match{Kind: KindMeasurement}, Entities: []Entity{{
		ID: "urn:ngsi-ld:T:x", Type: "T",
		Properties: []Property{{Target: "temperature", Type: TypeNumber, Source: Source{Resource: "5700", Object: "urn:oma:lwm2m:ext:3303"}}},
	}}}

	writes, err := NewEngine([]Rule{r}).Transform(m)
	is.NoErr(err)
	is.True(strings.Contains(marshalWrite(t, writes[0]), `"value":2`))
}

func TestMissingOptionalSourceIsSkipped(t *testing.T) {
	is := is.New(t)

	m := testMeasurement()

	r := Rule{Match: Match{Kind: KindMeasurement}, Entities: []Entity{{
		ID: "urn:ngsi-ld:T:x", Type: "T",
		Properties: []Property{{Target: "temperature", Type: TypeNumber, Source: Source{Resource: "9999"}}},
	}}}

	writes, err := NewEngine([]Rule{r}).Transform(m)
	is.NoErr(err)
	is.Equal(len(writes), 1)
	is.Equal(len(writes[0].Props), 0)
}

func TestObservedAtModes(t *testing.T) {
	is := is.New(t)

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	headerTime := base.Add(2 * time.Minute)
	recordTime := base.Add(5 * time.Minute)
	fieldTime := base.Add(3 * time.Minute)

	m := testMeasurement()
	m.Timestamp = base
	m.HeaderTime = headerTime
	m.HasHeaderTime = true
	m.Records = []Record{
		{Name: "5700", Value: ptr(1.0), Time: recordTime, HasTime: true},
		{Name: "obs", Time: fieldTime, HasTime: true},
	}

	ruleFor := func(oa ObservedAt) Rule {
		return Rule{Match: Match{Kind: KindMeasurement}, Entities: []Entity{{
			ID: "urn:ngsi-ld:T:x", Type: "T",
			Properties: []Property{{Target: "temperature", Type: TypeNumber, ObservedAt: oa, Source: Source{Resource: "5700"}}},
		}}}
	}

	observedAtOf := func(oa ObservedAt) string {
		writes, err := NewEngine([]Rule{ruleFor(oa)}).Transform(m)
		is.NoErr(err)
		b := marshalWrite(t, writes[0])
		idx := strings.Index(b, `"observedAt":"`)
		is.True(idx >= 0)
		rest := b[idx+len(`"observedAt":"`):]
		return rest[:strings.Index(rest, `"`)]
	}

	is.Equal(observedAtOf(ObservedAtHeader), headerTime.Format(time.RFC3339))
	is.Equal(observedAtOf(ObservedAtRecord), recordTime.Format(time.RFC3339))
	is.Equal(observedAtOf(ObservedAtMessage), base.Format(time.RFC3339))
	is.Equal(observedAtOf(ObservedAt("field:obs")), fieldTime.Format(time.RFC3339))
}

func TestMissingRequiredSourceSkipsEntity(t *testing.T) {
	is := is.New(t)

	m := testMeasurement()

	r := Rule{Match: Match{Kind: KindMeasurement}, Entities: []Entity{{
		ID: "urn:ngsi-ld:T:x", Type: "T",
		Properties: []Property{{Target: "temperature", Type: TypeNumber, Required: true, Source: Source{Resource: "9999"}}},
	}}}

	writes, err := NewEngine([]Rule{r}).Transform(m)
	is.NoErr(err)
	is.Equal(len(writes), 0)
}
