package transform

import (
	"encoding/json"
	"strings"
	"testing"

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
