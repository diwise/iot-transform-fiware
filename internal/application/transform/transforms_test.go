package transform

import (
	"strings"
	"testing"

	"github.com/matryer/is"
)

func boolptrForTransform(b bool) *bool { return &b }

func transformRule(typ PropertyType, src Source, transforms ...Transform) Rule {
	return Rule{Match: Match{Kind: KindMeasurement}, Entities: []Entity{{
		ID: "urn:ngsi-ld:T:x", Type: "T",
		Properties: []Property{{Target: "v", Type: typ, Source: src, Transform: transforms}},
	}}}
}

func renderFirst(t *testing.T, m Measurement, r Rule) string {
	t.Helper()
	writes, err := NewEngine([]Rule{r}).Transform(m)
	if err != nil {
		t.Fatalf("Transform: %v", err)
	}
	return marshalWrite(t, writes[0])
}

func TestNumericTransforms(t *testing.T) {
	is := is.New(t)

	m := func(v float64) Measurement {
		x := testMeasurement()
		x.Records = []Record{{Name: "5700", Value: ptr(v)}}
		return x
	}

	cases := []struct {
		name string
		tr   Transform
		in   float64
		want string
	}{
		{"multiply", Transform{Op: OpMultiply, Value: ptr(4)}, 2.5, `"value":10`},
		{"add", Transform{Op: OpAdd, Value: ptr(1.5)}, 2, `"value":3.5`},
		{"divide", Transform{Op: OpDivide, Value: ptr(2)}, 5, `"value":2.5`},
		{"floor", Transform{Op: OpFloor}, 2.9, `"value":2`},
		{"round", Transform{Op: OpRound}, 2.5, `"value":3`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			r := transformRule(TypeNumber, Source{Resource: "5700"}, tc.tr)
			is.True(strings.Contains(renderFirst(t, m(tc.in), r), tc.want))
		})
	}
}

func TestTransformsChain(t *testing.T) {
	is := is.New(t)

	x := testMeasurement()
	x.Records = []Record{{Name: "5700", Value: ptr(2.4)}}
	r := transformRule(TypeNumber, Source{Resource: "5700"},
		Transform{Op: OpMultiply, Value: ptr(10)},
		Transform{Op: OpRound},
	)

	is.True(strings.Contains(renderFirst(t, x, r), `"value":24`))
}

func TestBoolToOnOff(t *testing.T) {
	is := is.New(t)

	x := testMeasurement()
	x.Records = []Record{{Name: "5500", Bool: boolptrForTransform(true)}}
	r := transformRule(TypeText, Source{Resource: "5500"}, Transform{Op: OpBoolToOnOff})

	is.True(strings.Contains(renderFirst(t, x, r), `"value":"on"`))
}

func TestSafeURITransform(t *testing.T) {
	is := is.New(t)

	x := testMeasurement()
	x.Records = []Record{{Name: "name", String: strptr("a b/c")}}
	r := transformRule(TypeText, Source{Resource: "name"}, Transform{Op: OpSafeURI})

	is.True(strings.Contains(renderFirst(t, x, r), `"value":"a%20b%2Fc"`))
}

func TestDivideByZeroFails(t *testing.T) {
	is := is.New(t)

	x := testMeasurement()
	x.Records = []Record{{Name: "5700", Value: ptr(1.0)}}
	r := transformRule(TypeNumber, Source{Resource: "5700"}, Transform{Op: OpDivide, Value: ptr(0)})

	_, err := NewEngine([]Rule{r}).Transform(x)
	is.True(err != nil)
}
