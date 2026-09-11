package transform

import (
	"testing"

	"github.com/matryer/is"
)

func validEntity() Entity {
	return Entity{
		ID:   "urn:ngsi-ld:Thing:x",
		Type: "Thing",
		Properties: []Property{
			{Target: "temperature", Type: TypeNumber, Source: Source{Resource: "temperature"}},
		},
	}
}

func validRule() Rule {
	return Rule{Match: Match{Kind: KindMeasurement}, Entities: []Entity{validEntity()}}
}

func TestValidateAcceptsValidRule(t *testing.T) {
	is := is.New(t)
	is.NoErr(Validate([]Rule{validRule()}))
}

func TestValidateRejectsInvalidKind(t *testing.T) {
	is := is.New(t)
	r := validRule()
	r.Match.Kind = "banana"
	is.True(Validate([]Rule{r}) != nil)
}

func TestValidateRejectsNonURNObject(t *testing.T) {
	is := is.New(t)
	r := validRule()
	r.Match.Object = "not-a-urn"
	is.True(Validate([]Rule{r}) != nil)
}

func TestValidateRejectsMissingEntityID(t *testing.T) {
	is := is.New(t)
	r := validRule()
	r.Entities[0].ID = ""
	is.True(Validate([]Rule{r}) != nil)
}

func TestValidateRejectsMissingEntityType(t *testing.T) {
	is := is.New(t)
	r := validRule()
	r.Entities[0].Type = ""
	is.True(Validate([]Rule{r}) != nil)
}

func TestValidateRejectsUnknownPropertyType(t *testing.T) {
	is := is.New(t)
	r := validRule()
	r.Entities[0].Properties[0].Type = "Nope"
	is.True(Validate([]Rule{r}) != nil)
}

func TestValidateRejectsMissingSource(t *testing.T) {
	is := is.New(t)
	r := validRule()
	r.Entities[0].Properties[0].Source = Source{}
	is.True(Validate([]Rule{r}) != nil)
}

func TestValidateRejectsMultipleSources(t *testing.T) {
	is := is.New(t)
	r := validRule()
	r.Entities[0].Properties[0].Source = Source{Resource: "temperature", Field: "temp"}
	is.True(Validate([]Rule{r}) != nil)
}

func TestValidateRejectsStructuredWithoutSubproperties(t *testing.T) {
	is := is.New(t)
	r := validRule()
	r.Entities[0].Properties = []Property{{Target: "air", Type: TypeStructured}}
	is.True(Validate([]Rule{r}) != nil)
}

func TestValidateRejectsSubpropertiesOnScalar(t *testing.T) {
	is := is.New(t)
	r := validRule()
	r.Entities[0].Properties[0].Subproperties = []Property{
		{Target: "x", Type: TypeNumber, Source: Source{Resource: "temperature"}},
	}
	is.True(Validate([]Rule{r}) != nil)
}

func TestValidateRejectsUnknownTransformOp(t *testing.T) {
	is := is.New(t)
	r := validRule()
	r.Entities[0].Properties[0].Transform = []Transform{{Op: "explode"}}
	is.True(Validate([]Rule{r}) != nil)
}

func TestValidateRejectsTransformValueRequirements(t *testing.T) {
	is := is.New(t)

	r := validRule()
	r.Entities[0].Properties[0].Transform = []Transform{{Op: OpMultiply}}
	is.True(Validate([]Rule{r}) != nil) // multiply kräver value

	r = validRule()
	r.Entities[0].Properties[0].Transform = []Transform{{Op: OpFloor, Value: ptr(1.0)}}
	is.True(Validate([]Rule{r}) != nil) // floor får inte ha value

	r = validRule()
	r.Entities[0].Properties[0].Transform = []Transform{{Op: OpMultiply, Value: ptr(2.0)}}
	is.NoErr(Validate([]Rule{r}))
}

func TestValidateRejectsUnknownDeriveFn(t *testing.T) {
	is := is.New(t)
	r := validRule()
	r.Entities[0].Properties[0].Source = Source{}
	r.Entities[0].Properties[0].Derive = &Derive{Fn: "magic", Of: []Source{{Resource: "a"}}}
	is.True(Validate([]Rule{r}) != nil)
}

func TestValidateRejectsDeriveWithoutSources(t *testing.T) {
	is := is.New(t)
	r := validRule()
	r.Entities[0].Properties[0].Source = Source{}
	r.Entities[0].Properties[0].Derive = &Derive{Fn: FnSum}
	is.True(Validate([]Rule{r}) != nil)
}

func TestValidateRejectsTransformAndDeriveCombined(t *testing.T) {
	is := is.New(t)
	r := validRule()
	r.Entities[0].Properties[0].Derive = &Derive{Fn: FnSum, Of: []Source{{Resource: "temperature"}}}
	r.Entities[0].Properties[0].Transform = []Transform{{Op: OpFloor}}
	is.True(Validate([]Rule{r}) != nil)
}

func TestValidateAcceptsDerive(t *testing.T) {
	is := is.New(t)
	r := validRule()
	r.Entities[0].Properties[0].Source = Source{}
	r.Entities[0].Properties[0].Derive = &Derive{Fn: FnFirstPresent, Of: []Source{{Resource: "a"}, {Resource: "b"}}}
	is.NoErr(Validate([]Rule{r}))
}

func TestValidateRejectsConflictingEntityTypes(t *testing.T) {
	is := is.New(t)

	a := validRule()
	b := validRule()
	b.Entities[0].Type = "Other"

	is.True(Validate([]Rule{a, b}) != nil)
}

func TestValidateAcceptsSameEntityWithSameType(t *testing.T) {
	is := is.New(t)
	is.NoErr(Validate([]Rule{validRule(), validRule()}))
}

func ptr(f float64) *float64 { return &f }
