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
			{Target: "temperature", Type: TypeNumber, Resource: "temperature"},
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
		{Target: "x", Type: TypeNumber, Resource: "temperature"},
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
	r.Entities[0].Properties[0].Transform = []Transform{{Op: OpFloor, Value: new(1.0)}}
	is.True(Validate([]Rule{r}) != nil) // floor får inte ha value

	r = validRule()
	r.Entities[0].Properties[0].Transform = []Transform{{Op: OpMultiply, Value: new(2.0)}}
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

func TestValidateRejectsUnknownEntityReference(t *testing.T) {
	is := is.New(t)

	r := validRule()
	r.Entities[0].ID = "urn:ngsi-ld:Thing:{{entities.missing.id}}"
	r.Entities[0].Name = "thing"

	is.True(Validate([]Rule{r}) != nil)
}

func TestValidateRejectsCreateEntityReferencedTooEarly(t *testing.T) {
	is := is.New(t)

	ref := validEntity()
	ref.Name = "building"
	ref.Create = true
	ref.Properties[0] = Property{
		Target: "refBuilding",
		Type:   TypeRelationship,
		Value:  "urn:ngsi-ld:Building:{{entities.building.id}}",
	}

	referrer := validEntity()
	referrer.ID = "urn:ngsi-ld:Thing:{{entities.building.id}}"
	referrer.Name = "thing"

	// building skapas efter att thing refererar till den.
	r := Rule{Match: Match{Kind: KindMeasurement}, Entities: []Entity{referrer, ref}}
	is.True(Validate([]Rule{r}) != nil)

	// korrekt ordning: building först.
	r2 := Rule{Match: Match{Kind: KindMeasurement}, Entities: []Entity{ref, referrer}}
	is.NoErr(Validate([]Rule{r2}))
}

func TestValidateRejectsDuplicateEntityName(t *testing.T) {
	is := is.New(t)

	a := validEntity()
	a.Name = "same"
	b := validEntity()
	b.Name = "same"

	r := Rule{Match: Match{Kind: KindMeasurement}, Entities: []Entity{a, b}}
	is.True(Validate([]Rule{r}) != nil)
}

func TestValidateRejectsUnitOnNonNumber(t *testing.T) {
	is := is.New(t)

	r := validRule()
	r.Entities[0].Properties[0].Type = TypeText
	r.Entities[0].Properties[0].Unit = "Cel"
	is.True(Validate([]Rule{r}) != nil)

	r = validRule()
	r.Entities[0].Properties[0].Unit = "Cel"
	is.NoErr(Validate([]Rule{r}))
}

func TestValidateTemplateVariables(t *testing.T) {
	is := is.New(t)

	r := validRule()
	r.Entities[0].ID = "urn:ngsi-ld:Thing:{{deviceID}}-{{safeURI(field:name)}}"
	is.NoErr(Validate([]Rule{r}))

	r = validRule()
	r.Entities[0].ID = "urn:ngsi-ld:Thing:{{bogus}}"
	is.NoErr(Validate([]Rule{r}))
	_, err := NewEngine([]Rule{r}).Transform(Measurement{DeviceID: "stable-id"})
	is.True(err != nil) // syntaktiskt giltigt, men källa saknas vid körning

	r = validRule()
	r.Entities[0].Properties[0].Type = TypeRelationship
	r.Entities[0].Properties[0].Source = Source{Value: "urn:ngsi-ld:X:{{safeURI(nope)}}"}
	is.NoErr(Validate([]Rule{r}))
	_, err = NewEngine([]Rule{r}).Transform(Measurement{DeviceID: "stable-id"})
	is.True(err != nil)
}

func TestValidateAcceptsEveryKnownFunction(t *testing.T) {
	is := is.New(t)

	for _, op := range KnownTransformOps() {
		r := validRule()
		tr := Transform{Op: op}
		if transformNeedsValue[op] {
			tr.Value = new(float64(2))
		}
		r.Entities[0].Properties[0].Transform = []Transform{tr}
		is.NoErr(Validate([]Rule{r}))
	}

	for _, fn := range KnownDeriveFns() {
		r := validRule()
		r.Entities[0].Properties[0].Source = Source{}
		r.Entities[0].Properties[0].Derive = &Derive{Fn: fn, Of: []Source{{Resource: "a"}}}
		is.NoErr(Validate([]Rule{r}))
	}
}

//go:fix inline
func ptr(f float64) *float64 { return new(f) }
