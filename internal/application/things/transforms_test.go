package things

import (
	"testing"
	"time"

	"github.com/matryer/is"
)

func testLocation() Location {
	return Location{Latitude: 62.0, Longitude: 17.0}
}

func testTime() time.Time {
	return time.Date(2024, 11, 19, 10, 49, 59, 0, time.UTC)
}

// REV-013: transformation use cases are tested independently of
// RabbitMQ envelopes; the presentation adapters only decode envelopes
// and execute the returned writes in order.
func TestTransformContainerReturnsSingleMergeWrite(t *testing.T) {
	is := is.New(t)

	writes := TransformContainer(Container{
		ID:              "2bf440f4",
		Type:            "Container",
		SubType:         new("WasteContainer"),
		Name:            "Soptunnor.X",
		AlternativeName: "Soptunnor.XY",
		Location:        testLocation(),
		ObservedAt:      testTime(),
		Tenant:          "default",
		CurrentLevel:    0.91,
		Percent:         56,
	})

	is.Equal(len(writes), 1)
	is.Equal(writes[0].EntityID, "urn:ngsi-ld:WasteContainer:Soptunnor.XY")
	is.Equal(writes[0].TypeName, "WasteContainer")
	is.True(!writes[0].Create)
	is.Equal(len(writes[0].Props), 3)
}

func TestTransformPointOfInterestBeachReturnsCreatePlusMerge(t *testing.T) {
	is := is.New(t)

	ref := "12345"
	writes := TransformPointOfInterest(PointOfInterest{
		ID:          "poi-1",
		Type:        "PointOfInterest",
		SubType:     new("Beach"),
		Location:    testLocation(),
		ObservedAt:  testTime(),
		Tenant:      "default",
		Temperature: Measurement{Value: new(21.5), Timestamp: testTime()},
		Current:     Measurement{Ref: ref, Timestamp: testTime()},
	})

	is.Equal(len(writes), 2)
	is.True(writes[0].Create)
	is.True(!writes[1].Create)
}

func TestTransformPointOfInterestDefaultReturnsSingleMerge(t *testing.T) {
	is := is.New(t)

	writes := TransformPointOfInterest(PointOfInterest{
		ID:          "poi-2",
		Type:        "PointOfInterest",
		Location:    testLocation(),
		ObservedAt:  testTime(),
		Tenant:      "default",
		Temperature: Measurement{Value: new(21.5), Timestamp: testTime()},
	})

	is.Equal(len(writes), 1)
	is.True(!writes[0].Create)
}

func TestTransformSewerReturnsSingleMergeWrite(t *testing.T) {
	is := is.New(t)

	writes := TransformSewer(Sewer{
		ID:         "25ba0559",
		Type:       "Sewer",
		Location:   testLocation(),
		ObservedAt: testTime(),
		Tenant:     "default",
		LastAction: "overflow unknown",
	})

	is.Equal(len(writes), 1)
	is.True(!writes[0].Create)
	is.True(len(writes[0].Props) > 0)
}

//go:fix inline
func strptr(s string) *string { return new(s) }

//go:fix inline
func float64ptr(f float64) *float64 { return new(f) }
