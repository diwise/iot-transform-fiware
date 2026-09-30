package transform

import (
	"strings"
	"testing"

	"github.com/matryer/is"
)

// En relation till en entitet som antas finnas skrivs utan att målet skapas;
// endast regelns egna entiteter blir skrivningar.
func TestAssumedExistingEntityIsNotCreated(t *testing.T) {
	is := is.New(t)

	rule := relationRule(
		Property{Target: "refPointOfInterest", Type: TypeRelationship, Source: Source{Value: "urn:ngsi-ld:Beach:external"}},
	)

	writes, err := NewEngine([]Rule{rule}).Transform(relationThing())
	is.NoErr(err)
	is.Equal(len(writes), 1)
	is.Equal(writes[0].EntityID, "urn:ngsi-ld:Sewer:s-1")

	b := marshalWrite(t, writes[0])
	is.True(strings.Contains(b, `"object":"urn:ngsi-ld:Beach:external"`))
}
