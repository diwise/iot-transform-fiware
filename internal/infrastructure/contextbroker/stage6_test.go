package contextbroker

import (
	"testing"

	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities/decorators"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
)

func TestStage6InvalidLaterWritePreventsEntireBatch(t *testing.T) {
	for _, last := range []transform.EntityWrite{
		{EntityID: "relative-id", TypeName: "Generic", Delete: true},
		{EntityID: "urn:example:last", TypeName: "Generic", RemoveAttributes: []string{"id"}},
		{EntityID: "urn:example:last", TypeName: "bad type"},
	} {
		mock, calls, _ := recordingMock()
		first := transform.EntityWrite{EntityID: "urn:example:first", TypeName: "Generic", Props: []entities.EntityDecoratorFunc{decorators.Number("reading", 1)}}
		err := ApplyEntityWrites(t.Context(), mock, []transform.EntityWrite{first, last}, "")
		if err == nil || len(*calls) != 0 {
			t.Fatalf("batch partially written before validation: %v %v", *calls, err)
		}
	}
}
