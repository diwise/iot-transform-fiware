package features

import (
	"context"
	"testing"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild"
	"github.com/diwise/context-broker/pkg/ngsild/types"
	"github.com/diwise/context-broker/pkg/ngsild/types/properties"
	client "github.com/diwise/context-broker/pkg/test"
	"github.com/matryer/is"
)

func TestWaterQualityObserved(t *testing.T) {
	is := is.New(t)

	f := Feat{
		ID:       "waterQuality-01",
		Type:     "waterQuality",
		SubType:  "temperature",
		Location: nil,
		WaterQuality: &waterquality{
			Temperature: 20,
		},
		Timestamp: time.Date(2023, 2, 27, 12, 0, 0, 0, time.UTC),
	}

	var id string
	var temp float64
	var date string

	cbClient := &client.ContextBrokerClientMock{
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			id = entityID
			fragment.ForEachAttribute(func(attributeType, attributeName string, contents any) {
				if attributeType == "Property" {
					if attributeName == "temperature" {
						p := contents.(types.Property)
						temp = p.Value().(float64)
					}
					if attributeName == "dateObserved" {
						p := contents.(*properties.DateTimeProperty)
						date = p.Val.Value
					}
				}
			})
			return &ngsild.MergeEntityResult{}, nil
		},
	}

	err := WaterQualityObserved(context.Background(), f, cbClient)

	is.NoErr(err)
	is.Equal("urn:ngsi-ld:WaterQualityObserved:waterQuality:temperature:waterQuality-01", id)
	is.Equal(float64(20), temp)
	is.Equal("2023-02-27T12:00:00Z", date)
}
