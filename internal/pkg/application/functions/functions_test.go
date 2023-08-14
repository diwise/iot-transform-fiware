package functions

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

	f := Func{
		ID:       "waterQuality-01",
		Type:     "waterquality",
		SubType:  "beach",
		Location: nil,
		WaterQuality: &waterquality{
			Temperature: 20,
			Timestamp:   time.Date(2023, 2, 27, 12, 0, 0, 0, time.UTC),
		},
		Timestamp: time.Date(2023, 3, 27, 12, 0, 0, 0, time.UTC),
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
	is.Equal("urn:ngsi-ld:WaterQualityObserved:beach:waterQuality-01", id)
	is.Equal(float64(20), temp)
	is.Equal("2023-02-27T12:00:00Z", date)
}

func TestLifebuoy(t *testing.T) {
	is := is.New(t)

	f := Func{
		ID:       "lifebuoy-01",
		Type:     "presence",
		SubType:  "lifebuoy",
		Location: nil,
		Presence: &presence{
			State: true,
		},
		Timestamp: time.Date(2023, 3, 27, 12, 0, 0, 0, time.UTC),
	}

	var id string
	var state string

	cbClient := &client.ContextBrokerClientMock{
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			id = entityID

			fragment.ForEachAttribute(func(attributeType, attributeName string, contents any) {
				if attributeName == "status" {
					p := contents.(types.Property)
					state = p.Value().(string)
				}
			})

			return &ngsild.MergeEntityResult{}, nil
		},
	}

	err := Lifebuoy(context.Background(), f, cbClient)

	is.NoErr(err)
	is.Equal("urn:ngsi-ld:Lifebuoy:lifebuoy-01", id)
	is.Equal("on", state)
}

func TestDevice(t *testing.T) {
	is := is.New(t)

	f := Func{
		ID:       "device-01",
		Type:     "presence",
		SubType:  "desk",
		Location: nil,
		Presence: &presence{
			State: true,
		},
		Timestamp: time.Date(2023, 3, 27, 12, 0, 0, 0, time.UTC),
	}

	var id string

	cbClient := &client.ContextBrokerClientMock{
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			id = entityID
			return &ngsild.MergeEntityResult{}, nil
		},
	}

	err := Device(context.Background(), f, cbClient)

	is.NoErr(err)
	is.Equal("urn:ngsi-ld:Device:device-01", id)
}
