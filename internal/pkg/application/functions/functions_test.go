package functions

import (
	"context"
	"encoding/json"
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

	data, _ := json.Marshal(&waterquality{
		Temperature: 20,
		Timestamp:   time.Date(2023, 2, 27, 12, 0, 0, 0, time.UTC),
	})

	f := FnctUpdated{
		fnctMetadata: fnctMetadata{
			ID:       "waterQuality-01",
			Type:     "waterquality",
			SubType:  "beach",
			Location: nil,
		},
		Data: data,
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

func TestWQ(t *testing.T) {
	is := is.New(t)
	var v FnctUpdated
	err := json.Unmarshal([]byte(`{"id":"functionID","name":"name","type":"waterquality","subtype":"beach","data":{"temperature":2.3,"timestamp":"2023-06-05T11:26:57Z"}}`), &v)
	is.NoErr(err)

	cbClient := &client.ContextBrokerClientMock{
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			return &ngsild.MergeEntityResult{}, nil
		},
	}

	err = WaterQualityObserved(context.Background(), v, cbClient)
	is.NoErr(err)
}
