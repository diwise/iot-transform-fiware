package functions

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild"
	"github.com/diwise/context-broker/pkg/ngsild/types"
	"github.com/diwise/context-broker/pkg/ngsild/types/properties"
	client "github.com/diwise/context-broker/pkg/test"
	"github.com/diwise/messaging-golang/pkg/messaging"
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

func TestSewagePumpingStationBody(t *testing.T) {
	timestamp, _ := time.Parse(time.RFC3339, "2023-12-19T14:02:41.147069Z")
	sps := sewagepumpingstation{
		ID:        "spsID",
		Timestamp: timestamp,
		State:     false,
		Tenant:    "default",
	}

	is, incMsg, cbClientMock := testSetup(t, sps)
	is.True(strings.Contains(string(incMsg.Body()), "spsID"))

	expectation := `{"id":"spsID","state":false,"timestamp":"2023-12-19T14:02:41.147069Z","tenant":"default"}`
	is.Equal(string(incMsg.Body()), expectation)

	err := SewagePumpingStation(context.Background(), incMsg, cbClientMock)
	is.NoErr(err)

	is.Equal(len(cbClientMock.MergeEntityCalls()), 1)
}

func TestSewerBody(t *testing.T) {
	timestamp, _ := time.Parse(time.RFC3339, "2023-12-19T14:02:41.147069Z")
	sewer := sewer{
		ID:        "sewID",
		Timestamp: timestamp,
		Distance:  33,
		Tenant:    "default",
	}

	is, incMsg, cbClientMock := testSetup(t, sewer)
	is.True(strings.Contains(string(incMsg.Body()), "sewID"))

	expectation := `{"id":"sewID","distance":33,"timestamp":"2023-12-19T14:02:41.147069Z","tenant":"default"}`
	is.Equal(string(incMsg.Body()), expectation)

	err := Sewer(context.Background(), incMsg, cbClientMock)
	is.NoErr(err)

	is.Equal(len(cbClientMock.MergeEntityCalls()), 1)
}

func testSetup(t *testing.T, object any) (*is.I, *messaging.IncomingTopicMessageMock, *client.ContextBrokerClientMock) {
	is := is.New(t)

	incMsg := &messaging.IncomingTopicMessageMock{
		TopicNameFunc: func() string {
			return "msg.Name"
		},
		ContentTypeFunc: func() string {
			return "msg.ContenType"
		},
		BodyFunc: func() []byte {
			bytes, err := json.Marshal(object)
			is.NoErr(err)

			return bytes
		},
	}

	cbClientMock := &client.ContextBrokerClientMock{
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			return nil, nil
		},
	}

	return is, incMsg, cbClientMock
}
