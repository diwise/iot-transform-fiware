package main

import (
	"testing"

	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/matryer/is"
)

type contentTypeMessage struct{ contentType string }

func (m contentTypeMessage) Body() []byte        { return nil }
func (m contentTypeMessage) ContentType() string { return m.contentType }

// HARM-003: locks the subscribed topic names. The thing.updated envelope
// shape is produced by iot-things (pkg/types.ThingUpdated) and the
// message.accepted envelope by iot-core (pkg/messaging/events); renames
// require a compatibility/migration plan and producer-side tests.
func TestSubscribedTopicContracts(t *testing.T) {
	is := is.New(t)

	is.Equal(ThingUpdatedTopic, "thing.updated")
	is.Equal(MessageAcceptedTopic, "message.accepted")
}

// REV-016: every registered content-type filter must match exactly its
// own content type (case-insensitively) and reject the others. A
// changed or dropped filter silently reroutes or drops deliveries.
func TestContentTypeFiltersMatchExactly(t *testing.T) {
	is := is.New(t)

	messenger := &messaging.MsgContextMock{
		RegisterTopicMessageHandlerWithFilterFunc: func(string, messaging.TopicMessageHandler, messaging.MessageFilter) error {
			return nil
		},
		RegisterTopicMessageHandlerFunc: func(string, messaging.TopicMessageHandler) error {
			return nil
		},
	}

	is.NoErr(registerHandlers(messenger, nil))

	filtered := messenger.RegisterTopicMessageHandlerWithFilterCalls()
	is.Equal(len(filtered), 9)

	expected := []string{
		"application/vnd.diwise.building+json",
		"application/vnd.diwise.container+json",
		"application/vnd.diwise.lifebuoy+json",
		"application/vnd.diwise.passage+json",
		"application/vnd.diwise.pointofinterest+json",
		"application/vnd.diwise.pumpingstation+json",
		"application/vnd.diwise.room+json",
		"application/vnd.diwise.sewer+json",
		"application/vnd.diwise.desk+json",
	}

	for i, c := range filtered {
		is.True(c.Filter(contentTypeMessage{expected[i]}))
		is.True(c.Filter(contentTypeMessage{"APPLICATION/VND.DIWISE.CONTAINER+JSON"}) == (expected[i] == "application/vnd.diwise.container+json"))
		is.True(!c.Filter(contentTypeMessage{"application/json"}))
	}
}
