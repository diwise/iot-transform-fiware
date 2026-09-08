package main

import (
	"testing"

	"github.com/matryer/is"
)

// HARM-003: locks the subscribed topic names. The thing.updated envelope
// shape is produced by iot-things (pkg/types.ThingUpdated) and the
// message.accepted envelope by iot-core (pkg/messaging/events); renames
// require a compatibility/migration plan and producer-side tests.
func TestSubscribedTopicContracts(t *testing.T) {
	is := is.New(t)

	is.Equal(ThingUpdatedTopic, "thing.updated")
	is.Equal(MessageAcceptedTopic, "message.accepted")
}
