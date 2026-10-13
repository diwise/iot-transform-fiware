package things

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/diwise/iot-transform-fiware/internal/application/transform"
)

// thingLifecycleEnvelope är den lokala trådformen för versionerade
// livscykelhändelser (things.v1.lifecycle). Samma princip som values.go:
// egen typ, ingen import av producentpaketet.
type thingLifecycleEnvelope struct {
	Schema           string           `json:"schema"`
	EventID          string           `json:"eventId"`
	Tenant           string           `json:"tenant"`
	ThingID          string           `json:"thingId"`
	Kind             string           `json:"kind"`
	ThingType        string           `json:"thingType"`
	ThingTypeVersion string           `json:"thingTypeVersion"`
	ConfigRevision   int64            `json:"configRevision"`
	ChangedAt        time.Time        `json:"changedAt"`
	Thing            *thingDescriptor `json:"thing"`
}

// ThingLifecycleFromJSON tolkar en versionerad livscykelhändelse och bygger
// motorns indata. Saknat thingId och okänd kind är strukturfel (identitet
// och händelsetyp krävs); tom tenant är inget strukturfel (presentationen
// skippar utan broker-kontakt, samma grind som thing.updated).
//
// Saken behåller kuvertets stabila ID och typ; namn är enbart källdata.
// Deskriptorn är alltid satt via producenten, även vid deleted — saknat
// thing-objekt ger därför tomma namn men behåller ID/typ från kuvertet.
// Fields bär lifecycle.{kind,revision,changedAt}; ObservedAt är changedAt
// (händelsetid, aldrig transporttid).
func ThingLifecycleFromJSON(body []byte) (transform.Thing, error) {
	var env thingLifecycleEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return transform.Thing{}, err
	}
	if env.ThingID == "" {
		return transform.Thing{}, fmt.Errorf("things.v1.lifecycle: missing thingId")
	}
	switch transform.LifecycleKind(env.Kind) {
	case transform.LifecycleCreated, transform.LifecycleUpdated, transform.LifecycleDeleted:
	default:
		return transform.Thing{}, fmt.Errorf("things.v1.lifecycle: invalid kind %q (want created, updated or deleted)", env.Kind)
	}
	// Tom tenant är inget strukturfel: presentationen skippar utan
	// broker-kontakt (samma grind som thing.updated).

	input := descriptorInput(env.ThingID, env.ThingType, env.Tenant, env.Thing)
	input.Event, input.Lifecycle, input.ObservedAt = transform.EventThingsV1Lifecycle, env.Kind, env.ChangedAt
	event := map[string]any{
		"schema": env.Schema, "eventId": env.EventID, "tenant": env.Tenant, "thingId": env.ThingID,
		"thingType": env.ThingType, "thingTypeVersion": env.ThingTypeVersion, "configRevision": env.ConfigRevision, "kind": env.Kind,
	}
	addSourceTime(event, "changedAt", env.ChangedAt)
	input.Fields["event"] = event
	lifecycle := map[string]any{"kind": env.Kind, "revision": env.ConfigRevision}
	addSourceTime(lifecycle, "changedAt", env.ChangedAt)
	input.Fields["lifecycle"] = lifecycle
	return input, nil
}
