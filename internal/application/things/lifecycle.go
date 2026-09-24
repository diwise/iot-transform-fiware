package things

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/diwise/iot-transform-fiware/internal/application/transform"
)

// thingLifecycleDescriptor är sakdeskriptorn i en livscykelhändelse
// (samma form som värdeseventens thing-descriptor).
type thingLifecycleDescriptor struct {
	ThingID         string            `json:"thingId"`
	Name            string            `json:"name"`
	AlternativeName string            `json:"alternativeName"`
	SubType         string            `json:"subType"`
	Category        string            `json:"category"`
	Metadata        map[string]string `json:"metadata"`
}

// thingLifecycleEnvelope är den lokala trådformen för versionerade
// livscykelhändelser (things.v1.lifecycle). Samma princip som values.go:
// egen typ, ingen import av producentpaketet.
type thingLifecycleEnvelope struct {
	EventID          string                    `json:"eventId"`
	Tenant           string                    `json:"tenant"`
	ThingID          string                    `json:"thingId"`
	Kind             string                    `json:"kind"`
	ThingType        string                    `json:"thingType"`
	ThingTypeVersion string                    `json:"thingTypeVersion"`
	ConfigRevision   int64                     `json:"configRevision"`
	ChangedAt        time.Time                 `json:"changedAt"`
	Thing            *thingLifecycleDescriptor `json:"thing"`
}

// ThingLifecycleFromJSON tolkar en versionerad livscykelhändelse och bygger
// motorns indata. Saknat thingId och okänd kind är strukturfel (identitet
// och händelsetyp krävs); tom tenant är inget strukturfel (presentationen
// skippar utan broker-kontakt, samma grind som thing.updated).
//
// Saken blir motorns sak (ID/type/subType/namn med samma konvention som
// sakens egna regler, så {{nameOrID}}/{{type}} renderar identiskt).
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

	meta := Thing{
		ID:     env.ThingID,
		Type:   env.ThingType,
		Name:   env.ThingID,
		Tenant: env.Tenant,
	}
	if env.Thing != nil {
		if env.Thing.Name != "" {
			meta.Name = env.Thing.Name
		}
		meta.AlternativeName = env.Thing.AlternativeName
		if sub := env.Thing.SubType; sub != "" {
			meta.SubType = &sub
		}
	}

	fields := map[string]any{
		"lifecycle": map[string]any{
			"kind":      env.Kind,
			"revision":  env.ConfigRevision,
			"changedAt": env.ChangedAt.UTC().Format(time.RFC3339),
		},
	}

	return transform.Thing{
		ID:              meta.ID,
		Type:            meta.Type,
		SubType:         stringOrEmpty(meta.SubType),
		Name:            meta.Name,
		AlternativeName: meta.AlternativeName,
		Tenant:          meta.Tenant,
		TypeName:        meta.TypeName(),
		NameOrID:        meta.AlternativeNameOrNameOrID(),
		Event:           transform.EventThingsV1Lifecycle,
		Lifecycle:       env.Kind,
		ObservedAt:      env.ChangedAt,
		Fields:          fields,
	}, nil
}
