package measurements

import (
	"time"

	"github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/senml"
	diwisepkg "github.com/diwise/senml/diwise"
)

// PartitionMessage delar ett mätvärdesmeddelande i enobservations-meddelanden
// med delad packmetadata — samma normalisering som produktion
// (presentation/messaging/measurements) använder före transformation.
// Preview måste använda denna väg, annars kan helt paket direkt till
// DynamicTransformer ge annat urval/resultat än produktion (TF-06).
func PartitionMessage(msg events.MessageAccepted, now time.Time) ([]events.MessageAccepted, error) {
	parsed, err := diwisepkg.Parse(msg.Pack(), now)
	if err != nil {
		return nil, err
	}
	objs := parsed.Objects()
	out := make([]events.MessageAccepted, 0, len(objs))
	for _, o := range objs {
		scoped := ScopePack(o)
		out = append(out, events.MessageAccepted{
			Pack_:     scoped,
			Timestamp: msg.Timestamp,
		})
	}
	return out, nil
}

// ScopePack partitionerar en observation ur ett tolkat pack: headern, dess
// resurser samt packnivå- och observations egen metadata. Resultatet är ett
// fristående enobservationspack som befintliga transformers läser oförändrat.
// Delad med produktion — ändra inte ena vägen utan den andra (TF-06).
func ScopePack(o diwisepkg.Object) senml.Pack {
	var scoped senml.Pack
	scoped = append(scoped, o.Header())
	scoped = append(scoped, o.Resources()...)

	// The parser has already applied observation-local overrides. Scanning
	// by prefix would also pick up metadata from repeated observations.
	metadata := o.Metadata()
	for _, field := range []struct{ name, value string }{
		{"tenant", metadata.Tenant}, {"source", metadata.Source}, {"env", metadata.Environment},
	} {
		if field.value != "" {
			scoped = append(scoped, senml.Record{Name: o.Prefix() + field.name, StringValue: field.value})
		}
	}
	if metadata.Latitude != nil {
		scoped = append(scoped, senml.Record{Name: o.Prefix() + "lat", Unit: senml.UnitLat, Value: metadata.Latitude})
	}
	if metadata.Longitude != nil {
		scoped = append(scoped, senml.Record{Name: o.Prefix() + "lon", Unit: senml.UnitLon, Value: metadata.Longitude})
	}
	return scoped
}
