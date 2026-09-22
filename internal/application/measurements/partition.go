package measurements

import (
	"strconv"
	"strings"
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
		scoped := ScopePack(parsed, o)
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
func ScopePack(parsed *diwisepkg.Pack, o diwisepkg.Object) senml.Pack {
	devicePrefix := parsed.DeviceID() + "/"

	var scoped senml.Pack
	scoped = append(scoped, o.Header())
	scoped = append(scoped, o.Resources()...)

	for _, r := range parsed.Pack() {
		name := r.Name
		idx := strings.LastIndex(name, "/")
		if idx < 0 {
			continue
		}
		trailing, prefix := name[idx+1:], name[:idx+1]
		if trailing == "" || trailing == "0" {
			continue
		}
		if _, err := strconv.Atoi(trailing); err == nil {
			continue
		}
		if prefix != devicePrefix && prefix != o.Prefix() {
			continue
		}
		scoped = append(scoped, r)
	}

	return scoped
}
