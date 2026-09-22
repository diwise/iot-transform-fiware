package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/iot-transform-fiware/internal/application/measurements"
	"github.com/diwise/iot-transform-fiware/internal/application/things"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
)

// previewRequest är en torr körning: inline-regel + exempelhändelse.
// Aldrig broker-kontakt, aldrig persistens.
type previewRequest struct {
	Rule  transform.Rule `json:"rule"`
	Event previewEvent   `json:"event"`
}

type previewEvent struct {
	Kind string `json:"kind"`
	// Message är det råa kuvertet: thing.updated- eller
	// things.v1.values-JSON för saker, {pack, timestamp} för mätvärden.
	Message json.RawMessage `json:"message"`
	// SensorType kringgår device-management-uppslag för mätvärden
	// (offline-preview); tom sträng beter sig som produktion.
	SensorType string `json:"sensorType,omitempty"`
}

type previewResponse struct {
	Matched  bool            `json:"matched"`
	Entities []previewEntity `json:"entities"`
	// Observations är antal enobservations-indata som utvärderades
	// (multi-pack delas som i produktion, TF-06).
	Observations int `json:"observations"`
	// SkippedReason förklarar matchad regel utan entiteter (t.ex. required
	// saknas → undertryckt). Tom vid ingen matchning eller vid träffar.
	SkippedReason string `json:"skippedReason,omitempty"`
}

type previewEntity struct {
	ID         string         `json:"id"`
	Type       string         `json:"type"`
	Properties map[string]any `json:"properties"`
	// Context är entitetens JSON-LD-context (faller tillbaka till
	// deployment-default i produktion; tom här betyder default).
	Context string `json:"context,omitempty"`
	// Operation är create (POST) eller merge (PATCH/merge) — samma
	// semantik som produktion skriver mot brokern.
	Operation string `json:"operation"`
}

type staticSensorType string

func (s staticSensorType) SensorType(context.Context, string) (string, error) {
	return string(s), nil
}

func decodePreview(r *http.Request) (previewRequest, error) {
	var req previewRequest
	if err := decodeJSONBody(r, &req); err != nil {
		return req, err
	}
	if err := transform.Validate([]transform.Rule{req.Rule}); err != nil {
		return req, fmt.Errorf("%w: %v", errValidation, err)
	}
	if len(req.Event.Message) == 0 {
		return req, fmt.Errorf("%w: event.message is required", errValidation)
	}
	return req, nil
}

func (h *handler) validateModel(w http.ResponseWriter, r *http.Request) {
	if _, err := authorizedTenants(r, TransformWrite); err != nil {
		writeAPIError(w, err)
		return
	}
	if _, err := decodeRule(r); err != nil {
		writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"valid": true})
}

func (h *handler) previewModel(w http.ResponseWriter, r *http.Request) {
	if _, err := authorizedTenants(r, TransformWrite); err != nil {
		writeAPIError(w, err)
		return
	}
	req, err := decodePreview(r)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writes, matched, observations, err := previewTransform(r.Context(), req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	out := previewResponse{Matched: matched, Entities: []previewEntity{}, Observations: observations}
	if matched && len(writes) == 0 {
		out.SkippedReason = "matched rule produced no entities (required source missing or suppressed)"
	}
	for _, wr := range writes {
		props, err := entityProperties(wr.EntityID, wr.TypeName, wr.Props)
		if err != nil {
			writeAPIError(w, fmt.Errorf("%w: %v", errValidation, err))
			return
		}
		op := "merge"
		if wr.Create {
			op = "create"
		}
		out.Entities = append(out.Entities, previewEntity{ID: wr.EntityID, Type: wr.TypeName, Properties: props, Context: wr.Context, Operation: op})
	}
	writeJSON(w, http.StatusOK, out)
}

// entityProperties marshalar en EntityWrite till NGSI-LD-JSON och returnerar
// attributen (id/type/@context skalas av) — samma serialisering som mot
// context-broker i produktion.
func entityProperties(id, typeName string, props []entities.EntityDecoratorFunc) (map[string]any, error) {
	e, err := entities.New(id, typeName, props...)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	delete(m, "id")
	delete(m, "type")
	delete(m, "@context")
	if m == nil {
		m = map[string]any{}
	}
	return m, nil
}

// previewTransform kör en inline-regel torrt mot exempelhändelsen med samma
// normalisering som produktion (TF-06): mätvärdespack delas i
// enobservations-meddelanden före transformation. Returnerar skrivningar,
// om någon regel matchade (även undertryckt) samt antal observationer.
// ErrNoMatchingRule blir tomt resultat med matched=false, aldrig fel.
func previewTransform(ctx context.Context, req previewRequest) ([]transform.EntityWrite, bool, int, error) {
	engine := transform.NewEngine([]transform.Rule{req.Rule})
	switch req.Event.Kind {
	case string(transform.KindThing):
		th, err := things.ThingFromJSON(req.Event.Message)
		if err != nil {
			return nil, false, 0, fmt.Errorf("%w: invalid thing envelope: %v", errValidation, err)
		}
		matched := engine.Matched(th)
		if !matched {
			return nil, false, 1, nil
		}
		writes, err := engine.Transform(th)
		if errors.Is(err, transform.ErrNoMatchingRule) {
			return nil, false, 1, nil
		}
		if err != nil {
			return nil, true, 1, err
		}
		return writes, true, 1, nil
	case transform.EventThingsV1Values:
		th, err := things.ThingValuesFromJSON(req.Event.Message)
		if err != nil {
			return nil, false, 0, fmt.Errorf("%w: invalid values envelope: %v", errValidation, err)
		}
		matched := engine.Matched(th)
		if !matched {
			return nil, false, 1, nil
		}
		writes, err := engine.Transform(th)
		if errors.Is(err, transform.ErrNoMatchingRule) {
			return nil, false, 1, nil
		}
		if err != nil {
			return nil, true, 1, err
		}
		return writes, true, 1, nil
	case string(transform.KindMeasurement):
		var msg events.MessageAccepted
		if err := json.Unmarshal(req.Event.Message, &msg); err != nil {
			return nil, false, 0, fmt.Errorf("%w: invalid measurement envelope: %v", errValidation, err)
		}
		d := measurements.NewDynamicTransformer([]transform.Rule{req.Rule}, staticSensorType(req.Event.SensorType))
		// Samma delning som produktion: varje objektobservation för sig.
		observations, err := measurements.PartitionMessage(msg, time.Now().UTC())
		if err != nil {
			return nil, false, 0, fmt.Errorf("%w: invalid measurement pack: %v", errValidation, err)
		}
		// Tomt pack (inga objekt): samma som produktion — inga observationer,
		// ingen matchning.
		if len(observations) == 0 {
			return nil, false, 0, nil
		}
		var all []transform.EntityWrite
		matchedAny := false
		for _, obs := range observations {
			if d.Matched(ctx, obs) {
				matchedAny = true
			}
			writes, err := d.Transform(ctx, obs)
			if errors.Is(err, transform.ErrNoMatchingRule) {
				continue
			}
			if err != nil {
				return nil, true, len(observations), err
			}
			all = append(all, writes...)
		}
		if all == nil {
			all = []transform.EntityWrite{}
		}
		return all, matchedAny, len(observations), nil
	default:
		return nil, false, 0, fmt.Errorf("%w: event.kind must be %q, %q or %q", errValidation,
			transform.KindMeasurement, transform.KindThing, transform.EventThingsV1Values)
	}
}
