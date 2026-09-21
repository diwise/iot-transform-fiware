package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

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
}

type previewEntity struct {
	ID         string         `json:"id"`
	Type       string         `json:"type"`
	Properties map[string]any `json:"properties"`
}

type staticSensorType string

func (s staticSensorType) SensorType(context.Context, string) (string, error) {
	return string(s), nil
}

func decodePreview(r *http.Request) (previewRequest, error) {
	var req previewRequest
	if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		return req, fmt.Errorf("%w: content-type must be application/json", errValidation)
	}
	if r.Body == nil {
		return req, fmt.Errorf("%w: empty body", errValidation)
	}
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return req, fmt.Errorf("%w: invalid preview body: %v", errValidation, err)
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
	if _, err := decodeRule(r); err != nil {
		writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"valid": true})
}

func (h *handler) previewModel(w http.ResponseWriter, r *http.Request) {
	req, err := decodePreview(r)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writes, err := previewTransform(r.Context(), req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	out := previewResponse{Matched: len(writes) > 0, Entities: []previewEntity{}}
	for _, wr := range writes {
		props, err := entityProperties(wr.EntityID, wr.TypeName, wr.Props)
		if err != nil {
			writeAPIError(w, fmt.Errorf("%w: %v", errValidation, err))
			return
		}
		out.Entities = append(out.Entities, previewEntity{ID: wr.EntityID, Type: wr.TypeName, Properties: props})
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
// adaptrar som produktion (live-paritet). ErrNoMatchingRule blir tomt
// resultat, aldrig fel.
func previewTransform(ctx context.Context, req previewRequest) ([]transform.EntityWrite, error) {
	engine := transform.NewEngine([]transform.Rule{req.Rule})
	switch req.Event.Kind {
	case string(transform.KindThing):
		th, err := things.ThingFromJSON(req.Event.Message)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid thing envelope: %v", errValidation, err)
		}
		writes, err := engine.Transform(th)
		if errors.Is(err, transform.ErrNoMatchingRule) {
			return nil, nil
		}
		return writes, err
	case transform.EventThingsV1Values:
		th, err := things.ThingValuesFromJSON(req.Event.Message)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid values envelope: %v", errValidation, err)
		}
		writes, err := engine.Transform(th)
		if errors.Is(err, transform.ErrNoMatchingRule) {
			return nil, nil
		}
		return writes, err
	case string(transform.KindMeasurement):
		var msg events.MessageAccepted
		if err := json.Unmarshal(req.Event.Message, &msg); err != nil {
			return nil, fmt.Errorf("%w: invalid measurement envelope: %v", errValidation, err)
		}
		d := measurements.NewDynamicTransformer([]transform.Rule{req.Rule}, staticSensorType(req.Event.SensorType))
		writes, err := d.Transform(ctx, msg)
		if errors.Is(err, transform.ErrNoMatchingRule) {
			return nil, nil
		}
		return writes, err
	default:
		return nil, fmt.Errorf("%w: event.kind must be %q, %q or %q", errValidation,
			transform.KindMeasurement, transform.KindThing, transform.EventThingsV1Values)
	}
}
