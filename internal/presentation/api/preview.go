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
	// Message är det råa kuvertet: thing.updated-, things.v1.values- eller
	// things.v1.relations-JSON för saker, {pack, timestamp} för mätvärden.
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
	// RuleID/RuleRevision/SnapshotVersion är den valda regelns identitet
	// (PLAN1050 E). Inline-preview bär tom ID/0-versioner; lagrade regler
	// bär full identitet via produktionssnapshoten. Alltid satt vid match.
	RuleID          string `json:"ruleId,omitempty"`
	RuleRevision    int64  `json:"ruleRevision,omitempty"`
	SnapshotVersion int64  `json:"snapshotVersion,omitempty"`
	// Competing är konkurrerande matchningar tenant-avgränsat (samma
	// ExplainMatch som produktion loggar; offline, ingen I/O).
	Competing []transform.MatchDiagnostic `json:"competing,omitempty"`
	// Skipped är precisa skiporsaker per entitet/attribut med D:s
	// diagnoskoder (required-missing m.fl.). Tom vid ingen matchning
	// eller vid träffar.
	Skipped []transform.Diagnostic `json:"skipped,omitempty"`
}

type previewEntity struct {
	ID         string         `json:"id"`
	Type       string         `json:"type"`
	Properties map[string]any `json:"properties"`
	// Context är entitetens JSON-LD-context (faller tillbaka till
	// deployment-default i produktion; tom här betyder default).
	Context string `json:"context,omitempty"`
	// EffectiveContext är den faktiska context som produktion serialiserar
	// med (explicit eller bibliotekets default). Aldrig tom vid träff.
	EffectiveContext string `json:"effectiveContext,omitempty"`
	// Operation är create (POST), merge (PATCH/merge), remove
	// (PATCH-null) eller delete (DELETE) — samma semantik som produktion
	// skriver mot brokern.
	Operation string `json:"operation"`
	// RemoveAttributes bär de rensade attributen vid operation remove
	// (explicit rensning; tom annars).
	RemoveAttributes []string `json:"removeAttributes,omitempty"`
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
	// Offline torr körning: samma normalisering/validering som produktion
	// (delad C1-adapter + engine), aldrig brokerkontakt eller mutation.
	detail, err := previewDetailed(r.Context(), req)
	if err != nil {
		// Preview har inga infradependenser (ingen DB/broker); fel från
		// regel/exempel är klientfel (400), aldrig 500. Bevara meddelandet.
		if errors.Is(err, errValidation) {
			writeAPIError(w, err)
		} else {
			writeAPIError(w, fmt.Errorf("%w: %v", errValidation, err))
		}
		return
	}
	out := previewResponse{
		Matched: detail.Matched, Entities: []previewEntity{},
		Observations: detail.Observations,
		RuleID:       detail.RuleID, RuleRevision: detail.RuleRevision,
		SnapshotVersion: detail.SnapshotVersion,
		Competing:       detail.Competing, Skipped: detail.Skipped,
	}
	if detail.Matched && len(detail.Writes) == 0 {
		out.SkippedReason = "matched rule produced no entities (required source missing or suppressed)"
		if len(detail.Skipped) == 0 {
			// Fallback när motorn inte kunde precisera (t.ex. mätvärden
			// utan per-attributdiagnostik): behåll ärlig, om än generell,
			// orsak i stället för tyst tomhet.
			out.Skipped = []transform.Diagnostic{{
				Code: transform.CodeRequiredMissing, RuleID: detail.RuleID,
				RuleRevision: detail.RuleRevision, SnapshotVersion: detail.SnapshotVersion,
				Detail: "matched rule produced no entities (required source missing or suppressed)",
			}}
		}
	}
	for _, wr := range detail.Writes {
		props, err := entityProperties(wr.EntityID, wr.TypeName, wr.Props)
		if err != nil {
			writeAPIError(w, fmt.Errorf("%w: %v", errValidation, err))
			return
		}
		op := "merge"
		if wr.Create {
			op = "create"
		}
		if len(wr.RemoveAttributes) > 0 {
			op = "remove"
		}
		if wr.Delete {
			op = "delete"
		}
		effective := wr.Context
		if effective == "" {
			effective = entities.DefaultContextURL
		}
		out.Entities = append(out.Entities, previewEntity{
			ID: wr.EntityID, Type: wr.TypeName, Properties: props,
			Context: wr.Context, EffectiveContext: effective, Operation: op,
			RemoveAttributes: append([]string(nil), wr.RemoveAttributes...),
		})
	}
	// Validera faktisk serialiserad NGSI-LD för hela batchen före svar
	// (samma ValidateEntityWrites som produktion kör före broker-I/O).
	if err := transform.ValidateEntityWrites(detail.Writes, ""); err != nil {
		writeAPIError(w, fmt.Errorf("%w: %v", errValidation, err))
		return
	}
	writeJSON(w, http.StatusOK, out)
}

// previewDetail är den rika torra körningen för PLAN1050 E: skrivningar +
// vald regel/revision/snapshotversion, konkurrerande matchningar
// tenant-avgränsat och precisa skiporsaker. Offline: samma C1-normalisering
// och engine som produktion, aldrig brokerkontakt eller mutation.
type previewDetail struct {
	Writes          []transform.EntityWrite
	Matched         bool
	Observations    int
	RuleID          string
	RuleRevision    int64
	SnapshotVersion int64
	Competing       []transform.MatchDiagnostic
	Skipped         []transform.Diagnostic
}

// previewDetailed kör en inline-regel torrt med full diagnostik. Delar
// normalisering med produktion (samma ThingValuesFromJSON/
// ThingRelationsFromJSON/ThingLifecycleFromJSON/PartitionMessage + engine).
func previewDetailed(ctx context.Context, req previewRequest) (previewDetail, error) {
	engine := transform.NewEngine([]transform.Rule{req.Rule})
	detailFor := func(in transform.Thing, observations int) (previewDetail, error) {
		matched := engine.Matched(in)
		if !matched {
			return previewDetail{Matched: false, Observations: observations,
				Writes: []transform.EntityWrite{}}, nil
		}
		rule, _ := engine.MatchedRule(in)
		competing := engine.ExplainMatch(in)
		writes, err := engine.Transform(in)
		if errors.Is(err, transform.ErrNoMatchingRule) {
			return previewDetail{Matched: false, Observations: observations,
				Writes: []transform.EntityWrite{}}, nil
		}
		if err != nil {
			return previewDetail{Matched: true, Observations: observations,
				RuleID: rule.ID, RuleRevision: rule.Revision, SnapshotVersion: rule.SnapshotVersion,
				Competing: competing}, err
		}
		var skipped []transform.Diagnostic
		if len(writes) == 0 {
			skipped = engine.ExplainSkipped(in)
		}
		return previewDetail{Writes: writes, Matched: true, Observations: observations,
			RuleID: rule.ID, RuleRevision: rule.Revision, SnapshotVersion: rule.SnapshotVersion,
			Competing: competing, Skipped: skipped}, nil
	}
	switch req.Event.Kind {
	case transform.EventThingsV1Values:
		th, err := things.ThingValuesFromJSON(req.Event.Message)
		if err != nil {
			return previewDetail{}, fmt.Errorf("%w: invalid values envelope: %v", errValidation, err)
		}
		return detailFor(th, 1)
	case string(transform.KindMeasurement):
		var msg events.MessageAccepted
		if err := json.Unmarshal(req.Event.Message, &msg); err != nil {
			return previewDetail{}, fmt.Errorf("%w: invalid measurement envelope: %v", errValidation, err)
		}
		d := measurements.NewDynamicTransformer([]transform.Rule{req.Rule}, staticSensorType(req.Event.SensorType))
		// Samma delning som produktion: varje objektobservation för sig.
		observations, err := measurements.PartitionMessage(msg, time.Now().UTC())
		if err != nil {
			return previewDetail{}, fmt.Errorf("%w: invalid measurement pack: %v", errValidation, err)
		}
		if len(observations) == 0 {
			return previewDetail{Matched: false, Observations: 0, Writes: []transform.EntityWrite{}}, nil
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
				return previewDetail{Matched: true, Observations: len(observations),
					RuleID: req.Rule.ID, RuleRevision: req.Rule.Revision, SnapshotVersion: req.Rule.SnapshotVersion}, err
			}
			all = append(all, writes...)
		}
		if all == nil {
			all = []transform.EntityWrite{}
		}
		var skipped []transform.Diagnostic
		if matchedAny && len(all) == 0 {
			skipped = []transform.Diagnostic{{
				Code: transform.CodeRequiredMissing, RuleID: req.Rule.ID,
				RuleRevision: req.Rule.Revision, SnapshotVersion: req.Rule.SnapshotVersion,
				Detail: "matched rule produced no entities (required source missing or suppressed)",
			}}
		}
		// Konkurrerande matchningar för en-regels inline-preview är triviala;
		// tenant-avgränsning bevisas via match (främmande tenant ger no-match).
		var competing []transform.MatchDiagnostic
		if matchedAny {
			competing = []transform.MatchDiagnostic{{
				Index: 0, RuleID: req.Rule.ID, Revision: req.Rule.Revision,
				SnapshotVersion: req.Rule.SnapshotVersion, Priority: req.Rule.Priority,
				Specificity: req.Rule.Match.Specificity(), Winner: true, Reason: "winner",
			}}
		}
		_ = engine
		return previewDetail{Writes: all, Matched: matchedAny, Observations: len(observations),
			RuleID: req.Rule.ID, RuleRevision: req.Rule.Revision, SnapshotVersion: req.Rule.SnapshotVersion,
			Competing: competing, Skipped: skipped}, nil
	case transform.EventThingsV1Relations:
		th, err := things.ThingRelationsFromJSON(req.Event.Message)
		if err != nil {
			return previewDetail{}, fmt.Errorf("%w: invalid relations envelope: %v", errValidation, err)
		}
		return detailFor(th, 1)
	case transform.EventThingsV1Lifecycle:
		th, err := things.ThingLifecycleFromJSON(req.Event.Message)
		if err != nil {
			return previewDetail{}, fmt.Errorf("%w: invalid lifecycle envelope: %v", errValidation, err)
		}
		return detailFor(th, 1)
	default:
		return previewDetail{}, fmt.Errorf("%w: event.kind must be %q, %q, %q or %q", errValidation,
			transform.KindMeasurement, transform.EventThingsV1Values,
			transform.EventThingsV1Relations, transform.EventThingsV1Lifecycle)
	}
}

// entityProperties marshalar en EntityWrite till NGSI-LD-JSON och returnerar
// attributen (id/type/@context skalas av) — samma serialisering som mot
// context-broker i produktion.
func entityProperties(id, typeName string, props []entities.EntityDecoratorFunc) (map[string]any, error) {
	e, err := transform.BuildEntity(transform.EntityWrite{EntityID: id, TypeName: typeName, Props: props}, "")
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
	case transform.EventThingsV1Relations:
		th, err := things.ThingRelationsFromJSON(req.Event.Message)
		if err != nil {
			return nil, false, 0, fmt.Errorf("%w: invalid relations envelope: %v", errValidation, err)
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
	case transform.EventThingsV1Lifecycle:
		th, err := things.ThingLifecycleFromJSON(req.Event.Message)
		if err != nil {
			return nil, false, 0, fmt.Errorf("%w: invalid lifecycle envelope: %v", errValidation, err)
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
	default:
		return nil, false, 0, fmt.Errorf("%w: event.kind must be %q, %q, %q or %q", errValidation,
			transform.KindMeasurement, transform.EventThingsV1Values,
			transform.EventThingsV1Relations, transform.EventThingsV1Lifecycle)
	}
}
