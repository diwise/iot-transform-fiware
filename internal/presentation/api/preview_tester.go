package api

import (
	"context"
	"encoding/json"

	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
)

// Backend-previewtester för G/I/J (PLAN1050 E): offline torr körning utan
// HTTP/auth/broker. Delar normalisering och validering med produktion via
// previewDetailed (samma C1-adaptrar + engine), så webbklienter och
// J-harness ser identiska källvyer utan brokerkontakt eller mutation.
//
// GUI/templ tillhör I; detta kontrakt är avsiktligt minimalt och additivt:
// JSON-fältnamn speglar preview-svaret, ingen befintlig API-yta ändras.

// PreviewTestInput är indata till backend-previewtestern: inline-regel +
// exempelhändelse (samma form som previewRequest, utan HTTP-kuvert).
type PreviewTestInput struct {
	Rule       transform.Rule  `json:"rule"`
	Kind       string          `json:"kind"`
	Message    json.RawMessage `json:"message"`
	SensorType string          `json:"sensorType,omitempty"`
}

// PreviewTestEntity är en förhandsvisad entitet med produktionens operation,
// rensning och effektiva context.
type PreviewTestEntity struct {
	ID               string         `json:"id"`
	Type             string         `json:"type"`
	Properties       map[string]any `json:"properties"`
	Context          string         `json:"context,omitempty"`
	EffectiveContext string         `json:"effectiveContext,omitempty"`
	Operation        string         `json:"operation"`
	RemoveAttributes []string       `json:"removeAttributes,omitempty"`
}

// PreviewTestResult är utfallet: matchning, skrivningar, vald regel,
// konkurrerande tenant-avgränsat och precisa skiporsaker.
type PreviewTestResult struct {
	Matched         bool                        `json:"matched"`
	Observations    int                         `json:"observations"`
	Entities        []PreviewTestEntity         `json:"entities"`
	RuleID          string                      `json:"ruleId,omitempty"`
	RuleRevision    int64                       `json:"ruleRevision,omitempty"`
	SnapshotVersion int64                       `json:"snapshotVersion,omitempty"`
	Competing       []transform.MatchDiagnostic `json:"competing,omitempty"`
	Skipped         []transform.Diagnostic      `json:"skipped,omitempty"`
	SkippedReason   string                      `json:"skippedReason,omitempty"`
}

// RunPreviewTest kör en torr offline-preview utan brokerkontakt eller
// mutation. Fel från regel/exempel är klientfel (samma errValidation som
// previewModel mappar till 400); faktisk serialiserad NGSI-LD valideras för
// hela batchen före retur.
func RunPreviewTest(ctx context.Context, in PreviewTestInput) (PreviewTestResult, error) {
	detail, err := previewDetailed(ctx, previewRequest{
		Rule:  in.Rule,
		Event: previewEvent{Kind: in.Kind, Message: in.Message, SensorType: in.SensorType},
	})
	if err != nil {
		return PreviewTestResult{}, err
	}
	out := PreviewTestResult{
		Matched: detail.Matched, Observations: detail.Observations,
		Entities: []PreviewTestEntity{},
		RuleID:   detail.RuleID, RuleRevision: detail.RuleRevision,
		SnapshotVersion: detail.SnapshotVersion,
		Competing:       detail.Competing, Skipped: detail.Skipped,
	}
	if detail.Matched && len(detail.Writes) == 0 {
		out.SkippedReason = "matched rule produced no entities (required source missing or suppressed)"
	}
	for _, wr := range detail.Writes {
		props, err := entityProperties(wr.EntityID, wr.TypeName, wr.Props)
		if err != nil {
			return PreviewTestResult{}, err
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
		out.Entities = append(out.Entities, PreviewTestEntity{
			ID: wr.EntityID, Type: wr.TypeName, Properties: props,
			Context: wr.Context, EffectiveContext: effective, Operation: op,
			RemoveAttributes: append([]string(nil), wr.RemoveAttributes...),
		})
	}
	if err := transform.ValidateEntityWrites(detail.Writes, ""); err != nil {
		return PreviewTestResult{}, err
	}
	return out, nil
}
