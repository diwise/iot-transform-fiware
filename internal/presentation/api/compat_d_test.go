package api

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"testing"

	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/presentation/api/auth"
)

// PLAN1050 D: kompatibilitets-API (POST /models/compat). Torr offline-kontroll
// mot explicit mallversion + källkontrakt. Generiska regler/testdata, inga
// standardmodellnamn.

func compatBody(template map[string]any, sources map[string]any, rules []any, extra map[string]any) map[string]any {
	body := map[string]any{"template": template, "rules": rules}
	if sources != nil {
		body["sources"] = sources
	}
	maps.Copy(body, extra)
	return body
}

func genericCompatRule() map[string]any {
	return map[string]any{
		"match": map[string]any{"kind": "thing", "event": "things.v1.values", "type": "generic-template"},
		"entities": []any{map[string]any{
			"id": "urn:ngsi-ld:Generic:{{id}}", "type": "Generic", "name": "primary",
			"properties": []any{
				map[string]any{"target": "reading", "type": "Number", "field": "values.temperature.value", "required": true},
				map[string]any{"target": "label", "type": "Text", "field": "name", "observedAt": "none"},
			},
		}},
	}
}

func doCompat(t *testing.T, body map[string]any) (int, map[string]any) {
	t.Helper()
	h := &handler{models: NewModels(&stubRuleStore{}, nil)}
	rr := doModelAs(h.compatModel, http.MethodPost, "", body, nil, writeOnly("t"))
	var out map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode compat: %v (%s)", err, rr.Body.String())
	}
	return rr.Code, out
}

func TestCompatPositiveGeneric(t *testing.T) {
	template := map[string]any{"thingType": "generic-template", "thingTypeVersion": "2", "variantId": "variant-a", "variantVersion": "3"}
	sources := map[string]any{"properties": []any{map[string]any{"id": "temperature", "unit": "CEL"}}}
	body := compatBody(template, sources, []any{genericCompatRule()}, map[string]any{"tenant": "tenant-a", "thingId": "thing-1", "configRevision": 7})
	code, out := doCompat(t, body)
	if code != http.StatusOK {
		t.Fatalf("compat %d: %v", code, out)
	}
	if out["compatible"] != true {
		t.Fatalf("positive compat: %v", out)
	}
	if diags, _ := out["diagnostics"].([]any); len(diags) != 0 {
		t.Fatalf("unexpected diagnostics: %v", out)
	}
}

func TestCompatErrorsWithOtherwiseValidRules(t *testing.T) {
	baseSources := map[string]any{"properties": []any{map[string]any{"id": "temperature", "unit": "CEL"}}}
	// Version-filter som inte matchar mallen (regeln i övrigt giltig).
	rule := genericCompatRule()
	rule["match"].(map[string]any)["thingTypeVersion"] = "1"
	body := compatBody(map[string]any{"thingType": "generic-template", "thingTypeVersion": "9"}, baseSources, []any{rule}, map[string]any{"tenant": "tenant-a"})
	code, out := doCompat(t, body)
	if code != http.StatusOK || out["compatible"] != false {
		t.Fatalf("no-match: %d %v", code, out)
	}
	found := false
	for _, d := range out["diagnostics"].([]any) {
		m := d.(map[string]any)
		if m["code"] == "no-match" && m["tenant"] == "tenant-a" {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing no-match diagnostic: %v", out)
	}

	// Required-källa saknas i kontraktet.
	rule = genericCompatRule()
	props := rule["entities"].([]any)[0].(map[string]any)["properties"].([]any)
	props[0].(map[string]any)["field"] = "values.missing.value"
	body = compatBody(map[string]any{"thingType": "generic-template"}, baseSources, []any{rule}, map[string]any{"tenant": "tenant-a", "thingId": "thing-1", "configRevision": 7})
	code, out = doCompat(t, body)
	if code != http.StatusOK || out["compatible"] != false {
		t.Fatalf("required-missing: %d %v", code, out)
	}
	found = false
	for _, d := range out["diagnostics"].([]any) {
		m := d.(map[string]any)
		if m["code"] == "required-missing" && m["entity"] == "primary" && m["attribute"] == "reading" && m["thingId"] == "thing-1" && m["configRevision"] == float64(7) {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing required-missing with role/attribute: %v", out)
	}

	// Okänd målroll (regeln i övrigt giltig → 200 med target-notfound, inte 400;
	// 400 gäller validate/create med ogiltig regel, compat rapporterar mjukt).
	rule = genericCompatRule()
	rule["entities"].([]any)[0].(map[string]any)["id"] = "urn:ngsi-ld:Generic:{{entities.missing.id}}"
	body = compatBody(map[string]any{"thingType": "generic-template"}, nil, []any{rule}, nil)
	code, out = doCompat(t, body)
	if code != http.StatusOK || out["compatible"] != false {
		t.Fatalf("target ref: %d %v", code, out)
	}
}

func TestCompatStrictDecodeAndScope(t *testing.T) {
	h := &handler{models: NewModels(&stubRuleStore{}, nil)}

	// Tom regeluppsättning nekas.
	rr := doModelAs(h.compatModel, http.MethodPost, "", compatBody(map[string]any{"thingType": "generic-template"}, nil, []any{}, nil), nil, writeOnly("t"))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("empty rules: %d %s", rr.Code, rr.Body.String())
	}

	// Okända fält nekas strikt.
	bad := compatBody(map[string]any{"thingType": "generic-template"}, nil, []any{genericCompatRule()}, nil)
	bad["bogus"] = 1
	rr = doModelAs(h.compatModel, http.MethodPost, "", bad, nil, writeOnly("t"))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("unknown field: %d %s", rr.Code, rr.Body.String())
	}

	// Fel scope: create-only får inte köra compat (kräver write).
	rr = doModelAs(h.compatModel, http.MethodPost, "", compatBody(map[string]any{"thingType": "generic-template"}, nil, []any{genericCompatRule()}, nil), nil, fullAccess("t"))
	// fullAccess innehåller write → OK; prova utan write:
	readOnly := auth.WithAccess(context.Background(), map[string]map[auth.Scope]struct{}{"t": {TransformRead: {}}})
	rr = doModelAs(h.compatModel, http.MethodPost, "", compatBody(map[string]any{"thingType": "generic-template"}, nil, []any{genericCompatRule()}, nil), nil, readOnly)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("scope: %d %s", rr.Code, rr.Body.String())
	}
}

func readOnlyForCompat() {}

func TestCompatVersionVariantMatch(t *testing.T) {
	// Additiv bakåtkompat: gammal regel utan nya fält gäller mot versionerad mall.
	body := compatBody(map[string]any{"thingType": "generic-template", "thingTypeVersion": "5"}, nil, []any{genericCompatRule()}, nil)
	code, out := doCompat(t, body)
	if code != http.StatusOK || out["compatible"] != true {
		t.Fatalf("wildcard version must match: %d %v", code, out)
	}
	// Exakt variantmatchning.
	rule := genericCompatRule()
	rule["match"].(map[string]any)["variantId"] = "variant-a"
	body = compatBody(map[string]any{"thingType": "generic-template", "variantId": "variant-a"}, nil, []any{rule}, nil)
	code, out = doCompat(t, body)
	if code != http.StatusOK || out["compatible"] != true {
		t.Fatalf("variant match: %d %v", code, out)
	}
	body = compatBody(map[string]any{"thingType": "generic-template", "variantId": "other"}, nil, []any{rule}, nil)
	code, out = doCompat(t, body)
	if code != http.StatusOK || out["compatible"] != false {
		t.Fatalf("variant mismatch: %d %v", code, out)
	}
}

var _ = transform.CodeNoMatch
