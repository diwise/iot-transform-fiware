package api

import (
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TF-07: OpenAPI-kontraktet ska spegla kodens routes och motorns kapacitet:
// serverprefix /api/v0, observedAt med field:-stöd, preview med operation/context.
func TestOpenAPIContract(t *testing.T) {
	raw, err := os.ReadFile("../../../assets/docs/openapi.yaml")
	if err != nil {
		t.Fatalf("openapi read: %v", err)
	}
	var doc struct {
		Servers []struct {
			URL string `yaml:"url"`
		} `yaml:"servers"`
		Paths      map[string]map[string]any `yaml:"paths"`
		Components struct {
			Schemas map[string]struct {
				Properties map[string]struct {
					Enum        []string `yaml:"enum"`
					Description string   `yaml:"description"`
					Type        string   `yaml:"type"`
				} `yaml:"properties"`
				Required []string `yaml:"required"`
			} `yaml:"schemas"`
		} `yaml:"components"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("openapi parse: %v", err)
	}

	// Serverprefix /api/v0 måste deklareras (koden registrerar under /api/v0).
	found := false
	for _, s := range doc.Servers {
		if strings.Contains(s.URL, "/api/v0") {
			found = true
		}
	}
	if !found {
		t.Fatal("openapi servers must declare /api/v0 (code registers under /api/v0)")
	}

	// Paths är relativa mot servern: /models, /models/{id}, /models/validate, /models/preview.
	for _, p := range []string{"/models", "/models/{id}", "/models/validate", "/models/preview"} {
		if _, ok := doc.Paths[p]; !ok {
			t.Fatalf("openapi must document %s", p)
		}
	}

	// observedAt måste tillåta field:-vägar (motorn stöder dem) — inte bara enum.
	prop, ok := doc.Components.Schemas["Property"].Properties["observedAt"]
	if !ok {
		t.Fatal("Property must define observedAt")
	}
	if len(prop.Enum) > 0 {
		for _, e := range prop.Enum {
			if strings.HasPrefix(e, "field:") {
				goto fieldOK
			}
		}
		// Enum utan field:-stöd är för snävt — beskrivningen måste nämna field:.
		if !strings.Contains(prop.Description, "field:") {
			t.Fatalf("observedAt enum %v lacks field: support; description must mention field: paths", prop.Enum)
		}
	}
fieldOK:

	// Preview-svaret ska kräva operation (create/merge) och observations (TF-06).
	// Enkel yaml-kontroll: råtexten måste innehålla nycklarna.
	text := string(raw)
	for _, key := range []string{"observations:", "skippedReason", "operation:", "context:"} {
		if !strings.Contains(text, key) {
			t.Fatalf("openapi preview response must document %q", key)
		}
	}

	// GET /models ska dokumentera de valfria sökfiltren (PLAN002 1.1).
	for _, key := range []string{"name: kind", "name: event", "name: type", "name: tenant", "name: source"} {
		if !strings.Contains(text, key) {
			t.Fatalf("openapi GET /models must document filter %q", key)
		}
	}
}
