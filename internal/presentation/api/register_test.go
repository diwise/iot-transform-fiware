package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/diwise/iot-transform-fiware/internal/application/measurements"
	"github.com/diwise/iot-transform-fiware/internal/application/things"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/rules"
	"github.com/matryer/is"
)

// pgTestModels öppnar regel­lagret mot lokal testdatabas och tömmer det.
// Utan nåbar databas skippas testerna (husets mönster).
func pgTestModels(t *testing.T) *Models {
	t.Helper()
	get := func(key, def string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return def
	}
	store, err := rules.NewPostgres(context.Background(), rules.NewConfig(
		get("PG_TEST_HOST", "localhost"),
		get("PG_TEST_USER", "postgres"),
		get("PG_TEST_PASSWORD", "password"),
		get("PG_TEST_PORT", "5434"),
		get("PG_TEST_DBNAME", "diwise"),
		get("PG_TEST_SSLMODE", "disable"),
	))
	if err != nil {
		t.Skipf("no test database: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	// Töm via list+delete (samma API-yta som koden använder).
	list, err := store.ListRules()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	for _, st := range list {
		if _, err := store.DeleteRule(st.ID); err != nil {
			t.Fatalf("truncate %s: %v", st.ID, err)
		}
	}
	return NewModels(store,
		things.NewDynamicThingTransformer(nil),
		measurements.NewDynamicTransformer(nil, nil),
	)
}

func testRule() transform.Rule {
	return transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Type: "Room"},
		Entities: []transform.Entity{{
			ID:   "urn:ngsi-ld:Room:{{nameOrID}}",
			Type: "Room",
			Properties: []transform.Property{{
				Target: "name", Type: transform.TypeText, ObservedAt: transform.ObservedAtNone,
				Source: transform.Source{Field: "name"},
			}},
		}},
	}
}

func doModel(fn func(http.ResponseWriter, *http.Request), method, id string, body any, headers map[string]string) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req := httptest.NewRequest(method, "/x", &buf)
	if id != "" {
		req.SetPathValue("id", id)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rr := httptest.NewRecorder()
	fn(rr, req)
	return rr
}

func decodeModel(t *testing.T, rr *httptest.ResponseRecorder) Model {
	t.Helper()
	var m Model
	if err := json.Unmarshal(rr.Body.Bytes(), &m); err != nil {
		t.Fatalf("decode: %v (%s)", err, rr.Body.String())
	}
	return m
}

func TestModelsCRUD(t *testing.T) {
	is := is.New(t)
	m := pgTestModels(t)
	h := &handler{models: m}

	rr := doModel(h.createModel, http.MethodPost, "", testRule(), nil)
	is.Equal(rr.Code, http.StatusCreated)
	created := decodeModel(t, rr)
	is.True(created.ID != "")
	is.Equal(created.Revision, int64(1))
	is.Equal(created.Source, rules.SourceAPI)
	is.Equal(rr.Header().Get("ETag"), `"rev-1"`)

	rr = doModel(h.getModel, http.MethodGet, created.ID, nil, nil)
	is.Equal(rr.Code, http.StatusOK)
	is.Equal(rr.Header().Get("ETag"), `"rev-1"`)

	rr = doModel(h.listModels, http.MethodGet, "", nil, nil)
	is.Equal(rr.Code, http.StatusOK)
	var list struct {
		Models []Model `json:"models"`
	}
	is.NoErr(json.Unmarshal(rr.Body.Bytes(), &list))
	is.Equal(len(list.Models), 1)

	r := testRule()
	r.Match.Type = "Hall"
	rr = doModel(h.updateModel, http.MethodPut, created.ID, r, map[string]string{"If-Match": `"rev-1"`})
	is.Equal(rr.Code, http.StatusOK)
	updated := decodeModel(t, rr)
	is.Equal(updated.Revision, int64(2))
	is.Equal(updated.Rule.Match.Type, "Hall")

	rr = doModel(h.updateModel, http.MethodPut, created.ID, r, map[string]string{"If-Match": `"rev-1"`})
	is.Equal(rr.Code, http.StatusConflict)

	rr = doModel(h.getModel, http.MethodGet, "00000000-0000-0000-0000-000000000000", nil, nil)
	is.Equal(rr.Code, http.StatusNotFound)

	rr = doModel(h.deleteModel, http.MethodDelete, created.ID, nil, map[string]string{"If-Match": `"rev-2"`})
	is.Equal(rr.Code, http.StatusNoContent)

	rr = doModel(h.getModel, http.MethodGet, created.ID, nil, nil)
	is.Equal(rr.Code, http.StatusNotFound)
	rr = doModel(h.deleteModel, http.MethodDelete, created.ID, nil, map[string]string{"If-Match": `"rev-2"`})
	is.Equal(rr.Code, http.StatusNotFound)
}

func TestModelsStrictDecode(t *testing.T) {
	is := is.New(t)
	m := pgTestModels(t)
	h := &handler{models: m}

	withUnknown := map[string]any{
		"match":    map[string]any{"kind": "thing", "type": "Room", "bogus": 1},
		"entities": []any{},
	}
	rr := doModel(h.createModel, http.MethodPost, "", withUnknown, nil)
	is.Equal(rr.Code, http.StatusBadRequest)

	badKind := testRule()
	badKind.Match.Kind = "nope"
	rr = doModel(h.createModel, http.MethodPost, "", badKind, nil)
	is.Equal(rr.Code, http.StatusBadRequest)

	// Fel Content-Type nekas.
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(testRule())
	req := httptest.NewRequest(http.MethodPost, "/x", &buf)
	req.Header.Set("Content-Type", "text/yaml")
	rec := httptest.NewRecorder()
	h.createModel(rec, req)
	is.Equal(rec.Code, http.StatusBadRequest)

	// If-Match krävs på PUT.
	rr = doModel(h.updateModel, http.MethodPut, "x", testRule(), nil)
	is.Equal(rr.Code, http.StatusBadRequest)
	rr = doModel(h.updateModel, http.MethodPut, "x", testRule(), map[string]string{"If-Match": "bogus"})
	is.Equal(rr.Code, http.StatusBadRequest)
}

// Omladdning ska synas i körning: efter PUT matchar nya regler,
// efter DELETE matchar inget.
func TestModelsReloadVisibleInTransform(t *testing.T) {
	is := is.New(t)
	m := pgTestModels(t)
	h := &handler{models: m}

	rr := doModel(h.createModel, http.MethodPost, "", testRule(), nil)
	is.Equal(rr.Code, http.StatusCreated)
	created := decodeModel(t, rr)

	th := transform.Thing{
		ID: "r1", Type: "Room", TypeName: "Room", NameOrID: "r1",
		Tenant: "t", Fields: map[string]any{"name": "n"},
	}
	writes, err := m.things.Transform(th)
	is.NoErr(err)
	is.Equal(len(writes), 1)

	r := testRule()
	r.Match.Type = "Other"
	rr = doModel(h.updateModel, http.MethodPut, created.ID, r, map[string]string{"If-Match": `"rev-1"`})
	is.Equal(rr.Code, http.StatusOK)

	_, err = m.things.Transform(th)
	is.True(errors.Is(err, transform.ErrNoMatchingRule))

	rr = doModel(h.deleteModel, http.MethodDelete, created.ID, nil, map[string]string{"If-Match": `"rev-2"`})
	is.Equal(rr.Code, http.StatusNoContent)

	// Tomt regellager: inget matchar, inget fel i omladdningen.
	rr = doModel(h.listModels, http.MethodGet, "", nil, nil)
	is.Equal(rr.Code, http.StatusOK)
	is.True(strings.Contains(rr.Body.String(), `"models":[]`))
}
