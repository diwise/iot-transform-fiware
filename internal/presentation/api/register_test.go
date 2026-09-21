package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/rules"
	"github.com/diwise/iot-transform-fiware/internal/presentation/api/auth"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/matryer/is"
)

// pgTestModels öppnar regel­lagret mot lokal testdatabas och tömmer det.
// Utan nåbar databas skippas testerna (husets mönster).
// Egen databas per paket: go test kör paket parallellt och delad tabell
// ger interferens (TF-08). Kan överstyras med PG_TEST_DBNAME (samma namn
// i flera paket återinför interferens).
func pgTestModels(t *testing.T) *Models {
	t.Helper()
	get := func(key, def string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return def
	}
	newCfg := func(dbname string) rules.Config {
		return rules.NewConfig(
			get("PG_TEST_HOST", "localhost"),
			get("PG_TEST_USER", "postgres"),
			get("PG_TEST_PASSWORD", "password"),
			get("PG_TEST_PORT", "5434"),
			dbname,
			get("PG_TEST_SSLMODE", "disable"),
		)
	}
	dbname := get("PG_TEST_DBNAME", "diwise_test_api")
	admin, err := pgx.Connect(context.Background(), newCfg("postgres").ConnStr())
	if err != nil {
		t.Skipf("no test database server: %v", err)
	}
	_, err = admin.Exec(context.Background(), fmt.Sprintf(`CREATE DATABASE %s`, pgx.Identifier{dbname}.Sanitize()))
	admin.Close(context.Background())
	if err != nil {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42P04" {
			t.Skipf("cannot create test database: %v", err)
		}
	}
	store, err := rules.NewPostgres(context.Background(), newCfg(dbname))
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
		if err := store.DeleteRule(st.ID, st.Revision); err != nil {
			t.Fatalf("truncate %s: %v", st.ID, err)
		}
	}
	m := NewModels(store, nil)
	if err := m.Refresh(); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	return m
}

func testRule() transform.Rule {
	return transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Type: "Room", Tenant: "t"},
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

// fullAccess ger samtliga scopes i angivna tenants (testmotsvarigheten
// till en OPA-policy som beviljat allt).
func fullAccess(tenants ...string) context.Context {
	m := map[string]map[auth.Scope]struct{}{}
	for _, t := range tenants {
		m[t] = map[auth.Scope]struct{}{
			TransformRead:   {},
			TransformCreate: {},
			TransformUpdate: {},
			TransformDelete: {},
		}
	}
	return auth.WithAccess(context.Background(), m)
}

func doModel(fn func(http.ResponseWriter, *http.Request), method, id string, body any, headers map[string]string) *httptest.ResponseRecorder {
	return doModelAs(fn, method, id, body, headers, fullAccess("t"))
}

func doModelAs(fn func(http.ResponseWriter, *http.Request), method, id string, body any, headers map[string]string, ctx context.Context) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req := httptest.NewRequest(method, "/x", &buf)
	req = req.WithContext(ctx)
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
	writes, err := m.Snapshot().Things.Transform(th)
	is.NoErr(err)
	is.Equal(len(writes), 1)

	r := testRule()
	r.Match.Type = "Other"
	rr = doModel(h.updateModel, http.MethodPut, created.ID, r, map[string]string{"If-Match": `"rev-1"`})
	is.Equal(rr.Code, http.StatusOK)

	_, err = m.Snapshot().Things.Transform(th)
	is.True(errors.Is(err, transform.ErrNoMatchingRule))

	rr = doModel(h.deleteModel, http.MethodDelete, created.ID, nil, map[string]string{"If-Match": `"rev-2"`})
	is.Equal(rr.Code, http.StatusNoContent)

	// Tomt regellager: inget matchar, inget fel i omladdningen.
	rr = doModel(h.listModels, http.MethodGet, "", nil, nil)
	is.Equal(rr.Code, http.StatusOK)
	is.True(strings.Contains(rr.Body.String(), `"models":[]`))
}

func TestModelsTenantIsolation(t *testing.T) {
	is := is.New(t)
	m := pgTestModels(t)
	h := &handler{models: m}
	ctxA := fullAccess("a")

	// Setup direkt i lagret (utan auktorisering): en global regel samt
	// en regel per tenant. Lagret är tenant-okunnigt med flit; handlers
	// upprätthåller ägarskapet som härleds ur match.tenant.
	seed := func(tenant string) rules.StoredRule {
		t.Helper()
		r := testRule()
		r.Match.Tenant = tenant
		st, err := m.store.InsertRule(r)
		if err != nil {
			t.Fatalf("seed tenant %q: %v", tenant, err)
		}
		return st
	}
	global := seed("")
	ruleA := seed("a")
	ruleB := seed("b")

	decodeList := func(t *testing.T, rr *httptest.ResponseRecorder) []Model {
		t.Helper()
		var list struct {
			Models []Model `json:"models"`
		}
		if err := json.Unmarshal(rr.Body.Bytes(), &list); err != nil {
			t.Fatalf("decode list: %v (%s)", err, rr.Body.String())
		}
		return list.Models
	}

	// Listning som A: global + A syns, B döljs.
	rr := doModelAs(h.listModels, http.MethodGet, "", nil, nil, ctxA)
	is.Equal(rr.Code, http.StatusOK)
	ids := map[string]bool{}
	for _, mdl := range decodeList(t, rr) {
		ids[mdl.ID] = true
	}
	is.True(ids[global.ID])
	is.True(ids[ruleA.ID])
	is.True(!ids[ruleB.ID])

	// Direktläsning av B:s regel som A läcker inte existens.
	rr = doModelAs(h.getModel, http.MethodGet, ruleB.ID, nil, nil, ctxA)
	is.Equal(rr.Code, http.StatusNotFound)

	// Skrivning med B-tenant i bodyn som A ger 403 — oavsett om id:t
	// finns (ingen skillnad mot okänt id: ingen existensläcka).
	rb := testRule()
	rb.Match.Tenant = "b"
	rr = doModelAs(h.updateModel, http.MethodPut, ruleB.ID, rb, map[string]string{"If-Match": `"rev-1"`}, ctxA)
	is.Equal(rr.Code, http.StatusForbidden)
	rr = doModelAs(h.updateModel, http.MethodPut, "00000000-0000-0000-0000-000000000000", rb, map[string]string{"If-Match": `"rev-1"`}, ctxA)
	is.Equal(rr.Code, http.StatusForbidden)
	// Lagrad B-regel med A-tenant i bodyn: målet är osynligt -> 404.
	ra := testRule()
	ra.Match.Tenant = "a"
	rr = doModelAs(h.updateModel, http.MethodPut, ruleB.ID, ra, map[string]string{"If-Match": `"rev-1"`}, ctxA)
	is.Equal(rr.Code, http.StatusNotFound)
	rr = doModelAs(h.deleteModel, http.MethodDelete, ruleB.ID, nil, map[string]string{"If-Match": `"rev-1"`}, ctxA)
	is.Equal(rr.Code, http.StatusNotFound)

	// Globala regler är synliga men låsta för API-skrivning (seed-ägda).
	rr = doModelAs(h.getModel, http.MethodGet, global.ID, nil, nil, ctxA)
	is.Equal(rr.Code, http.StatusOK)
	rg := testRule()
	rg.Match.Tenant = ""
	rr = doModelAs(h.updateModel, http.MethodPut, global.ID, rg, map[string]string{"If-Match": `"rev-1"`}, ctxA)
	is.Equal(rr.Code, http.StatusForbidden)
	rr = doModelAs(h.deleteModel, http.MethodDelete, global.ID, nil, map[string]string{"If-Match": `"rev-1"`}, ctxA)
	is.Equal(rr.Code, http.StatusForbidden)

	// Skapande kräver icke-tom tenant inom egna auktoriserade tenants.
	rr = doModelAs(h.createModel, http.MethodPost, "", rb, nil, ctxA)
	is.Equal(rr.Code, http.StatusForbidden)
	rg = testRule()
	rg.Match.Tenant = ""
	rr = doModelAs(h.createModel, http.MethodPost, "", rg, nil, ctxA)
	is.Equal(rr.Code, http.StatusForbidden)
	rr = doModelAs(h.createModel, http.MethodPost, "", ra, nil, ctxA)
	is.Equal(rr.Code, http.StatusCreated)

	// Tenantbyte A->B i uppdatering nekas trots att den lagrade regeln är synlig.
	rr = doModelAs(h.updateModel, http.MethodPut, ruleA.ID, rb, map[string]string{"If-Match": `"rev-1"`}, ctxA)
	is.Equal(rr.Code, http.StatusForbidden)

	// Utan access-context: 401 på läsning.
	rr = doModelAs(h.listModels, http.MethodGet, "", nil, nil, context.Background())
	is.Equal(rr.Code, http.StatusUnauthorized)
}

func TestValidateModel(t *testing.T) {
	is := is.New(t)
	m := pgTestModels(t)
	h := &handler{models: m}

	rr := doModel(h.validateModel, http.MethodPost, "", testRule(), nil)
	is.Equal(rr.Code, http.StatusOK)
	is.True(strings.Contains(rr.Body.String(), `"valid":true`))

	bad := testRule()
	bad.Match.Kind = "nope"
	rr = doModel(h.validateModel, http.MethodPost, "", bad, nil)
	is.Equal(rr.Code, http.StatusBadRequest)
}

func TestPreviewThing(t *testing.T) {
	is := is.New(t)
	m := pgTestModels(t)
	h := &handler{models: m}

	body := map[string]any{
		"rule": testRule(),
		"event": map[string]any{
			"kind": "thing",
			"message": map[string]any{
				"thing":     map[string]any{"id": "r1", "type": "Room", "name": "Rum", "tenant": "t"},
				"tenant":    "t",
				"timestamp": "2024-01-01T00:00:00Z",
			},
		},
	}
	rr := doModel(h.previewModel, http.MethodPost, "", body, nil)
	is.Equal(rr.Code, http.StatusOK)
	var out previewResponse
	is.NoErr(json.Unmarshal(rr.Body.Bytes(), &out))
	is.True(out.Matched)
	is.Equal(len(out.Entities), 1)
	is.Equal(out.Entities[0].Type, "Room")

	// Icke-matchande typ: tomt resultat, inget fel.
	body["event"].(map[string]any)["message"].(map[string]any)["thing"].(map[string]any)["type"] = "Other"
	rr = doModel(h.previewModel, http.MethodPost, "", body, nil)
	is.Equal(rr.Code, http.StatusOK)
	is.NoErr(json.Unmarshal(rr.Body.Bytes(), &out))
	is.True(!out.Matched)
	is.Equal(len(out.Entities), 0)

	// Ogiltig kind nekas.
	body["event"] = map[string]any{"kind": "nope", "message": map[string]any{}}
	rr = doModel(h.previewModel, http.MethodPost, "", body, nil)
	is.Equal(rr.Code, http.StatusBadRequest)
}

func TestPreviewMeasurement(t *testing.T) {
	is := is.New(t)
	m := pgTestModels(t)
	h := &handler{models: m}

	v := 22.5
	pack := []map[string]any{
		{"n": "dev-1/0", "vs": "urn:oma:lwm2m:ext:3303", "t": 1720000000},
		{"n": "dev-1/5700", "v": v},
		{"n": "dev-1/env", "vs": "indoors"},
		{"n": "dev-1/lat", "v": 62.36, "u": "lat"},
		{"n": "dev-1/lon", "v": 17.51, "u": "lon"},
		{"n": "dev-1/tenant", "vs": "default"},
	}
	rule := map[string]any{
		"match": map[string]any{"kind": "measurement", "object": "urn:oma:lwm2m:ext:3303", "env": "indoors"},
		"entities": []any{map[string]any{
			"id":   "urn:ngsi-ld:IndoorEnvironmentObserved:{{deviceID}}",
			"type": "IndoorEnvironmentObserved",
			"properties": []any{
				map[string]any{"target": "temperature", "type": "Number", "resource": "5700", "observedAt": "header"},
			},
		}},
	}
	body := map[string]any{
		"rule": rule,
		"event": map[string]any{
			"kind": "measurement",
			"message": map[string]any{
				"pack":      pack,
				"timestamp": "2024-06-03T00:00:00Z",
			},
			"sensorType": "st-test",
		},
	}
	rr := doModel(h.previewModel, http.MethodPost, "", body, nil)
	is.Equal(rr.Code, http.StatusOK)
	var out previewResponse
	is.NoErr(json.Unmarshal(rr.Body.Bytes(), &out))
	is.True(out.Matched)
	is.Equal(len(out.Entities), 1)
}

const muxAllowPolicy = `package example.authz
default allow := false
allow := {"tenants": ["t"]} if { input.token == "good" }
`

// Full varv genom mux + middleware + handler + lagring: fångar
// route-krockar (panik) och scope-fel vid registrering.
func TestRegisterHandlersRoundTrip(t *testing.T) {
	is := is.New(t)
	m := pgTestModels(t)

	mux := http.NewServeMux()
	policies := strings.NewReader(muxAllowPolicy)
	is.NoErr(RegisterHandlers(context.Background(), mux, m, policies))

	do := func(method, path, token string, body any) *httptest.ResponseRecorder {
		var buf bytes.Buffer
		if body != nil {
			_ = json.NewEncoder(&buf).Encode(body)
		}
		req := httptest.NewRequest(method, path, &buf)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)
		return rr
	}

	rr := do(http.MethodPost, "/api/v0/models/validate", "good", testRule())
	is.Equal(rr.Code, http.StatusOK)

	rr = do(http.MethodPost, "/api/v0/models", "good", nil)
	is.Equal(rr.Code, http.StatusBadRequest)

	rr = do(http.MethodPost, "/api/v0/models", "good", testRule())
	is.Equal(rr.Code, http.StatusCreated)
	created := decodeModel(t, rr)

	rr = do(http.MethodGet, "/api/v0/models", "good", nil)
	is.Equal(rr.Code, http.StatusOK)

	rr = do(http.MethodGet, "/api/v0/models/"+created.ID, "good", nil)
	is.Equal(rr.Code, http.StatusOK)
	is.Equal(rr.Header().Get("ETag"), `"rev-1"`)

	rr = do(http.MethodGet, "/api/v0/models", "bad", nil)
	is.Equal(rr.Code, http.StatusUnauthorized)
	rr = do(http.MethodGet, "/api/v0/models", "", nil)
	is.Equal(rr.Code, http.StatusUnauthorized)

	preview := map[string]any{
		"rule": testRule(),
		"event": map[string]any{
			"kind": "thing",
			"message": map[string]any{
				"thing":     map[string]any{"id": "r1", "type": "Room", "name": "Rum", "tenant": "t"},
				"tenant":    "t",
				"timestamp": "2024-01-01T00:00:00Z",
			},
		},
	}
	rr = do(http.MethodPost, "/api/v0/models/preview", "good", preview)
	is.Equal(rr.Code, http.StatusOK)
}
