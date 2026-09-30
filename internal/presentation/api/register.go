package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/diwise/iot-transform-fiware/internal/application/measurements"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/rules"
	"github.com/diwise/iot-transform-fiware/internal/presentation/api/auth"
	"github.com/diwise/service-chassis/pkg/infrastructure/net/http/router"
	"github.com/google/uuid"
)

// Scopes för regel-API:t (transforms.*-namnrymden; create läggs till i
// compose-policyn, read/update/delete finns där).
const (
	TransformRead   auth.Scope = "transforms.read"
	TransformCreate auth.Scope = "transforms.create"
	TransformUpdate auth.Scope = "transforms.update"
	TransformDelete auth.Scope = "transforms.delete"
	// TransformWrite är avtalat scope för torra författarverktyg
	// (validate/preview): ingen persistens, ingen tenant-påverkan.
	// Separat från create så att läs+skriv-rätt inte ger skapanderätt
	// (TF-07). Endpointspecifik middleware, inte grupp-scope.
	TransformWrite auth.Scope = "transforms.write"
)

type registerOptions struct {
	authOptions []auth.Option
}

// RegisterOption configures API registration.
type RegisterOption func(*registerOptions)

// WithAccessObjectAuthorization toggles the OPA access-object result model.
func WithAccessObjectAuthorization(enabled bool) RegisterOption {
	return func(o *registerOptions) {
		o.authOptions = append(o.authOptions, auth.WithAccessObjectAuthorization(enabled))
	}
}

// Models är regelhanteringen för det publika API:t: CRUD mot regel­lagret
// plus atomisk omladdning av körningsmotorerna. Mutexen serialiserar
// samtidiga skrivningar så att DB-skrivning och omladdning alltid hör ihop;
// krasch emellan läks vid omstart (snapshot byggs från DB).
type Models struct {
	mu       sync.Mutex
	store    RuleStore
	resolver measurements.SensorTypeResolver
	snap     atomic.Pointer[Snapshot]
}

func NewModels(store RuleStore, resolver measurements.SensorTypeResolver) *Models {
	m := &Models{store: store, resolver: resolver}
	empty := buildSnapshot(nil, resolver)
	empty.Version = -1 // oladdad: första SyncOnce laddar alltid om
	m.snap.Store(empty)
	return m
}

// Model är API-kuvertet för en regel: identitet/livscykel utanför regeln
// (id finns aldrig i YAML).
type Model struct {
	ID       string         `json:"id"`
	Revision int64          `json:"revision"`
	Source   string         `json:"source"`
	SeedKey  string         `json:"seedKey,omitempty"`
	Kind     string         `json:"kind"`
	Rule     transform.Rule `json:"rule"`
}

func toModel(st rules.StoredRule) Model {
	return Model{
		ID: st.ID, Revision: st.Revision, Source: st.Source,
		SeedKey: st.SeedKey, Kind: st.Kind, Rule: st.Rule,
	}
}

// RegisterHandlers wires the public models API onto mux: router with
// /api/v0 prefix, OPA auth groups per scope.
func RegisterHandlers(ctx context.Context, mux *http.ServeMux, models *Models, policies io.Reader, opts ...RegisterOption) error {
	const apiPrefix string = "/api/v0"

	registerOpts := registerOptions{}
	for _, apply := range opts {
		apply(&registerOpts)
	}

	authz, err := auth.NewAuthenticator(ctx, policies, registerOpts.authOptions...)
	if err != nil {
		return fmt.Errorf("failed to create api authenticator: %w", err)
	}

	h := &handler{models: models}

	r := router.New(mux, router.WithPrefix(apiPrefix))

	r.Route("models", func(r router.ServeMux) {
		r.Group(func(r router.ServeMux) {
			r.Use(authz.RequireAccess(TransformRead))
			r.Get("", h.listModels)
			r.Get("{id}", h.getModel)
		})
		// Endpointspecifik middleware (TF-07): två POST-grupper under samma
		// prefix panikkar muxen, så grupp-scope kan inte uttrycka avtalade
		// scopes (create för POST "", write för validate/preview). Varje
		// POST lindas därför med sitt eget scope här; handlers verifierar
		// dessutom samma scope via authorizedTenants så att direktanrop
		// aldrig kringgår tenant-scoping.
		r.Post("", withAccess(authz, TransformCreate, h.createModel))
		r.Post("validate", withAccess(authz, TransformWrite, h.validateModel))
		r.Post("preview", withAccess(authz, TransformWrite, h.previewModel))
		r.Group(func(r router.ServeMux) {
			r.Use(authz.RequireAccess(TransformUpdate))
			r.Put("{id}", h.updateModel)
		})
		r.Group(func(r router.ServeMux) {
			r.Use(authz.RequireAccess(TransformDelete))
			r.Delete("{id}", h.deleteModel)
		})
	})

	return nil
}

// withAccess lindar en handler med endpointspecifikt scope (TF-07).
// Grupp-scope kan inte skilja POST "" (create) från POST validate/preview
// (write) utan mux-panik; därför bär varje POST sitt eget scope här.
func withAccess(authz auth.Enticator, scope auth.Scope, h func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
	mw := authz.RequireAccess(scope)
	return func(w http.ResponseWriter, r *http.Request) {
		mw(http.HandlerFunc(h)).ServeHTTP(w, r)
	}
}

type handler struct {
	models *Models
}

var errValidation = errors.New("invalid request")

// errUnauthorized signals a missing or empty access context. The OPA
// middleware normally rejects such requests first; handlers check again
// so direct calls can never bypass tenant scoping.
var errUnauthorized = errors.New("unauthorized")

// errForbidden signals an authenticated request that must not affect the
// target: a rule owned by another tenant, or a global rule. Global rules
// (empty match.tenant) are administered via seed files, never via API.
var errForbidden = errors.New("forbidden")

// authorizedTenants resolves the tenants the caller holds scope for.
// Ownership of a rule is derived from its match.tenant: a rule is visible
// and mutable only within its owner tenant set. Empty match.tenant means
// a global (seed-owned) rule: visible to all readers, mutable by none
// via the API.
func authorizedTenants(r *http.Request, scope auth.Scope) ([]string, error) {
	authorized := auth.GetTenantsWithAllowedScopes(r.Context(), scope)
	if len(authorized) == 0 {
		return nil, errUnauthorized
	}
	return authorized, nil
}

// ruleVisible reports whether a stored rule may be disclosed to a caller
// holding the given tenants: global rules are shared configuration,
// tenant rules require ownership.
func ruleVisible(rule transform.Rule, authorized []string) bool {
	return rule.Match.Tenant == "" || slices.Contains(authorized, rule.Match.Tenant)
}

// requireRuleTenant enforces write ownership: the rule must declare a
// non-empty tenant that the caller is authorized for. Global rules are
// seed-administered and rejected here.
func requireRuleTenant(rule transform.Rule, authorized []string) error {
	if rule.Match.Tenant == "" {
		return fmt.Errorf("%w: global rules are administered via seed files", errForbidden)
	}
	if !slices.Contains(authorized, rule.Match.Tenant) {
		return fmt.Errorf("%w: rule tenant %q is outside authorized tenants", errForbidden, rule.Match.Tenant)
	}
	return nil
}

func writeAPIError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errValidation):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, errUnauthorized):
		writeError(w, http.StatusUnauthorized, "unauthorized")
	case errors.Is(err, errForbidden):
		writeError(w, http.StatusForbidden, err.Error())
	case errors.Is(err, rules.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, rules.ErrConflict):
		writeError(w, http.StatusConflict, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// decodeJSONBody avkodar exakt ett JSON-dokument strikt (TF-07):
// mediatypen parsas (application/json-invalid accepteras inte), okända fält
// nekas och efterföljande data (trailing JSON) nekas via EOF-kontroll.
func decodeJSONBody(r *http.Request, dst any) error {
	ct := r.Header.Get("Content-Type")
	mt, _, err := mime.ParseMediaType(ct)
	if err != nil || mt != "application/json" {
		return fmt.Errorf("%w: content-type must be application/json", errValidation)
	}
	if r.Body == nil {
		return fmt.Errorf("%w: empty body", errValidation)
	}
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("%w: invalid JSON body: %v", errValidation, err)
	}
	// Exakt ett dokument: trailing JSON/whitespace-bara? Andra Decode måste
	// ge io.EOF, annars finns extra data efter första dokumentet.
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("%w: trailing data after JSON document", errValidation)
		}
		return fmt.Errorf("%w: invalid JSON trailing data: %v", errValidation, err)
	}
	return nil
}

// decodeRule avkodar ett regelobjekt strikt: fel Content-Type, ogiltig JSON,
// trailing data och okända fält (stavfel) nekas i stället för att tyst tappas.
func decodeRule(r *http.Request) (transform.Rule, error) {
	var rule transform.Rule
	if err := decodeJSONBody(r, &rule); err != nil {
		return rule, err
	}
	if err := transform.Validate([]transform.Rule{rule}); err != nil {
		return rule, fmt.Errorf("%w: %v", errValidation, err)
	}
	return rule, nil
}

// requireModelID validerar sökvägens id som UUID före lagret (TF-07):
// ogiltigt UUID ger 400, inte 500 från PostgreSQL.
func requireModelID(r *http.Request) (string, error) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		return "", fmt.Errorf("%w: invalid id %q: must be UUID", errValidation, id)
	}
	return id, nil
}

// formatETag/parseETag speglar iot-things-v2 ("rev-<n>" med eller utan citattecken).
func formatETag(revision int64) string {
	return fmt.Sprintf(`"rev-%d"`, revision)
}

func parseETag(v string) (int64, error) {
	s := strings.TrimSpace(v)
	s = strings.TrimPrefix(s, "W/")
	s = strings.Trim(s, `"`)
	s = strings.TrimSpace(s)
	n, ok := strings.CutPrefix(s, "rev-")
	if !ok {
		return 0, fmt.Errorf("invalid ETag %q: want rev-<n>", v)
	}
	rev, err := strconv.ParseInt(n, 10, 64)
	if err != nil || rev < 0 {
		return 0, fmt.Errorf("invalid ETag %q: want rev-<n>", v)
	}
	return rev, nil
}

func requireIfMatch(r *http.Request) (int64, error) {
	raw := r.Header.Get("If-Match")
	if raw == "" {
		return 0, fmt.Errorf("%w: missing If-Match header", errValidation)
	}
	rev, err := parseETag(raw)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid If-Match header: %v", errValidation, err)
	}
	return rev, nil
}

// mutate serialiserar skrivning + omladdning: DB-skrivning, sedan rebuild
// av snapshoten. Misslyckad omladdning efter committad skrivning ger 500;
// nästa synk läker (snapshot byggs alltid från DB).
func (m *Models) mutate(fn func() (StoredResult, error)) (StoredResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	res, err := fn()
	if err != nil {
		return StoredResult{}, err
	}
	if err := m.Refresh(); err != nil {
		return StoredResult{}, err
	}
	return res, nil
}

// StoredResult bär en lagrad regel genom mutate.
type StoredResult struct {
	Rule rules.StoredRule
}

func (h *handler) listModels(w http.ResponseWriter, r *http.Request) {
	authorized, err := authorizedTenants(r, TransformRead)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	stored, err := h.models.store.ListRules()
	if err != nil {
		writeAPIError(w, err)
		return
	}
	f := parseListFilters(r)
	out := make([]Model, 0, len(stored))
	for _, st := range stored {
		if !ruleVisible(st.Rule, authorized) {
			continue
		}
		if !f.matches(st.Rule, st.Source) {
			continue
		}
		out = append(out, toModel(st))
	}
	writeJSON(w, http.StatusOK, map[string][]Model{"models": out})
}

// listFilters är de valfria, additiva sökfiltren på GET /models (PLAN002
// 1.1): alla omitempty, kombineras med AND. Okända värden ger tom lista,
// aldrig 400. Precedensordningen från lagret är oförändrad.
type listFilters struct {
	Kind   string
	Event  string
	Type   string
	Tenant string
	Source string
}

func parseListFilters(r *http.Request) listFilters {
	q := r.URL.Query()
	return listFilters{
		Kind:   q.Get("kind"),
		Event:  q.Get("event"),
		Type:   q.Get("type"),
		Tenant: q.Get("tenant"),
		Source: q.Get("source"),
	}
}

func (f listFilters) matches(rule transform.Rule, source string) bool {
	if f.Kind != "" && string(rule.Match.Kind) != f.Kind {
		return false
	}
	if f.Event != "" && rule.Match.Event != f.Event {
		return false
	}
	if f.Type != "" && rule.Match.Type != f.Type {
		return false
	}
	if f.Tenant != "" && rule.Match.Tenant != f.Tenant {
		return false
	}
	if f.Source != "" && source != f.Source {
		return false
	}
	return true
}

func (h *handler) getModel(w http.ResponseWriter, r *http.Request) {
	authorized, err := authorizedTenants(r, TransformRead)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	id, err := requireModelID(r)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	st, err := h.models.store.GetRule(id)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	if !ruleVisible(st.Rule, authorized) {
		writeAPIError(w, fmt.Errorf("%w: %s", rules.ErrNotFound, st.ID))
		return
	}
	m := toModel(st)
	w.Header().Set("ETag", formatETag(m.Revision))
	writeJSON(w, http.StatusOK, m)
}

func (h *handler) createModel(w http.ResponseWriter, r *http.Request) {
	rule, err := decodeRule(r)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	authorized, err := authorizedTenants(r, TransformCreate)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	if err := requireRuleTenant(rule, authorized); err != nil {
		writeAPIError(w, err)
		return
	}
	res, err := h.models.mutate(func() (StoredResult, error) {
		st, err := h.models.store.InsertRule(rule)
		if err != nil {
			return StoredResult{}, err
		}
		return StoredResult{Rule: st}, nil
	})
	if err != nil {
		writeAPIError(w, err)
		return
	}
	m := toModel(res.Rule)
	w.Header().Set("ETag", formatETag(m.Revision))
	writeJSON(w, http.StatusCreated, m)
}

func (h *handler) updateModel(w http.ResponseWriter, r *http.Request) {
	rule, err := decodeRule(r)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	rev, err := requireIfMatch(r)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	authorized, err := authorizedTenants(r, TransformUpdate)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	if err := requireRuleTenant(rule, authorized); err != nil {
		writeAPIError(w, err)
		return
	}
	id, err := requireModelID(r)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	res, err := h.models.mutate(func() (StoredResult, error) {
		st, err := h.models.store.GetRule(id)
		if err != nil {
			return StoredResult{}, err
		}
		if !ruleVisible(st.Rule, authorized) {
			return StoredResult{}, fmt.Errorf("%w: %s", rules.ErrNotFound, id)
		}
		if err := requireRuleTenant(st.Rule, authorized); err != nil {
			return StoredResult{}, err
		}
		// Både gammalt och nytt ägarskap är kontrollerat. SQL kräver
		// samma revision, så en samtidig ändring kan inte ersätta den
		// kontrollerade regeln före skrivningen.
		st, err = h.models.store.UpdateRule(id, rule, rev)
		if err != nil {
			return StoredResult{}, err
		}
		return StoredResult{Rule: st}, nil
	})
	if err != nil {
		writeAPIError(w, err)
		return
	}
	m := toModel(res.Rule)
	w.Header().Set("ETag", formatETag(m.Revision))
	writeJSON(w, http.StatusOK, m)
}

func (h *handler) deleteModel(w http.ResponseWriter, r *http.Request) {
	rev, err := requireIfMatch(r)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	authorized, err := authorizedTenants(r, TransformDelete)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	id, err := requireModelID(r)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	_, err = h.models.mutate(func() (StoredResult, error) {
		st, err := h.models.store.GetRule(id)
		if err != nil {
			return StoredResult{}, err
		}
		if !ruleVisible(st.Rule, authorized) {
			return StoredResult{}, fmt.Errorf("%w: %s", rules.ErrNotFound, id)
		}
		if err := requireRuleTenant(st.Rule, authorized); err != nil {
			return StoredResult{}, err
		}
		// Revisionen jämförs atomärt i SQL (TF-04): ingen TOCTOU-glipa
		// mellan läsning och radering, även mellan instanser.
		if err := h.models.store.DeleteRule(id, rev); err != nil {
			return StoredResult{}, err
		}
		return StoredResult{Rule: st}, nil
	})
	if err != nil {
		writeAPIError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
