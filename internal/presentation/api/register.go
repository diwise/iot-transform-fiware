package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/diwise/iot-transform-fiware/internal/application/measurements"
	"github.com/diwise/iot-transform-fiware/internal/application/things"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/rules"
	"github.com/diwise/iot-transform-fiware/internal/presentation/api/auth"
	"github.com/diwise/service-chassis/pkg/infrastructure/net/http/router"
)

// Scopes för regel-API:t (transforms.*-namnrymden; create läggs till i
// compose-policyn, read/update/delete finns där).
const (
	TransformRead   auth.Scope = "transforms.read"
	TransformCreate auth.Scope = "transforms.create"
	TransformUpdate auth.Scope = "transforms.update"
	TransformDelete auth.Scope = "transforms.delete"
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
	mu           sync.Mutex
	store        *rules.PgStore
	things       *things.DynamicThingTransformer
	measurements *measurements.DynamicTransformer
}

func NewModels(store *rules.PgStore, things *things.DynamicThingTransformer, measurements *measurements.DynamicTransformer) *Models {
	return &Models{store: store, things: things, measurements: measurements}
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
		r.Group(func(r router.ServeMux) {
			r.Use(authz.RequireAccess(TransformCreate))
			r.Post("", h.createModel)
			// Samma grupp+scope av routertekniska skäl: två POST-grupper
			// under samma prefix panikkar muxen. Validate/preview är
			// författarverktyg (torra, utan persistens) och hör till
			// create-scopet; därför behövs ingen separat transforms.write.
			r.Post("validate", h.validateModel)
			r.Post("preview", h.previewModel)
		})
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

// decodeRule avkodar ett regelobjekt strikt: fel Content-Type, ogiltig JSON
// och okända fält (stavfel) nekas i stället för att tyst tappas.
func decodeRule(r *http.Request) (transform.Rule, error) {
	var rule transform.Rule
	if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		return rule, fmt.Errorf("%w: content-type must be application/json", errValidation)
	}
	if r.Body == nil {
		return rule, fmt.Errorf("%w: empty body", errValidation)
	}
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&rule); err != nil {
		return rule, fmt.Errorf("%w: invalid rule body: %v", errValidation, err)
	}
	if err := transform.Validate([]transform.Rule{rule}); err != nil {
		return rule, fmt.Errorf("%w: %v", errValidation, err)
	}
	return rule, nil
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

// reload bygger om körningsmotorerna från lagret. Anropas under skrivlås
// efter varje lyckad mutation; samtidiga meddelanden ser alltid en komplett
// gammal eller ny uppsättning.
func (m *Models) reload() error {
	stored, err := m.store.ListRules()
	if err != nil {
		return err
	}
	rs := make([]transform.Rule, 0, len(stored))
	for _, st := range stored {
		rs = append(rs, st.Rule)
	}
	m.things.Reload(rs)
	m.measurements.Reload(rs)
	return nil
}

// mutate serialiserar skrivning + omladdning: DB-skrivning, sedan rebuild.
// Misslyckad omladdning efter committad skrivning ger 500; nästa mutation
// eller omstart läker (snapshot byggs alltid från DB).
func (m *Models) mutate(fn func() (StoredResult, error)) (StoredResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	res, err := fn()
	if err != nil {
		return StoredResult{}, err
	}
	if err := m.reload(); err != nil {
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
	out := make([]Model, 0, len(stored))
	for _, st := range stored {
		if !ruleVisible(st.Rule, authorized) {
			continue
		}
		out = append(out, toModel(st))
	}
	writeJSON(w, http.StatusOK, map[string][]Model{"models": out})
}

func (h *handler) getModel(w http.ResponseWriter, r *http.Request) {
	authorized, err := authorizedTenants(r, TransformRead)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	st, err := h.models.store.GetRule(r.PathValue("id"))
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
	id := r.PathValue("id")
	res, err := h.models.mutate(func() (StoredResult, error) {
		st, err := h.models.store.GetRule(id)
		if err != nil {
			return StoredResult{}, err
		}
		if !ruleVisible(st.Rule, authorized) {
			return StoredResult{}, fmt.Errorf("%w: %s", rules.ErrNotFound, id)
		}
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
	id := r.PathValue("id")
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
		if st.Revision != rev {
			return StoredResult{}, fmt.Errorf("%w: %s has revision %d, request had %d", rules.ErrConflict, id, st.Revision, rev)
		}
		ok, err := h.models.store.DeleteRule(id)
		if err != nil {
			return StoredResult{}, err
		}
		if !ok {
			return StoredResult{}, fmt.Errorf("%w: %s", rules.ErrNotFound, id)
		}
		return StoredResult{Rule: st}, nil
	})
	if err != nil {
		writeAPIError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
