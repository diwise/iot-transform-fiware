package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/matryer/is"
)

const legacyPolicy = `package example.authz
default allow := false
allow := {"tenants": ["t1", "t2"]} if { input.token == "good" }
`

const accessObjectPolicy = `package example.authz
default allow := false
allow := {"tenants": ["t1", "t2"], "access": {
	"t1": ["transforms.read", "transforms.update"],
	"t2": ["transforms.read"],
}} if { input.token == "good" }
`

const denyPolicy = `package example.authz
default allow := false
`

func doAuth(ent Enticator, scopes []Scope, token string) (int, context.Context) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	req := httptest.NewRequest(http.MethodGet, "/api/v0/models", nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rr := httptest.NewRecorder()
	h := ent.RequireAccess(scopes...)
	var ctx context.Context
	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx = r.Context()
		next.ServeHTTP(w, r)
	})
	h(wrapped).ServeHTTP(rr, req)
	return rr.Code, ctx
}

// Legacy-modellen: giltig token ger begärt scope i policyns tenants.
func TestRequireAccessLegacyTenants(t *testing.T) {
	is := is.New(t)
	ent, err := NewAuthenticator(context.Background(), strings.NewReader(legacyPolicy))
	is.NoErr(err)

	code, ctx := doAuth(ent, []Scope{"transforms.read"}, "good")
	is.Equal(code, http.StatusTeapot)
	got := GetTenantsWithAllowedScopes(ctx, "transforms.read")
	sort.Strings(got)
	is.Equal(got, []string{"t1", "t2"})

	code, _ = doAuth(ent, []Scope{"transforms.read"}, "bad")
	is.Equal(code, http.StatusUnauthorized)
	code, _ = doAuth(ent, []Scope{"transforms.read"}, "")
	is.Equal(code, http.StatusUnauthorized)
}

// Access-objekt-modellen: scope filtreras per tenant mot policyns access.
func TestRequireAccessObjectScopes(t *testing.T) {
	is := is.New(t)
	ent, err := NewAuthenticator(context.Background(), strings.NewReader(accessObjectPolicy), WithAccessObjectAuthorization(true))
	is.NoErr(err)

	code, ctx := doAuth(ent, []Scope{"transforms.read"}, "good")
	is.Equal(code, http.StatusTeapot)
	got := GetTenantsWithAllowedScopes(ctx, "transforms.read")
	sort.Strings(got)
	is.Equal(got, []string{"t1", "t2"})

	// update finns bara i t1: t2 filtreras bort.
	code, ctx = doAuth(ent, []Scope{"transforms.update"}, "good")
	is.Equal(code, http.StatusTeapot)
	is.Equal(GetTenantsWithAllowedScopes(ctx, "transforms.update"), []string{"t1"})

	// delete finns ingenstans: 401.
	code, _ = doAuth(ent, []Scope{"transforms.delete"}, "good")
	is.Equal(code, http.StatusUnauthorized)
}

// Ren bool-falsk policy nekar; access-läge mot legacy-policy felar högljutt.
func TestRequireAccessDenyAndMisconfig(t *testing.T) {
	is := is.New(t)
	ent, err := NewAuthenticator(context.Background(), strings.NewReader(denyPolicy))
	is.NoErr(err)
	code, _ := doAuth(ent, []Scope{"transforms.read"}, "good")
	is.Equal(code, http.StatusUnauthorized)

	ent, err = NewAuthenticator(context.Background(), strings.NewReader(legacyPolicy), WithAccessObjectAuthorization(true))
	is.NoErr(err)
	code, _ = doAuth(ent, []Scope{"transforms.read"}, "good")
	is.Equal(code, http.StatusInternalServerError)
}
