package contextbroker

import (
	"context"
	"strings"
	"testing"

	"github.com/diwise/context-broker/pkg/ngsild"
	"github.com/diwise/context-broker/pkg/ngsild/client"
	ngsilderrors "github.com/diwise/context-broker/pkg/ngsild/errors"
	"github.com/diwise/context-broker/pkg/ngsild/types"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities/decorators"
	"github.com/matryer/is"
)

type fakeClient struct {
	client.ContextBrokerClient
	created []types.Entity
}

func (f *fakeClient) CreateEntity(_ context.Context, e types.Entity, _ map[string][]string) (*ngsild.CreateEntityResult, error) {
	f.created = append(f.created, e)
	return &ngsild.CreateEntityResult{}, nil
}

func (f *fakeClient) MergeEntity(_ context.Context, _ string, _ types.EntityFragment, _ map[string][]string) (*ngsild.MergeEntityResult, error) {
	return nil, ngsilderrors.ErrNotFound
}

func TestCreateUsesConfiguredContext(t *testing.T) {
	is := is.New(t)

	fake := &fakeClient{}
	custom := "https://example.invalid/context.jsonld"

	err := MergeOrCreateWithContext(context.Background(), fake, "urn:ngsi-ld:Thing:x", "Thing", custom,
		[]entities.EntityDecoratorFunc{decorators.Text("name", "x")})
	is.NoErr(err)
	is.Equal(len(fake.created), 1)

	b, err := fake.created[0].MarshalJSON()
	is.NoErr(err)
	is.True(strings.Contains(string(b), custom))
	is.True(!strings.Contains(string(b), entities.DefaultContextURL))
}

func TestCreateUsesDefaultContextWhenUnset(t *testing.T) {
	is := is.New(t)

	fake := &fakeClient{}

	err := MergeOrCreateWithContext(context.Background(), fake, "urn:ngsi-ld:Thing:y", "Thing", "",
		[]entities.EntityDecoratorFunc{decorators.Text("name", "y")})
	is.NoErr(err)
	is.Equal(len(fake.created), 1)

	b, err := fake.created[0].MarshalJSON()
	is.NoErr(err)
	is.True(strings.Contains(string(b), entities.DefaultContextURL))
}
