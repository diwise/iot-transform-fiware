package contextbroker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	ngsilderrors "github.com/diwise/context-broker/pkg/ngsild/errors"
	"github.com/diwise/context-broker/pkg/ngsild/types"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

// nullAttributeFragment är ett PATCH-fragment som rensar namngivna attribut
// via NGSI-LD-null. Bibliotekets fragment-byggare tappar null-värden vid
// avkodning, så fragmentet byggs här direkt; MergeEntity använder endast
// MarshalJSON.
type nullAttributeFragment struct {
	context []string
	attrs   []string
}

func (f nullAttributeFragment) ForEachAttribute(func(attributeType, attributeName string, contents any)) error {
	return nil
}

func (f nullAttributeFragment) MarshalJSON() ([]byte, error) {
	m := make(map[string]any, len(f.attrs)+1)
	for _, a := range f.attrs {
		m[a] = nil
	}
	if len(f.context) > 0 {
		m["@context"] = f.context
	}
	return json.Marshal(m)
}

var _ types.EntityFragment = nullAttributeFragment{}

// ApplyEntityWrites exekverar entitetsskrivningar i listordning med delad
// semantik för alla presentationsvägar (things, values, measurements):
// create (redan-existerar är OK), merge-or-create och removeAttributes
// (PATCH-null, saknad entitet är OK; delete tillkommer med lifecycle).
// Tillfälliga fel propageras för retry; anroparen klassificerar
// irreparabla fel.
func ApplyEntityWrites(ctx context.Context, cbClient client.ContextBrokerClient, writes []transform.EntityWrite, defaultContext string) error {
	log := logging.GetFromContext(ctx)

	for _, w := range writes {
		entityContext := w.Context
		if entityContext == "" {
			entityContext = defaultContext
		}

		if w.Create {
			err := CreateNewEntityWithContext(ctx, cbClient, w.EntityID, w.TypeName, entityContext, w.Props)
			if err != nil && !errors.Is(err, ErrEntityAlreadyExists) {
				return fmt.Errorf("failed to create entity %s: %w", w.EntityID, err)
			}
			continue
		}

		if len(w.RemoveAttributes) > 0 {
			if err := removeAttributes(ctx, cbClient, w.EntityID, entityContext, w.RemoveAttributes); err != nil {
				return fmt.Errorf("failed to remove attributes from entity %s: %w", w.EntityID, err)
			}
			continue
		}

		if err := MergeOrCreateWithContext(ctx, cbClient, w.EntityID, w.TypeName, entityContext, w.Props); err != nil {
			return fmt.Errorf("failed to merge or create entity %s: %w", w.EntityID, err)
		}
	}

	log.Debug("entity writes applied", "count", len(writes))
	return nil
}

// removeAttributes rensar namngivna attribut med ett null-fragment.
// Saknad entitet är framgång (inget att rensa) för idempotent retry.
func removeAttributes(ctx context.Context, cbClient client.ContextBrokerClient, id, contextURL string, attrs []string) error {
	var context []string
	if contextURL != "" {
		context = []string{contextURL}
	}
	_, err := cbClient.MergeEntity(ctx, id,
		nullAttributeFragment{context: context, attrs: append([]string(nil), attrs...)},
		map[string][]string{"Content-Type": {"application/ld+json"}})
	if err != nil {
		if errors.Is(err, ngsilderrors.ErrNotFound) {
			return nil
		}
		return err
	}
	return nil
}
