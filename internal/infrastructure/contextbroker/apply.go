package contextbroker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	ngsilderrors "github.com/diwise/context-broker/pkg/ngsild/errors"
	"github.com/diwise/context-broker/pkg/ngsild/types"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"go.opentelemetry.io/otel/attribute"
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
func ApplyEntityWrites(ctx context.Context, cbClient BrokerClient, writes []transform.EntityWrite, defaultContext string) (err error) {
	ctx, span := tracer.Start(ctx, "apply-writes")
	defer func() { endSpan(err, span) }()
	log := logging.GetFromContext(ctx)
	if err := transform.ValidateEntityWrites(writes, defaultContext); err != nil {
		return err
	}

	for index, w := range writes {
		span.SetAttributes(attribute.String("rule_id", w.RuleID), attribute.Int64("rule_revision", w.RuleRevision), attribute.Int64("snapshot_version", w.SnapshotVersion), attribute.String("entity_id", w.EntityID))
		log = logging.GetFromContext(ctx).With("rule_id", w.RuleID, "rule_revision", w.RuleRevision, "snapshot_version", w.SnapshotVersion, "entity_id", w.EntityID)
		operationCtx := logging.NewContextWithLogger(ctx, log)
		entityContext := w.Context
		if entityContext == "" {
			entityContext = defaultContext
		}

		if w.Create {
			err := CreateNewEntityWithContext(operationCtx, cbClient, w.EntityID, w.TypeName, entityContext, w.Props)
			if err != nil && !errors.Is(err, ErrEntityAlreadyExists) {
				return &WriteBatchError{Completed: index, Write: w, Operation: "create", Cause: err}
			}
			continue
		}

		if w.Delete {
			if err := deleteEntity(operationCtx, cbClient, w.EntityID); err != nil {
				return &WriteBatchError{Completed: index, Write: w, Operation: "delete", Cause: err}
			}
			continue
		}

		if len(w.RemoveAttributes) > 0 {
			if err := removeAttributes(operationCtx, cbClient, w.EntityID, entityContext, w.RemoveAttributes); err != nil {
				return &WriteBatchError{Completed: index, Write: w, Operation: "remove", Cause: err}
			}
			continue
		}

		if err := MergeOrCreateWithContext(operationCtx, cbClient, w.EntityID, w.TypeName, entityContext, w.Props); err != nil {
			return &WriteBatchError{Completed: index, Write: w, Operation: "merge", Cause: err}
		}
	}

	log.Debug("entity writes applied", "count", len(writes))
	return nil
}

// removeAttributes rensar namngivna attribut med ett null-fragment.
// Saknad entitet är framgång (inget att rensa) för idempotent retry.
func removeAttributes(ctx context.Context, cbClient BrokerClient, id, contextURL string, attrs []string) error {
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

// deleteEntity raderar en entitet. Saknad entitet är framgång (inget att
// radera) för idempotent retry.
func deleteEntity(ctx context.Context, cbClient BrokerClient, id string) error {
	_, err := cbClient.DeleteEntity(ctx, id)
	if err != nil {
		if errors.Is(err, ngsilderrors.ErrNotFound) {
			return nil
		}
		return err
	}
	return nil
}

type WriteBatchError struct {
	Completed int
	Write     transform.EntityWrite
	Operation string
	Cause     error
}

func (e *WriteBatchError) Error() string {
	return fmt.Sprintf("broker %s failed at entity %s after %d completed writes: %v", e.Operation, e.Write.EntityID, e.Completed, e.Cause)
}
func (e *WriteBatchError) Unwrap() error { return e.Cause }
