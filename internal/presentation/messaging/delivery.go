package messaging

import (
	"context"
	"errors"
	"log/slog"

	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
	rmq "github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/tracing"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("iot-transform-fiware/messaging")

func Begin(ctx context.Context, log *slog.Logger, name string) (context.Context, *slog.Logger, func(error)) {
	ctx, span := tracer.Start(ctx, name)
	_, ctx, log = o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)
	return ctx, log, func(err error) { tracing.RecordAnyErrorAndEndSpan(err, span) }
}

// Pure transformation/validation errors cannot be repaired by re-delivery.
func DataError(err error) error { return rmq.Permanent(err) }

// Unknown dependency errors retain the locked library's temporary default.
// Permanent brokerfel (400/422/413 via PermanentBrokerError, exklusive
// NonexistentTenant) blir Permanent; 401/403/429/500/503 och
// PartialUpdateError forblir Temporary. WriteBatchError bevarar slutforda
// operationer via Unwrap/Cause - klassificeringen foljer orsaken utan att
// tappa Completed i felrapporten.
func DependencyError(err error) error {
	if contextbroker.PermanentBrokerError(err) {
		return rmq.Permanent(err)
	}
	return rmq.Temporary(err)
}

// WriteBatchProgress extraherar slutforda operationer ur ett brokerfel utan
// att andra ack/retry-kontraktet. Anvands av C2-handlers for diagnostik
// (completed/operation loggas, aldrig payloads).
func WriteBatchProgress(err error) (completed int, operation string, ok bool) {
	var batch *contextbroker.WriteBatchError
	if errors.As(err, &batch) && batch != nil {
		return batch.Completed, batch.Operation, true
	}
	return 0, "", false
}
