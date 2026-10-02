package messaging

import (
	"context"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
	rmq "github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/tracing"
	"go.opentelemetry.io/otel"
	"log/slog"
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
func DependencyError(err error) error {
	if contextbroker.PermanentBrokerError(err) {
		return rmq.Permanent(err)
	}
	return rmq.Temporary(err)
}
