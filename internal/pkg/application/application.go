package application

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	iotCore "github.com/diwise/iot-core/pkg/messaging/events"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/functions"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/measurements"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/registry"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

func NewMeasurementTopicMessageHandler(messenger messaging.MsgContext, getClientForTenant func(string) client.ContextBrokerClient) messaging.TopicMessageHandler {
	transformerRegistry := registry.NewTransformerRegistry()

	return func(ctx context.Context, msg messaging.IncomingTopicMessage, logger *slog.Logger) {
		messageAccepted := iotCore.MessageAccepted{}

		err := json.Unmarshal(msg.Body(), &messageAccepted)
		if err != nil {
			logger.Error("unable to unmarshal incoming message", "err", err.Error())
			return
		}

		measurementType := measurements.GetMeasurementType(messageAccepted)

		logger = logger.With(
			slog.String("measurement_type", measurementType),
			slog.String("device_id", messageAccepted.DeviceID()),
		)
		ctx = logging.NewContextWithLogger(ctx, logger)

		transformer := transformerRegistry.GetTransformerForMeasurement(ctx, measurementType)
		if transformer == nil {
			logger.Debug("transformer not found", "device_id", messageAccepted.DeviceID(), "measurement_type", measurementType)
			return
		}

		cbClient := getClientForTenant(messageAccepted.Tenant())
		err = transformer(ctx, messageAccepted, cbClient)
		if err != nil {
			logger.Error("transform failed", "err", err.Error())
			return
		}
	}
}

func NewFunctionUpdatedTopicMessageHandler(messenger messaging.MsgContext, getClientForTenant func(string) client.ContextBrokerClient) messaging.TopicMessageHandler {
	transformerRegistry := registry.NewTransformerRegistry()

	return func(ctx context.Context, msg messaging.IncomingTopicMessage, logger *slog.Logger) {
		fn := functions.Func{}

		err := json.Unmarshal(msg.Body(), &fn)
		if err != nil {
			logger.Error("failed to unmarshal message body", "err", err.Error())
			return
		}

		logger = logger.With(slog.String("function_type", fmt.Sprintf("%s:%s", fn.Type, fn.SubType)), slog.String("function_id", fn.ID))

		ctx = logging.NewContextWithLogger(ctx, logger)

		if fn.Timestamp.IsZero() {
			fn.Timestamp = time.Now().UTC()
		}

		transformer := transformerRegistry.GetTransformerForFunction(ctx, fn.Type)
		if transformer == nil {
			logger.Debug("transformer not found")
			return
		}

		cbClient := getClientForTenant(fn.Tenant)
		err = transformer(ctx, fn, cbClient)
		if err != nil {
			logger.Error("transform failed", "err", err.Error())
			return
		}
	}
}

func NewCipFunctionUpdatedTopicMessageHandler(messenger messaging.MsgContext, getClientForTenant func(string) client.ContextBrokerClient) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) {
		log := logging.GetFromContext(ctx)
		var handler messaging.TopicMessageHandler

		contentType := strings.ToLower(itm.ContentType())
		log.Debug(fmt.Sprintf("cip-function.updated handler for contentType %s", contentType))

		switch contentType {
		case "application/vnd.diwise.sewagepumpingstation+json":
			handler = NewSewagePumpingStationHandler(messenger, getClientForTenant)
		case "application/vnd.diwise.wastecontainer+json":
			handler = NewWasteContainerHandler(messenger, getClientForTenant)
		case "application/vnd.diwise.sewer+json":
			handler = NewSewerHandler(messenger, getClientForTenant)
		case "application/vnd.diwise.combinedsewageoverflow+json":
			handler = NewCombinedSewageOverflowHandler(messenger, getClientForTenant)
		default:
			log.Debug(fmt.Sprintf("no handler found for %s", contentType))
			return
		}

		handler(ctx, itm, l)
	}
}
func NewSewagePumpingStationHandler(messenger messaging.MsgContext, getClientForTenant func(string) client.ContextBrokerClient) messaging.TopicMessageHandler {
	return func(ctx context.Context, msg messaging.IncomingTopicMessage, logger *slog.Logger) {
		logger = logger.With(
			slog.String("function_type", "sewagepumpingstation"),
		)
		ctx = logging.NewContextWithLogger(ctx, logger)

		logger.Debug("handling message")

		tenant := struct {
			Tenant string `json:"tenant"`
		}{}
		err := json.Unmarshal(msg.Body(), &tenant)
		if err != nil {
			logger.Error("failed to retrieve tenant from message body")
		}

		cbClient := getClientForTenant(tenant.Tenant)

		err = functions.SewagePumpingStation(ctx, msg, cbClient)
		if err != nil {
			logger.Error("transform failed", "err", err.Error())
			return
		}
	}
}

func NewWasteContainerHandler(messenger messaging.MsgContext, getClientForTenant func(string) client.ContextBrokerClient) messaging.TopicMessageHandler {
	return func(ctx context.Context, msg messaging.IncomingTopicMessage, logger *slog.Logger) {
		logger = logger.With(slog.String("function_type", "wastecontainer"))
		ctx = logging.NewContextWithLogger(ctx, logger)

		tenant := struct {
			Tenant string `json:"tenant"`
		}{}

		err := json.Unmarshal(msg.Body(), &tenant)
		if err != nil {
			logger.Error("failed to retrieve tenant from message body")
		}

		cbClient := getClientForTenant(tenant.Tenant)

		err = functions.WasteContainer(ctx, msg, cbClient)
		if err != nil {
			logger.Error("transform failed", "err", err.Error())
			return
		}
	}
}

func NewSewerHandler(messenger messaging.MsgContext, getClientForTenant func(string) client.ContextBrokerClient) messaging.TopicMessageHandler {
	return func(ctx context.Context, msg messaging.IncomingTopicMessage, logger *slog.Logger) {
		logger = logger.With(slog.String("function_type", "sewer"))
		ctx = logging.NewContextWithLogger(ctx, logger)

		tenant := struct {
			Tenant string `json:"tenant"`
		}{}

		err := json.Unmarshal(msg.Body(), &tenant)
		if err != nil {
			logger.Error("failed to retrieve tenant from message body")
		}

		cbClient := getClientForTenant(tenant.Tenant)

		err = functions.Sewer(ctx, msg, cbClient)
		if err != nil {
			logger.Error("transform failed", "err", err.Error())
			return
		}
	}
}

func NewCombinedSewageOverflowHandler(messenger messaging.MsgContext, getClientForTenant func(string) client.ContextBrokerClient) messaging.TopicMessageHandler {
	return func(ctx context.Context, msg messaging.IncomingTopicMessage, logger *slog.Logger) {
		logger = logger.With(slog.String("function_type", "combinedSewageOverflow"))
		ctx = logging.NewContextWithLogger(ctx, logger)

		tenant := struct {
			Tenant string `json:"tenant"`
		}{}

		err := json.Unmarshal(msg.Body(), &tenant)
		if err != nil {
			logger.Error("failed to retrieve tenant from message body")
		}

		if tenant.Tenant == "" {
			logger.Error("no tenant information found on incoming message")
			return
		} else {
			logger.Debug(fmt.Sprintf("using tenant %s for %s", tenant.Tenant, msg.ContentType()))
		}

		cbClient := getClientForTenant(tenant.Tenant)

		err = functions.CombinedSewageOverflow(ctx, msg, cbClient)
		if err != nil {
			logger.Error("transform failed", "err", err.Error())
			return
		}
	}
}
