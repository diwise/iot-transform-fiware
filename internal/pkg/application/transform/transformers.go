package transform

import (
	"context"

	fiware "github.com/diwise/ngsi-ld-golang/pkg/datamodels/fiware"	
	"github.com/rs/zerolog/log"
)

type MessageTransformerFunc func(ctx context.Context, msg []byte) (Entity, error)

func WeatherObserved(ctx context.Context, msg []byte) (Entity, error) {
	// här måste vi göra om iot-core meddelandet till något som vi kan göra ett WO av	
	wo := fiware.NewWeatherObserved("device", 0.0, 0.0, "observedAt")
	err := wo.UnmarshalJSON(msg)

	if (err != nil) {
		log.Err(err).Msgf("unable to unmarshal WeatherObserved data")
		return nil, err
	}

	return wo, nil
}

type Entity interface { }
