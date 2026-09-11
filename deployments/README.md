# Drift

## Bygge

```bash
docker build -f deployments/Dockerfile . -t diwise/iot-transform-fiware:latest
```

## Modeller

Tjänsten laddar YAML-modeller från `MODELS_DIR`
(`/opt/diwise/config/models`) i lexikografisk filnamnsordning vid start.
Strikt avkodning och validering av `required`-attribut m.m. körs; fel i någon
fil stoppar starten med ett tydligt fel. Imagen har standardmodellerna från
`deployments/models` inbakade i `/opt/diwise/config/models`, så den kör utan
mount. Montera en egen katalog read-only för att ersätta dem:

```bash
docker run --rm \
  -e RABBITMQ_DISABLED=true -e MODELS_DIR=/models \
  -v "$PWD/deployments/models:/models:ro" \
  diwise/iot-transform-fiware:latest
```

## Miljövariabler

| Variabel | Notering |
| --- | --- |
| `MODELS_DIR` | Katalog med YAML-modeller (lexikografisk ordning) |
| `DEVICE_MGMT_URL` | Sensortyp-uppslag med TTL-cache |
| `NGSI_CB_URL`, `OAUTH2_*` | Context-broker och OAuth2 |
| `RABBITMQ_*` | Se `messaging.LoadConfiguration` |

## Rökprov utan RabbitMQ/context-broker

Startar med `RABBITMQ_DISABLED=true`; kontrollservern svarar `204` på `/health`
och `/readyz`. En trasig `MODELS_DIR` ska ge `failed to load model
configuration` och avsluta processen.

## e2e i compose

Den kompletta systemmiljön (RabbitMQ, context-broker, device-management) finns i
`docker-compose` i `diwise`-repot. Bygg om imagen, montera modellkatalogen och
verifiera att

- ett mätvärde på `message.accepted` och
- en sak på `thing.updated`

ger förväntade NGSI-LD-entiteter enligt modellerna.
