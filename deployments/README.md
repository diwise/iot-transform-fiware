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

## Utrullningsordning

1. Bygg och tagga imagen. Standardmodellerna är inbakade, så ingen separat
   config-fil behövs för grundfallet.
2. Används en delad `MODELS_DIR`-mount: uppdatera modellerna och imagen i samma
   rullning så att versionerna inte glider isär. Tjänsten validerar modellerna
   vid start och vägrar starta vid fel.
3. Rulla ut och verifiera `/health`/`/readyz` samt att `message.accepted` och
   `thing.updated` ger förväntade entiteter i context-broker.

Det finns ingen `DYNAMIC_MODELS`-flagga längre; den dynamiska motorn är den
enda vägen.

## Rollback

Tjänsten är tillståndslös och skriver entiteter med idempotent merge/create, så
en rollback är att rulla tillbaka till föregående image-tagg (och, om mount
används, föregående modellkatalog).

Observera att entiteter som redan skrivits ligger kvar i context-broker. Eftersom
migreringen bytte vissa attributnamn till modellens (`co2` i stället för `CO2`,
`relativeHumidity` i stället för `humidity`, `LAeq` i stället för `noiseLevel`)
kan gamla och nya attribut existera sida vid sida under en övergång.
NGSI-LD-merge är additivt; städa bort inaktuella attribut separat om så behövs.
