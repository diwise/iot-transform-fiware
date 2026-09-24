# Drift

## Bygge

```bash
docker build -f deployments/Dockerfile . -t diwise/iot-transform-fiware:latest
```

## Modeller

Tjänsten lagrar regler i postgres (tabell `transform_rules`) och seedar
YAML-modeller från `MODELS_DIR` (`/opt/diwise/config/models`) i
lexikografisk filnamnsordning vid start (aldrig överskrivning av
API-ändringar; varje API-skrivning laddar om körningsmotorerna atomärt).
Strikt avkodning och validering stoppar starten vid fel. Imagen har
standardmodellerna från `deployments/models` inbakade som seed.

```bash
docker run --rm \
  -e RABBITMQ_DISABLED=true -e MODELS_DIR=/models \
  -e POSTGRES_HOST=postgres -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=password \
  -v "$PWD/deployments/models:/models:ro" \
  diwise/iot-transform-fiware:latest
```
Regel­lagring kräver PostgreSQL (enda backend); seed-monteringen ovan kan
vara read-only eftersom seed bara läser.

## Miljövariabler

| Variabel | Notering |
| --- | --- |
| `MODELS_DIR` | YAML-seed (lexikografisk ordning; aldrig overskrivning av API-rader) |
| `POSTGRES_*` | Regel­lagring (kravs, enda backend) |
| `SERVICE_PORT` | Publik server: regel-API:t (`/api/v0/models`, OPA-skyddat) |
| `POLICIES_FILE` | OPA-policy for regel-API:t |
| `AUTHZ_ACCESS_OBJECT_ENABLED` | `true` for access-objekt-modell |
| `DEV_MGMT_URL` | Sensortyp-uppslag med TTL-cache |
| `DEFAULT_CONTEXT_URL` | JSON-LD-context som fallback när en entitet inte anger `context` |
| `NGSI_CB_URL`, `OAUTH2_*` | Context-broker och OAuth2 |
| `RABBITMQ_*` | Se `messaging.LoadConfiguration` |
| `THINGS_V1_VALUES` | Endast exakt `true` prenumererar pa `things.v1.*` (E7-cutover: values, relations, lifecycle) |

## Rökprov utan RabbitMQ/context-broker (med postgres)

Startar med `RABBITMQ_DISABLED=true` mot postgres; kontrollservern svarar
`204` på `/health` och `/readyz`. En trasig `MODELS_DIR` ska ge seed-fel
och avsluta processen. Regel-API:t (`SERVICE_PORT`) svarar 401 utan token.

## e2e i compose

Den kompletta systemmiljön (RabbitMQ, context-broker, device-management) finns i
`docker-compose` i `diwise`-repot. Bygg om imagen, montera modellkatalogen och
verifiera att

- ett mätvärde på `message.accepted` och
- ett versionerat värdesevent på `things.v1.values` (regler med
  `match.event`, t.ex. `90-room-things-v1.yaml`)

ger förväntade NGSI-LD-entiteter enligt modellerna. Verifiera dessutom att
en partOf-länk på `things.v1.relations` sätter/rensar relationsattribut
(`94-relations-things-v1.yaml`) och att en `deleted`-händelse på
`things.v1.lifecycle` raderar entiteten (`93-lifecycle-things-v1.yaml`).
`thing.updated` konsumeras inte längre (legacy-reglerna är ersatta).

## Utrullningsordning

1. Bygg och tagga imagen. Standardmodellerna är inbakade som seed, så ingen separat
   config-fil behövs för grundfallet. Sätt `POSTGRES_*` (kravs) och
   `SERVICE_PORT` + `POLICIES_FILE` för regel-API:t.
2. Används en delad `MODELS_DIR`-mount: uppdatera modellerna och imagen i samma
   rullning så att versionerna inte glider isär. Seed skriver aldrig över
   API-ändringar (`source=api`); redigerade seed-filer uppdaterar seed-rader
   vid omstart. Tjänsten validerar modellerna vid start och vägrar starta vid fel.
3. Rulla ut och verifiera `/health`/`/readyz` samt att `message.accepted` och
   `things.v1.values` ger förväntade entiteter i context-broker. Sätt
   `THINGS_V1_VALUES=true` först vid cutover till versionerade kontrakt;
   utan flaggan prenumereras `things.v1.*` aldrig (samma image kan
   rulla före cutover; rollback av cutover är att slå av flaggan).

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
