# Plan: regelstyrd transformation till NGSI-LD

## Mål och avgränsning

Measurements och things är källdata och behöver inte känna till FIWARE.
`iot-transform-fiware` mappar dessa data till Smart Data Models genom regler.
Det avsedda flödet är att tjänsten lyssnar på källhändelser från
`iot-things-v2` eller `iot-events`, tolkar dem genom källtrogna adaptrar och
använder regler för att skapa, uppdatera och/eller ta bort NGSI-LD-entiteter
i context-broker. Reglerna anger mappningen till önskade Smart Data Models
och vilken brokeroperation händelsen ska ge upphov till, inom tjänstens
dokumenterade operationsstöd. Producent, topic och payload för varje flöde
ska verifieras mot de faktiska källkontrakten; ett eventnamn eller en
källtyp får inte implicit välja modell eller brokeroperation i motorn.
Tjänsten får känna till och implementera begrepp, strukturer och krav som
beskrivs i NGSI-LD-specifikationen inom ETSI CIM, enligt tjänstens dokumenterade
funktionsprofil. Det omfattar exempelvis entiteter, Properties, Relationships,
GeoProperties, identifierare, tidsmetadata och JSON-LD-context samt
grundläggande protokollvalidering. Sådan protokollkunskap är skild från
modellberoende mappningar i standardreglerna.

- Modellval ligger i regler: entitetstyp, ID-mall, attribut, värdekällor,
  relationer, enheter, tidskällor och JSON-LD-context.
- Mappningarna som följer med tjänsten är fördefinierade standardregler i
  regelfiler, inte inbyggd Go-logik. De får förfinas som regler. Regelmotorn
  läser, tolkar och validerar regler med generella funktioner; dess kod får inte
  känna till standardreglernas modellnamn, attributval eller mappningsinnehåll.
  NGSI-LD-/ETSI CIM-definierade strukturfält och metadata får däremot vara kända
  i koden; förbudet gäller modellberoende val, inte protokollimplementationen.
- Standard-ID använder stabilt käll-ID (helst GUID/UUID) via `{{id}}`; namn
  mappas till attribut. `nameOrID` tas bort. `{{variabel}}` får läsa vilket
  exponerat källfält som helst, inklusive nästlade vägar, citerade nycklar och index.
- Generella funktioner som `safeURI` och `firstPresent` är
  önskvärda. Funktionernas beteende ska vara dokumenterat och får inte implicit
  välja en Smart Data Model utifrån källans domäntyp.
- Adaptrar tolkar källkontrakten och exponerar källtrogen data. Råa identiteter,
  namn, typer, enheter och tider ska kunna användas även när genvägar finns.
- Samma regel- och utdata-validering används i produktion och preview.
- Arbetet utgår från tom regeldatabas. Ingen migrering av befintliga regler,
  kompatibilitetsalias eller stegvis utfasning av gamla adapterfält behövs.
  Tömning av en faktisk databas är en separat driftoperation.

## Arbetsordning

Genomför etapperna i små, verifierbara ändringar. Blanda inte mekaniska flyttar,
ändringar i producentkontrakt och nya tillståndsfunktioner med motorförbättringarna.
Bevara tenant-isolering, källidentiteter, enheter, mättider och avtalad
kvalitetsfiltrering. Markera en punkt klar först när dess verifiering är utförd.

## Etapp 1 — fastställ kontrakt och testfall

- [x] Dokumentera vald ETSI/NGSI-LD-version och tjänstens stödda funktionsprofil.
      Skilj grundvalidering från fullständig ETSI- och Smart Data Models-konformitet.
- [x] Beskriv källvyerna för `message.accepted`, `things.v1.values`,
      `things.v1.relations` och `things.v1.lifecycle`: tillgängliga fält,
      identiteter, proveniens, kvalitet och tidssemantik.
- [x] Fastställ dokumenterad semantik för `{{id}}`, generella fältuttryck, typvariabler, URI-kodning,
      `firstPresent`, tomma värden och saknade källor. Behåll användbara generella
      genvägar utan att ersätta åtkomsten till råa källfält.
- [x] Definiera matchningskontraktet: en vinnande regel per observation/händelse,
      prioritet, tie-break och beteende när ingen regel matchar. Flera entiteter
      inom en regel stöds; flera matchande regler kombineras inte i denna etapp.
- [x] Definiera utfall för saknad källa, felaktig indata, ogiltig regel,
      ogiltig NGSI-LD-utdata och tillfälligt beroendefel.
- [ ] Lägg meningsfulla regressionstester för identifierade korrekthetsfel
      innan respektive rättning. Testerna ska använda i övrigt giltiga regler
      så att ett orelaterat valideringsfel inte ger falskt positivt resultat.

## Etapp 2 — säker regelhantering och konsistenta snapshots

Berör främst `internal/presentation/api/{register,snapshot}.go` och
`internal/infrastructure/rules`.

- [x] Kontrollera skrivägarskap på både befintlig och ny regel vid PUT.
      En global regel får inte kunna tas över genom att body anger egen tenant.
      Verifiera även tenantbyte och samtidiga revisionsändringar.
- [x] Läs regeluppsättning och versionsnummer konsistent från databasen.
      Förhindra att äldre regler märks med en nyare version.
- [x] Samordna samtidiga omladdningar så att en äldre snapshot inte kan
      publiceras efter en nyare. Behåll föregående fungerande snapshot vid fel.
- [x] Validera/kompilera hela kandidatuppsättningen före publicering.
      Seed, API och omladdning ska använda samma regler för giltighet.
- [x] Gör seedändringar och versionsbump atomiska. Verifiera rollback vid fel
      mitt i seedningen och att andra instanser ser en komplett uppsättning.
- [x] För context genom API, regelgränssnitt och lagring. Bevara cancellation,
      tidsbudget och spårningskedja; skilj DB-fel från saknad rad och konflikt.
- [x] Behåll regel-ID, revision och uppsättningsversion som körningsmetadata.
      Säkerställ att den publicerade konfigurationen är oföränderlig under körning.
- [x] Verifiera start, omstart, synkfel och samtidig mutation/omladdning med
      deterministiska tester, relevanta race-tester och isolerad testdatabas.

## Etapp 3 — gemensam regelvalidering och uttryckshantering

Berör främst `internal/application/transform/{schema,validate,variables,engine}.go`.

- [x] Använd gemensam parsning för malluttryck och fältsökvägar i validering och
      körning. Stöd dokumenterad nästling, citerade nycklar och index; avvisa
      felaktigt balanserade eller otillåtna uttryck.
- [x] Registrera generella funktioner med dokumenterade argument, returtyper
      och beteende vid saknad källa. Validera fältsyntax; dynamiska fälts närvaro
      avgörs mot respektive källa vid körning eller preview.
- [x] Definiera och implementera konsekvent referenshantering för entitets-ID:n.
      Välj explicit ordningskrav eller beroendeupplösning; valideringen får inte
      godkänna framåtreferenser som körningen inte kan lösa.
- [x] Kontrollera referenser i samtliga relevanta uttryck, inklusive ID, typ,
      property-värden, subproperties och `observedBy`. Upptäck okända namn och cykler.
- [x] Avvisa kombinationer som saknar effekt eller bryter typkontraktet,
      exempelvis `latlon` på annan måltyp än Geo och metadata som inte stöds.
- [x] Kontrollera dubbletter och konflikter mellan attribut, samt kollisioner
      med NGSI-LD-strukturfält såsom `id`, `type` och `@context`.
- [x] Kontrollera statiska konstanter, URI:er, datum och transformargument där
      felet kan upptäckas utan indata, inklusive division med konstant noll.
      Kontrollerna ska vara generella och får inte hårdkoda de medföljande
      standardreglernas modellval eller innehåll.
- [x] Gör regeluppsättningskontroller med hänsyn till tenant och matchning.
      Likadana ID-mallar i åtskilda tenants får inte ge falska konflikter.
- [x] Verifiera att varje accepterad konstruktion har definierat körningsbeteende
      och att felmeddelanden pekar ut regel, entitet, attribut och uttryck.

### Arbetsplan för statisk validering (3.7)

1. Kartlägg befintliga kontroller och körningens konvertering. Kontrollera
   gemensam validering vid YAML/seed, API, snapshot och offline-preview.
2. Lägg regressionstester före rättning med i övrigt giltiga regler för
   statiska URI:er, datum, värdetyper, transformargument och konstanta aggregat.
   Lägg positiva fall för generiska modellval, dynamiska uttryck, noll och false.
3. Återanvänd generella körningsfunktioner för statiska värden och samla
   URI-/transformkontroller. Avvisa division med noll, icke-finita argument,
   felaktiga konstanta ingångar och ogiltiga konstanta resultat. Kontrollera
   delproperties rekursivt och ge fel med regel-/entitets-/attributkontext.
4. Gör review och ta bort dubblerad validering. Verifiera faktisk serialisering
   och att ogiltiga kandidater inte publiceras. Kör gofmt, modultester och vet.
5. Dokumentera verifiering och begränsningar innan punkten markeras klar.
   Dynamiska resultat, fullständig GeoJSON-struktur och tenant-/matchnings-
   konflikter har fortsatt egna punkter i planen.

Genomfört och granskat 2026-10-01: regressionstesterna påvisade felen före
rättning. Statiska värden använder körningens konvertering och generella
URI-/transformkontroller. Finita tal kontrolleras även efter transformsteg,
i numeriska listor och koordinater. Dubblerad entitetsmallvalidering och en
oanvänd relationshjälpare togs bort. Snapshot-validering får nu regel-ID
innan kandidaten valideras; ett ogiltigt statiskt värde behåller föregående
fungerande snapshot. Modellmappningar ligger fortsatt i regelfiler.

Verifiering i `iot-transform-fiware`:

- `gofmt -w` kördes på samtliga ändrade Go-filer.
- `go test ./...` och `go vet ./...` passerade.
- `go test ./internal/application/transform -run 'TestStatic' -count=1`
  påvisade de ursprungliga valideringsluckorna före implementationen.
- `go test -race ./internal/application/transform ./internal/presentation/api -run 'TestStatic|TestDynamicValues|TestSnapshot' -count=1`
  passerade.
- `PG_TEST_HOST=127.0.0.1 PG_TEST_PORT=32768 go test ./internal/infrastructure/rules ./internal/presentation/api -count=1 -v`
  passerade utan skippade tester mot en tillfällig, isolerad PostgreSQL 16.
  Kontrollerade seed, rollback, cancellation, omstart, samtidig mutation,
  kandidatvalidering och preview. Första försöket utan DB skippade DB-testerna;
  det ersattes av denna verifierade körning.
- `PG_TEST_HOST=127.0.0.1 PG_TEST_PORT=32768 go test ./... -count=1`
  passerade mot samma isolerade testinstans, inklusive medföljande standardregler.
- `PG_TEST_HOST=127.0.0.1 PG_TEST_PORT=32768 PG_TEST_DBNAME=diwise_static_race go test -race ./internal/presentation/api -run 'TestStatic|TestSnapshot|TestDatabaseSnapshotRestartAndConcurrentMutation|TestModelsTenantTransferAndConcurrentRevision' -count=1`
  passerade med en separat race-testdatabas.
- `git diff --check` passerade. README, regelkontrakt och OpenAPI uppdaterades.

### Arbetsplan för regeluppsättningskontroller (3.8)

1. Kartlägg `Validate`, motorns matchning och kandidatvalideringen i
   YAML/seed, API och snapshot. Definiera kontrollen som en jämförelse av
   identiska ID-mallar med olika typmallar inom samma regel eller mellan
   regler som potentiellt matchar samma indata. En vinnande regel gäller
   fortsatt; överlappskontrollen kombinerar inte regler.
2. Lägg regressionstester före rättning för separata tenants, global kontra
   tenantspecifik regel och åtskilda/överlappande matchningar. Kontrollera
   wildcard, sensorType utan hänsyn till skiftläge och event/lifecycle med
   exakt jämförelse. Olika resource-villkor kan överlappa i samma observation.
3. Ersätt det globala ID-/typregistret med en matchningsmedveten kontroll som
   behåller samtliga tidigare definitioner per ID-mall. Bevara lokala
   konflikter och ge fel med båda reglernas och entiteternas identitet.
4. Verifiera YAML, API-mutation, seed och omladdning, inklusive rollback,
   versionsnummer och föregående snapshot vid verklig konflikt. Gör review
   och förenkla relevant kod; dokumentera kontrollens statiska begränsningar.
5. Kör gofmt, go test ./..., go vet ./..., därefter go fix ./..., relevanta race-tester och
   ocachade DB-tester i isolerad PostgreSQL. Markera punkten klar och committa
   först efter verifieringen.

Genomfört och granskat 2026-10-01: det globala ID-/typregistret ersattes av
definitioner grupperade per ID-mall med kontroll mot matchningsöverlapp.
Alla tidigare definitioner behålls, så en åtskild tenant mellan två
konflikter inte kan dölja den första definitionen. Lokala konflikter och
potentiellt överlappande olika typmallar avvisas, medan separata tenants och
oförenliga filter accepteras. Fel anger båda regel-ID:na och entitetsindexen;
DB-kandidatvalidering tilldelar nu regel-ID före kontrollen. Motorns
matchningsbeteende och modellmappningar ändrades inte.

Verifiering i `iot-transform-fiware`:

- `go test ./internal/application/transform -run 'TestRuleSet' -count=1`
  påvisade falska konflikter före rättningen med i övrigt giltiga regler.
- `gofmt -w` kördes på samtliga ändrade Go-filer.
- `go test ./...` och `go vet ./...` passerade.
- `PG_TEST_HOST=127.0.0.1 PG_TEST_PORT=32769 go test ./internal/infrastructure/rules ./internal/presentation/api -count=1 -v`
  passerade utan skippade tester mot tillfällig, isolerad PostgreSQL 16.
  Kontrollerade API/seed, rollback, revisions-/versionsbevarande, omstart,
  samtidig mutation och att en ogiltig kandidat behåller föregående snapshot.
- `PG_TEST_HOST=127.0.0.1 PG_TEST_PORT=32769 go test ./... -count=1`
  och efterföljande `go vet ./...` passerade, inklusive medföljande standardregler.
- `PG_TEST_HOST=127.0.0.1 PG_TEST_PORT=32769 PG_TEST_DBNAME=diwise_rule_set_race go test -race ./internal/application/transform ./internal/infrastructure/rules -run 'TestRuleSet|TestTenantAware|TestWholeCandidateValidation|TestSeedConcurrentBoots' -count=1`
  passerade.
- `PG_TEST_HOST=127.0.0.1 PG_TEST_PORT=32769 PG_TEST_DBNAME=diwise_rule_set_api_race go test -race ./internal/presentation/api -run 'TestTenantAware|TestSnapshot|TestDatabaseSnapshotRestartAndConcurrentMutation|TestModelsTenantTransferAndConcurrentRevision' -count=1`
  passerade med separat race-testdatabas.
- `git diff --check` passerade. README, regelkontrakt och OpenAPI beskriver
  uppsättningskontrollen och dess statiska begränsningar.

### Genomförd konstruktionsparitet (3.9)

Genomfört och granskat 2026-10-01. Regressionstester med i övrigt giltiga
regler påvisade att `derive.of` tappade const/value/latlon/timestamp och
ingångsprefix, att Structured saknade typkonvertering/derive och att
metadata kunde accepteras utan effekt. Ett separat regressionstest påvisade
att prefix på icke-text kunde ignoreras.

Vanliga properties, aggregatingångar och Structured-delvärden delar nu
källutvärdering. Attribut och Structured delar typkonvertering, inklusive
kontroller av finita tal, datum, URI-referenser och stödd Geo-form.
Ingångsprefix tillämpas före aggregatet, propertyprefix efteråt; statisk
validering använder samma prefixregler. Prefix på fel typ ger fel.
Metadata på Structured-delvärden och explicit observedAt på DateTime
avvisas, liksom object/env utan resource/name. Geo använder alltid regelns
target, även för latlon utan explicit observationstid.

Referensfel anger regel, entitet, attributväg och uttryck. Körningsfel har
motsvarande kontext för properties, aggregatingångar och Structured;
felkedjan bevaras med wrapping. Testerna kontrollerar faktisk JSON,
dynamiska measurement- och things-källor, noll/false, samtliga stödda
delvärdestyper, transform/prefix, required, API/preview och bevarande av
föregående snapshot när en ny kandidat är ogiltig. Inga modellval infördes
i motorn.

Etapp 3 är därmed slutförd. Källornas närvaro och tidssemantik, aggregatens
policy vid tomma ingångar/dynamiska typfel och fullständig dynamisk
NGSI-LD-utdatavalidering följer fortfarande sina separata punkter i etapp
5–6; detta innebär inte att de etapperna är färdiga. Se även den verifierade
funktionsprofilen i `assets/docs/transform-contract.md`.

Verifiering i `iot-transform-fiware`:

- `go test ./internal/application/transform -run TestExecutionParity -count=1`
  påvisade käll-/konverterings-/metadatafelen före rättningen.
- `go test ./internal/application/transform -run TestExecutionParityPrefix -count=1`
  påvisade prefixluckan före dess rättning.
- `gofmt -w` kördes på samtliga ändrade Go-filer.
- `PG_TEST_HOST=127.0.0.1 PG_TEST_PORT=32768 go test ./... -count=1`
  och `go vet ./...` passerade, inklusive medföljande standardregler.
- `PG_TEST_HOST=127.0.0.1 PG_TEST_PORT=32768 go test ./internal/infrastructure/rules ./internal/presentation/api -count=1 -v`
  passerade utan skippade tester mot tillfällig, isolerad PostgreSQL 16.
  Verifierade seed/kandidatvalidering, rollback, cancellation, omstart,
  samtidiga mutationer, snapshot och offline-preview.
- `PG_TEST_HOST=127.0.0.1 PG_TEST_PORT=32768 PG_TEST_DBNAME=diwise_parity_race go test -race ./internal/application/transform ./internal/presentation/api -run 'TestExecutionParity|TestParity|TestStatic|TestRuleSet|TestTenantAware|TestSnapshot|TestDatabaseSnapshotRestartAndConcurrentMutation|TestModelsTenantTransferAndConcurrentRevision' -count=1`
  passerade med separat race-testdatabas.
- README, regelkontrakt och OpenAPI uppdaterades; `git diff --check` passerade.

## Etapp 4 — källtrogna measurement- och things-adaptrar

Berör `internal/application/measurements` och `internal/application/things`.

### Arbetsplan för etapp 4

1. Verifiera de lokala things-kuverten mot `iot-things-v2/pkg/types` och
   measurement-normaliseringen mot de låsta core-/senml-versionerna.
2. Lägg neutrala kontrakts- och regressionstester före rättning för
   kuvertmetadata, descriptor/variant, proveniens, Point/Polygon och
   återkommande objektprefix med olika metadata. Verifiera råa identiteter,
   noll/false och subsekundprecision.
3. Samla descriptor-/geometriexponering för values/lifecycle och relationer.
   Exponera eventfält och full värdeproveniens. Separat observationsvy bär
   kvalitet/proveniens men inga numeriska värden; skrivvärden finns endast
   bakom befintlig ok/uncertain-grind i `values`.
4. Bevara measurement-resursers fullständiga namn, objekt, kanal, enhet och
   tid och exponera dem för generella uttryck. Använd parserns effektiva
   observationsmetadata i den gemensamma partitioneringen, även i preview.
5. Verifiera faktisk regelkörning/JSON och preview-paritet. Kör gofmt,
   go test ./..., go vet ./... och därefter go fix ./...; granska fixändringar
   och kör om formatering/test/vet om koden ändras. Kör relevanta race- och
   ocachade DB-tester mot isolerad PostgreSQL och dokumentera innan avbockning.

- [x] Ta bort `relation.from.waterQualityObserved` och konstruktionen av
      `urn:ngsi-ld:WaterQualityObserved:...` från relationsadaptern.
- [x] Exponera generella descriptors för både `relation.from` och `relation.to`:
      käll-ID, rå typ, undertyp, namn, alternativnamn, kategori samt relevant
      plats-, metadata- och variantinformation ur producentkontraktet.
- [x] Exponera relevant descriptor-, event- och proveniensdata konsekvent för
      values och lifecycle. Rätta fält som avkodas men försvinner före regelkörning.
- [x] Bevara råa namn och typer. Gör det tydligt vilka variabler som är råa och
      vilka som är beräknade genvägar; undvik implicit ändring av rå `type`.
- [x] Exponera geometridata konsekvent för Point och övriga stödda geometrier.
      Koordinatgenvägar får komplettera, men inte dölja, den ursprungliga geometrin.
- [x] Bevara measurement-resursers fullständiga identitet, objekt, kanal,
      värde, enhet och observationstid. Gör relevanta uppgifter åtkomliga för regler.
- [x] Fastställ `port` mot det verkliga källkontraktet. Fyll det från korrekt
      källa; anta inte att kanal betyder port. Port är sensorns fysiska anslutning:
      `{device_id}/{object}/{resource}` utan port och
      `{device_id}/{port}/{object}/{resource}` med port. Härled från observationens
      motsvarande header efter normalisering; explicit `0` skiljs från portlös.
- [x] Behåll gemensam flerobjektspartitionering för produktion och preview.
      Verifiera att resurser och metadata aldrig hämtas från fel observation.
- [x] Bevara kontraktsregeln att ogiltiga things-värden inte ersätter aktuella.
      Exponera kvalitet/proveniens utan att kringgå filtreringen av skrivbara värden.
- [x] Ta bort oanvänd `Thing.EntityID()` och granska kvarvarande äldre typer
      utifrån faktisk användning innan de tas bort eller behålls.
- [x] Verifiera adaptrarna mot producenternas kontrakt med neutrala testdata
      som inte förutsätter FIWARE-ID:n eller modellnamn i källpayloaden.

### Genomfört och verifierat

Etapp 4 slutförd 2026-10-01. De lokala JSON-kuverten granskades mot
`iot-things-v2/pkg/types`, och measurement-vägen mot core/senml-versionerna
låsta i tjänstens `go.mod`. Producenternas payloads och topics ändrades inte.
Regressionstester påvisade förlorad proveniens/variant/geometri och att
återkommande objektprefix kunde få en tidigare observations metadata.

Values/lifecycle/relationer delar nu descriptor-/geometriexponering.
Kuvertmetadata finns under `event`; values bevarar hela proveniensen och
eventets inbäddade relationer. Alla källtider i de nya vyerna bevarar
subsekundprecision; saknade tider utelämnas. `observations[]` exponerar
samtliga observationers metadata och valuePresent men inga värden.
Skrivvärden finns endast under den oförändrade ok/uncertain-/icke-null-
grinden i `values`. Testad regelkörning visar att ingen alternativ vy kan
användas för att skriva det ogiltiga mätvärdet.

Measurements bevarar fullständiga resursnamn, objekt, kanal, enhet, nativa
värden och observationstider under `records[]`. Partitioneringen använder
parserns effektiva observationsmetadata i stället för att återleta poster
via prefix. Lokala overrides, paketdefaults, explicit noll, återkommande
prefix och relativ SenML-tid verifierades. Produktion och offline-preview
ger samma serialiserade resultat för neutrala flerobservationspaket.

Den interna äldre Thing-typen och stringOrEmpty togs bort efter kontroll
av kvarvarande användning; descriptorInput ersätter mellanrepresentationen.
README, källvyer/avgränsningar i regelkontraktet och OpenAPI uppdaterades.
Automatisk observedAt-policy, de äldre tidsgenvägarnas formattering,
matchning och modellregelgranskning följer fortfarande etapp 5–8.

Verifiering i `iot-transform-fiware`:

- `go test ./internal/application/things ./internal/application/measurements -run TestSourceContract -count=1`
  påvisade adapter-/partitioneringsfelen före rättning.
- `gofmt -w` kördes på ändrade Go-filer.
- `PG_TEST_HOST=127.0.0.1 PG_TEST_PORT=32769 go test ./... -count=1`,
  därefter `go vet ./...` och `go fix ./...` passerade.
  Fixändringarna till `maps.Copy` och direkt numerisk pekarkonstruktion
  i LanguageMap-testet granskades; efterföljande formatering,
  fulla tester och vet passerade. Sista fixkörningen gav inga ytterligare ändringar.
- `PG_TEST_HOST=127.0.0.1 PG_TEST_PORT=32769 go test ./internal/infrastructure/rules ./internal/presentation/api -count=1 -v`
  passerade utan skippade tester mot tillfällig, isolerad PostgreSQL 16.
  Seed, rollback, cancellation, omstart, samtidiga mutationer, snapshot
  och preview verifierades.
- `PG_TEST_HOST=127.0.0.1 PG_TEST_PORT=32769 PG_TEST_DBNAME=diwise_adapter_race go test -race ./internal/application/measurements ./internal/application/things ./internal/application/transform ./internal/presentation/messaging/measurements ./internal/presentation/messaging/things ./internal/presentation/api -count=1`
  passerade med separat race-testdatabas.
- Producenternas kontraktstester passerade: `go test ./pkg/types -count=1`
  i `iot-things-v2` och `go test ./pkg/messaging/events -count=1` i `iot-core`.
- `git diff --check` passerade. Den tillfälliga DB-instansen städades efter verifiering.

## Etapp 5 — konsekvent matchning, värdekällor och tid

Berör främst `internal/application/transform/{engine,measurement,thing,values,functions}.go`.

### Arbetsplan för etapp 5

1. Regressionstesta prioritet/specificitet/stabil ordning, env/port,
   tenantisolering, otillgängliga matchfält och relationens råa måltyp.
   Inför `priority` på regeln och `match.toType` för relationshändelser.
   Lifecycle kräver explicit created/updated/deleted enligt målkontraktet.
   Ge modellneutral diagnostik för konkurrerande matchningar och överlapp.
2. Samla närvarokontroll före modifierare: null/tom text/lista/mapp och
   saknad property-mall ger skip; noll/false är närvarande. Required
   undertrycker entiteten. Aggregat använder närvarande ingångar men avvisar
   samtliga typfel, även efter en tidigare avgörande boolean/coalesce-ingång.
   Alla-saknade och tom Structured ger ingen skrivning.
3. Förmedla observationstid/enhet från direkta källor och enkla fältmallar.
   Default är värdets observationstid, measurement-header som fallback;
   konstanter/namn får ingen implicit tid. Aggregat använder senaste
   närvarande ingångstid, coalesce/firstPresent den valda ingångens tid.
   Separera things publiceringstid (`message`) från observation (`record`)
   och max-observation/changedAt (`header`). Bevara RFC3339Nano i källvyer,
   trunkera broker-utdata (DateTime-värden och observedAt) till
   millisekunder, utelämna
   saknad tid och stöd tidsmetadata på alla attributtyper, inte delvärden.
4. Behåll källenheter åtkomliga. Känd källenhet får inte märkas om utan
   ekvivalent enhetskod eller explicit numerisk omvandling. Inför generisk
   `convert: {from, to, factor, offset}` före befintliga transformer;
   factor default 1, offset default 0. Saknad källenhet kräver författarens
   uttryckliga from-deklaration vid konvertering. Ingen modellberoende
   omvandling eller implicit numerisk omvandling används.
5. Transformera measurements före broker/tokenförberedelse och testa no-match,
   required/alla-saknade och fel utan brokerkontakt. Uppdatera regel-API/YAML,
   dokumentation och webbens kopierade typer/formulär så nya fält inte tappas.
6. Kör gofmt, fulla tester, vet och därefter go fix i berörda moduler;
   granska fixändringar och kör om format/test/vet när koden ändras. Kör
   race och ocachade DB-/seed-/API-tester isolerat innan etappen avbockas.

- [x] Implementera det fastställda prioritetskontraktet. Hantera avgränsningar
      som env och port konsekvent och diagnostisera överlappande/skuggade regler.
- [x] Avvisa matchningsfält som källtypen inte kan tillhandahålla. Definiera
      explicit om lifecycle krävs eller om wildcard stöds.
- [x] Lägg till regelstyrd avgränsning för relationens målsida, exempelvis måltyp
      eller generella fältpredikat, så att tankregeln kan kräva växthus som mål.
- [x] Använd en gemensam värdekällsutvärderare för properties, `derive.of` och
      Structured. Alla tillåtna källor och modifierare ska faktiskt utvärderas.
      Genomfört och verifierat i 3.9.
- [x] Definiera `required`, `when: present`, tomma värden och saknad mallkälla
      konsekvent. Gör aggregatens beteende vid saknade ingångar explicit.
      Beslut: saknad/null/tom källa ger ingen skrivning; noll och false är giltiga.
      Rensning är explicit. Inget aggregat skriver när alla ingångar saknas.
- [x] Skilj saknad källa från typfel. Aggregat får inte tyst ignorera felaktiga
      typer utan ett dokumenterat regelval.
- [x] Säkerställ att Structured-delvärden konverteras enligt deklarerad typ.
      Avvisa metadata/funktioner på delvärden som inte har implementerad semantik.
      Genomfört och verifierat i 3.9.
- [x] Exponera observationstid, publiceringstid och förändringstid separat.
      Låt regler välja tidskälla för både attributvärden och `observedAt`.
- [x] Gör tidsmetadata konsekvent över stödda property-typer och dokumentera
      vad `header`, `record`, `message`, `field` och `none` betyder för varje källa.
      Beslut: använd värdets egen observationstid, med objektets bastid som
      measurement-fallback. Things-values använder respektive observedAt;
      relation/lifecycle använder changedAt där regeln efterfrågar den.
      Konstanter/namn får ingen automatisk tid. Saknad tid fabriceras inte.
- [x] Bevara tillgänglig subsekundprecision i källvyer. Verifiera relativ SenML-tid,
      saknad tid och flera observationer med olika mättider. Broker-utdata
      (DateTime-värden och observedAt) trunkeras till millisekunder eftersom
      Orion avvisar nanosekunder.
- [x] Gör källenheter åtkomliga och låt regler ange målens enheter och eventuell
      omvandling. Märk inte om enheten utan motsvarande dokumenterad omvandling.
- [x] Flytta measurement-vägens broker-/tokenförberedelse efter transformation,
      så att ingen match eller inga skrivningar kan hanteras utan brokerberoende.

### Genomfört och verifierat

Etapp 5 slutförd 2026-10-01. Regressionstester påvisade först att prioritet
saknades, device-filter accepterades för things, tomma källor kunde ge typfel
eller tomma brokerskrivningar, aggregat ignorerade fel och fabricerade false,
tider ersattes av bastid/tappade precision och källenheter kunde märkas om.
Handler-regressioner påvisade broker/tokenförberedelse före no-match,
required/alla-saknade och transformationsfel.

`priority` väger nu före specificitet/stabil ordning; env/port hanteras
konsekvent. `match.toType` matchar rå relationstyp på målsidan, lifecycle
kräver explicit kind och things avvisar otillgängligt device-filter.
Motorn ger diagnostik för potentiella överlapp och faktisk per-input-
skuggning utan falska tenantkonflikter eller kombinerade regler.

Gemensam närvarosemantik gäller före modifierare och efter transform.
Required undertrycker entiteten; tom Structured, saknad mallkälla och
alla-saknade aggregat skriver inte. Samtliga närvarande numeriska/boolean-
aggregatingångar typkontrolleras före reducering; noll/false bevaras.
Tids-/enhetsmetadata följer direkta fält, mallar och aggregat. Samtliga
attributtyper kan bära observedAt; källvyer bevarar RFC3339Nano medan
broker-utdata trunkeras till millisekunder; saknad/zero tid
fabriceras inte och fälttidsfel skiljs från frånvaro. Detaljer för header,
record, message, field och none finns i regelkontraktets tidstabell.

Kända källenheter får inte märkas om utan ekvivalent kod eller uttrycklig
affin `convert`. Omvandlingens from verifieras när källenheten är känd;
factor/offset/resultat måste vara finita, olika enheter kräver en faktisk
numerisk förändring och numeriska listor konverteras utan mutation.
Enhetskoder och omvandling är generiska; författaren ansvarar för fysisk
korrekthet och för from-deklaration när källenheten saknas.

Measurement-vägen transformerar en gång före broker/token och hoppar över
no-match/inga skrivningar utan det beroendet. Regel/API/seed/snapshot och
offline-preview delar semantiken. Webbens kopierade typer, formulär och JSON
bevarar priority/toType/convert; dess templ-källor genererades om. Tankreglerna
avgränsar explicit greenhouse som mål och befintlig Pa→KPA-omvandling
deklareras i regeln. Ingen modellberoende motorkod infördes.

Verifiering:

- `go test ./internal/application/transform -run TestStage5 -count=1`
  påvisade motorfelen före rättning.
- `go test ./internal/presentation/messaging/measurements -run TestStage5 -count=1`
  påvisade den för tidiga brokerförberedelsen före rättning.
- Gofmt kördes på ändrade Go-filer. I `iot-transform-fiware` passerade
  `PG_TEST_HOST=127.0.0.1 PG_TEST_PORT=32770 go test ./... -count=1`,
  därefter `go vet ./...` och `go fix ./...`. Fixändringarna granskades;
  formatering, fulla tester och vet kördes därefter om och passerade.
- `PG_TEST_HOST=127.0.0.1 PG_TEST_PORT=32770 go test ./internal/infrastructure/rules ./internal/presentation/api -count=1 -v`
  passerade utan skips i isolerad PostgreSQL 16, inklusive nya rulefält i
  DB/snapshot, seed, rollback, cancellation, omstart och revisionskonkurrens.
- `PG_TEST_HOST=127.0.0.1 PG_TEST_PORT=32770 PG_TEST_DBNAME=diwise_stage5_race go test -race ./internal/application/transform ./internal/application/measurements ./internal/application/things ./internal/presentation/messaging/measurements ./internal/presentation/messaging/things ./internal/presentation/api -count=1`
  passerade med separat race-testdatabas.
- I `diwise-web` passerade `go run github.com/a-h/templ/cmd/templ@v0.3.1020 generate`,
  `go test ./...`, `go vet ./...` och därefter `go fix ./...`. Fixändringen
  i things-v2-testets pekarkonstruktion granskades; efterföljande tester och vet
  passerade. `go test -race ./internal/presentation/api/handlers/rules ./internal/presentation/web/components/features/rules -count=1`
  passerade också. Nya tester kontrollerar faktisk formulär-/JSON-serialisering
  och avvisar felaktig priority/convert-JSON.
- README, OpenAPI och regelkontrakt uppdaterades. `git diff --check` passerade
  i båda repon. Den tillfälliga DB-instansen städades efter verifiering.

Etapp 6–9 återstår för full dynamisk NGSI-LD-validering, brokersvar/
leveransklassificering, beständig relationsprojektion och aktiv preview/
slutacceptans. Dessa krav är inte avbockade genom motor-/previewtesterna ovan.

## Etapp 6 — generisk NGSI-LD-byggning och grundvalidering

### Arbetsplan för etapp 6

1. Regressionstesta dynamiska entitets-ID:n/typer, observedBy, reserverade
   attribut, faktisk serialisering och sena ogiltiga skrivningar i en batch.
   Verifiera att fel upptäcks före broker/token och att preview använder
   samma kontroll. Leveransklassificering och partiella brokersvar följer etapp 7.
2. Ersätt Geo-kontrollens numeriska löv med generell GeoJSON-validering för
   de sex stödda geometrityperna: finita faktiska tal, 2D/3D med konsekvent
   dimension, WGS84 lon/lat-intervall, rätt nästling och minsta antal
   positioner/ringar, slutna polygonringar. Inga modellberoende geografival.
3. Bygg och validera den serialiserade kompakta NGSI-LD-entiteten med vald
   singleton-context. Kontrollera URI-ID, typ-/attributtermer, reserverade
   fält, Property/GeoProperty/Relationship/LanguageProperty, typad DateTime,
   observedAt/observedBy/unitCode, numeriska värden och osupporterade metadata.
   Delete/remove kontrolleras som egna operationer. Hela batchen förvalideras.
4. Återanvänd protokollhjälpare i statisk och dynamisk validering, motor,
   presentationsgrindar, skrivarens sista kontroll och offline-preview.
   Dokumentera profilen: ingen context-hämtning/expansion, datasetId, avancerade
   attributformer, GeometryCollection eller temporal lagring; ingen full SDM-
   validering eller geometrisk topologikontroll. Required är fortsatt mappningspolicy.
5. Kontrollera samtliga typer genom faktisk JSON och kontrollerade brokeranrop;
   dokumentera utfalls-/scopegränser. Kör gofmt, fulla tester/vet/go fix,
   granska fixändringar och återverifiera, relevanta race-tester samt ocachade
   DB-/seed-/API-tester isolerat innan avbockning.

- [x] Låt Geo alltid använda regelns `target`, oavsett koordinatkälla och
      `observedAt`. Ta bort kodvägar som implicit väljer attributet `location`.
      Genomfört och verifierat genom gemensam konvertering i 3.9.
- [x] Inför generisk relationslistform, exempelvis `array: true` på Relationship.
      Skalär blir enelementslista; befintlig lista får inte dubbelnästlas.
- [x] Validera relationsreferenser och listelement strikt enligt vald profil.
      Konvertera inte godtyckliga tal/booleska värden till URI:er och tappa inte
      felaktiga listelement tyst.
- [x] Kontrollera finita tal efter konvertering och transformation, även i listor.
- [x] Kontrollera datum/tid och grundläggande GeoJSON-struktur, inklusive
      geometriernas koordinatstruktur och tillämpliga dimensions-/ringkrav.
- [x] Validera renderade entitets-ID:n, typer, attribut och metadata innan
      brokeranrop. Använd samma utdata-validerare i preview.
- [x] Säkerställ att varje property-typ serialiseras enligt den dokumenterade
      NGSI-LD-profilen. Testa faktisk JSON, inte bara att en dekoratör skapades.
- [x] Dokumentera begränsningar såsom datasetId, avancerade attributformer,
      context-former och temporal lagring; stöd ska inte antydas av schemat om det saknas.
- [x] Håll Smart Data Models-validering åtskild från protokollvalidering.
      `required` är en mappningsregel, inte ett bevis på full modellkonformitet.

### Genomfört och verifierat

Etapp 6 slutförd 2026-10-01. Regressionstester med dynamiska, i övrigt giltiga
regler påvisade otestade ID-/typ-/observedBy-/attribut-/unitCode-resultat,
felaktiga GeoJSON-dimensioner/nästling/ringar och att en senare ogiltig
skrivning kunde passera efter en tidigare brokeroperation.

Gemensamma BuildEntity/ValidateEntityWrites bygger och validerar den verkliga
normaliserade JSON-formen. Motor och preview delar kontrollen; handlers
verifierar även effektiv fallback-context före broker/token. Skrivarens
sista grind förvaliderar hela batchen och låg-nivå create/merge använder
samma grundkontroller och context-form utan att mutera dekoratörslistan.
Delete/remove validerar header/operation/attribut, inklusive strukturfält
och dubbletter. Eventuella brokerfel efter godkänd förvalidering är fortsatt
separata leveransutfall i etapp 7.

Attributkontrollen omfattar finita numeriska värden/listor/Structured,
normaliserade scalar-/list-/object-former, typad DateTime, GeoProperty,
Relationship och LanguageProperty samt observedAt/observedBy/unitCode.
URI-percentkodning, termsyntax och context kontrolleras generellt; statisk
regelvalidering återanvänder samma protokollhjälpare. Metadata och former
utanför profilen avvisas, utan modellnamn eller modellberoende kodval.

GeoJSON-kontrollen kräver faktiska finita tal, konsekventa 2D/3D-positioner,
WGS84-intervall, rätt struktur och minsta storlek samt slutna polygonringar.
Testerna täcker alla sex stödda geometrier, alla property-typers faktiska
JSON (inklusive zero/false, DateTime och relationslista), ogiltiga metadata,
oförändrad källgeometri, batchgrind och identiska produktions-/previewfel.

README, OpenAPI och protokollprofilen dokumenterar avgränsningen: ingen
context-expansion, full ETSI-/SDM-konformitet, geometrisk topologi/orientering,
datasetId, avancerade attributinstanser, användardefinierad context-array/
inline-context eller temporal lagring. Required är fortfarande frånvaropolicy.

Verifiering i `iot-transform-fiware`:

- `go test ./internal/application/transform ./internal/infrastructure/contextbroker -run TestStage6 -count=1`
  påvisade de ursprungliga output-/geo-/batchfelen före rättning.
- Gofmt kördes på ändrade Go-filer. `PG_TEST_HOST=127.0.0.1 PG_TEST_PORT=32771 go test ./... -count=1`,
  därefter `go vet ./...` och `go fix ./...` passerade. Fixändringarna
  granskades; efterföljande formatering, fulla tester och vet passerade.
- `PG_TEST_HOST=127.0.0.1 PG_TEST_PORT=32771 go test ./internal/infrastructure/rules ./internal/presentation/api -count=1 -v`
  passerade utan skips mot tillfällig, isolerad PostgreSQL 16. Seed,
  kandidatvalidering, rollback, cancellation, omstart och samtidig mutation
  verifierades med befintliga och nya regel-/previewtester.
- `PG_TEST_HOST=127.0.0.1 PG_TEST_PORT=32771 PG_TEST_DBNAME=diwise_stage6_race go test -race ./internal/application/transform ./internal/infrastructure/contextbroker ./internal/presentation/messaging/measurements ./internal/presentation/messaging/things ./internal/presentation/api -count=1`
  passerade med separat race-testdatabas.
- Berörd webbklient verifierades med `go test ./internal/application/transform ./internal/presentation/api/handlers/rules ./internal/presentation/web/components/features/rules` i `diwise-web`.
- `git diff --check` passerade. Den tillfälliga PostgreSQL-instansen städades
  efter verifieringen. Brokersvars-/ack-/retry-klassificering, aktiv preview-
  diagnostik och systemacceptans följer fortfarande etapp 7–9.

## Etapp 7 — brokerskrivning och leveransutfall

Berör `internal/infrastructure/contextbroker`, messaging-handlers och vid behov
det versionslåsta `context-broker`-biblioteket.

### Arbetsplan för etapp 7

1. Verifiera låsta bibliotek: brokerklientens MergeEntityResult tappar HTTP
   207-body; messaging nil→ack, Permanent→reject utan requeue, övriga fel→
   tillfälligt, confirm-before-ack-retry upp till MaxDeliveryAttempts (default 5).
   Kör bibliotekets egna executor-/retrytester före ny klassificering.
2. Regressionstesta riktiga HTTP-svar och tokenflöden. Inför en smal skrivklient
   i transform-modulen som behåller status/body och UpdateResult för 207;
   updated/notUpdated valideras och delvis misslyckad skrivning ger fel.
   Skilj brokerstatus/beroendefel från irreparabla data-/utdatafel.
3. Behåll create-if-absent, merge-or-create (404→create, 409-race→ett merge),
   attributrensning och delete med dokumenterat HTTP-beteende. Förvalidera
   leveransen före I/O, avbryt skrivbatchen vid första fel och bevara redan
   slutförda operationer i felrapport. Testa replay och idempotenta 404/409.
4. Förmedla request-context till token och broker. Lägg konfigurerbara positiva
   tidsbudgetar (broker 10s, token 5s), återanvänd HTTP-klienter och cached token
   med cancellation-medveten refresh. Ersätt fast sleep och mutex-väntan med
   verifierat synkront svar respektive context-medvetet lås.
5. Lägg spans och strukturerade korrelationsfält för transport/applikation/
   operation/HTTP/token, inklusive regel-ID, revision, snapshot och tenant/ID:n.
   Kontrollera faktisk parent-kedja och Error-status; logga inte tokens,
   credentials, hela payloads eller råa broker-/tokenbody i felsträngar.
6. Kör gofmt, fulla tester/vet/go fix och granska fixändringar, relevanta race-
   tester samt ocachade DB-/API-tester isolerat. Dokumentera profilutökningar,
   retry/dedupgränser och att replay inte skyddar mot sena/omordnade händelser.

- [x] Bevara och tolka partiella brokersvar, inklusive HTTP 207. En delvis
      misslyckad skrivning får inte rapporteras som full framgång.
- [x] Klassificera irreparabla indata-/transformations-/utdatafel separat från
      tillfälliga beroendefel. Verifiera ack, retry och slutlig felhantering i den
      låsta messaging-versionen innan klassificeringen ändras.
- [x] Dokumentera create-if-absent, merge-or-create, attributrensning och delete.
      Verifiera operationernas HTTP- och felbeteende mot vald NGSI-LD-profil.
- [x] Inför tydliga tidsbudgetar för broker- och tokenanrop. Ersätt eventuell
      fast väntan med dokumenterat, cancellation-medvetet beteende där väntan behövs.
- [x] Verifiera partiellt lyckade flerskrivningar och replay. Beskriv att
      idempotens för en isolerad operation inte ger ordningsoberoende korrekthet.
- [x] Förmedla context och trace till klientanrop. Logga regel-ID, regelrevision,
      snapshotversion och relevanta käll-/mål-ID:n utan hemligheter eller hela payloads.

### Genomfört och verifierat

Etapp 7 slutförd 2026-10-02. Kodgranskning verifierade att låsta broker-
bibliotekets MergeEntityResult/NewMergeEntityResult tappar 207-body; dess
HTTP-klient är inte injicerbar. En smal skrivklient i tjänsten återanvänder
bibliotekets typer men bevarar status/body och UpdateResult. HTTP-regressioner
påvisade falsk framgång för partiellt/malformed 207 före rättning.

Pure transformfel är Permanent; kända irreparabla brokerrequeststatus skiljs
från auth/tenant/timeout/transport/5xx/okända beroendefel. 207-delutfall är
retrybart och bevarat. Alla observationer i measurement-leveransen valideras
före I/O; skrivbatchen avbryts på första fel och WriteBatchError bevarar
slutförda tidigare skrivningar samt failed-operation/regel/orsak. Replay,
404/409 och bounded create-race testades med riktiga HTTP-klienter.

NGSI_CB_TIMEOUT/OAUTH2_TOKEN_TIMEOUT har positiva durationer och default
10s/5s. Token hämtas under aktuell context, cache och keyed-lock kan avbrytas,
klienter återanvänds, 401 invalidierar cached token, ingen tokenfels-fallback
till anonymous sker. Fast sleep efter merge togs bort. Named-return-spans
för handler, applikation, apply/operation, HTTP och token ser faktiskt fel;
OTel-parent och HTTP traceparent samt regel/revision/snapshot verifierades.

Operations-/retryprofil och avgränsningar dokumenteras i regelkontraktet,
inklusive att PATCH-null är broker/facade-kompatibilitet och inte fullt
ETSI-attribut-delete, och att idempotenta omtag inte ger recency-/ordningsskydd.
Ingen producentpayload, persistent dedup eller relationsprojektion ändrades.

Verifiering:

- `go test ./internal/infrastructure/contextbroker -run TestStage7 -count=1`
  påvisade den låsta klientens 207-luckor före rättning.
- `go test github.com/diwise/messaging-golang/pkg/messaging -count=1`
  passerade mot låst v0.0.0-20260910052156-098a088f6b4f, inklusive fake-
  acknowledger/executor/retry- och slutlig-failure-tester. Kod verifierade
  nil→ack, Permanent→reject(false), kontrollerad temporary retry med confirm
  före ack och default fem totala försök. DLX-kö är separat driftkonfiguration.
- Gofmt, `PG_TEST_HOST=127.0.0.1 PG_TEST_PORT=32772 go test ./... -count=1`,
  `go vet ./...` och därefter `go fix ./...` passerade. Fixändringar granskades
  och efterföljande formatering/test/vet passerade.
- `PG_TEST_HOST=127.0.0.1 PG_TEST_PORT=32772 go test ./internal/infrastructure/rules ./internal/presentation/api -count=1 -v`
  passerade utan skips mot tillfällig, isolerad PostgreSQL 16, med seed,
  rollback, cancellation, omstart och samtidig mutation.
- `go test -race ./internal/infrastructure/contextbroker ./internal/presentation/messaging/... ./cmd/iot-transform-fiware -count=1 -timeout=60s`
  passerade. HTTP-/token-/lock-budget, authorization, replay och faktisk
  OTel parent/Error/traceparent kontrollerades i nya tester.
- README, konfiguration och regelkontrakt uppdaterades; `git diff --check`
  passerade. Den temporära DB-instansen städades efter verifiering.

Full Compose/systemacceptans och aktiv preview/relationsprojektion följer
fortfarande etapp 8–9; bibliotekstester och kontrollerade HTTP-servrar bevisar
inte en viss produktionsbrokers DLX/topologi eller interna läskonsistens.

## Etapp 8 — modellregler, preview och dokumentation

De medföljande modellmappningarna förvaltas som fördefinierade standardregler
i regelfiler. Förfiningar av dessa görs i reglerna och deras modelltester;
regelmotorn ska inte innehålla specialfall för en viss standardregel eller modell.

- [x] Uppdatera tank/växthus-regeln till regelstyrd referensmall och listform.
      Modellnamn och URN-prefix får endast finnas i regeln och modelltesterna.
- [x] Uppdatera medföljande ID-mallar till `{{id}}` respektive generella relationsfält och
      verifiera konsistens mellan values-, relations- och lifecycle-regler.
- [x] Rätta modellregler som läser oexponerade fält, exempelvis POI-beskrivning,
      och gör platsmappning konsekvent för Point och Polygon.
      Verifierat i PLAN1050 J2: POI-beskrivning läses från
      `metadata.description` (identitetstest); plats läses via
      `field: geometry` (rå GeoJSON för Point/Polygon/3D) i samtliga
      things-värde- och lifecycle-regler (90/91/93/96/97/98/99/100/101);
      J2-modelltester bevisar Point- och Polygon-mappning end-to-end.
- [x] Gå igenom modellernas tidskällor, enheter och obligatoriska värdekällor.
      Redovisa olösta modellfrågor i reglernas dokumentation.
      Verifierat i PLAN1050 J2: `dateObserved` (`timestamp: true`) är
      max-observedAt (mättid, aldrig publiceringstid); Sewer kräver plats/tid
      (skärpt) medan `overflowTime`/`refDevice` är dokumenterade avsteg
      (ingen v2-källa); enheter bevaras genom grinden utan förvanskning.
      Tidigare växthus-CO2-beslut (PPM-avsteg) är överspelat: greenhouse/
      tank-reglerna är borttagna i väntan på nya mallar (J2e) och ingen
      AgriGreenhouse-regel levereras.
- [ ] Håll växthusets tankreferenser kompletta vid set/remove/flytt: flera tankar
      per växthus, högst ett växthus per tank. Varje tank behåller egen mätentitet;
      inga tankvärden aggregeras till växthusets mätdata.
      Vald arkitektur: modellneutral, tenant-isolerad relationsprojektion i
      transform-tjänstens PostgreSQL. Verifiera transaktioner, samtidig mutation,
      brokerfel/replay, omstart och bootstrap/återsynk av befintliga relationer.
- [x] Dokumentera namnbaserade ID:n, namnbyten och kollisionsrisk. Regler får
      välja denna policy, men den ska inte införas implicit av adaptern.
- [ ] Utöka preview med konkret operation, rensade attribut och precisa
      validerings-/skiporsaker. Visa effektiv context eller tydlig fallback.
- [ ] Lägg till diagnostik mot aktiv regeluppsättning: vald regel, revision,
      snapshotversion, prioritet och konkurrerande matchningar. Bevara offline-preview.
- [ ] Normalisera preview-indata en gång och använd samma berikning för
      matchningsrapport och transformation. Beskriv eventuella avvikelser från
      produktionsgrindar, såsom tenant-/identitetskontroll.
- [ ] Klassificera fel i preview som användbara klientfel när de beror på regel
      eller exempeldata, inte som generiska interna fel.
- [ ] Uppdatera `README.md`, `assets/docs/openapi.yaml`, relevanta
      konfigurationsexempel och modellkommentarer. Ta bort föråldrade legacy-hänvisningar.
- [x] Uppdatera relationsexemplet i syskonrepots
      `iot-things-v2/docs/CONSUMER-BINDING.md` utan att ändra producentens eventpayload.

## Etapp 9 — verifiering och acceptans

- [ ] Kör `gofmt` på ändrade Go-filer samt `go test ./...`, `go vet ./...`
      och därefter `go fix ./...` i varje berörd Go-modul. Granska fixändringarna
      och kör om formatering, tester och vet om koden ändras.
      Redovisa exakt vad som körts och eventuella hinder.
- [ ] Kör relevanta race-tester för snapshot, omladdning och livscykel.
- [ ] Kör berörda DB-/seedintegrationstester ocachat (`-count=1`) med isolerad
      testdatabas. Kontrollera att testerna faktiskt körs och inte skippas.
- [ ] Verifiera rollback, cancellation, startfel, omstart och samtidiga
      mutationer där ändringarna berör dessa beteenden.
- [ ] Testa hela kedjan med verkliga neutrala källpayloads, levererade regler
      och kontrollerade brokersvar, inklusive partiella och tillfälliga fel.
- [ ] Bevisa regelstyrningen: samma källdata kan få olika modelltyp, ID,
      attribut och relationer genom enbart regeländringar.
- [ ] Bevisa källparitet för de gemensamma funktionerna med dynamiska
      measurement- och things-värden, inte enbart konstanta testvärden.
- [ ] Verifiera tenant-isolering för administration, matchning och brokerskrivning.
- [ ] Verifiera att saknad regel inte ger brokerkontakt, och att godkända regler
      inte faller på konstruktioner som valideringen borde ha avvisat.
- [ ] Kontrollera att adaptrarna saknar modellberoende URN-konstruktioner och
      att protokollvalideringen följer den dokumenterade profilen.

## Separata designbeslut

Följande kräver egna beslut och ska inte smygas in i ovanstående rättningar.

- [x] Besluta om flera matchande regler ska kunna kombineras.
      Beslut: en vinnande regel per observation/händelse; flera entiteter inom regeln.
- [x] Besluta om flerrelationshantering. Beslut: ett växthus kan ha flera tankar;
      en tank kan ha högst ett växthus. Referenserna behöver hanteras som en mängd
      vid set/remove/flytt. Tankmätvärden aggregeras inte till växthuset.
- [x] Besluta om recency-/revisionsskydd mot gamla händelser.
      Beslut: sådant skydd införs inte i nuläget.
- [x] Besluta om join mellan separata measurements/things-event.
      Beslut: event behandlas självständigt; ingen sådan join i denna etapp.
- [x] Besluta om hantering av tidigare målidentiteter vid namn-/typändringar.
      Beslut: standardregler använder stabilt käll-ID, helst GUID/UUID, och
      fasta modellprefix. Namn är attribut och påverkar inte ID. Egna namn-/
      typbaserade ID-mallar är regelns författares ansvar; ingen automatisk
      historik eller städning av tidigare mål-ID:n införs.
- [x] Besluta om valfri versionerad Smart Data Models-schemavalidering.
      Beslut: grundläggande NGSI-LD-validering nu; full versionerad SDM-validering
      är en framtida valfri funktion. Medföljande modellregler granskas uttryckligen.

Övrigt fastställt: `{{id}}` är standard även för measurements. Separata
portentiteter väljs uttryckligen i regeln när de behövs; normalfallet är att
portarnas mätströmmar binds till olika egenskaper i en sak.

## Klart när

- [ ] Nya modellmappningar kan införas via YAML eller regel-API utan Go-ändringar
      så länge de använder dokumenterade, generella motorfunktioner.
- [ ] Medföljande mappningar är fördefinierade standardregler i regelfiler och
      kan förfinas utan modellberoende kod eller specialfall i regelmotorn.
- [ ] Measurements/things-kontrakten behöver inte bära FIWARE-modellkunskap.
- [ ] Regelvalidering och körning har samma uppfattning om stödda konstruktioner.
- [ ] Felaktig NGSI-LD-utdata upptäcks före brokeranrop och ger rätt leveransutfall.
- [ ] Regelsnapshots är konsistenta och skrivbehörighet bevarar tenant-isolering.
- [ ] Preview och loggning kan förklara regelval, transformation och uteblivna skrivningar.
- [ ] Berörda tester, dokumentation och kontrakt är verifierade och uppdaterade.
