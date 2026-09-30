# Plan: regelstyrd transformation till NGSI-LD

## Mål och avgränsning

Measurements och things är källdata och behöver inte känna till FIWARE.
`iot-transform-fiware` mappar dessa data till Smart Data Models genom regler.
Tjänsten får känna till ETSI/NGSI-LD och utföra grundläggande protokollvalidering.

- Modellval ligger i regler: entitetstyp, ID-mall, attribut, värdekällor,
  relationer, enheter, tidskällor och JSON-LD-context.
- Generella funktioner som `{{nameOrID}}`, `safeURI` och `firstPresent` är
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
- [x] Fastställ dokumenterad semantik för `nameOrID`, typvariabler, URI-kodning,
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
- [ ] Behåll regel-ID, revision och uppsättningsversion som körningsmetadata.
      Säkerställ att den publicerade konfigurationen är oföränderlig under körning.
- [ ] Verifiera start, omstart, synkfel och samtidig mutation/omladdning med
      deterministiska tester, relevanta race-tester och isolerad testdatabas.

## Etapp 3 — gemensam regelvalidering och uttryckshantering

Berör främst `internal/application/transform/{schema,validate,variables,engine}.go`.

- [ ] Använd gemensam parsning för malluttryck och fältsökvägar i validering och
      körning. Stöd dokumenterad nästling, citerade nycklar och index; avvisa
      felaktigt balanserade eller otillåtna uttryck.
- [ ] Registrera generella funktioner med dokumenterade argument, returtyper
      och beteende vid saknad källa. Validera tillgängliga variabler per källtyp.
- [ ] Definiera och implementera konsekvent referenshantering för entitets-ID:n.
      Välj explicit ordningskrav eller beroendeupplösning; valideringen får inte
      godkänna framåtreferenser som körningen inte kan lösa.
- [ ] Kontrollera referenser i samtliga relevanta uttryck, inklusive ID, typ,
      property-värden, subproperties och `observedBy`. Upptäck okända namn och cykler.
- [ ] Avvisa kombinationer som saknar effekt eller bryter typkontraktet,
      exempelvis `latlon` på annan måltyp än Geo och metadata som inte stöds.
- [ ] Kontrollera dubbletter och konflikter mellan attribut, samt kollisioner
      med NGSI-LD-strukturfält såsom `id`, `type` och `@context`.
- [ ] Kontrollera statiska konstanter, URI:er, datum och transformargument där
      felet kan upptäckas utan indata, inklusive division med konstant noll.
- [ ] Gör regeluppsättningskontroller med hänsyn till tenant och matchning.
      Likadana ID-mallar i åtskilda tenants får inte ge falska konflikter.
- [ ] Verifiera att varje accepterad konstruktion har definierat körningsbeteende
      och att felmeddelanden pekar ut regel, entitet, attribut och uttryck.

## Etapp 4 — källtrogna measurement- och things-adaptrar

Berör `internal/application/measurements` och `internal/application/things`.

- [ ] Ta bort `relation.from.waterQualityObserved` och konstruktionen av
      `urn:ngsi-ld:WaterQualityObserved:...` från relationsadaptern.
- [ ] Exponera generella descriptors för både `relation.from` och `relation.to`:
      käll-ID, rå typ, undertyp, namn, alternativnamn, kategori samt relevant
      plats-, metadata- och variantinformation ur producentkontraktet.
- [ ] Exponera relevant descriptor-, event- och proveniensdata konsekvent för
      values och lifecycle. Rätta fält som avkodas men försvinner före regelkörning.
- [ ] Bevara råa namn och typer. Gör det tydligt vilka variabler som är råa och
      vilka som är beräknade genvägar; undvik implicit ändring av rå `type`.
- [ ] Exponera geometridata konsekvent för Point och övriga stödda geometrier.
      Koordinatgenvägar får komplettera, men inte dölja, den ursprungliga geometrin.
- [ ] Bevara measurement-resursers fullständiga identitet, objekt, kanal,
      värde, enhet och observationstid. Gör relevanta uppgifter åtkomliga för regler.
- [ ] Fastställ `port` mot det verkliga källkontraktet. Fyll det från korrekt
      källa eller avvisa stöd som inte finns; anta inte att kanal betyder port.
- [ ] Behåll gemensam flerobjektspartitionering för produktion och preview.
      Verifiera att resurser och metadata aldrig hämtas från fel observation.
- [ ] Bevara kontraktsregeln att ogiltiga things-värden inte ersätter aktuella.
      Exponera kvalitet/proveniens utan att kringgå filtreringen av skrivbara värden.
- [ ] Ta bort oanvänd `Thing.EntityID()` och granska kvarvarande äldre typer
      utifrån faktisk användning innan de tas bort eller behålls.
- [ ] Verifiera adaptrarna mot producenternas kontrakt med neutrala testdata
      som inte förutsätter FIWARE-ID:n eller modellnamn i källpayloaden.

## Etapp 5 — konsekvent matchning, värdekällor och tid

Berör främst `internal/application/transform/{engine,measurement,thing,values,functions}.go`.

- [ ] Implementera det fastställda prioritetskontraktet. Hantera avgränsningar
      som env och port konsekvent och diagnostisera överlappande/skuggade regler.
- [ ] Avvisa matchningsfält som källtypen inte kan tillhandahålla. Definiera
      explicit om lifecycle krävs eller om wildcard stöds.
- [ ] Lägg till regelstyrd avgränsning för relationens målsida, exempelvis måltyp
      eller generella fältpredikat, så att tankregeln kan kräva växthus som mål.
- [ ] Använd en gemensam värdekällsutvärderare för properties, `derive.of` och
      Structured. Alla tillåtna källor och modifierare ska faktiskt utvärderas.
- [ ] Definiera `required`, `when: present`, tomma värden och saknad mallkälla
      konsekvent. Gör aggregatens beteende vid saknade ingångar explicit.
- [ ] Skilj saknad källa från typfel. Aggregat får inte tyst ignorera felaktiga
      typer utan ett dokumenterat regelval.
- [ ] Säkerställ att Structured-delvärden konverteras enligt deklarerad typ.
      Avvisa metadata/funktioner på delvärden som inte har implementerad semantik.
- [ ] Exponera observationstid, publiceringstid och förändringstid separat.
      Låt regler välja tidskälla för både attributvärden och `observedAt`.
- [ ] Gör tidsmetadata konsekvent över stödda property-typer och dokumentera
      vad `header`, `record`, `message`, `field` och `none` betyder för varje källa.
- [ ] Bevara tillgänglig subsekundprecision. Verifiera relativ SenML-tid,
      saknad tid och flera observationer med olika mättider.
- [ ] Gör källenheter åtkomliga och låt regler ange målens enheter och eventuell
      omvandling. Märk inte om enheten utan motsvarande dokumenterad omvandling.
- [ ] Flytta measurement-vägens broker-/tokenförberedelse efter transformation,
      så att ingen match eller inga skrivningar kan hanteras utan brokerberoende.

## Etapp 6 — generisk NGSI-LD-byggning och grundvalidering

- [ ] Låt Geo alltid använda regelns `target`, oavsett koordinatkälla och
      `observedAt`. Ta bort kodvägar som implicit väljer attributet `location`.
- [ ] Inför generisk relationslistform, exempelvis `array: true` på Relationship.
      Skalär blir enelementslista; befintlig lista får inte dubbelnästlas.
- [ ] Validera relationsreferenser och listelement strikt enligt vald profil.
      Konvertera inte godtyckliga tal/booleska värden till URI:er och tappa inte
      felaktiga listelement tyst.
- [ ] Kontrollera finita tal efter konvertering och transformation, även i listor.
- [ ] Kontrollera datum/tid och grundläggande GeoJSON-struktur, inklusive
      geometriernas koordinatstruktur och tillämpliga dimensions-/ringkrav.
- [ ] Validera renderade entitets-ID:n, typer, attribut och metadata innan
      brokeranrop. Använd samma utdata-validerare i preview.
- [ ] Säkerställ att varje property-typ serialiseras enligt den dokumenterade
      NGSI-LD-profilen. Testa faktisk JSON, inte bara att en dekoratör skapades.
- [ ] Dokumentera begränsningar såsom datasetId, avancerade attributformer,
      context-former och temporal lagring; stöd ska inte antydas av schemat om det saknas.
- [ ] Håll Smart Data Models-validering åtskild från protokollvalidering.
      `required` är en mappningsregel, inte ett bevis på full modellkonformitet.

## Etapp 7 — brokerskrivning och leveransutfall

Berör `internal/infrastructure/contextbroker`, messaging-handlers och vid behov
det versionslåsta `context-broker`-biblioteket.

- [ ] Bevara och tolka partiella brokersvar, inklusive HTTP 207. En delvis
      misslyckad skrivning får inte rapporteras som full framgång.
- [ ] Klassificera irreparabla indata-/transformations-/utdatafel separat från
      tillfälliga beroendefel. Verifiera ack, retry och slutlig felhantering i den
      låsta messaging-versionen innan klassificeringen ändras.
- [ ] Dokumentera create-if-absent, merge-or-create, attributrensning och delete.
      Verifiera operationernas HTTP- och felbeteende mot vald NGSI-LD-profil.
- [ ] Inför tydliga tidsbudgetar för broker- och tokenanrop. Ersätt eventuell
      fast väntan med dokumenterat, cancellation-medvetet beteende där väntan behövs.
- [ ] Verifiera partiellt lyckade flerskrivningar och replay. Beskriv att
      idempotens för en isolerad operation inte ger ordningsoberoende korrekthet.
- [ ] Förmedla context och trace till klientanrop. Logga regel-ID, regelrevision,
      snapshotversion och relevanta käll-/mål-ID:n utan hemligheter eller hela payloads.

## Etapp 8 — modellregler, preview och dokumentation

- [ ] Uppdatera tank/växthus-regeln till regelstyrd referensmall och listform.
      Modellnamn och URN-prefix får endast finnas i regeln och modelltesterna.
- [ ] Uppdatera medföljande ID-mallar till dokumenterade hjälpfunktioner och
      verifiera konsistens mellan values-, relations- och lifecycle-regler.
- [ ] Rätta modellregler som läser oexponerade fält, exempelvis POI-beskrivning,
      och gör platsmappning konsekvent för Point och Polygon.
- [ ] Gå igenom modellernas tidskällor, enheter och obligatoriska värdekällor.
      Redovisa olösta modellfrågor, exempelvis greenhouse-CO2, i reglernas dokumentation.
- [ ] Dokumentera namnbaserade ID:n, namnbyten och kollisionsrisk. Regler får
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
- [ ] Uppdatera relationsexemplet i syskonrepots
      `iot-things-v2/docs/CONSUMER-BINDING.md` utan att ändra producentens eventpayload.

## Etapp 9 — verifiering och acceptans

- [ ] Kör `gofmt` på ändrade Go-filer samt `go test ./...` och `go vet ./...`
      i varje berörd Go-modul. Redovisa exakt vad som körts och eventuella hinder.
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

- [ ] Besluta om flera matchande regler ska kunna kombineras; definiera i så
      fall ordning, attributkonflikter och operationskonflikter.
- [ ] Besluta om flerrelationsaggregation. `array: true` löser representationen,
      inte set/remove för flera tankar; dokumentera entank-antagandet tills vidare.
- [ ] Besluta om recency-/revisionsskydd mot gamla händelser, inklusive sena
      values efter delete och replay som blandas med nyare uppdateringar.
- [ ] Besluta om någon framtida join mellan separata measurements/things-event
      behövs och vem som då äger tillstånd, lagring och återhämtning.
- [ ] Besluta om hantering av tidigare målidentiteter vid namn-/typändringar.
- [ ] Besluta om valfri versionerad Smart Data Models-schemavalidering utöver
      motorns grundläggande NGSI-LD-validering.

## Klart när

- [ ] Nya modellmappningar kan införas via YAML eller regel-API utan Go-ändringar
      så länge de använder dokumenterade, generella motorfunktioner.
- [ ] Measurements/things-kontrakten behöver inte bära FIWARE-modellkunskap.
- [ ] Regelvalidering och körning har samma uppfattning om stödda konstruktioner.
- [ ] Felaktig NGSI-LD-utdata upptäcks före brokeranrop och ger rätt leveransutfall.
- [ ] Regelsnapshots är konsistenta och skrivbehörighet bevarar tenant-isolering.
- [ ] Preview och loggning kan förklara regelval, transformation och uteblivna skrivningar.
- [ ] Berörda tester, dokumentation och kontrakt är verifierade och uppdaterade.
