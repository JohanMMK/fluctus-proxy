# tarieven.json — bouwen en jaarlijks bijwerken

Fluctus.net CVSO · alle 13 Belgische distributienetbeheerders · 26 records (LS + MS per zone)

## De jaarlijkse update in 5 stappen

De VREG publiceert de tarieflijsten voor jaar N rond **november van jaar N-1**. CWaPE volgt in december.

**1. Download de 8 Vlaamse PDF's** naar `tariefkaarten/<jaar>/`, met exact deze bestandsnamen:

```
FA - 2027 - ELEK.pdf      Fluvius Antwerpen
FHV - 2027 - ELEK.pdf     Fluvius Halle-Vilvoorde
FI - 2027 - ELEK.pdf      Fluvius Imewo
FK - 2027 - ELEK.pdf      Fluvius Kempen
FL - 2027 - ELEK.pdf      Fluvius Limburg
FMV - 2027 - ELEK.pdf     Fluvius Midden-Vlaanderen
FW - 2027 - ELEK.pdf      Fluvius West
FZD - 2027 - ELEK.pdf     Fluvius Zenne-Dijle
```

URL-patroon: `https://assets.vlaamsenutsregulator.be/<jjjj-mm>/<CODE>%20-%20<jaar>%20-%20ELEK.pdf`
waarbij `<jjjj-mm>` de publicatiemaand is (voor tariefjaar 2026 was dat `2025-11`).
Overzichtspagina: https://www.vlaamsenutsregulator.be/elektriciteit-en-aardgas/nettarieven/hoeveel-bedragen-de-distributienettarieven

**2. Werk `wallonie_brussel_<jaar>.json` bij.** Handmatig — zie "Waarom Wallonië/Brussel handwerk blijft". Gebruik de **XLSX**-versies van CWaPE (https://www.cwape.be/node/176, filter op GRD + jaar), niet de PDF's: de PDF-tekstextractie geeft losse getalblokken die je niet betrouwbaar aan een kolom kunt koppelen. Zet `_geverifieerd_op` op de datum waarop je het effectief tegen de bron gelegd hebt.

**3. Werk `VLAAMSE_HEFFINGEN_2026` bij** in `build_tarieven.py` (energiefonds + accijnzen). Die staan **niet** in de VREG-lijst — ze komen van de Vlaamse/federale overheid.

**4. Genereer:**

```bash
python3 build_tarieven.py --pdf-dir ./tariefkaarten/2027 --jaar 2027
```

**5. Lees `tarieven_rapport.txt`.** Bij een blokkerende fout wordt `tarieven.json` **niet** geschreven. Bij een waarschuwing: niet deployen voor je snapt waarom.

Toets daarna af aan een echte klantfactuur van dat tariefjaar.

## Wat er fout was (en waarom dit script bestaat)

De handgemaakte `tarieven.json` had drie fouten die elkaar deels maskeerden. Ze vielen pas op toen een klantcase een LS/MS-verschil van €35.000/jaar gaf dat niemand kon verklaren.

**1. Dubbeltelling van de Elia-transmissiekosten.** De VREG-lijst zegt letterlijk: *"Deze tarieflijst omvat de distributienettarieven inclusief de transmissienetkosten."* Wij telden er een apart `transport_*`-blok bovenop. Op LS **+278,91 €/kW/jaar** bovenop een tarief van 57,10 (**5,9×**), op MS **+60,17** bovenop 101,41 (**1,6×**). Die asymmetrie blies het LS/MS-verschil op met ~€32.000/jaar — meer dan het hele werkelijke verschil (~€3.000). Op de Elindus-factuur zat de simulator daardoor **+65,7%** te hoog.

**2. LS-posten `odv` / `surcharges` / `reactief` stonden op 0.** Voor West moet dat 33,2887 / 1,8227 / 13,4149 zijn.

**3. Het LS-transportblok was een kolomverschuiving van MS.** Bij alle 13 DNB's schoven dezelfde zes waarden exact één positie op.

Fout 2 en 3 maskeerden elkaar: de verschoven `marktintegratie` (27,52 €/MWh) stond toevallig ongeveer waar de ontbrekende ODV (25–33 €/MWh) hoorde. Op de €/MWh-as viel het grotendeels weg tegen elkaar, op de €/kW-as niet. Precies daarom is handwerk hier gevaarlijk.

## De regionale regel — de kern van het transport-verhaal

| regio | regulator | zit Elia-transport in het DNB-tarief? | `transport_*` |
|---|---|---|---|
| Vlaanderen | VREG | **Ja**, expliciet vermeld | **0** |
| Brussel | BRUGEL | **Ja**, als aparte regel in dezelfde grille (0,0214403 €/kWh) | **0** (verwerkt in `odv`) |
| Wallonië | CWaPE | **Nee** — aparte, geperequateerde lijst | **ingevuld** |

Wie één van beide modellen op heel België toepast, krijgt exact de fout die we net rechtgezet hebben.

## Waarom Wallonië/Brussel handwerk blijft

Hun tariefstructuur past niet op ons schema en is niet veilig automatisch te mappen:

- **Wallonië kent geen "toegangsvermogen"** (€/kVA/jaar). Capaciteit wordt op de *gemeten* piek gefactureerd. Onze mapping is een bewuste vertaling: `pointe annuelle` (€/kW/maand) × 12 → `toegangsvermogen`, `pointe mensuelle` × 12 → `maandpiek`. Geverifieerd: CWaPE ORES 2026 `pointe mensuelle` 2,7943885 × 12 = 33,532662 — exact onze `ORES|MS` maandpiek.
- Wallonië differentieert proportioneel naar HP/HC; wij hebben een vlak tarief (we nemen HP, conservatief).
- Brussel rekent in €/kW/**dag** met een degressiviteitscoëfficiënt.

Een parser die dit gokt is erger dan een mens die het bewust invult.

## Wat het script controleert

Alle checks zijn hard (blokkerend) tenzij anders vermeld:

- **Controlewaarden** — 9 handmatig geverifieerde waarden. Wijzigt de VREG-lay-out, dan faalt het script hierop en niet stilletjes op de cijfers.
- **Volledigheid** — elk van de 24 verplichte velden aanwezig en niet-null.
- **Regio-regel transport** — Vlaanderen/Brussel moeten `transport_* = 0` hebben (anders: dubbeltelling). Wallonië met 0 geeft een waarschuwing (ondertelling).
- **Waalse perequatie** — Elia-transport is sinds 01/03/2019 identiek voor álle Waalse GRD's. Wijkt een zone af, dan is er per zone geknoeid of staat er een waarde uit het verkeerde jaar.
- **Plausibiliteit** — maandpiek buiten 10–200 €/kW/jaar (waarschuwing).
- **Structuur** — MS met volumetrisch netgebruikstarief in Vlaanderen, LS met toegangsvermogen, LS met `odv = 0` (dat was fout #2).

Regressietest: de oude `tarieven.json` door deze validator geeft **42 blokkerende fouten**. Hij zou v0 dus tegengehouden hebben.

## Status van de validatie

**Vlaanderen — geverifieerd.** Twee onafhankelijke extracties uit de VREG-PDF's kwamen 14/14 exact overeen. Getoetst aan de Elindus-factuur (Zwaarveld/Hamme, Fluvius Midden-Vlaanderen, MS, april 2026):

| post | factuur | nieuw | oud |
|---|---|---|---|
| Toegangsvermogen | 283,36 | 283,36 | 283,36 |
| Maandpiek | 295,83 | 295,83 | 295,83 |
| Tarief overschrijding | 123,67 | 123,65 | 123,65 |
| Tarief dataservice | 4,74 | 4,74 | 4,74 |
| Overige nettarieven | 93,15 | 60,67 | 205,52 |
| Transport (apart) | — | 0,00 | 413,97 |
| **TOTAAL** | **800,75** | **768,25 (−4,1%)** | **1327,08 (+65,7%)** |

**Openstaand — het resterende gat van 2,03 €/MWh.** De factuur rekent 5,82 €/MWh "Overige nettarieven", wij 3,79 (odv 3,587 + toeslagen 0,2011). Op deze factuur is dat €32/maand. De klant zit bevestigd op de kolom `1-26 kV-net` (toegangsvermogen matcht exact op 35,18), dus het is geen kolomfout. Vermoedelijk bundelt Elindus nog een post onder die noemer. **Uit te zoeken tegen een tweede factuur van een andere leverancier.**

**Wallonië/Brussel — ONGEVALIDEERD.** De distributieposten zijn geverifieerd via de ×12-match, maar het geheel is nooit tegen een echte Waalse of Brusselse factuur gelegd. Zie `_openstaand` in `wallonie_brussel_2026.json`. Zolang Fluctus in Vlaanderen werkt is dat geen blokker, maar simuleer geen Waalse case zonder eerst één factuur te controleren.

## Bestanden

| bestand | rol |
|---|---|
| `build_tarieven.py` | de generator + alle validatie |
| `vlaanderen_2026.json` | geverifieerde VREG-waarden 2026 (bootstrap) |
| `wallonie_brussel_2026.json` | handmatig onderhouden, met bronvermelding per veld |
| `tarieven.json` | **het resultaat** — dit deploy je |
| `tarieven_rapport.txt` | bouwrapport, lees dit altijd |
| `tariefkaarten/<jaar>/` | de gedownloade bronbestanden |

`vlaanderen_2026.json` is een **bootstrap**: de PDF's konden bij het opzetten niet lokaal gedownload worden, dus zijn de waarden via twee onafhankelijke extracties geverifieerd en vastgelegd. Bij de update voor 2027 gebruik je `--pdf-dir` en wordt dit bestand niet meer gebruikt.

> De PDF-parser (`--pdf-dir`) is getest op de VREG-tekst van Fluvius Antwerpen en reproduceert alle 14 waarden exact. Hij is echter nog nooit op een écht PDF-bestand via pdfplumber gedraaid — de tekstextractie van pdfplumber kan licht afwijken. Draai bij de eerste jaarlijkse update `--pdf-dir` en vergelijk met `--vlaanderen-json`; de controlewaarden vangen elk verschil hard af.

## Bronnen

- VREG, tarieflijsten 2026: https://www.vlaamsenutsregulator.be/elektriciteit-en-aardgas/nettarieven/hoeveel-bedragen-de-distributienettarieven
- CWaPE, distributietarieven: https://www.cwape.be/node/176
- CWaPE, transporttarieven (perequatie): https://www.cwape.be/node/179
- Wallonië transport 2026 (alle GRD): https://media.ores.be/ores-cms/sjfflfhm/ed_transport_2026.pdf
- Sibelga grille tarifaire 2026: https://www.sibelga.be/asset/file/a377fe5d-d127-49ea-a3a2-bfa1cf538eee
