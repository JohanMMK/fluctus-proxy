'use strict';
// ============================================================================
// FLUCTUS PROXY SERVER
// Versie:        v15.22.0 (batterij-sweep in fysieke eenheden 120 kW / 260 kWh)
// Wijziging v15.22.0 vs v15.21.0: de batterij groeit voortaan in gehele eenheden van
//   120 kW / 260 kWh (Johan §4.3) i.p.v. continu in kWh. _dimZet snapt de maat op een
//   geheel aantal eenheden (min. 1), _opstellingUi start de advies-batterij op hele
//   eenheden, en batterijCustom draagt nu 'aantal_batterijen'. De zoeklus (groeien/
//   krimpen/verfijnen) blijft ongewijzigd; ze quantiseert alleen. ⚠ De dispatch-uitkomst
//   met deze discrete stappen op een echt kwartierprofiel is NIET in deze omgeving
//   gedraaid — vereist één live smoke-test vóór productie (zie deploy-checklist).
// Versie:        v15.21.0 (LS/MS-poort: GET /api/ls-ms-poort — arithmetiek, geen sim)
// Wijziging v15.21.0 vs v15.20.4: de LS/MS-keuze is een POORT die je vooraf beslist
//   (overdracht §4). Nieuwe endpoint /api/ls-ms-poort geeft, puur uit de tariefkaarten:
//   netkosten LS vs MS/jaar bij (verbruik E, piek P), het kantelpunt E*=a·P+b, en de
//   payback van de cabine (€108.000). Geen dispatch. Geverifieerd tegen de kantelpunt-tabel
//   uit §4 (a/b/E* exact voor alle 8 Vlaamse zones; Δvast Midden-Vl. +2.224). Wallonië/Brussel
//   worden als niet-gevalideerd gevlagd (gevalideerd:false, openstaand punt 54). Geen
//   wijziging aan bestaande endpoints of de dispatch.
// Versie:        v15.20.4 (opstelling 3 'ms_batterij' verwijderd — derde is altijd 'mix')
// Wijziging v15.20.4 vs v15.20.3: de LS/MS-keuze is een POORT, geen scenario-as
//   (overdracht §4 + §4bis.B). De vroegere 'ms_batterij'-opstelling zette LS-met-batterij
//   tegen MS-met-batterij — precies de vergelijking die we niet willen. Verwijderd uit
//   OPSTELLING_LABEL, _isBatterijOpstelling, _opstellingUi (de spanning='MS'-omzetting) en
//   de vergelijking (tariefkaart_effect_* vervalt). _derdeOpstelling geeft nu altijd 'mix'
//   — Johans batterij-sweep (binnengebied tussen verzwaren en volledige batterij), op de
//   vooraf vastgelegde tariefkaart. 'mix' werkt identiek op LS en MS. Geen gedragswijziging
//   voor opstelling 1 en 2, noch voor sites die al op MS stonden (daar was de derde al 'mix').
// Versie:        v15.20.3 (fix: _grdNaarZone bestond niet — regio-tarieven gaf 500)
// Wijziging v15.20.3 vs v15.20.2: /api/regio-tarieven verwees naar _grdNaarZone(), een
//   helper die niet bestaat. De ternary-guard (_grdNaarZone ? ... : null) beschermt daar
//   NIET tegen: een niet-gedeclareerde identifier gooit een ReferenceError, geen
//   undefined. Nu dezelfde zone-afleiding als _kiesTarieven: GRD_NAAR_ZONE[grd] met
//   terugval op de naam zonder 'Fluvius '-prefix.
// Versie:        v15.20.2 (regio-tarieven geeft een oordeel: welke tariefkaart draait er?)
// Wijziging v15.20.2 vs v15.20.1: /api/regio-tarieven gaf losse getallen terug die je
//   zelf moest duiden. Daardoor kon de proxy op de OUDE tarieven.json blijven draaien
//   zonder dat iemand het merkte — het kwam pas uit toen een klantcase een
//   capaciteitskost van 33.292 EUR toonde op een LS-aansluiting van 100 kVA (plafond:
//   5.012 EUR). Nu geeft de endpoint een expliciet oordeel (OK / OUDE_KAART / VERDACHT /
//   GEEN_KAART) op basis van de regio-regel: Vlaanderen en Brussel horen transport_* = 0
//   te hebben (VREG-kaart bevat de transmissiekosten al), Wallonie juist wel. Plus
//   tariefjaar, bron en de kerncijfers. Bedoeld als jaarlijkse deploy-check in november.
// Wijziging v15.20.1 vs v15.20.0:
//   1. ADAPTIEVE DERDE OPSTELLING. 'ms_batterij' was altijd opstelling 3, maar staat de
//      site AL op MS dan is dat een exacte kopie van opstelling 2 (zelfde batterij,
//      zelfde aansluiting, zelfde kaart, geen cabine): vijf sim-runs om hetzelfde getal
//      twee keer te tonen, en een "keuze" die geen keuze is. Nu: al op MS -> 'mix'.
//      Opstelling 1 en 2 zijn de twee UITERSTEN (alles-aansluiting vs alles-batterij);
//      het optimum ligt bijna altijd in het binnengebied, want kWh is duur (350 EUR) en
//      kVA goedkoop (100 EUR). _dimensioneerMix doorloopt drie mengverhoudingen, zoekt
//      per punt de kleinst werkende batterij, en kiest op TOTALE eigendomskost
//      (investering + factuur x horizon). De constanten komen uit de frontend
//      (input._investering) zodat ze op een plek staan; ontbreken ze, dan valt de keuze
//      terug op het middelste punt MET expliciete waarschuwing i.p.v. een vals optimum.
//   2. KRIMPFASE. De zoeklus groeide alleen. Voor opstelling 2 klopt dat meestal (de
//      vuistregel is te klein), maar bij een mix met ruimere aansluiting volstond de
//      startbatterij vaak meteen — en die werd dan geaccepteerd terwijl de helft ook had
//      gekund. Mix 67% kreeg zo 279 kWh waar 130 volstond: 149 kWh en ~52.000 EUR
//      fantoom-capex, wat juist de kVA-rijke mixen onterecht afstrafte. Dat vertekende
//      exact de vergelijking waarvoor de mix bestaat (TCO-spreiding 55.000 -> 10.000 EUR).
//      Nu: slaagt de startmaat meteen, dan krimpen tot het NIET meer past, daarna binair
//      verfijnen. Symmetrisch aan de groeifase.
// Wijziging v15.20.0 vs v15.19.1:
//   1. OPSTELLING 3 — 'ms_batterij'. Zelfde batterij en zelfde aansluiting als opstelling
//      2, maar op de MS-tariefkaart. Reden: op LS is de distributie grotendeels
//      VOLUMETRISCH (netgebruik 23-28 + ODV 24-33 EUR/MWh), op MS is dat nul en zit alles
//      in EUR/kW. Een batterij vlakt vermogen af — de as waar MS zijn geld haalt — maar
//      kan niets doen aan een tarief per MWh. Op LS wordt hij dus afgestraft op een as
//      waar hij geen invloed op heeft. Kantelpunt (Fluvius West 2026): ~93 MWh/jaar; elk
//      laadplein zit daarboven. Opstelling 2 en 3 verschillen ENKEL in de spanning, zodat
//      vergelijking.tariefkaart_effect_* exact de LS/MS-keuze isoleert. De cabinekost
//      (~90.000 EUR) hoort in de investeringsvergelijking, niet in de sim.
//      Dimensionering, haalbaarheidscriterium en zoeklus zijn identiek aan opstelling 2.
//   2. ASYNC + LIVE LOG. Drie opstellingen x tot 7 sim-runs loopt op tot enkele minuten:
//      te lang voor een blokkerende POST (proxy kapt af) en de verkoper keek al die tijd
//      naar een dood scherm. POST met _async:true geeft nu meteen een job_id terug en
//      draait door in de achtergrond; GET /api/sim-voortgang/:id geeft status + logregels.
//      Zonder _async blijft het synchrone pad exact zoals vroeger (geen breuk).
// Wijziging v15.19.1 vs v15.19.0: het criterium "0 verloren dagen" was FOUT voor beide
// Wijziging v15.19.1 vs v15.19.0: het criterium "0 verloren dagen" was FOUT voor beide
//   opstellingen. Opstelling 1 wordt ongestuurd beoordeeld; simulator.py bouwt de EV-last
//   dan op een onbeperkte aansluiting (1e12) → nooit een tekort, nooit een verloren dag,
//   de site betaalt gewoon overschrijding. De lus stopte dus bij iteratie 0 en liet een te
//   kleine verzwaring staan. Opstelling 2 laat simulator.py ZELF de aansluiting verhogen
//   als de batterij tekortschiet (toegangsvermogen_verhoogd_kw) — ook onzichtbaar in
//   verloren_dagen. Nu: _opstellingHaalbaar toetst per opstelling op respectievelijk
//   overschrijdingskost = 0 en geen geforceerde aansluitingsverhoging.
// Wijziging v15.19.0 vs v15.18.0: _dimensioneerTotHaalbaar groeit de bepalende maat van
//   ELKE opstelling tot de LP-dispatch 0 verloren dagen meldt (max 4 iteraties, +30%/stap).
//   Opstelling 1 wordt beoordeeld ZONDER sturing (dat is het basisscenario), opstelling 2
//   MET sturing 2 (zo wordt de batterij ingezet). Voorheen kwamen beide maten uit vuistregels
//   op gemiddelden, terwijl de dispatch per kwartier rekent — resultaat: 260/365 verloren
//   dagen, overschrijdingskosten, en een vergelijking tussen twee falende opstellingen.
//   Bij groeien wordt de LS/MS-grens opnieuw getoetst. Lukt het niet binnen 4 iteraties, dan
//   komt dat eerlijk terug in opstellingen[x].dimensionering.haalbaar = false.
// Wijziging v15.18.0 vs v15.17.0: _opstellingUi zet opstelling 1 ('verhogen') op de
//   MS-tariefkaart zodra het benodigde toegangsvermogen boven 100 kVA gaat — LS bestaat
//   daarboven niet. Voorheen rekende een verzwaring naar bv. 250 kW nog op LS-tarieven
//   en viel opstelling 1 dus veel te goedkoop uit. Opstelling 2 (batterij) blijft op de
//   spanning uit stap 9, want die vermijdt de verzwaring juist. Elke opstelling geeft nu
//   ook config{spanning, spanning_omgezet, aansluiting_kva, toegangsvermogen_kw, batterij}
//   terug zodat de UI kan tonen dát er twee verschillende tariefkaarten vergeleken worden.
// Wijziging v15.17.0 vs v15.16.0: POST/GET /api/factuuranalyse bewaren en halen de
//   VOLLEDIGE factuuranalyse (incl. de 3 profielen-arrays, ~700 KB) als JSON-object op
//   uit de private bucket. In het scenario komt enkel het pad. Zo is een heropend
//   onderhandelingsmarge-rapport identiek aan het origineel (zelfde marge, zelfde
//   heatmaps) i.p.v. herberekend — geen drift. Auth-vereist; max 8 MB.
// Wijziging v15.16.0 vs v15.15.8: de geüploade factuur wordt na een geslaagde scan
//   bewaard in de PRIVATE Supabase-bucket (env FACTUREN_BUCKET, default 'facturen');
//   de verwijzing komt in baseCase.factuur_bestanden[]. Nieuw endpoint
//   GET /api/factuur-bestand?pad=... geeft een kortlevende signed URL (10 min) terug,
//   auth-vereist. BEWUST niet in de GitHub-scenario-repo: git-historiek is onuitwisbaar
//   (AVG) en elke auto-save zou megabytes committen. Opslag is best-effort: een fout
//   laat de extractie nooit mislukken.
// Wijziging v15.15.8 vs v15.15.7: de 504-foutmelding bij /api/factuur-extract vermeldt
//   niet langer een harde "30s" (de timeout in factuur/extract.js is verhoogd naar 120s).
// Versie:        v15.15.7 (gecontracteerd toegangsvermogen ≠ fysiek aansluitvermogen)
// Wijziging v15.15.7 vs v15.15.6: buildSimInput geeft nu aansluiting.toegangsvermogen_kw
//   door (facturatiebasis Groep B/D, uit de factuur; ui.toegangsvermogen_kw), LOS van
//   max_afname_kw_hard (fysiek aansluitvermogen = dispatch-cap). Vroeger factureerde de
//   sim het toegangsvermogen op het fysieke aansluitvermogen → een klant met 100 kVA
//   aansluiting maar 35 kW gecontracteerd kreeg te hoge netkosten in de 'betere' factuur,
//   terwijl die t.o.v. de bestaande factuur (zelfde verbruik) gelijk horen te zijn. Bij
//   de 'verhogen'-opstelling wordt toegangsvermogen_kw mee opgetrokken (nieuwe basis).
//   Zonder factuurwaarde → terugval op aanslKw (ongewijzigd gedrag).
// Versie:        v15.15.6 (SHA-conflict-retry bij scenario-commit → geen 409 meer)
// Wijziging v15.15.6 vs v15.15.5: _scenariosGithubWrite hertest bij HTTP 409/422
//   ("is at X but expected Y") met een VERSE blob-sha (_scenariosGithubSha,
//   cache-buster) en retry't de PUT max 3× met backoff. Loste de intermittente
//   "GitHub-commit faalde: HTTP 409"-fout op bij snel opeenvolgende scenario-saves.
// Versie:        v15.15.5 (tariefkaart-selectie per netbeheerder + spanning)
// Wijziging v15.15.5 vs v15.15.4: buildSimInput koos ALTIJD TARIEVEN_LS →
//   MS-klanten kregen LS-tarieven (toegangsvermogen = 0). Nu selecteert
//   _kiesTarieven(grd, spanning) de juiste kaart uit data/tarieven.json
//   ("<zone>|<spanning>"), met GRD_NAAR_ZONE-alias + veilige fallback. Ook het
//   overschrijdingstarief in aansluiting volgt nu de gekozen kaart.
//   ⚠ De GRD→zone-alias bevat best-guesses (Enet/Gaselwest/Mechelen/Brabant/IECBW)
//   die Johan nog moet bevestigen.
// Versie:        v15.15.4 (laadpleinen doorgeven + profiel-normalisatie + sim-3)
// Wijziging v15.15.4 vs v15.15.3: buildSimInput geeft ui.laadpleinen door aan
//   simulator.py v1.8 (flexibele EV-laadvraag). Zonder lijst = inert.
// Wijziging v15.15.3 vs v15.15.2: profiel-lookup matcht nu op genormaliseerde
//   naam. Root cause bug 1: de profielenlijst toont nette namen met spaties
//   ("Boer aardappel", "Opslag / Magazijn") terwijl de bestanden underscores
//   gebruiken (boer_aardappel.json, opslag___magazijn.json). De oude
//   exact/lowercase-lookup vond enkel enkelwoord-profielen; meerwoord-profielen
//   gaven 404 op POST /api/factuur-staffel-bepalen, waardoor de verkoper in de
//   factuur-modal geen profiel kon "aanvaarden". Stap 3 (GET /api/profiel)
//   verborg ditzelfde probleem via zijn MARKT-fallback (rekende dan fout op het
//   default-profiel). Fix: gedeelde _profielFileNormalize() in beide routes +
//   MARKT-fallback als laatste vangnet in _laadProfielKwartier.
// Wijziging v15.15.2 vs v15.15.1: GET /api/projecten doet read-through naar
//   de GitHub projecten/-directory wanneer de in-memory cache leeg is (na
//   elke Railway-restart). Pre-existente bug, zichtbaar geworden door de
//   9a-deploys. Gevonden bij deploy-stap A5 (07/07).
// Basis:         v15.15.1 (hotfix sessie 9a — CORS Authorization-header)
// Wijziging v15.15.1 vs v15.15: Access-Control-Allow-Headers uitgebreid met
//   'Authorization'. Zonder die header blokkeerde de browser-preflight ALLE
//   cross-origin calls met Bearer-token (fluctus.net -> railway.app):
//   migratie-endpoint, app-access/check, activity-log en scenario-routes.
//   Gevonden bij deploy-stap A4 (07/07). Geen andere wijzigingen.
// Basis:         v15.15 (sessie 9a — Fluctus App Access / Manager Control Plane)
// Geproduceerd:  2026-07-06
// Doelomgeving:  Railway (lucid-amazement-production.up.railway.app)
// Repo:          JohanMMK/fluctus-proxy (auto-deploy bij merge naar main)
// Vereist:       Simulator.txt v1.20+ / simulator.py v1.7+ / Supabase-migratie
//                supabase_migratie_9a.sql uitgevoerd (apps, user_app_access,
//                app_activity_log).
// Wijzigingen v15.15 vs v15.14.1 (SESSIE 9a):
//   - NIEUWE ENV: SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY (Railway env-vars).
//     Optioneel FLUCTUS_AUTH_ENFORCE ('true'/'false', default 'true') als
//     rollback-schakelaar. Zonder geldige Supabase-env valt enforcement
//     automatisch UIT met een console-warn — server brickt nooit op auth.
//   - POST /api/app-access/check — valideert Supabase-JWT, leest profiel,
//     checkt user_app_access. Managers (role='manager', status='active')
//     hebben impliciet toegang tot alle apps. Best-effort certificaten-lijst.
//   - POST /api/app-activity/log — best-effort audit-insert in
//     app_activity_log. Antwoordt ALTIJD 200 {ok}, ook bij Supabase-fout
//     (non-blocking by design, zie roadmap 9a).
//   - GET  /api/manager/activity — manager-only log-viewer met filters
//     verkoper/app/klant_btw/van/tot/limit, namen verrijkt uit profielen.
//   - SCENARIO-OWNERSHIP: /api/scenarios, /api/scenario, /api/scenario-bewaren
//     en /api/scenarios-batch-bewaren vereisen nu een geldige Bearer-token
//     (bij enforcement aan). Verkopers zien/schrijven enkel scenarios met
//     eigen owner_uid; managers zien alles. Nieuwe saves worden automatisch
//     gestempeld met data.owner_uid + data.owner_naam.
//   - POST /api/admin/migrate-scenario-owners — eenmalige, manager-only
//     migratie: alle scenario-JSONs in fluctus-scenarios zonder owner_uid
//     krijgen owner_uid = Johan Konings (auth-uid 36802fa6-..., gecheckt in
//     Supabase Auth 06/07 — roadmap-UUIDs waren geen auth-ids). Idempotent.
//   - Token-validatie-cache 60s (in-memory) om Supabase-roundtrips per
//     wizard-klik te vermijden.
//   - FIX VÓÓR DEPLOY (naspeuring Academy-broncode): profiel-lookup gebruikt
//     tabel 'profiles' + kolom auth_uid (Academy-realiteit), NIET 'profielen'
//     + id (roadmap-naam). Role-waarden zijn 'manager'/'seller'; er is geen
//     status-kolom (default 'active'). Fallback op 'profielen' blijft staan.
//   - GEEN wijziging aan simulatie-, markt- of factuur-routes.
// Wijzigingen v15.14.1 vs v15.14 (HOTFIX productie-bug 503 Marktdata):
//   Symptoom: HTTP 503 "Marktdata nog niet geladen" bleef permanent hangen.
//   Oorzaak: laadMarktdata() faalde stil bij koude Railway-start (prebuild >60s
//     of cache-fetch fout) → MARKT bleef null → elke /api/nominatie-sim gaf 503.
//     De melding beloofde "probeer over 30s opnieuw" maar niets laadde ooit
//     opnieuw (geen retry-mechanisme).
//   FIX 1: status-tracking (MARKT_STATUS init/loading/ok/failed) + automatische
//     retry-ladder. Bij falen retry na 30s, daarna elke 5 min tot geladen.
//     Server herstelt zichzelf zonder redeploy.
//   FIX 2: prebuild-timeout 60s → 120s (koude ENTSO-E/Elia fetch kan traag zijn).
//   FIX 3: informatieve 503 reflecteert werkelijke status (loading vs failed
//     + laatste_fout). Health + / tonen markt_status.
//   FIX 4: nieuw POST /api/markt-reload voor handmatige reload zonder redeploy.
// Wijzigingen v15.14 vs v15.13.1:
//   - HEADER-BUMP voor sessie 7. Geen functionele wijziging in de
//     buildSimInput payload-structuur: simulator.py v1.7 leest de tarieven
//     uit inp.netbeheer.tarieven (al aanwezig in v15.13.1 payload) en bouwt
//     daarmee de monthly_peak-kost-term in de LP-objective op.
//   - Resultaat-structuur uitgebreid: lp_diagnostics bevat nu naast de
//     bestaande dag-niveau-velden ook totaal_maanden / optimal_maanden /
//     retry1_maanden / retry2_maanden / verloren_maanden. Server.js geeft
//     deze ongewijzigd door (geen serialisatie-specifieke handling nodig).
//   - Verwachte impact SMARTUNIT_v10 Sc4: subtotaal €14.898/jaar → ≤€13.500/jaar
//     (+€1.500-2.500 extra besparing per jaar). Zie sessie 7 acceptatie-criteria.
// Wijzigingen v15.13.1 vs v15.13:
//   - PROFIELPIEK-HEURISTIEK voor max_afname_kw_zacht in buildSimInput.
//     buildSimInput berekent profielpiekKw uit het basisprofiel × jaarverbruik
//     en stelt max_afname_kw_zacht = min(aanslKw, ceil(profielpiekKw × 1.20))
//     in plaats van het oude aanslKw. max_afname_kw_hard blijft aanslKw.
//   - DOEL: voorkomt dat BSP-modus de aansluitingscap volledig benut voor
//     BESS-laden, wat onnodig de Groep B (maandpiek) kost de hoogte injaagt.
//     Bewezen op SMARTUNIT_v10 Sc4: gem(maandpieken_afname) was 126 kW i.p.v.
//     profielpiek 92 kW = +€3.578/jaar onterechte capaciteit. LP voelt nu
//     pen_afname_zacht × overschrijding boven 111 kW en kiest andere laad-momenten.
//   - UI-override: ui.max_afname_zacht_kw / ui.maxAfnameZachtKw heeft voorrang.
//     Sales kan dit handmatig finetunen per scenario indien gewenst.
//   - BUFFER 20%: dekt aanvullingen (laadinfra/elektrificatie niet in basisprofiel),
//     kwartier-variabiliteit, sporadische werkdag-pieken. Conservatief.
//   - Anti-regressie: Sc1-3 zonder PV/BESS: profielpiek × 1.20 < aanslKw → zacht
//     is dezelfde of lager dan voorheen. Bij identieke LP-resultaten geen impact
//     (LP raakt zacht-cap niet). Bij BSP-pad merkbaar lagere maandpieken.
//   - Sessie 7: optie 3 (Groep B-kost in LP-objective via monthly-peak constraint).
// Wijzigingen v15.13 vs v15.12.1-diag:
//   - DIAG-blok in /api/nominatie-sim verwijderd (was tijdelijk voor RCA
//     sessie 6 toegangsvermogen-bug; root-cause nu opgelost in simulator.py v1.6).
//   - ASYMMETRIE afname ≠ injectie in buildSimInput aansluiting-blok:
//       max_afname_kw_*  = aanslKw (contractueel toegangsvermogen)
//       max_injectie_kw_* = maxInjectieKw (default = pvInverterKw + batt.kw,
//                          override via ui.max_injectie_kw)
//     Reden: Belgisch tarief weegt afname-piek (Groep B/D) zwaar, injectie-cap
//     is fysiek bepaald door inverter-vermogen. v1.5 stuurde beide identiek,
//     wat scenario's met PV+BESS achter een kleine aansluiting onnodig duur
//     deed lijken (LP injectie-cap = afname-cap maakt curtailment kunstmatig).
//   - NIEUW veld pv.inverter_kw doorgegeven aan simulator.py (default via
//     _invTabel: 125→96, 150→115, 200→153, anders 0.77 × kWp).
//   - Anti-regressie: bij payloads zonder ui.pv_inverter_kw / ui.max_injectie_kw
//     worden defaults berekend op basis van pvKwp en batt.kw — identieke
//     scenario's met catalogus-batterijen krijgen consistent grotere
//     max_injectie_kw_hard dan v15.12 (= zelfde aanslKw). Voor Sc1-3 zonder
//     PV/BESS: maxInjectieKw = max(1, 0+0) = 1, wat injectie effectief blokkeert.
//     Voor afname-only scenarios geen verschil op factuur.
// Wijzigingen v15.12.0 vs v15.11.1:
//   - BESS-CUSTOM detectie in buildSimInput: wanneer ui.batterijId === 'CUSTOM'
//     en ui.batterijCustom aanwezig is, gebruik die dict in plaats van de
//     catalogus-lookup. Stuurt {kw, kwh, dod_pct, rte_pct, capex_eur, max_cycli}
//     door naar simulator.py — dezelfde shape die simulator.py v1.5 al accepteert.
//     Anti-regressie: catalogus-lookup-pad (ui.batterijId !== 'CUSTOM') is exact
//     onveranderd. Smartunit/Steylaert/Advario regressie-baselines blijven gelden.
//   - NIEUWE endpoint POST /api/scenarios-batch-bewaren: wrapper rond bestaande
//     _scenariosGithubWrite. Schrijft N scenario's sequentieel naar
//     fluctus-scenarios repo. Returnt per scenario {scenario, ok, source,
//     message}. Best-effort: als één commit faalt, gaat de batch door en
//     wordt het resultaat per scenario gerapporteerd. Body-shape:
//       { project: 'SMARTUNIT',
//         scenarios: [{scenario: '2_DynamischContract_01-26', data: {...}},
//                     {scenario: '3_DynamischContract_12M',  data: {...}},
//                     {scenario: '4_Voorstel_PV_BESS',       data: {...}}] }
// Wijzigingen v15.11.1 vs v15.11:
//   - periodeTot inclusief→exclusief conversie (+1 dag) bij jaar='specifiek'
//     (anders mist simulator de laatste factuurdag — bv. 31 jan)
// Wijzigingen v15.11 vs v15.10:
//   - _sliceMarktVoorPeriode: marktdata exact gesliceerd op simPeriode
//     (fixt ook latente kalenderjaar-bug van v15.10)
//   - Scenario-routes: read-through cache + GitHub persistentie
//     naar JohanMMK/fluctus-scenarios (was alleen in-memory in v15.10)
//   - Simulatieperiode-modus 'specifiek' doorgestuurd naar simulator.py v1.5
// ============================================================================

const express     = require('express');
const compression = require('compression');
const { spawn, execFileSync } = require('child_process');
const path        = require('path');
const fs          = require('fs');
const factuurExtract = require('./factuur/extract');
const { projectJaarverbruik } = require('./project_jaarverbruik.js');

const app  = express();
const PORT = process.env.PORT || 3000;

// ─── MIDDLEWARE ───────────────────────────────────────────────────────────────
app.use(compression());
app.use(express.json({ limit: '20mb' }));
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  // v15.15.1 hotfix: Authorization toegestaan voor FluctusAppAuth-calls
  // (app-access/check, activity/log, scenario-routes met Bearer-token).
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

// ─── MARKTDATA: laad bij startup via Python prebuild script ──────────────────
let MARKT = null;  // { spot_q, imb_q, solar_norm, profiel, van, tot }
// v15.14.1 hotfix: status-tracking + retry-ladder voor robuuste markt-loading.
// Symptoom (productie): HTTP 503 "Marktdata nog niet geladen" bleef hangen omdat
// laadMarktdata() bij koude Railway-start stil faalde (prebuild >60s of cache-fetch
// fout) en MARKT permanent null bleef zonder enige herpoging.
let MARKT_STATUS = 'init';   // 'init' | 'loading' | 'ok' | 'failed'
let MARKT_LAATSTE_FOUT = null;
let MARKT_POGINGEN = 0;
let _marktRetryTimer = null;

function laadMarktdata(isRetry = false) {
  const prebuildScript = path.join(__dirname, 'prebuild_data.py');
  if (!fs.existsSync(prebuildScript)) {
    console.warn('[markt] prebuild_data.py niet gevonden — simulator zal lege marktdata gebruiken');
    MARKT_STATUS = 'failed';
    MARKT_LAATSTE_FOUT = 'prebuild_data.py ontbreekt';
    return;
  }
  MARKT_STATUS = 'loading';
  MARKT_POGINGEN += 1;
  try {
    console.log(`[markt] Marktdata pre-bouwen... (poging ${MARKT_POGINGEN}${isRetry ? ', retry' : ''})`);
    // v15.14.1: timeout verhoogd van 60s naar 120s (koude ENTSO-E/Elia fetch kan
    // bij eerste run van de dag traag zijn; cache-fetch daarna is snel).
    execFileSync('python3', [prebuildScript], { timeout: 120000 });
    const marktPath = '/tmp/fluctus_markt.json';
    if (fs.existsSync(marktPath)) {
      MARKT = JSON.parse(fs.readFileSync(marktPath, 'utf8'));
      MARKT_STATUS = 'ok';
      MARKT_LAATSTE_FOUT = null;
      if (_marktRetryTimer) { clearInterval(_marktRetryTimer); _marktRetryTimer = null; }
      console.log(`[markt] OK — ${MARKT.n_kwartieren} kwartieren, periode ${MARKT.van} → ${MARKT.tot}`);
    } else {
      throw new Error('prebuild voltooide maar /tmp/fluctus_markt.json ontbreekt');
    }
  } catch (e) {
    MARKT_STATUS = 'failed';
    MARKT_LAATSTE_FOUT = e.message;
    console.error(`[markt] Pre-build gefaald (poging ${MARKT_POGINGEN}):`, e.message);
    // v15.14.1: automatische retry-ladder. Bij falen, herprobeer met groeiende
    // interval (30s, dan elke 5 min) zodat de server zichzelf herstelt zonder
    // handmatige redeploy. Stopt zodra MARKT geladen is.
    if (!_marktRetryTimer) {
      const eersteRetryMs = 30000;  // 30s na eerste falen
      console.log(`[markt] Automatische retry over ${eersteRetryMs/1000}s ingepland`);
      setTimeout(() => {
        laadMarktdata(true);
        // Daarna elke 5 minuten blijven proberen tot het lukt
        if (!_marktRetryTimer && MARKT_STATUS !== 'ok') {
          _marktRetryTimer = setInterval(() => {
            if (MARKT_STATUS === 'ok') {
              clearInterval(_marktRetryTimer); _marktRetryTimer = null; return;
            }
            laadMarktdata(true);
          }, 5 * 60 * 1000);
        }
      }, eersteRetryMs);
    }
  }
}

// ─── INLINE DATA ──────────────────────────────────────────────────────────────
const POSTCODE_GRD = {};
function addRange(ranges, grd, dnb) {
  for (const [from, to] of ranges)
    for (let pc = from; pc < to; pc++)
      POSTCODE_GRD[String(pc)] = { grd, dnb };
}
addRange([[8000,8800],[8900,9000]],            'Fluvius West',     'Fluvius West');
addRange([[8800,8900]],                         'Fluvius Gaselwest','Fluvius Gaselwest');
addRange([[9000,10000]],                        'Fluvius Imewo',    'Fluvius Imewo');  // coarse default 9xxx (Gent/Imewo); precieze zones overriden hieronder
addRange([[2000,3000]],                         'Fluvius Antwerpen','Fluvius Antwerpen');
// v15.15.5: Vlaams-Brabant splitst in twee tariefzones (postcode-afhankelijk):
//   1500–2000 = Halle-Vilvoorde-zone · 3000–3500 = Zenne-Dijle-zone (Leuven).
addRange([[1500,2000]],                         'Fluvius Halle-Vilvoorde', 'Fluvius Brabant');
addRange([[3000,3500]],                         'Fluvius Leuven',          'Fluvius Brabant');
addRange([[3500,3900],[3900,4000]],             'Fluvius Limburg',  'Fluvius Limburg');
addRange([[1000,1300]],                         'Sibelga',          'Sibelga');
addRange([[1300,1500]],                         'IECBW',            'IECBW');
addRange([[4000,5000]],                         'RESA',             'RESA');
addRange([[5000,6000],[6000,7000],[7000,8000]], 'ORES',             'ORES');
for (const pc of [2800,2801,2811,2812,2820,2830])
  POSTCODE_GRD[String(pc)] = { grd: 'Fluvius Mechelen', dnb: 'Fluvius Mechelen' };

// v15.15.5: Oost-Vlaanderen (9xxx) splitst in DRIE tariefzones — Imewo (Gent,
// Meetjesland, Waasland-noord), Midden-Vl. (Waasland-kern, Dendermonde, Aalst,
// Ninove, Zottegem) en West (Vlaamse Ardennen: Oudenaarde, Ronse). Exacte
// postcode→zone uit Fluvius Open Data 2025. Overridet de coarse 9xxx-default.
const OVL_9XXX = {
  'Imewo': ['9000','9030','9031','9032','9040','9041','9042','9050','9051','9052','9060','9070','9080','9090','9160','9180','9185','9230','9240','9260','9270','9290','9340','9520','9521','9800','9810','9820','9830','9831','9840','9850','9860','9880','9881','9900','9910','9920','9921','9930','9931','9932','9940','9950','9960','9961','9968','9970','9971','9980','9981','9982','9988','9990','9991','9992'],
  'Midden-Vl.': ['9100','9111','9112','9120','9130','9140','9150','9170','9190','9200','9220','9250','9255','9280','9300','9308','9310','9320','9400','9401','9402','9403','9404','9406','9420','9450','9451','9470','9472','9473','9500','9506','9550','9551','9552','9570','9571','9572','9620','9660','9661'],
  'West': ['9600','9630','9636','9667','9680','9681','9688','9690','9700','9750','9770','9771','9772','9790','9870','9890'],
};
for (const [zone, pcs] of Object.entries(OVL_9XXX))
  for (const pc of pcs)
    POSTCODE_GRD[pc] = { grd: 'Fluvius ' + zone, dnb: 'Fluvius Oost-Vlaanderen' };

const TARIEVEN_MAP = {};  // wordt gevuld vanuit data/tarieven.json
const TARIEVEN_LS = {
  maandpiek_eur_kw_jaar: 57.4, toegangsvermogen_eur_kw_jaar: 0,
  overschrijding_toegangsvermogen_eur_kw_jaar: 62.47,
  proportioneel_eur_mwh: 4.96, databeheer_eur_jaar: 96.0,
  reactief_eur_mvarh: 0, injectie_proportioneel_eur_mwh: 0,
  injectie_capaciteit_eur_kva_maand: 0, injectie_databeheer_eur_jaar: 0,
  injectie_vaste_vergoeding_eur_jaar: 0,
  transport_maandpiek_eur_kw_mnd: 1.50, transport_jaarpiek_eur_kw_jaar: 0,
  transport_systeembeheer_eur_mwh: 2.61, transport_reserves_eur_mwh: 2.74,
  transport_marktintegratie_eur_mwh: 0.19, transport_beschikbaar_eur_kva_jaar: 0,
  transport_reactief_eur_mvarh: 0, odv_eur_mwh: 0,
  surcharges_eur_mwh: 0, soldes_eur_mwh: 0, accijns_basis_eur_mwh: 0,
  accijnzen_staffel: [[999999, 15.08]], energiefonds_eur_jaar: 114.84,
};

const CONTRACT_STAFFEL = [
  { min_mwh:0,    max_mwh:100,    label:'0-100 MWh',    code:'S1', consumption_dam_markup:20.0, consumption_imbalance_markup:5.0, injection_dam_markdown:0.0, injection_imbalance_markdown:11.0 },
  { min_mwh:100,  max_mwh:200,    label:'100-200 MWh',  code:'S2', consumption_dam_markup:19.0, consumption_imbalance_markup:5.0, injection_dam_markdown:0.0, injection_imbalance_markdown:11.0 },
  { min_mwh:200,  max_mwh:300,    label:'200-300 MWh',  code:'S3', consumption_dam_markup:18.0, consumption_imbalance_markup:5.0, injection_dam_markdown:0.0, injection_imbalance_markdown:11.0 },
  { min_mwh:300,  max_mwh:400,    label:'300-400 MWh',  code:'S4', consumption_dam_markup:17.0, consumption_imbalance_markup:5.0, injection_dam_markdown:0.0, injection_imbalance_markdown:11.0 },
  { min_mwh:400,  max_mwh:500,    label:'400-500 MWh',  code:'S5', consumption_dam_markup:16.0, consumption_imbalance_markup:5.0, injection_dam_markdown:0.0, injection_imbalance_markdown:11.0 },
  { min_mwh:500,  max_mwh:600,    label:'500-600 MWh',  code:'S6', consumption_dam_markup:15.0, consumption_imbalance_markup:5.0, injection_dam_markdown:0.0, injection_imbalance_markdown:11.0 },
  { min_mwh:600,  max_mwh:700,    label:'600-700 MWh',  code:'S7', consumption_dam_markup:14.0, consumption_imbalance_markup:5.0, injection_dam_markdown:0.0, injection_imbalance_markdown:11.0 },
  { min_mwh:700,  max_mwh:800,    label:'700-800 MWh',  code:'S8', consumption_dam_markup:13.5, consumption_imbalance_markup:5.0, injection_dam_markdown:0.0, injection_imbalance_markdown:11.0 },
  { min_mwh:800,  max_mwh:900,    label:'800-900 MWh',  code:'S9', consumption_dam_markup:13.0, consumption_imbalance_markup:5.0, injection_dam_markdown:0.0, injection_imbalance_markdown:11.0 },
  { min_mwh:900,  max_mwh:1000,   label:'900-1000 MWh', code:'S10',consumption_dam_markup:12.5, consumption_imbalance_markup:5.0, injection_dam_markdown:0.0, injection_imbalance_markdown:11.0 },
  { min_mwh:1000, max_mwh:2000,   label:'1-2 GWh',      code:'S11',consumption_dam_markup:8.0,  consumption_imbalance_markup:5.0, injection_dam_markdown:0.0, injection_imbalance_markdown:11.0 },
  { min_mwh:2000, max_mwh:5000,   label:'2-5 GWh',      code:'S12',consumption_dam_markup:5.0,  consumption_imbalance_markup:5.0, injection_dam_markdown:0.0, injection_imbalance_markdown:11.0 },
  { min_mwh:5000, max_mwh:999999, label:'>5 GWh',       code:'S13',consumption_dam_markup:3.5,  consumption_imbalance_markup:5.0, injection_dam_markdown:0.0, injection_imbalance_markdown:11.0 },
];

// BATTERIJEN wordt geladen vanuit data/batterijen.json (zie hieronder)

const PROFIELEN = [
  { naam:'slager',     beschrijving:'Slager / voedingszaak — dagprofiel met ochtend- en middagpiek' },
  { naam:'bakker',     beschrijving:'Bakkerij — vroege ochtendpiek (3-7u)' },
  { naam:'kantoor',    beschrijving:'Kantoor — weekdag 8-18u, weekend laag' },
  { naam:'supermarkt', beschrijving:'Supermarkt — dag 7-22u, 7 dagen/week' },
  { naam:'industrie',  beschrijving:'Industrie — 2-ploegensysteem' },
  { naam:'horeca',     beschrijving:'Horeca — middaglunch + avondspits' },
];

// ── Laad alle data-bestanden uit data/ bij startup ──────────────────────────
function loadJson(relPath, fallback = null) {
  const fp = path.join(__dirname, relPath);
  if (fs.existsSync(fp)) {
    try {
      const d = JSON.parse(fs.readFileSync(fp, 'utf8'));
      console.log(`[data] geladen: ${relPath}`);
      return d;
    } catch(e) { console.error(`[data] parse fout ${relPath}: ${e.message}`); }
  } else {
    console.warn(`[data] niet gevonden: ${relPath}`);
  }
  return fallback;
}

// Gemeenten
let GEMEENTEN_LIJST = loadJson('data/gemeenten.json', []);
const PC_GEMEENTE_INDEX = {};
for (const g of GEMEENTEN_LIJST) {
  if (!PC_GEMEENTE_INDEX[g.postcode]) PC_GEMEENTE_INDEX[g.postcode] = [];
  PC_GEMEENTE_INDEX[g.postcode].push(g.gemeente);
}

// Postcodes (rijke shape met dnb per postcode)
const POSTCODES_DATA = loadJson('data/postcodes.json', null);
// Bouw GRD index uit postcodes.json als die bestaat, anders gebruik inline POSTCODE_GRD
if (POSTCODES_DATA) {
  const entries = Array.isArray(POSTCODES_DATA) ? POSTCODES_DATA : Object.entries(POSTCODES_DATA).map(([pc,v]) => ({postcode:pc,...(typeof v==='string'?{grd:v,dnb:v}:v)}));
  for (const e of entries) {
    POSTCODE_GRD[String(e.postcode)] = { grd: e.grd || e.dnb, dnb: e.dnb || e.grd };
    if (e.gemeente) {
      if (!PC_GEMEENTE_INDEX[String(e.postcode)]) PC_GEMEENTE_INDEX[String(e.postcode)] = [];
      if (!PC_GEMEENTE_INDEX[String(e.postcode)].includes(e.gemeente))
        PC_GEMEENTE_INDEX[String(e.postcode)].push(e.gemeente);
    }
  }
  console.log(`[postcodes] ${entries.length} postcodes geladen`);
}

// ─── POSTCODE-FALLBACK INDEX (v15.10, BaseCase Uitbreiding Fase 2 sessie 3) ──
// Pre-bouw sorted array voor O(log n) laagste-buurman lookup. Wordt gebruikt
// door POST /api/postcode-fallback. Anti-regressie: alleen ADD, geen MODIFY.
const POSTCODE_FALLBACK_MAX_DELTA = 50;
const POSTCODE_KEYS_SORTED = Object.keys(POSTCODE_GRD)
  .filter(k => /^\d{4}$/.test(k))
  .map(k => parseInt(k, 10))
  .sort((a, b) => a - b);
console.log(`[postcode-fallback] index gebouwd: ${POSTCODE_KEYS_SORTED.length} postcodes`);

// Binary search: returnt index van grootste element ≤ target, of -1 als geen.
function _laagsteBuurmanIndex(target) {
  let lo = 0, hi = POSTCODE_KEYS_SORTED.length - 1, res = -1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (POSTCODE_KEYS_SORTED[mid] <= target) {
      res = mid;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  return res;
}

// ─── MARKTDATA-SLICE (v15.11, BaseCase Uitbreiding Fase 2 sessie 4) ──────────
// Slice MARKT.spot_q / imb_q op de exacte simPeriode.
//
// Probleem dat dit oplost:
// - MARKT (uit prebuild_data.py) bevat rolling 12 maanden gestart op MARKT.van.
// - Vroeger (v15.10) werd MARKT.spot_q letterlijk doorgegeven aan simulator.py,
//   die met [:N] simpele truncate deed. Voor rolling12 toevallig OK (MARKT.van ==
//   simPeriode.van). Voor kalenderjaar 2025 LATENT-BUG: pakte de eerste N van
//   MARKT.van (≈apr 2025), NIET 2025-01-01 → 2025-12-31. Voor specifieke periode
//   (jan 2026): zou ook fout zijn.
// - Deze helper bepaalt de juiste OFFSET in MARKT.spot_q op basis van MARKT.van
//   en simPeriode.van (in dagen × 96 kwartieren), slicet N kwartieren uit,
//   en clampt bij overschrijding (met pad-fallback op laatste waarde).
//
// Anti-regressie: voor rolling12 met simPeriode.van == MARKT.van geeft dit
// IDENTIEKE arrays als de v15.10 truncate. Bewezen via diff op een rolling12
// run (zie test_marktdata_slice.js, sessie 4 artefacten).
function _sliceMarktVoorPeriode(MARKT, simPeriode) {
  if (!MARKT || !Array.isArray(MARKT.spot_q)) {
    return { spot_q: [], imb_q: [], n: 0, offset: 0, mode: 'no-markt' };
  }
  const spotFull = MARKT.spot_q;
  const imbFull  = MARKT.imb_q || spotFull;
  const marktVan = new Date(MARKT.van + 'T00:00:00Z');
  const simVan   = new Date(simPeriode.van + 'T00:00:00Z');
  const simTot   = new Date(simPeriode.tot + 'T00:00:00Z');
  const KWARTIER_MS = 15 * 60 * 1000;
  const N = Math.round((simTot - simVan) / KWARTIER_MS);
  const offset = Math.round((simVan - marktVan) / KWARTIER_MS);

  // Edge cases
  if (N <= 0) {
    return { spot_q: [], imb_q: [], n: 0, offset, mode: 'empty-periode' };
  }
  // Volledige periode binnen MARKT
  if (offset >= 0 && offset + N <= spotFull.length) {
    return {
      spot_q: spotFull.slice(offset, offset + N),
      imb_q:  imbFull.slice(offset, offset + N),
      n: N, offset, mode: 'binnen-markt',
    };
  }
  // Buiten bereik (gedeeltelijk of geheel) — pad met dichtsbij beschikbare waarde.
  // Dit gebeurt typisch wanneer simPeriode in de toekomst ligt of vóór MARKT-start.
  // We construeren een N-array waarbij elementen buiten [0, spotFull.length) terugvallen
  // op de dichtstbij beschikbare waarde (links of rechts).
  console.warn(`[markt-slice] simPeriode (${simPeriode.van}→${simPeriode.tot}) ` +
               `valt buiten MARKT (${MARKT.van}→${MARKT.tot}): offset=${offset}, N=${N}, ` +
               `spotLen=${spotFull.length}. Pad-fallback toegepast.`);
  const spot_q = new Array(N);
  const imb_q  = new Array(N);
  for (let i = 0; i < N; i++) {
    let idx = offset + i;
    if (idx < 0) idx = 0;
    else if (idx >= spotFull.length) idx = spotFull.length - 1;
    spot_q[i] = spotFull[idx];
    imb_q[i]  = imbFull[idx];
  }
  return { spot_q, imb_q, n: N, offset, mode: 'gepad' };
}

// Batterijen
let BATTERIJEN = loadJson('data/batterijen.json', [
  { id:'bess-50',  naam:'BESS 50 kWh / 25 kW',  kwh:50,  kw:25, eta:0.85, dod:0.90, capex:20000, max_cycli:8000 },
  { id:'bess-100', naam:'BESS 100 kWh / 49 kW', kwh:100, kw:49, eta:0.85, dod:0.90, capex:35000, max_cycli:8000 },
  { id:'bess-200', naam:'BESS 200 kWh / 79 kW', kwh:200, kw:79, eta:0.85, dod:0.90, capex:62000, max_cycli:8000 },
]);
if (!Array.isArray(BATTERIJEN)) BATTERIJEN = BATTERIJEN.batterijen || [];

// Leveringscontract
const CONTRACT_RAW = loadJson('data/leveringscontract.json', null);
if (CONTRACT_RAW) {
  if (CONTRACT_RAW.schijven) CONTRACT_STAFFEL.splice(0, CONTRACT_STAFFEL.length, ...CONTRACT_RAW.schijven);
  else if (CONTRACT_RAW.staffel) CONTRACT_STAFFEL.splice(0, CONTRACT_STAFFEL.length, ...CONTRACT_RAW.staffel);
  else if (Array.isArray(CONTRACT_RAW)) CONTRACT_STAFFEL.splice(0, CONTRACT_STAFFEL.length, ...CONTRACT_RAW);
}

// Tarieven
const TARIEVEN_RAW = loadJson('data/tarieven.json', null);
if (TARIEVEN_RAW) {
  if (Array.isArray(TARIEVEN_RAW)) {
    for (const t of TARIEVEN_RAW) { if (t.grd) TARIEVEN_MAP[t.grd] = t; }
  } else {
    Object.assign(TARIEVEN_MAP, TARIEVEN_RAW);
  }
}

// v15.15.5: tariefkaart-selectie per netbeheerder + spanning (LS/MS).
// data/tarieven.json is gekeyd op "<zone>|<spanning>" (bv. "West|MS").
// De postcode-GRD-namen ("Fluvius Antwerpen/Brabant/Enet/Gaselwest/Mechelen…")
// matchen niet 1-op-1 met de tariefzones → deze alias-tabel vertaalt ze.
// ⚠ TE BEVESTIGEN door Johan: de gemarkeerde (?) mappings zijn een best-guess.
const GRD_NAAR_ZONE = {
  'Fluvius Antwerpen':       'Antwerpen',
  'Fluvius Limburg':         'Limburg',
  'Fluvius West':            'West',
  'Fluvius Gaselwest':       'West',          // bevestigd (Johan)
  'Fluvius Mechelen':        'Zenne-Dijle',   // bevestigd (Johan) — 2800/2820/2830…
  'Fluvius Halle-Vilvoorde': 'Halle-Vilv.',   // Brabant 1500–2000
  'Fluvius Leuven':          'Zenne-Dijle',   // Brabant 3000–3500
  'Fluvius Imewo':           'Imewo',         // Oost-Vl. (Gent e.o.) — per postcode
  'Fluvius Midden-Vl.':      'Midden-Vl.',    // Oost-Vl. (Dendermonde/Aalst) — per postcode
  'ORES':    'ORES',   'RESA': 'RESA',   'Sibelga': 'Sibelga',
  'IECBW':   'ORES',   // bevestigd (Johan) — Waals-Brabant
};
function _kiesTarieven(grd, spanning) {
  const sp = (spanning === 'MS' || spanning === 'LS') ? spanning : 'LS';
  const zone = GRD_NAAR_ZONE[grd] || (grd || '').replace(/^Fluvius\s+/, '');
  let kaart = TARIEVEN_MAP[`${zone}|${sp}`]
           || TARIEVEN_MAP[`West|${sp}`]   // fallback: representatieve zone, juiste spanning
           || TARIEVEN_LS;                 // laatste redmiddel
  if (!TARIEVEN_MAP[`${zone}|${sp}`]) {
    console.warn(`[tarieven] geen exacte kaart voor grd="${grd}" (zone="${zone}"), spanning="${sp}" — fallback gebruikt`);
  }
  // v15.15.5: de json-kaart heeft losse accijns_schijf*-velden; simulator.py
  // verwacht 'accijnzen_staffel' = [[grens_mwh, tarief], …]. Bouw die af zodat
  // de kaart-accijns correct doorstroomt (en /api/regio-tarieven niet crasht).
  if (kaart && kaart.accijns_schijf1_3mwh !== undefined && !kaart.accijnzen_staffel) {
    kaart = { ...kaart, accijnzen_staffel: [
      [3,       kaart.accijns_schijf1_3mwh],
      [20,      kaart.accijns_schijf2_20mwh],
      [50,      kaart.accijns_schijf3_50mwh],
      [1000,    kaart.accijns_schijf4_1000mwh],
      [9999999, kaart.accijns_schijf5_inf],
    ] };
  }
  return kaart;
}

// Profielen laden uit data/profielen-lijst.json
let PROFIELEN_LIJST = [
  { naam:'Slager',     beschrijving:'sterk weekdagprofiel, overwegend dag, seizoensstabiel, piek 7u' },
  { naam:'Kantoor',    beschrijving:'weekdagprofiel, overwegend dag, sterk seizoensgebonden, variabel, piek 11u' },
  { naam:'Horeca',     beschrijving:'weekdagprofiel, overwegend dag, zomerpiek, variabel, piek 17u' },
];
const profielenLijstPath = path.join(__dirname, 'data', 'profielen-lijst.json');
if (fs.existsSync(profielenLijstPath)) {
  PROFIELEN_LIJST = JSON.parse(fs.readFileSync(profielenLijstPath, 'utf8'));
  console.log(`[profielen] ${PROFIELEN_LIJST.length} profielen geladen`);
} else {
  console.warn('[profielen] data/profielen-lijst.json niet gevonden');
}

// v15.15.3 (bug 1): profiel-naam → bestandsnaam normalisatie. De profielenlijst
// toont nette namen met spaties/hoofdletters ("Boer aardappel", "Opslag /
// Magazijn"), maar de bestanden in data/profielen/ heten met underscores
// (boer_aardappel.json, opslag___magazijn.json). De oude exact/lowercase-lookup
// vond enkel enkelwoord-profielen; meerwoord-profielen gaven 404 op
// /api/factuur-staffel-bepalen (en stap 3 verborg dat via de MARKT-fallback).
// We normaliseren beide kanten (lowercase, niet-alfanumeriek → '_', runs
// gecollapst, rand-underscores gestript) en vergelijken dan.
function _profielFileNormalize(s) {
  return String(s || '')
    .toLowerCase()
    .replace(/\.json$/, '')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

// ─── SCENARIO-PERSISTENTIE GITHUB (v15.11 sessie 4 sub-track 4) ──────────────
// Bug ontdekt 12 mei 2026: POST /api/scenario-bewaren sloeg scenarios alleen
// op in in-memory SCENARIOS_DB. Bij Railway-restart waren ze weg. UI claimde
// onterecht "Scenario bewaard in fluctus-scenarios repo".
//
// Fix: scenarios worden nu écht naar github.com/<owner>/fluctus-scenarios
// gecommit, met pad-conventie projecten/{project}/{scenario}.json.
// SCENARIOS_DB blijft een lokale cache (read-through). Bij read-miss wordt
// GitHub geprobeerd.
//
// Anti-regressie regel 3: NIEUWE helpers met andere naam dan market-data
// githubRead/githubWrite (die blijven exact zoals ze waren). Geen wijziging
// aan bestaande markt-data routes.
const SCENARIOS_REPO_OWNER = process.env.SCENARIOS_OWNER || process.env.GITHUB_OWNER || 'JohanMMK';
const SCENARIOS_REPO_NAME  = process.env.SCENARIOS_REPO  || 'fluctus-scenarios';
const SCENARIOS_PATH_PREFIX = 'projecten';  // pad in repo: projecten/{project}/{scenario}.json

function _scenarioPad(project, scenario) {
  // GitHub paden mogen geen path-separators of vreemde chars hebben.
  const cleanProject  = String(project).replace(/[\/\\?#]/g, '_');
  const cleanScenario = String(scenario).replace(/[\/\\?#]/g, '_');
  return `${SCENARIOS_PATH_PREFIX}/${cleanProject}/${cleanScenario}.json`;
}

async function _scenariosGithubRead(filepath) {
  const apiUrl = `https://api.github.com/repos/${SCENARIOS_REPO_OWNER}/${SCENARIOS_REPO_NAME}/contents/${filepath}`;
  const headers = { 'User-Agent': 'fluctus-proxy', 'Accept': 'application/vnd.github.v3+json' };
  if (GITHUB_TOKEN) headers['Authorization'] = `token ${GITHUB_TOKEN}`;
  const metaResp = await fetch(apiUrl, { headers });
  if (!metaResp.ok) throw new Error(`scenarios read ${filepath}: HTTP ${metaResp.status}`);
  const meta = await metaResp.json();
  const sha = meta.sha;
  const rawUrl = `https://raw.githubusercontent.com/${SCENARIOS_REPO_OWNER}/${SCENARIOS_REPO_NAME}/main/${filepath}`;
  const rawHeaders = { 'User-Agent': 'fluctus-proxy' };
  if (GITHUB_TOKEN) rawHeaders['Authorization'] = `token ${GITHUB_TOKEN}`;
  const rawResp = await fetch(rawUrl, { headers: rawHeaders });
  if (!rawResp.ok) throw new Error(`scenarios raw read ${filepath}: HTTP ${rawResp.status}`);
  const content = await rawResp.text();
  return { data: JSON.parse(content), sha };
}

// ─── v15.16: FACTUUR-OPSLAG in Supabase Storage ──────────────────────────────
// De geüploade factuur wordt bewaard zodat ze later naast de analyse getoond en
// als bijlage gemaild kan worden. BEWUST NIET in de GitHub-scenario-repo:
//   - git-historiek is onuitwisbaar → een AVG-verwijderverzoek is onmogelijk te
//     honoreren zonder history rewrite;
//   - elke auto-save zou megabytes base64 committen (repo-bloat).
// De bucket 'facturen' is PRIVAAT. Alleen de service-role-key (server-side) mag
// schrijven/lezen; de browser krijgt enkel een kortlevende signed URL.
const FACTUREN_BUCKET = process.env.FACTUREN_BUCKET || 'facturen';

function _factuurPad(meta, mediaType) {
  const ext = mediaType === 'application/pdf' ? 'pdf'
            : (mediaType || '').startsWith('image/') ? (mediaType.split('/')[1] || 'bin') : 'bin';
  const veilig = (s, fb) => String(s || fb).replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 60);
  const klant = veilig(meta.klantBtw || meta.klantNaam, 'onbekend');
  const nr    = veilig(meta.factuurNummer, 'factuur');
  return `${klant}/${nr}-${Date.now()}.${ext}`;
}

async function _factuurUpload(base64, mediaType, pad) {
  if (!SUPABASE_OK) throw new Error('Supabase niet geconfigureerd');
  const url = `${SUPABASE_URL}/storage/v1/object/${FACTUREN_BUCKET}/${pad}`;
  const r = await fetch(url, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`,
      'Content-Type': mediaType || 'application/octet-stream',
      'x-upsert': 'true',
    },
    body: Buffer.from(base64, 'base64'),
  });
  if (!r.ok) throw new Error(`storage upload ${pad}: HTTP ${r.status} ${(await r.text()).slice(0, 200)}`);
  return pad;
}

// v15.17: object terug ophalen (server-side, met service-key). Gebruikt voor de
// bewaarde factuuranalyse-JSON, zodat een heropend rapport IDENTIEK is aan het
// origineel — geen herberekening, dus geen drift in de cijfers.
async function _factuurDownload(pad) {
  if (!SUPABASE_OK) throw new Error('Supabase niet geconfigureerd');
  const url = `${SUPABASE_URL}/storage/v1/object/${FACTUREN_BUCKET}/${pad}`;
  const r = await fetch(url, { headers: { 'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}` } });
  if (!r.ok) throw new Error(`storage download ${pad}: HTTP ${r.status}`);
  return r.text();
}

// Kortlevende signed URL (default 10 min) — de bucket blijft privaat.
async function _factuurSignedUrl(pad, expiresIn = 600) {
  if (!SUPABASE_OK) throw new Error('Supabase niet geconfigureerd');
  const url = `${SUPABASE_URL}/storage/v1/object/sign/${FACTUREN_BUCKET}/${pad}`;
  const r = await fetch(url, {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ expiresIn }),
  });
  if (!r.ok) throw new Error(`storage sign ${pad}: HTTP ${r.status} ${(await r.text()).slice(0, 200)}`);
  const j = await r.json();
  if (!j.signedURL) throw new Error('storage sign: geen signedURL in antwoord');
  return `${SUPABASE_URL}/storage/v1${j.signedURL}`;
}

// v15.15.6: haal ENKEL de huidige blob-sha op (verse read, cache-buster) — voor
// de conflict-retry in _scenariosGithubWrite. Returnt undefined bij 404 (create).
async function _scenariosGithubSha(filepath) {
  const apiUrl = `https://api.github.com/repos/${SCENARIOS_REPO_OWNER}/${SCENARIOS_REPO_NAME}/contents/${filepath}?ref=main&_=${Date.now()}`;
  const headers = { 'User-Agent': 'fluctus-proxy', 'Accept': 'application/vnd.github.v3+json' };
  if (GITHUB_TOKEN) headers['Authorization'] = `token ${GITHUB_TOKEN}`;
  const r = await fetch(apiUrl, { headers, cache: 'no-store' });
  if (r.status === 404) return undefined;
  if (!r.ok) throw new Error(`scenarios sha ${filepath}: HTTP ${r.status}`);
  const j = await r.json();
  return j.sha;
}

async function _scenariosGithubWrite(filepath, data, sha) {
  const url = `https://api.github.com/repos/${SCENARIOS_REPO_OWNER}/${SCENARIOS_REPO_NAME}/contents/${filepath}`;
  const content = Buffer.from(JSON.stringify(data, null, 2)).toString('base64');
  const headers = { 'User-Agent': 'fluctus-proxy', 'Content-Type': 'application/json' };
  if (GITHUB_TOKEN) headers['Authorization'] = `token ${GITHUB_TOKEN}`;
  const _put = (useSha) => {
    const body = {
      message: `auto: scenario ${filepath.replace(SCENARIOS_PATH_PREFIX + '/', '').replace('.json', '')} (${new Date().toISOString().slice(0,10)})`,
      content,
    };
    if (useSha) body.sha = useSha;
    return fetch(url, { method: 'PUT', headers, body: JSON.stringify(body) });
  };
  // v15.15.6: SHA-conflict-retry. Een 409/422 "is at X but expected Y" betekent dat
  // de meegegeven sha verouderd is (bv. twee snelle commits op hetzelfde bestand).
  // We halen dan de VERSE sha op en proberen de PUT opnieuw (max 3×, met backoff).
  let r = await _put(sha);
  let poging = 0;
  while ((r.status === 409 || r.status === 422) && poging < 3) {
    poging++;
    let verseSha;
    try { verseSha = await _scenariosGithubSha(filepath); } catch (_) { /* laat r ongewijzigd */ }
    await new Promise((res) => setTimeout(res, 300 * poging));
    r = await _put(verseSha);
  }
  if (!r.ok) {
    const t = await r.text();
    throw new Error(`scenarios write ${filepath}: HTTP ${r.status} ${t.slice(0,200)}`);
  }
  return r.json();
}

async function _scenariosGithubListProject(project) {
  // Returnt array van scenario-namen (zonder .json) in projecten/{project}/.
  // Lege array bij 404 (project bestaat nog niet).
  const cleanProject = String(project).replace(/[\/\\?#]/g, '_');
  const apiUrl = `https://api.github.com/repos/${SCENARIOS_REPO_OWNER}/${SCENARIOS_REPO_NAME}/contents/${SCENARIOS_PATH_PREFIX}/${cleanProject}`;
  const headers = { 'User-Agent': 'fluctus-proxy', 'Accept': 'application/vnd.github.v3+json' };
  if (GITHUB_TOKEN) headers['Authorization'] = `token ${GITHUB_TOKEN}`;
  const r = await fetch(apiUrl, { headers });
  if (r.status === 404) return [];
  if (!r.ok) throw new Error(`scenarios list ${cleanProject}: HTTP ${r.status}`);
  const arr = await r.json();
  return arr
    .filter(e => e.type === 'file' && e.name.endsWith('.json'))
    .map(e => e.name.replace(/\.json$/, ''));
}

const SCENARIOS_DB = {};
const PROJECTEN_DB = new Set();

// ─── SESSIE 9a: FLUCTUS APP ACCESS (Supabase) ────────────────────────────────
// Fundament voor alle apps in HTML-blocks. Server valideert Supabase-JWTs
// van de Academy en beheert permissies + activity-log via de service-role
// key (passeert RLS; client-RLS staat in supabase_migratie_9a.sql).
const SUPABASE_URL         = (process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || '';
const SUPABASE_OK          = !!(SUPABASE_URL && SUPABASE_SERVICE_KEY);
// Rollback-schakelaar: FLUCTUS_AUTH_ENFORCE=false schakelt scenario-gating
// uit zonder redeploy van code. Zonder Supabase-env automatisch uit.
const AUTH_ENFORCE = SUPABASE_OK && (process.env.FLUCTUS_AUTH_ENFORCE || 'true') === 'true';
if (!SUPABASE_OK) {
  console.warn('[auth] SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY ontbreken — ' +
    'app-access-endpoints antwoorden 503, scenario-gating staat UIT.');
} else if (!AUTH_ENFORCE) {
  console.warn('[auth] FLUCTUS_AUTH_ENFORCE=false — scenario-gating staat UIT (rollback-modus).');
}

// Default-owner voor de eenmalige scenario-migratie.
// FIX na Supabase Auth-screenshot (06/07): de roadmap-UUIDs (c54ca361-... /
// 9cce5f61-...) bestaan NIET in auth.users — vermoedelijk profiles-PKs of
// verouderd. Owner_uid = auth.users.id (komt uit de token), dus:
//   johan@fluctus.net      = 36802fa6-c567-41cd-83e5-d4de4a3c73dd
//   daviddecock@live.be    = 5cae0b46-b267-4cd4-b687-1346ee6d4222
//   admin@fluctus.net      = 7d85b5eb-7a8a-4b0a-8219-3f0d17ce621f
const MIGRATIE_DEFAULT_OWNER = '36802fa6-c567-41cd-83e5-d4de4a3c73dd';

async function _sbRest(padEnQuery, opts) {
  // Kleine wrapper rond de Supabase REST-API (PostgREST) met service-role key.
  const o = opts || {};
  const r = await fetch(`${SUPABASE_URL}/rest/v1/${padEnQuery}`, {
    method: o.method || 'GET',
    headers: Object.assign({
      'apikey': SUPABASE_SERVICE_KEY,
      'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`,
      'Content-Type': 'application/json',
    }, o.headers || {}),
    body: o.body ? JSON.stringify(o.body) : undefined,
  });
  if (!r.ok) {
    const t = await r.text();
    throw new Error(`supabase ${o.method || 'GET'} ${padEnQuery.split('?')[0]}: HTTP ${r.status} ${t.slice(0, 200)}`);
  }
  const txt = await r.text();
  return txt ? JSON.parse(txt) : null;
}

// Token-validatie-cache: JWT → {val, exp}. 60s TTL; houdt Supabase-roundtrips
// laag bij wizard-gebruik (elke apiGet/apiPost stuurt dezelfde token mee).
const _AUTH_CACHE = new Map();
const _AUTH_CACHE_TTL_MS = 60 * 1000;

async function resolveUser(req) {
  // Returnt {id, email, naam, role, status} of null. Gooit nooit.
  try {
    if (!SUPABASE_OK) return null;
    const h = req.headers['authorization'] || '';
    const m = h.match(/^Bearer\s+(.+)$/i);
    if (!m) return null;
    const jwt = m[1];
    const hit = _AUTH_CACHE.get(jwt);
    if (hit && hit.exp > Date.now()) return hit.val;
    // 1) JWT valideren bij Supabase Auth
    const uResp = await fetch(`${SUPABASE_URL}/auth/v1/user`, {
      headers: { 'apikey': SUPABASE_SERVICE_KEY, 'Authorization': `Bearer ${jwt}` },
    });
    if (!uResp.ok) return null;
    const user = await uResp.json();
    if (!user || !user.id) return null;
    // 2) profiel ophalen. FIX na naspeuring Academy-broncode (vóór deploy):
    //    de Academy gebruikt tabel 'profiles' met kolom auth_uid (→ auth.users.id),
    //    naam-kolom 'name' en role 'manager'/'seller'. GEEN status-kolom.
    //    Fallback op 'profielen'/id voor het geval de roadmap-naam ooit komt.
    let profiel = null;
    try {
      const rows = await _sbRest(`profiles?auth_uid=eq.${encodeURIComponent(user.id)}&select=*`);
      profiel = Array.isArray(rows) && rows.length ? rows[0] : null;
    } catch (e) {
      console.warn(`[auth] profiles-lookup faalde voor ${user.id}: ${e.message}`);
    }
    if (!profiel) {
      try {
        const rows = await _sbRest(`profielen?id=eq.${encodeURIComponent(user.id)}&select=*`);
        profiel = Array.isArray(rows) && rows.length ? rows[0] : null;
      } catch (_) { /* fallback-tabel bestaat niet — ok */ }
    }
    const val = {
      id: user.id,
      email: user.email || (profiel && profiel.email) || '',
      naam: (profiel && (profiel.name || profiel.naam || profiel.full_name)) || user.email || user.id,
      role: (profiel && profiel.role) || 'seller',
      // profiles heeft geen status-kolom → default 'active'
      status: (profiel && profiel.status) || 'active',
    };
    _AUTH_CACHE.set(jwt, { val, exp: Date.now() + _AUTH_CACHE_TTL_MS });
    // Cache-grootte begrenzen (Railway long-running proces)
    if (_AUTH_CACHE.size > 500) {
      const oudste = _AUTH_CACHE.keys().next().value;
      _AUTH_CACHE.delete(oudste);
    }
    return val;
  } catch (e) {
    console.warn(`[auth] resolveUser fout: ${e.message}`);
    return null;
  }
}

function _isManager(u) {
  return !!(u && u.role === 'manager' && u.status === 'active');
}

async function _heeftAppToegang(u, appId) {
  if (!u || u.status !== 'active') return false;
  if (_isManager(u)) return true; // managers impliciet alle apps
  try {
    const rows = await _sbRest(
      `user_app_access?user_id=eq.${encodeURIComponent(u.id)}&app_id=eq.${encodeURIComponent(appId)}&select=app_id`);
    return Array.isArray(rows) && rows.length > 0;
  } catch (e) {
    console.warn(`[auth] toegangs-lookup faalde: ${e.message}`);
    return false;
  }
}

function _normBtw(btw) {
  // 'BE 0757.494.180' → 'BE0757494180' zodat klant-attributie-groepering klopt.
  if (!btw) return null;
  const n = String(btw).toUpperCase().replace(/[^A-Z0-9]/g, '');
  return n || null;
}

// POST /api/app-access/check  { app_id }  + Authorization: Bearer <supabase-jwt>
// → { toegang, user:{id,naam,email,role}, app_id, certificaten:[] }
app.post('/api/app-access/check', async (req, res) => {
  if (!SUPABASE_OK) {
    return res.status(503).json({ toegang: false, reden: 'auth_niet_geconfigureerd' });
  }
  const appId = (req.body || {}).app_id;
  if (!appId) return res.status(400).json({ toegang: false, reden: 'app_id verplicht' });
  const u = await resolveUser(req);
  if (!u) return res.status(401).json({ toegang: false, reden: 'niet_ingelogd' });
  const toegang = await _heeftAppToegang(u, appId);
  // Certificaten best-effort: tabel kan (nog) niet bestaan in de Academy —
  // fout wordt stil genegeerd, lege lijst terug.
  let certificaten = [];
  try {
    const rows = await _sbRest(`certificaten?user_id=eq.${encodeURIComponent(u.id)}&select=*`);
    certificaten = Array.isArray(rows) ? rows : [];
  } catch (_) { /* tabel ontbreekt of ander schema — geen blocker */ }
  return res.json({
    toegang,
    app_id: appId,
    user: { id: u.id, naam: u.naam, email: u.email, role: u.role },
    certificaten,
  });
});

// POST /api/app-activity/log  { app_id, actie, klant_btw?, klant_naam?, details? }
// Best-effort by design: antwoordt ALTIJD 200 {ok:...}. Een falende log mag
// nooit een sim of save blokkeren (roadmap 9a: "best-effort, non-blocking").
app.post('/api/app-activity/log', async (req, res) => {
  try {
    if (!SUPABASE_OK) return res.json({ ok: false, reden: 'auth_niet_geconfigureerd' });
    const b = req.body || {};
    if (!b.app_id || !b.actie) return res.json({ ok: false, reden: 'app_id en actie verplicht' });
    const u = await resolveUser(req);
    if (!u) return res.json({ ok: false, reden: 'niet_ingelogd' });
    await _sbRest('app_activity_log', {
      method: 'POST',
      headers: { 'Prefer': 'return=minimal' },
      body: {
        user_id: u.id,
        app_id: b.app_id,
        actie: String(b.actie).slice(0, 120),
        klant_btw: _normBtw(b.klant_btw),
        klant_naam: b.klant_naam ? String(b.klant_naam).slice(0, 200) : null,
        details: (b.details && typeof b.details === 'object') ? b.details : {},
      },
    });
    return res.json({ ok: true });
  } catch (e) {
    console.warn(`[activity] log-insert faalde: ${e.message}`);
    return res.json({ ok: false, reden: 'insert_faalde' });
  }
});

// GET /api/manager/activity?verkoper=<uuid>&app=<id>&klant_btw=&van=&tot=&limit=
// Manager-only. Retourneert log-rijen (nieuwste eerst) verrijkt met
// verkoper_naam uit profielen.
app.get('/api/manager/activity', async (req, res) => {
  if (!SUPABASE_OK) return res.status(503).json({ error: 'auth_niet_geconfigureerd' });
  const u = await resolveUser(req);
  if (!u) return res.status(401).json({ error: 'niet ingelogd' });
  if (!_isManager(u)) return res.status(403).json({ error: 'alleen voor managers' });
  try {
    const q = req.query || {};
    const delen = ['select=*', 'order=ts.desc'];
    const limit = Math.min(Math.max(parseInt(q.limit, 10) || 100, 1), 1000);
    delen.push(`limit=${limit}`);
    if (q.verkoper)  delen.push(`user_id=eq.${encodeURIComponent(q.verkoper)}`);
    if (q.app)       delen.push(`app_id=eq.${encodeURIComponent(q.app)}`);
    if (q.klant_btw) delen.push(`klant_btw=eq.${encodeURIComponent(_normBtw(q.klant_btw))}`);
    if (q.van)       delen.push(`ts=gte.${encodeURIComponent(q.van)}`);
    if (q.tot)       delen.push(`ts=lte.${encodeURIComponent(q.tot)}`);
    const rijen = await _sbRest(`app_activity_log?${delen.join('&')}`);
    // Namen verrijken in één tweede query
    const ids = [...new Set((rijen || []).map(r => r.user_id).filter(Boolean))];
    const namen = {};
    if (ids.length) {
      try {
        const profs = await _sbRest(`profiles?auth_uid=in.(${ids.map(encodeURIComponent).join(',')})&select=*`);
        for (const p of (profs || [])) namen[p.auth_uid] = p.name || p.naam || p.full_name || p.email || p.auth_uid;
      } catch (_) { /* namen-verrijking best-effort */ }
    }
    return res.json({
      activiteit: (rijen || []).map(r => Object.assign({}, r, { verkoper_naam: namen[r.user_id] || r.user_id })),
      limit,
    });
  } catch (e) {
    console.error(`[manager/activity] fout: ${e.message}`);
    return res.status(500).json({ error: e.message });
  }
});

// POST /api/admin/migrate-scenario-owners  { default_owner_uid? }
// Eenmalige migratie (manager-only, idempotent): loopt alle scenario-JSONs in
// de fluctus-scenarios repo af en stempelt owner_uid = Johan waar het veld
// ontbreekt (roadmap v6 §2.1). Bestaande owner_uid wordt NOOIT overschreven.
app.post('/api/admin/migrate-scenario-owners', async (req, res) => {
  if (!SUPABASE_OK) return res.status(503).json({ error: 'auth_niet_geconfigureerd' });
  const u = await resolveUser(req);
  if (!u) return res.status(401).json({ error: 'niet ingelogd' });
  if (!_isManager(u)) return res.status(403).json({ error: 'alleen voor managers' });
  const eigenaar = (req.body || {}).default_owner_uid || MIGRATIE_DEFAULT_OWNER;
  const samenvatting = { eigenaar, projecten: 0, scenarios_totaal: 0, gemigreerd: 0, overgeslagen: 0, fouten: [] };
  try {
    // Projectenlijst = directories onder projecten/ in de repo
    const apiUrl = `https://api.github.com/repos/${SCENARIOS_REPO_OWNER}/${SCENARIOS_REPO_NAME}/contents/${SCENARIOS_PATH_PREFIX}`;
    const headers = { 'User-Agent': 'fluctus-proxy', 'Accept': 'application/vnd.github.v3+json' };
    if (GITHUB_TOKEN) headers['Authorization'] = `token ${GITHUB_TOKEN}`;
    const r = await fetch(apiUrl, { headers });
    if (r.status === 404) return res.json(Object.assign(samenvatting, { melding: 'projecten/ map bestaat (nog) niet' }));
    if (!r.ok) throw new Error(`projecten-lijst: HTTP ${r.status}`);
    const entries = await r.json();
    const projecten = entries.filter(e => e.type === 'dir').map(e => e.name);
    samenvatting.projecten = projecten.length;
    for (const project of projecten) {
      let namen = [];
      try { namen = await _scenariosGithubListProject(project); }
      catch (e) { samenvatting.fouten.push(`${project}: list faalde (${e.message})`); continue; }
      for (const naam of namen) {
        samenvatting.scenarios_totaal++;
        const pad = _scenarioPad(project, naam);
        try {
          const { data, sha } = await _scenariosGithubRead(pad);
          if (data && data.owner_uid) { samenvatting.overgeslagen++; continue; }
          const nieuw = Object.assign({}, data, {
            owner_uid: eigenaar,
            _owner_gemigreerd_op: new Date().toISOString(),
          });
          await _scenariosGithubWrite(pad, nieuw, sha);
          // Cache verversen zodat read-through direct de gestempelde versie ziet
          if (SCENARIOS_DB[project]) SCENARIOS_DB[project][naam] = nieuw;
          samenvatting.gemigreerd++;
        } catch (e) {
          samenvatting.fouten.push(`${project}/${naam}: ${e.message}`);
        }
      }
    }
    return res.json(samenvatting);
  } catch (e) {
    console.error(`[migrate-owners] fout: ${e.message}`);
    return res.status(500).json({ error: e.message, samenvatting });
  }
});

// Guard-helper voor scenario-routes. Returnt user-object of null; bij null is
// de response al verstuurd (401/403). Bij enforcement UIT: returnt een
// permissief pseudo-user zodat bestaande flows blijven werken (rollback-pad).
async function _scenarioGuard(req, res) {
  if (!AUTH_ENFORCE) return { id: null, naam: null, role: 'manager', status: 'active', _enforcementUit: true };
  const u = await resolveUser(req);
  if (!u) {
    res.status(401).json({ error: 'Niet ingelogd. Log in via de Fluctus Academy.' });
    return null;
  }
  if (u.status !== 'active') {
    res.status(403).json({ error: 'Account niet actief. Neem contact op met uw manager.' });
    return null;
  }
  return u;
}

function _magScenarioZien(u, data) {
  if (_isManager(u)) return true;
  // Verkoper: enkel eigen scenarios. Zonder owner_uid (nog niet gemigreerd)
  // → niet zichtbaar voor verkopers; migratie-endpoint lost dit op.
  return !!(data && data.owner_uid && data.owner_uid === u.id);
}

// ─── ROUTES ───────────────────────────────────────────────────────────────────
app.get('/',       (req, res) => res.json({
  status:'ok', version:'15.15.2', ts:new Date().toISOString(), markt_geladen: !!MARKT,
  markt_status: MARKT_STATUS, markt_pogingen: MARKT_POGINGEN,
  markt_laatste_fout: MARKT_LAATSTE_FOUT,
  markt_periode: MARKT ? { van: MARKT.van, tot: MARKT.tot, n_kwartieren: MARKT.n_kwartieren } : null,
  market_config: {
    owner: MARKET_DATA_OWNER,
    repo: MARKET_DATA_REPO,
    path: MARKET_DATA_PATH,
    has_token: !!GITHUB_TOKEN
  }
}));
app.get('/health', (req, res) => res.json({ status:'ok', markt_status: MARKT_STATUS }));

// v15.14.1: handmatige markt-reload endpoint. Forceert een nieuwe laadpoging
// zonder Railway-redeploy. Idempotent: geen effect als al aan het laden.
app.post('/api/markt-reload', (req, res) => {
  if (MARKT_STATUS === 'loading') {
    return res.status(409).json({ status: 'loading', error: 'Markt wordt al geladen' });
  }
  console.log('[markt] Handmatige reload aangevraagd via /api/markt-reload');
  setImmediate(() => laadMarktdata(true));
  res.json({ status: 'reload_gestart', vorige_status: MARKT_STATUS, pogingen: MARKT_POGINGEN });
});

app.get('/api/postcode-grd', (req, res) => {
  const pc = String(req.query.postcode||'').trim();
  if (!/^\d{4}$/.test(pc)) return res.status(400).json({ error:'postcode moet 4 cijfers zijn' });
  const hit = POSTCODE_GRD[pc] || POSTCODE_GRD[String(Math.floor(parseInt(pc)/10)*10)];
  if (!hit) return res.status(404).json({ error:`Postcode ${pc} niet gevonden` });
  const gemeenten = (PC_GEMEENTE_INDEX[pc] || []);
  res.json({ postcode:pc, grd:hit.grd, dnb_volledig:hit.dnb, gemeenten });
});

// ─── POSTCODE FALLBACK (v15.10, BaseCase Uitbreiding Fase 2 sessie 3) ────────
// Body: { postcode: "8409" }
// Strategie A (laagste-buurman): bij MISS zoekt route de numeriek dichtstbij-
// zijnde LAGER genummerde postcode in POSTCODE_GRD, binnen radius 50.
// Geverifieerd: 8401→8400 (Δ=1), 8409→8400 (Δ=9), 3541→3540, 1001→1000,
// 9999→9992. Zie sessie-3 voortgangslog §11.2.
app.post('/api/postcode-fallback', (req, res) => {
  const body = req.body || {};
  const pcRaw = (body.postcode == null ? '' : String(body.postcode)).trim();
  if (!/^\d{4}$/.test(pcRaw)) {
    return res.status(400).json({ ok:false, error:'postcode moet 4 cijfers zijn' });
  }
  const pcInt = parseInt(pcRaw, 10);

  // Directe hit
  if (POSTCODE_GRD[pcRaw]) {
    const hit = POSTCODE_GRD[pcRaw];
    const gemeenten = (PC_GEMEENTE_INDEX[pcRaw] || []);
    return res.json({
      ok: true,
      postcode: pcRaw,
      postcodeFallback: pcRaw,
      afstand: 0,
      grd: hit.grd,
      dnb_volledig: hit.dnb,
      gemeenten,
      confidence: 'exact'
    });
  }

  // Laagste-buurman binnen radius
  const idx = _laagsteBuurmanIndex(pcInt);
  if (idx === -1) {
    return res.status(404).json({
      ok: false,
      postcode: pcRaw,
      reden: `Geen lager genummerde postcode in DB (range start ${POSTCODE_KEYS_SORTED[0]||'?'})`,
      confidence: 'none'
    });
  }
  const buurmanInt = POSTCODE_KEYS_SORTED[idx];
  const afstand = pcInt - buurmanInt;
  if (afstand > POSTCODE_FALLBACK_MAX_DELTA) {
    return res.status(404).json({
      ok: false,
      postcode: pcRaw,
      reden: `Geen buurpostcode binnen ${POSTCODE_FALLBACK_MAX_DELTA} (dichtstbij: ${String(buurmanInt).padStart(4,'0')}, Δ=${afstand})`,
      confidence: 'none'
    });
  }
  const buurmanStr = String(buurmanInt).padStart(4, '0');
  const hit = POSTCODE_GRD[buurmanStr];
  const gemeenten = (PC_GEMEENTE_INDEX[buurmanStr] || []);
  return res.json({
    ok: true,
    postcode: pcRaw,
    postcodeFallback: buurmanStr,
    afstand,
    grd: hit.grd,
    dnb_volledig: hit.dnb,
    gemeenten,
    confidence: 'fallback'
  });
});

app.get('/api/gemeenten-lijst', (req, res) => {
  res.json({ gemeenten: GEMEENTEN_LIJST });
});

// v15.20.2: WELKE TARIEFKAART DRAAIT ER?
// Deze endpoint bestond al maar gaf enkel losse getallen terug — je moest zelf weten
// welke waarde 'goed' was. Daardoor draaide de proxy een tijd op de oude kaart zonder
// dat het opviel; het kwam pas uit toen een klantcase een capaciteitskost van 33.292
// EUR toonde op een LS-aansluiting van 100 kVA, waar het plafond 5.012 EUR is.
// Nu geeft hij een expliciet oordeel i.p.v. cijfers die je zelf moet duiden.
//
// Nu geeft hij een expliciet oordeel. De diagnose leunt op één veld:
// transport_maandpiek_eur_kw_mnd hoort in Vlaanderen/Brussel 0 te zijn (de VREG-kaart
// bevat de transmissiekosten al). Staat er 21,77 dan draait de oude kolomverschuiving,
// die op LS ~26.000 EUR/jaar fantoomkost aanrekende bij 100 kW.
app.get('/api/regio-tarieven', (req, res) => {
  try {
    const grd = req.query.grd || 'Fluvius West';
    const spanning = req.query.spanning || 'LS';
    const t = _kiesTarieven(grd, spanning) || {};
    const _n = v => Number(v) || 0;
    const trKw  = _n(t.transport_maandpiek_eur_kw_mnd) * 12 + _n(t.transport_jaarpiek_eur_kw_jaar)
                + _n(t.transport_beschikbaar_eur_kva_jaar);
    const trMwh = _n(t.transport_systeembeheer_eur_mwh) + _n(t.transport_reserves_eur_mwh)
                + _n(t.transport_marktintegratie_eur_mwh);
    const regio = t._regio || null;
    const verwachtNul = (regio === 'Vlaanderen' || regio === 'Brussel');
    let oordeel, uitleg;
    if (!TARIEVEN_MAP || !Object.keys(TARIEVEN_MAP).length) {
      oordeel = 'GEEN_KAART';
      uitleg = 'data/tarieven.json is niet geladen — de server draait op de ingebouwde fallback.';
    } else if (regio == null) {
      oordeel = 'OUDE_KAART';
      uitleg = 'Geen _regio-veld: dit is een tarieven.json van vóór build_tarieven.py. ' +
               'Genereer opnieuw met tools/tarieven/build_tarieven.py.';
    } else if (verwachtNul && (trKw > 0.01 || trMwh > 0.01)) {
      oordeel = 'OUDE_KAART';
      uitleg = `Regio ${regio} heeft transport_* != 0 (${trKw.toFixed(2)} EUR/kW/jaar + ` +
               `${trMwh.toFixed(2)} EUR/MWh). Daar zit de Elia-dubbeltelling nog in.`;
    } else if (regio === 'Wallonie' && trKw === 0 && trMwh === 0) {
      oordeel = 'VERDACHT';
      uitleg = 'Wallonie hoort transport_* WEL te hebben (Elia wordt daar apart doorgerekend).';
    } else {
      oordeel = 'OK';
      uitleg = `Regio ${regio}: transportbehandeling klopt.`;
    }
    // Een onbekende GRD valt in _kiesTarieven stil terug op de West-kaart. Dan een
    // vrolijke "OK" teruggeven is misleidend: je beoordeelt een kaart die niet van
    // deze klant is. Expliciet melden.
    const zoneKey = (GRD_NAAR_ZONE[grd] || String(grd || '').replace(/^Fluvius\s+/, '')) + '|' + spanning;
    const exact = !!(TARIEVEN_MAP && TARIEVEN_MAP[zoneKey]);
    if (!exact && oordeel === 'OK') {
      oordeel = 'FALLBACK';
      uitleg = `Geen kaart voor "${zoneKey}" — teruggevallen op West|${spanning}. ` +
               `Het oordeel gaat dus NIET over deze netbeheerder.`;
    }
    return res.json({
      // De zone-afleiding staat in _kiesTarieven; hier dezelfde regel, niet een
      // verzonnen helper. (v15.20.2 verwees naar _grdNaarZone, dat niet bestaat.)
      grd, spanning, zone: (GRD_NAAR_ZONE[grd] || String(grd || '').replace(/^Fluvius\s+/, '')),
      exacte_kaart: exact,
      oordeel, uitleg,
      tariefjaar: t._tariefjaar || null,
      regio, bron: t._bron || null,
      gegenereerd: (TARIEVEN_MAP._meta && TARIEVEN_MAP._meta.gegenereerd_op) || null,
      kerncijfers: {
        netgebruik_eur_mwh: _n(t.proportioneel_eur_mwh),
        odv_eur_mwh: _n(t.odv_eur_mwh),
        toeslagen_eur_mwh: _n(t.surcharges_eur_mwh),
        volumetrisch_totaal_eur_mwh: _n(t.proportioneel_eur_mwh) + _n(t.odv_eur_mwh) + _n(t.surcharges_eur_mwh),
        maandpiek_eur_kw_jaar: _n(t.maandpiek_eur_kw_jaar),
        toegangsvermogen_eur_kw_jaar: _n(t.toegangsvermogen_eur_kw_jaar),
        transport_eur_kw_jaar: trKw,
        transport_eur_mwh: trMwh,
      },
      raw: t,
    });
  } catch (e) {
    console.error('[regio-tarieven] fout:', e.message);
    return res.status(500).json({ error: 'regio-tarieven gefaald: ' + e.message });
  }
});

// ─── v15.21.0 — DE LS/MS-POORT ──────────────────────────────────────────────
// De LS/MS-keuze is een POORT die je één keer vooraf beslist (overdracht §4), geen
// scenario-as. Puur arithmetiek: alle termen zijn bekend zodra het laadplein is
// ingevuld — geen dispatch, milliseconde. We ADVISEREN niet hard, we tonen twee
// getallen en de verkoper kiest:
//   1. netkosten LS vs MS per jaar bij dit verbruik (E) en deze piek (P)
//   2. payback van de cabine (€108.000 = €90.000 + 20% kabeltracé) op de jaarlijkse
//      netkostenbesparing
//
//   MS goedkoper ⟺ E·(vol_LS − vol_MS) > P·(mp_MS + tv_MS − mp_LS) + Δvast
//   vol   = proportioneel + odv + surcharges + soldes + accijns_basis   [€/MWh]
//   mp/tv = maandpiek / toegangsvermogen                                [€/kW/jaar]
//   Δvast = (databeheer_MS + energiefonds_MS) − (databeheer_LS + energiefonds_LS)
//   Kantelpunt: E* = a·P + b  met a = (mp_MS+tv_MS−mp_LS)/(vol_LS−vol_MS), b = Δvast/(vol_LS−vol_MS)
//
// P = HUIDIG toegangsvermogen UIT DE FACTUUR (overdracht §4): het max-batterij-scenario
// is per definitie "de aansluiting hoeft niet omhoog", dus het huidige vermogen ís het
// ontwerpdoel — en daarmee de juiste conventie voor de poort.
//
// Geverifieerd tegen de kantelpunt-tabel uit §4: a/b/E* exact voor alle 8 Vlaamse zones,
// Δvast Midden-Vl. = +2.224. Wallonië/Brussel: transport_* zit hier WEL in de netkost,
// maar het kantelpunt is daar nooit factuur-gevalideerd (openstaand punt 54) → we vlaggen
// het resultaat als niet-gevalideerd zodra regio ≠ Vlaanderen.
const POORT_CABINE_EUR = 108000; // €90.000 cabine + 20% kabeltracé (§4 / naamgeving overdracht §6)
function _poortVolMwh(k) {
  return (Number(k.proportioneel_eur_mwh) || 0) + (Number(k.odv_eur_mwh) || 0)
       + (Number(k.surcharges_eur_mwh) || 0) + (Number(k.soldes_eur_mwh) || 0)
       + (Number(k.accijns_basis_eur_mwh) || 0);
}
// transport per kaart (Vlaanderen/Brussel = 0; Wallonië ingevuld) — zelfde afleiding als /api/regio-tarieven
function _poortTransportKw(k) {
  return (Number(k.transport_maandpiek_eur_kw_mnd) || 0) * 12 + (Number(k.transport_jaarpiek_eur_kw_jaar) || 0)
       + (Number(k.transport_beschikbaar_eur_kva_jaar) || 0);
}
function _poortTransportMwh(k) {
  return (Number(k.transport_systeembeheer_eur_mwh) || 0) + (Number(k.transport_reserves_eur_mwh) || 0)
       + (Number(k.transport_marktintegratie_eur_mwh) || 0);
}
// Volledige netkost van één kaart bij (E MWh, P kW). Bevat NIET de commodity (die is
// leveranciersafhankelijk en identiek voor LS/MS) — enkel netbeheer + heffingen die
// tussen LS en MS verschillen. tv_LS = 0 in de data, dus toegangsvermogen telt alleen op MS.
function _poortNetkost(k, E_mwh, P_kw) {
  return _poortVolMwh(k) * E_mwh
       + ((Number(k.maandpiek_eur_kw_jaar) || 0) + (Number(k.toegangsvermogen_eur_kw_jaar) || 0)) * P_kw
       + _poortTransportKw(k) * P_kw + _poortTransportMwh(k) * E_mwh
       + (Number(k.databeheer_eur_jaar) || 0) + (Number(k.energiefonds_eur_jaar) || 0);
}
function _lsMsPoort(grd, E_mwh, P_kw) {
  const LS = _kiesTarieven(grd, 'LS') || {};
  const MS = _kiesTarieven(grd, 'MS') || {};
  const volLS = _poortVolMwh(LS), volMS = _poortVolMwh(MS);
  const dVol = volLS - volMS;                                            // €/MWh, >0 (LS volumetrisch, MS niet)
  const dCap = (Number(MS.maandpiek_eur_kw_jaar) || 0) + (Number(MS.toegangsvermogen_eur_kw_jaar) || 0)
             - (Number(LS.maandpiek_eur_kw_jaar) || 0);                  // €/kW/jaar
  const dVast = ((Number(MS.databeheer_eur_jaar) || 0) + (Number(MS.energiefonds_eur_jaar) || 0))
              - ((Number(LS.databeheer_eur_jaar) || 0) + (Number(LS.energiefonds_eur_jaar) || 0));
  const a = dVol !== 0 ? dCap / dVol : null;                             // E* = a·P + b
  const b = dVol !== 0 ? dVast / dVol : null;
  const Estar = (a != null) ? a * P_kw + b : null;                       // MWh/jaar waarboven MS goedkoper
  const nkLS = _poortNetkost(LS, E_mwh, P_kw);
  const nkMS = _poortNetkost(MS, E_mwh, P_kw);
  const besparing = nkLS - nkMS;                                         // >0 → MS goedkoper op de factuur
  const regio = LS._regio || MS._regio || null;
  return {
    grd, verbruik_mwh: E_mwh, piek_kw: P_kw, regio,
    tariefjaar: LS._tariefjaar || MS._tariefjaar || null,
    netkost_ls_eur_jaar: Math.round(nkLS),
    netkost_ms_eur_jaar: Math.round(nkMS),
    netkosten_besparing_ms_eur_jaar: Math.round(besparing),   // negatief = LS goedkoper
    ms_goedkoper: besparing > 0,
    kantelpunt_mwh: Estar != null ? Math.round(Estar * 10) / 10 : null,
    boven_kantelpunt: (Estar != null) ? (E_mwh > Estar) : null,
    helling_a: a != null ? Math.round(a * 1000) / 1000 : null,
    intercept_b: b != null ? Math.round(b * 10) / 10 : null,
    delta_vast_eur_jaar: Math.round(dVast),
    cabine_eur: POORT_CABINE_EUR,
    // payback cabine = investering / jaarlijkse netkostenbesparing (alleen zinvol als MS goedkoper)
    cabine_payback_jaar: besparing > 0 ? Math.round(POORT_CABINE_EUR / besparing * 10) / 10 : null,
    gevalideerd: regio === 'Vlaanderen',   // Wallonië/Brussel: kantelpunt nooit factuur-gevalideerd (openstaand 54)
  };
}
// GET /api/ls-ms-poort?grd=Fluvius%20West&verbruik_mwh=384&piek_kw=100
// E = bestaand jaarverbruik + laadplein-energie; P = huidig toegangsvermogen uit de factuur.
app.get('/api/ls-ms-poort', (req, res) => {
  try {
    const grd = req.query.grd || 'Fluvius West';
    const E = Number(req.query.verbruik_mwh);
    const P = Number(req.query.piek_kw);
    if (!(E >= 0) || !(P >= 0)) {
      return res.status(400).json({ error: 'verbruik_mwh en piek_kw zijn verplicht en >= 0' });
    }
    return res.json(_lsMsPoort(grd, E, P));
  } catch (e) {
    console.error('[ls-ms-poort] fout:', e.message);
    return res.status(500).json({ error: 'ls-ms-poort gefaald: ' + e.message });
  }
});

app.get('/api/leveringscontract-staffel', (req, res) => {
  const meta = CONTRACT_RAW || {};
  res.json({ leverancier: meta.leverancier||'Enwyse', schijven:CONTRACT_STAFFEL, staffel:CONTRACT_STAFFEL,
             vergroening_eur_per_mwh: meta.vergroening_eur_per_mwh||2.50,
             vast_eur_per_maand: meta.vast_eur_per_maand||10.00,
             gsc_eur_mwh: meta.gsc_eur_mwh||11.0,
             wkk_eur_mwh: meta.wkk_eur_mwh||4.20 });
});

app.get('/api/profielen-lijst', (req, res) => res.json({ profielen:PROFIELEN_LIJST }));

app.get('/api/profiel', (req, res) => {
  const naam = req.query.naam || 'Slager';
  // Zoek profiel in data/profielen/<naam>.json (case-insensitive bestandsnaam)
  const profielDir = path.join(__dirname, 'data', 'profielen');
  if (fs.existsSync(profielDir)) {
    // Probeer exacte naam, dan lowercase
    for (const kandidaat of [naam + '.json', naam.toLowerCase() + '.json']) {
      const fp = path.join(profielDir, kandidaat);
      if (fs.existsSync(fp)) {
        const data = JSON.parse(fs.readFileSync(fp, 'utf8'));
        return res.json({ naam, kwartier: Array.isArray(data) ? data : data.profiel_kwartier || [] });
      }
    }
    // Probeer case-insensitive match in de directory
    try {
      const files = fs.readdirSync(profielDir);
      const _target = _profielFileNormalize(naam);
      const match = files.find(f => f.toLowerCase() === naam.toLowerCase() + '.json')
                 || files.find(f => _profielFileNormalize(f) === _target);
      if (match) {
        const data = JSON.parse(fs.readFileSync(path.join(profielDir, match), 'utf8'));
        return res.json({ naam, kwartier: Array.isArray(data) ? data : data.profiel_kwartier || [] });
      }
    } catch(e) {}
  }
  // Fallback: gebruik MARKT profiel (slager als default)
  if (MARKT && MARKT.profiel && MARKT.profiel.length === 35040) {
    console.warn(`[profiel] '${naam}' niet gevonden, gebruik default (slager)`);
    return res.json({ naam, kwartier: MARKT.profiel });
  }
  res.status(404).json({ error:`Profiel '${naam}' niet gevonden` });
});

app.get('/api/batterijen', (req, res) => res.json({ batterijen:BATTERIJEN }));

app.post('/api/batterij-toevoegen', (req, res) => {
  const { naam, kwh, kw, eta, dod, max_cycli, capex } = req.body || {};
  if (!naam||!kwh||!kw) return res.status(400).json({ error:'naam, kwh en kw zijn verplicht' });
  const id = naam.toLowerCase().replace(/\s+/g,'-');
  BATTERIJEN.push({ id, naam, kwh:Number(kwh), kw:Number(kw), eta:Number(eta)||0.85, dod:Number(dod)||0.90, capex:Number(capex)||0, max_cycli:Number(max_cycli)||8000 });
  res.json({ ok:true, id, totaal:BATTERIJEN.length });
});

// v15.15.2 hotfix: PROJECTEN_DB is in-memory en start LEEG na elke
// Railway-restart — dropdown bleef dan leeg tot een scenario-route het
// project aanraakte (bestond al vóór 9a, werd gemaskeerd doordat de server
// zelden herstartte; door de 9a-deploys zichtbaar geworden). Fix:
// read-through naar de GitHub projecten/-directory bij lege cache.
app.get('/api/projecten', async (req, res) => {
  if (PROJECTEN_DB.size === 0) {
    try {
      const apiUrl = `https://api.github.com/repos/${SCENARIOS_REPO_OWNER}/${SCENARIOS_REPO_NAME}/contents/${SCENARIOS_PATH_PREFIX}`;
      const headers = { 'User-Agent': 'fluctus-proxy', 'Accept': 'application/vnd.github.v3+json' };
      if (GITHUB_TOKEN) headers['Authorization'] = `token ${GITHUB_TOKEN}`;
      const r = await fetch(apiUrl, { headers });
      if (r.ok) {
        const entries = await r.json();
        entries.filter(e => e.type === 'dir').forEach(e => PROJECTEN_DB.add(e.name));
        console.log(`[projecten] ${PROJECTEN_DB.size} projecten geladen uit GitHub (cache was leeg)`);
      } else if (r.status !== 404) {
        console.warn(`[projecten] GitHub-lijst faalde: HTTP ${r.status}`);
      }
    } catch (e) { console.warn(`[projecten] GitHub-lijst faalde: ${e.message}`); }
  }
  res.json({ projecten: [...PROJECTEN_DB] });
});

// v15.11 sessie 4 sub-track 4: GET /api/scenarios — read-through cache.
// Bij cache-miss (eerste call voor project, of na Railway-restart):
// listen we projecten/{project}/ in de fluctus-scenarios repo.
// v15.15 sessie 9a: owner-filtering. Managers zien alle scenarios; verkopers
// enkel scenarios met eigen owner_uid. Filtering vereist de scenario-inhoud
// (owner staat IN de JSON), dus voor verkopers doen we een read-through per
// naam. Projecten hebben typisch ≤ 6 scenarios — cache houdt dit snel.
async function _filterScenarioNamen(u, project, namen) {
  if (_isManager(u)) return namen;
  const zichtbaar = [];
  for (const naam of namen) {
    let data = (SCENARIOS_DB[project] || {})[naam];
    if (!data) {
      try {
        const gelezen = await _scenariosGithubRead(_scenarioPad(project, naam));
        data = gelezen.data;
        if (!SCENARIOS_DB[project]) SCENARIOS_DB[project] = {};
        SCENARIOS_DB[project][naam] = data;
      } catch (_) { continue; } // onleesbaar → niet tonen
    }
    if (_magScenarioZien(u, data)) zichtbaar.push(naam);
  }
  return zichtbaar;
}

app.get('/api/scenarios', async (req, res) => {
  const project = req.query.project;
  if (!project) return res.status(400).json({ error: 'project query-param verplicht' });
  const u = await _scenarioGuard(req, res);
  if (!u) return;
  // Cache hit: returnt direct (na owner-filter)
  if (SCENARIOS_DB[project]) {
    const namen = await _filterScenarioNamen(u, project, Object.keys(SCENARIOS_DB[project]));
    return res.json({ scenarios: namen, source: 'cache' });
  }
  // Cache miss: probeer GitHub
  try {
    const names = await _scenariosGithubListProject(project);
    if (names.length > 0) {
      SCENARIOS_DB[project] = SCENARIOS_DB[project] || {};
      // Markeer aanwezigheid (lazy load van inhoud bij /api/scenario)
      for (const n of names) {
        if (!SCENARIOS_DB[project][n]) SCENARIOS_DB[project][n] = null;
      }
      PROJECTEN_DB.add(project);
    }
    const namen = await _filterScenarioNamen(u, project, names);
    return res.json({ scenarios: namen, source: 'github' });
  } catch (e) {
    console.warn(`[scenarios] list ${project} fail: ${e.message}`);
    // Bij fout: returnt lege array (niet 500, want UI moet kunnen verder)
    return res.json({ scenarios: [], source: 'github-error', error: e.message });
  }
});

// v15.11 sessie 4: GET /api/scenario — read-through cache, lazy load van GitHub.
app.get('/api/scenario', async (req, res) => {
  const project = req.query.project;
  const scenario = req.query.scenario;
  if (!project || !scenario) {
    return res.status(400).json({ error: 'project en scenario query-params verplicht' });
  }
  const u = await _scenarioGuard(req, res); // v15.15 sessie 9a
  if (!u) return;
  // Cache hit (en data niet null = niet alleen lazy-marker)
  const cached = (SCENARIOS_DB[project] || {})[scenario];
  if (cached) {
    if (!_magScenarioZien(u, cached)) {
      return res.status(403).json({ error: 'Geen toegang tot dit scenario.' });
    }
    return res.json({ data: cached, source: 'cache' });
  }
  // Cache miss of lazy-marker: lees van GitHub
  try {
    const { data } = await _scenariosGithubRead(_scenarioPad(project, scenario));
    if (!SCENARIOS_DB[project]) SCENARIOS_DB[project] = {};
    SCENARIOS_DB[project][scenario] = data;
    PROJECTEN_DB.add(project);
    if (!_magScenarioZien(u, data)) {
      return res.status(403).json({ error: 'Geen toegang tot dit scenario.' });
    }
    return res.json({ data, source: 'github' });
  } catch (e) {
    console.warn(`[scenario] read ${project}/${scenario} fail: ${e.message}`);
    return res.status(404).json({ error: 'Scenario niet gevonden', detail: e.message });
  }
});

// v15.11 sessie 4: POST /api/scenario-bewaren — schrijf naar GitHub + cache.
// Bug-fix: vroeger alleen in-memory cache; UI loog "Bewaard in fluctus-scenarios
// repo" zonder dat het waar was. Nu écht naar github.com/<owner>/fluctus-scenarios
// gecommit, met read-through cache update.
app.post('/api/scenario-bewaren', async (req, res) => {
  const { project, scenario, data } = req.body || {};
  if (!project || !scenario) {
    return res.status(400).json({ error: 'project en scenario zijn verplicht' });
  }
  // v15.15 sessie 9a: owner-stempel + schrijf-guard.
  const u = await _scenarioGuard(req, res);
  if (!u) return;
  if (data && typeof data === 'object') {
    const bestaand = (SCENARIOS_DB[project] || {})[scenario];
    if (bestaand && bestaand.owner_uid && !_magScenarioZien(u, bestaand)) {
      return res.status(403).json({ error: 'Dit scenario is van een andere verkoper.' });
    }
    if (!data.owner_uid && u.id) {
      data.owner_uid  = u.id;
      data.owner_naam = u.naam;
    } else if (data.owner_uid && !_magScenarioZien(u, data)) {
      return res.status(403).json({ error: 'Dit scenario is van een andere verkoper.' });
    }
  }
  // Cache update eerst (zodat UI direct kan lezen, ook als GitHub traag is)
  if (!SCENARIOS_DB[project]) SCENARIOS_DB[project] = {};
  SCENARIOS_DB[project][scenario] = data;
  PROJECTEN_DB.add(project);

  // GitHub-commit. Bij fout: meld eerlijk dat alleen in-memory bewaard is.
  const filepath = _scenarioPad(project, scenario);
  let sha;
  try {
    const existing = await _scenariosGithubRead(filepath);
    sha = existing.sha;
  } catch (_) {
    // Bestand bestaat nog niet — sha blijft undefined, dat is OK voor create
  }
  try {
    await _scenariosGithubWrite(filepath, data, sha);
    return res.json({
      ok: true,
      source: 'github',
      message: `Scenario bewaard in ${SCENARIOS_REPO_OWNER}/${SCENARIOS_REPO_NAME}`,
      path: filepath,
    });
  } catch (e) {
    console.error(`[scenario-bewaren] GitHub write fail: ${e.message}`);
    // Geef partial-success terug: in-memory wel, GitHub niet.
    return res.status(207).json({
      ok: false,
      source: 'cache-only',
      message: `Scenario bewaard in geheugen, maar GitHub-commit faalde: ${e.message}`,
      cached: true,
      path: filepath,
    });
  }
});

// v15.12 sessie 5b: POST /api/scenarios-batch-bewaren — sequentieel meerdere
// scenario's persistéren in fluctus-scenarios repo + cache. Wrapper rond
// _scenariosGithubWrite, gebruikt door de "Maak voorstel"-flow in Simulator.txt
// v1.17 om in één click Sc2 + Sc3 + Sc4 aan te maken na factuur-vergelijking.
//
// Body:
//   { project: 'SMARTUNIT',
//     scenarios: [{scenario: '2_DynamischContract_01-26', data: {...}},
//                 {scenario: '3_DynamischContract_12M',  data: {...}},
//                 {scenario: '4_Voorstel_PV_BESS',       data: {...}}] }
//
// Response:
//   { ok: true|false,                          // false als > 0 fouten
//     results: [
//       {scenario, ok: true,  source: 'github',     message, path},
//       {scenario, ok: false, source: 'cache-only', message, path, error},
//       ...
//     ],
//     summary: { totaal: 3, github: 2, cacheOnly: 1 } }
//
// Best-effort: een github-fout op één scenario stopt de batch NIET. Cache
// wordt voor ALLE scenario's bijgewerkt zodat de UI ze direct kan tonen.
app.post('/api/scenarios-batch-bewaren', async (req, res) => {
  const { project, scenarios } = req.body || {};
  if (!project || !Array.isArray(scenarios) || scenarios.length === 0) {
    return res.status(400).json({ error: 'project en scenarios[] zijn verplicht' });
  }
  // v15.15 sessie 9a: owner-stempel op elk scenario in de batch.
  const u = await _scenarioGuard(req, res);
  if (!u) return;
  for (const s of scenarios) {
    if (s && s.data && typeof s.data === 'object' && !s.data.owner_uid && u.id) {
      s.data.owner_uid  = u.id;
      s.data.owner_naam = u.naam;
    }
  }
  if (scenarios.length > 10) {
    return res.status(400).json({ error: 'Max 10 scenarios per batch' });
  }
  for (const s of scenarios) {
    if (!s || !s.scenario || !s.data) {
      return res.status(400).json({ error: 'elk scenarios[] item moet {scenario, data} hebben' });
    }
  }

  // Cache-update eerst voor alle scenario's (idem aan single-bewaren patroon)
  if (!SCENARIOS_DB[project]) SCENARIOS_DB[project] = {};
  for (const s of scenarios) {
    SCENARIOS_DB[project][s.scenario] = s.data;
  }
  PROJECTEN_DB.add(project);

  // GitHub-commit sequentieel. We doen niet Promise.all want fluctus-scenarios
  // is een kleine repo en parallelle commits kunnen sha-conflicten geven.
  const results = [];
  let okCount = 0;
  let cacheOnlyCount = 0;

  for (const s of scenarios) {
    const filepath = _scenarioPad(project, s.scenario);
    let sha;
    try {
      const existing = await _scenariosGithubRead(filepath);
      sha = existing.sha;
    } catch (_) {
      // Bestand bestaat nog niet — sha undefined = create i.p.v. update
    }
    try {
      await _scenariosGithubWrite(filepath, s.data, sha);
      results.push({
        scenario: s.scenario,
        ok: true,
        source: 'github',
        message: `Scenario bewaard in ${SCENARIOS_REPO_OWNER}/${SCENARIOS_REPO_NAME}`,
        path: filepath
      });
      okCount++;
    } catch (e) {
      console.error(`[scenarios-batch-bewaren] GitHub write fail ${s.scenario}: ${e.message}`);
      results.push({
        scenario: s.scenario,
        ok: false,
        source: 'cache-only',
        message: `Bewaard in geheugen, GitHub-commit faalde: ${e.message}`,
        path: filepath,
        error: e.message
      });
      cacheOnlyCount++;
    }
  }

  const allOk = cacheOnlyCount === 0;
  res.status(allOk ? 200 : 207).json({
    ok: allOk,
    results,
    summary: {
      totaal: scenarios.length,
      github: okCount,
      cacheOnly: cacheOnlyCount
    }
  });
});

// ─── BASE CASE FACTUUR-EXTRACTIE (Fase 1) ────────────────────────────────────
// Accepteert PDF (of image) als base64 in JSON body, stuurt naar Anthropic API
// met vision support, returnt gestructureerde JSON volgens STATE.baseCase.
app.post('/api/factuur-extract', async (req, res) => {
  const startTime = Date.now();
  try {
    const apiKey = process.env.ANTHROPIC_API_KEY;
    if (!apiKey) {
      return res.status(503).json({ error: 'ANTHROPIC_API_KEY niet geconfigureerd op Railway' });
    }

    const { files, model } = req.body || {};
    if (!Array.isArray(files) || files.length === 0) {
      return res.status(400).json({ error: 'files[] is verplicht en mag niet leeg zijn' });
    }
    if (files.length > 10) {
      return res.status(400).json({ error: 'Max 10 bestanden per request' });
    }

    const allowedTypes = new Set([
      'application/pdf', 'image/jpeg', 'image/png', 'image/gif', 'image/webp'
    ]);
    let totaalBytes = 0;
    for (const f of files) {
      if (!f || typeof f !== 'object') return res.status(400).json({ error: 'elk file element moet een object zijn' });
      if (!f.base64 || typeof f.base64 !== 'string') return res.status(400).json({ error: 'base64 verplicht per bestand' });
      if (!allowedTypes.has(f.mediaType)) {
        return res.status(415).json({
          error: `mediaType '${f.mediaType}' niet ondersteund. Toegestaan: ${[...allowedTypes].join(', ')}`,
          hint: "HEIC foto's: gebruik de Fluctus snippet om foto's naar PDF te converteren in de browser"
        });
      }
      totaalBytes += Math.floor(f.base64.length * 0.75);
    }
    if (totaalBytes > 10 * 1024 * 1024) {
      return res.status(413).json({
        error: `Totale upload ${(totaalBytes/1024/1024).toFixed(1)} MB overschrijdt limiet van 10 MB`,
        hint: 'Verklein foto resolutie of splits in meerdere requests'
      });
    }

    console.log(`[factuur-extract] start — ${files.length} bestand(en), ${(totaalBytes/1024).toFixed(0)} KB totaal`);

    const result = await factuurExtract.run({
      files,
      postcodes: POSTCODES_DATA || {},
      tarieven: TARIEVEN_MAP || {},
      apiKey,
      model
    });

    console.log(`[factuur-extract] OK in ${Date.now()-startTime}ms — model=${result._meta.model}, tokens=${result._meta.input_tokens||'?'}/${result._meta.output_tokens||'?'}`);

    // v15.16: bewaar de originele factuur in Supabase Storage (privaat) zodat ze
    // naast de analyse getoond en later als bijlage gemaild kan worden.
    // De verwijzing hoort IN result.baseCase — de wizard doet STATE.baseCase = r.baseCase,
    // dus alles wat op het top-level staat zou verloren gaan.
    // BEST-EFFORT: een opslagfout mag een geslaagde extractie nooit laten mislukken.
    const _bc = result.baseCase || (result.baseCase = {});
    _bc.factuur_bestanden = [];
    if (SUPABASE_OK) {
      for (const f of files) {
        try {
          const pad = _factuurPad(_bc, f.mediaType);   // leest klantBtw/factuurNummer uit baseCase
          await _factuurUpload(f.base64, f.mediaType, pad);
          _bc.factuur_bestanden.push({
            bucket: FACTUREN_BUCKET, pad,
            naam: f.fileName || f.naam || null,
            mediaType: f.mediaType,
            bytes: Math.floor(f.base64.length * 0.75),
          });
          console.log(`[factuur-extract] factuur bewaard: ${FACTUREN_BUCKET}/${pad}`);
        } catch (e) {
          console.warn(`[factuur-extract] opslag factuur faalde (niet-blokkerend): ${e.message}`);
        }
      }
    } else {
      console.warn('[factuur-extract] Supabase niet geconfigureerd — factuur niet bewaard');
    }

    res.json(result);
  } catch (e) {
    console.error('[factuur-extract] FOUT:', e.message);
    if (/HTTP 4|niet-ondersteund/i.test(e.message)) {
      res.status(422).json({ error: e.message });
    } else if (/timeout|abort/i.test(e.message)) {
      res.status(504).json({ error: 'Factuur-extractie duurde te lang — probeer opnieuw (of upload een kleinere/duidelijkere scan).' });
    } else {
      res.status(500).json({ error: e.message });
    }
  }
});


// ─── FACTUUR-STAFFEL ──────────────────────────────────────────────────────────
// ─── v15.16: GET /api/factuur-bestand?pad=... ────────────────────────────────
// Geeft een KORTLEVENDE signed URL (10 min) terug voor een bewaarde factuur.
// De bucket blijft privaat: de browser krijgt nooit de service-key, en de URL
// verloopt. Vereist een ingelogde gebruiker — een factuur bevat klantgegevens
// (naam, BTW, adres, EAN, verbruik) en mag niet vrij opvraagbaar zijn.
app.get('/api/factuur-bestand', async (req, res) => {
  try {
    if (!SUPABASE_OK) return res.status(503).json({ error: 'Opslag niet geconfigureerd' });
    const u = await resolveUser(req);
    if (!u) return res.status(401).json({ error: 'Niet ingelogd' });
    const pad = String(req.query.pad || '');
    if (!pad || pad.includes('..')) return res.status(400).json({ error: 'Ongeldig pad' });
    const url = await _factuurSignedUrl(pad, 600);
    return res.json({ url, verloopt_over_sec: 600 });
  } catch (e) {
    console.error('[factuur-bestand] fout:', e.message);
    return res.status(500).json({ error: e.message });
  }
});

// ─── v15.17: factuuranalyse bewaren/ophalen (bytes in Storage, ref in scenario) ──
// De volledige factuuranalyse — INCLUSIEF de drie profielen-arrays (3 × 35.040
// waarden, ~700 KB) — gaat als één JSON-object naar de private bucket. In het
// scenario komt alleen het pad. Zo is een heropend onderhandelingsmarge-rapport
// IDENTIEK aan het origineel: zelfde marge, zelfde heatmaps, geen herberekening
// en dus geen drift. In GitHub zou dit onaanvaardbaar zijn (repo-bloat + AVG).
app.post('/api/factuuranalyse', async (req, res) => {
  try {
    if (!SUPABASE_OK) return res.status(503).json({ error: 'Opslag niet geconfigureerd' });
    const u = await resolveUser(req);
    if (!u) return res.status(401).json({ error: 'Niet ingelogd' });
    const b = req.body || {};
    if (!b.data || typeof b.data !== 'object') return res.status(400).json({ error: 'data (object) verplicht' });
    const veilig = (s, fb) => String(s || fb).replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 60);
    const pad = `${veilig(b.klant, 'onbekend')}/analyse-${veilig(b.stempel, String(Date.now()))}.json`;
    const json = JSON.stringify(b.data);
    if (json.length > 8 * 1024 * 1024) return res.status(413).json({ error: 'Analyse te groot (>8 MB)' });
    await _factuurUpload(Buffer.from(json, 'utf8').toString('base64'), 'application/json', pad);
    console.log(`[factuuranalyse] bewaard: ${FACTUREN_BUCKET}/${pad} (${(json.length/1024).toFixed(0)} KB)`);
    return res.json({ ok: true, bucket: FACTUREN_BUCKET, pad, bytes: json.length });
  } catch (e) {
    console.error('[factuuranalyse] bewaren faalde:', e.message);
    return res.status(500).json({ error: e.message });
  }
});

app.get('/api/factuuranalyse', async (req, res) => {
  try {
    if (!SUPABASE_OK) return res.status(503).json({ error: 'Opslag niet geconfigureerd' });
    const u = await resolveUser(req);
    if (!u) return res.status(401).json({ error: 'Niet ingelogd' });
    const pad = String(req.query.pad || '');
    if (!pad || pad.includes('..')) return res.status(400).json({ error: 'Ongeldig pad' });
    const txt = await _factuurDownload(pad);
    return res.type('application/json').send(txt);
  } catch (e) {
    console.error('[factuuranalyse] ophalen faalde:', e.message);
    return res.status(500).json({ error: e.message });
  }
});

// POST /api/factuur-staffel-bepalen
// Body: { profielNaam, afnameKwh, periodeVan, periodeTot, [staffel] }
// Response (zie project_jaarverbruik.js):
//   ok=true:  { ok, geprojecteerdJaarverbruikMWh, tier, _diagnose }
//   ok=false: { ok=false, status: "ONBETROUWBAAR" | "FOUT", reden, _diagnose }
// HTTP status codes:
//   200 — ok=true OF ok=false met status ONBETROUWBAAR (beide normale flow)
//   400 — body-validatie faalde
//   404 — profiel niet gevonden in data/profielen/
//   500 — onverwachte server-fout

// Helper: laad één profiel uit data/profielen/<naam>.json (case-insensitive),
// dezelfde zoeklogica als de bestaande GET /api/profiel route.
function _laadProfielKwartier(profielNaam) {
  const profielDir = path.join(__dirname, 'data', 'profielen');
  if (!fs.existsSync(profielDir)) return null;
  for (const kandidaat of [profielNaam + '.json', profielNaam.toLowerCase() + '.json']) {
    const fp = path.join(profielDir, kandidaat);
    if (fs.existsSync(fp)) {
      try {
        const data = JSON.parse(fs.readFileSync(fp, 'utf8'));
        return Array.isArray(data) ? data : (data.profiel_kwartier || null);
      } catch (e) {
        return null;
      }
    }
  }
  try {
    const files = fs.readdirSync(profielDir);
    const _target = _profielFileNormalize(profielNaam);
    const match = files.find(f => f.toLowerCase() === profielNaam.toLowerCase() + '.json')
               || files.find(f => _profielFileNormalize(f) === _target);
    if (match) {
      const data = JSON.parse(fs.readFileSync(path.join(profielDir, match), 'utf8'));
      return Array.isArray(data) ? data : (data.profiel_kwartier || null);
    }
  } catch (e) {}
  // v15.15.3 (bug: profiel niet "aanvaard" in factuur-modal): zelfde fallback
  // als GET /api/profiel. Zonder profiel-bestand gaf /api/factuur-staffel-bepalen
  // een 404 (profiel niet gevonden), terwijl stap 3 via /api/profiel wél werkte
  // omdat die al terugvalt op het in-memory MARKT-profiel. Nu consistent, zodat
  // de tier-bepaling in de modal niet meer faalt.
  if (MARKT && MARKT.profiel && MARKT.profiel.length === 35040) {
    console.warn(`[factuur-staffel] profiel '${profielNaam}' niet gevonden — fallback naar MARKT-profiel`);
    return MARKT.profiel;
  }
  return null;
}

app.post('/api/factuur-staffel-bepalen', (req, res) => {
  const body = req.body || {};
  const { profielNaam, afnameKwh, periodeVan, periodeTot, staffel } = body;

  if (typeof profielNaam !== 'string' || !profielNaam.trim()) {
    return res.status(400).json({ error: 'profielNaam is verplicht' });
  }
  if (typeof afnameKwh !== 'number' || !isFinite(afnameKwh)) {
    return res.status(400).json({ error: 'afnameKwh moet een getal zijn' });
  }
  if (typeof periodeVan !== 'string' || typeof periodeTot !== 'string') {
    return res.status(400).json({ error: 'periodeVan en periodeTot zijn verplicht (ISO YYYY-MM-DD)' });
  }

  const profielKwartier = _laadProfielKwartier(profielNaam);
  if (!profielKwartier) {
    return res.status(404).json({ error: `Profiel '${profielNaam}' niet gevonden` });
  }
  if (!Array.isArray(profielKwartier) || profielKwartier.length !== 35040) {
    return res.status(500).json({
      error: `Profiel '${profielNaam}' heeft ongeldige lengte: ${profielKwartier.length} (verwacht 35040)`
    });
  }

  const gebruikStaffel = (Array.isArray(staffel) && staffel.length > 0)
    ? staffel
    : CONTRACT_STAFFEL;

  try {
    const result = projectJaarverbruik({
      profielNaam,
      profielKwartier,
      afnameKwh,
      periodeVan,
      periodeTot,
      staffel: gebruikStaffel
    });
    return res.json(result);
  } catch (e) {
    console.error('[factuur-staffel-bepalen] onverwachte fout:', e);
    return res.status(500).json({ error: 'Server-fout: ' + e.message });
  }
});


// ─── SIMULATIE ────────────────────────────────────────────────────────────────
app.post('/api/nominatie-sim', (req, res) => {
  const input = req.body;
  if (!input || typeof input !== 'object')
    return res.status(400).json({ error:'body is verplicht' });
  if (!MARKT) {
    // v15.14.1: informatieve 503 op basis van werkelijke status + actieve retry-ladder.
    if (MARKT_STATUS === 'loading') {
      return res.status(503).json({
        error: 'Marktdata wordt geladen — probeer over 30 seconden opnieuw',
        status: 'loading', pogingen: MARKT_POGINGEN,
      });
    }
    if (MARKT_STATUS === 'failed') {
      return res.status(503).json({
        error: 'Marktdata kon niet geladen worden. De server probeert automatisch opnieuw (elke 5 min). ' +
               'Indien dit blijft duren, contacteer beheer.',
        status: 'failed', pogingen: MARKT_POGINGEN, laatste_fout: MARKT_LAATSTE_FOUT,
      });
    }
    return res.status(503).json({
      error: 'Marktdata nog niet geladen — probeer over 30 seconden opnieuw',
      status: MARKT_STATUS,
    });
  }

  const simulatorPath = path.join(__dirname, 'simulator.py');
  if (!fs.existsSync(simulatorPath))
    return res.status(500).json({ error:'simulator.py niet gevonden' });

  const startTime = Date.now();
  // Debug: log MARKT status
  console.log('[sim] MARKT status:', MARKT ? {
    n_kwartieren: MARKT.n_kwartieren,
    solar_kwartieren: MARKT.solar_norm ? MARKT.solar_norm.length : 0,
    solar_nonzero: MARKT.solar_norm ? MARKT.solar_norm.filter(v=>v>0).length : 0,
    van: MARKT.van, tot: MARKT.tot
  } : 'NULL');
  const simInput  = buildSimInput(input);
  console.log('[sim] pvVorm length:', simInput.pv ? simInput.pv.vorm_kwartier.length : 0,
    'nonzero:', simInput.pv ? simInput.pv.vorm_kwartier.filter(v=>v>0).length : 0);

  const proc = spawn('python3', [simulatorPath], { env:{...process.env, PYTHONUNBUFFERED:'1'} });

  let stdout = '', stderr = '';
  proc.stdout.on('data', c => { stdout += c.toString(); });
  proc.stderr.on('data', c => { stderr += c.toString(); });

  proc.on('close', code => {
    const elapsed = Date.now() - startTime;
    console.log(`[sim] exit=${code} elapsed=${elapsed}ms`);
    if (code !== 0) {
      console.error('[sim] stderr:', stderr.slice(-2000));
      return res.status(500).json({ error:'Simulator gefaald', exit_code:code, detail:stderr.slice(-1000) });
    }
    const s = stdout.indexOf('{'), e = stdout.lastIndexOf('}');
    if (s === -1 || e === -1)
      return res.status(500).json({ error:'Geen JSON output', raw:stdout.slice(0,500) });
    let result;
    try { result = JSON.parse(stdout.slice(s, e+1)); }
    catch (err) { return res.status(500).json({ error:'JSON parse fout', detail:err.message }); }
    result._meta = { elapsed_ms:elapsed, server_version:'15.11.1' };
    result._serverLog = stderr;
    res.json(result);
  });

  proc.on('error', err => res.status(500).json({ error:'Spawn error: '+err.message }));
  proc.stdin.write(JSON.stringify(simInput));
  proc.stdin.end();
});

// ─── 3-STURINGEN (v15.15.3) ──────────────────────────────────────────────────
// Draait per simulatie 3 sturing-varianten en geeft de meerwaarde-KPI's terug.
// simulator.py blijft ONGEWIJZIGD: we spawnen 'm 3× met per-variant aangepaste
// input. buildSimInput leidt de sturing af uit bsp.actief / pv_curtailment.actief
// / batterijId / pvInjStrategie, dus we hoeven enkel die vlaggen te zetten.
function _runSimulatorOnce(simInput) {
  return new Promise((resolve, reject) => {
    const simulatorPath = path.join(__dirname, 'simulator.py');
    const t0 = Date.now();
    const proc = spawn('python3', [simulatorPath], { env:{...process.env, PYTHONUNBUFFERED:'1'} });
    let stdout = '', stderr = '';
    proc.stdout.on('data', c => { stdout += c.toString(); });
    proc.stderr.on('data', c => { stderr += c.toString(); });
    proc.on('close', code => {
      const elapsed = Date.now() - t0;
      if (code !== 0) return reject(new Error('Simulator exit ' + code + ': ' + stderr.slice(-800)));
      const s = stdout.indexOf('{'), e = stdout.lastIndexOf('}');
      if (s === -1 || e === -1) return reject(new Error('Geen JSON output: ' + stdout.slice(0, 300)));
      let result;
      try { result = JSON.parse(stdout.slice(s, e + 1)); }
      catch (err) { return reject(new Error('JSON parse fout: ' + err.message)); }
      result._serverLog = stderr;
      result._elapsedMs = elapsed;
      resolve(result);
    });
    proc.on('error', err => reject(new Error('Spawn error: ' + err.message)));
    proc.stdin.write(JSON.stringify(simInput));
    proc.stdin.end();
  });
}

// Bouw een per-variant aangepaste UI-input. Zie buildSimInput voor hoe de
// vlaggen doorwerken (bsp.actief, pv_curtailment.actief, batterijId, contract.modus).
function _variantUi(ui, variant) {
  const v = JSON.parse(JSON.stringify(ui || {}));
  const heeftPv = (Number(v.pv_kwp || v.pvKwp || 0) > 0);
  v.pv_curtailment = v.pv_curtailment || {};
  v.bsp = v.bsp || {};
  v.geen_arbitrage = false;   // default; enkel variant 'geen' zet dit op true
  if (variant === 'geen') {
    // Geen sturing: batterij BLIJFT (indien aanwezig), maar wordt enkel gebruikt
    // voor zelfconsumptie (bij PV) + piekshaving — GEEN spot/IMB-arbitrage.
    // De vlag geen_arbitrage zet simulator.py in vlakke-dispatch-modus.
    // batterijId/batterijCustom ongewijzigd (batterij blijft dus in de sim).
    v.pvInjStrategie = 'geen';
    v.pv_curtailment.actief = false;
    v.bsp.actief = false;
    v.geen_arbitrage = true;
  } else if (variant === 'sturing') {
    // Volledige sturing EXCL. onbalans: zelfconsumptie + piekoptimalisatie +
    // spotmarkt-arbitrage (de LP doet dit inherent zodra er een batterij is).
    v.pvInjStrategie = heeftPv ? 'curtail_neg' : 'geen';
    v.pv_curtailment.actief = heeftPv;
    v.bsp.actief = false;
  } else { // 'onbalans'
    // Volledige sturing INCL. onbalans.
    v.pvInjStrategie = 'bsp_actief';
    v.pv_curtailment.actief = heeftPv;
    v.bsp.actief = true;
  }
  return v;
}

// v15.15.4: pas de config aan voor één van de twee opstellingen bij ontoereikend
// toegangsvermogen. 'verhogen' = toegangsvermogen optrekken tot benodigd niveau;
// 'batterij' = geadviseerde batterij (uit simulator.py capaciteit), aansluiting blijft.
// v15.18: LS/MS-drempel. Boven 100 kVA is een LS-aansluiting niet meer mogelijk —
// de klant gaat dan naar middenspanning, met een heel andere tariefkaart (MS heeft
// toegangsvermogen- en piektermen die LS niet kent). Zonder deze schakeling zou
// opstelling 1 een verzwaring naar bv. 250 kW nog steeds op LS-tarieven rekenen en
// dus veel te goedkoop uitvallen — precies de vergelijking die we willen maken.
const LS_MAX_KVA = 100;

// ─── v15.19: iteratieve, DISPATCH-GEVALIDEERDE dimensionering ────────────────
// Leest het aantal verloren dagen uit de sim-output. Dat is de enige harde bron:
// de LP-dispatch heeft dan écht geprobeerd te laden en het niet gekregen.
function _verlorenDagen(sim) {
  const d = (sim && sim.lp_diagnostics) || {};
  const vd = d.verloren_dagen;
  if (Array.isArray(vd)) return vd.length;
  return (typeof vd === 'number') ? vd : 0;
}
function _totaalDagen(sim) {
  const d = (sim && sim.lp_diagnostics) || {};
  return d.totaal_dagen || 365;
}

// v15.19.1 — HAALBAARHEID per opstelling. 'verloren_dagen' alléén volstaat NIET:
//   • Opstelling 1 wordt ongestuurd beoordeeld. simulator.py bouwt de EV-last dan op
//     een onbeperkte aansluiting (1e12) → de energie komt er ALTIJD, er is nooit een
//     tekort en nooit een verloren dag. De site overschrijdt simpelweg het contract en
//     betaalt overschrijding. 'Verzwaren' betekent dus: groot genoeg dat dat NIET gebeurt
//     → criterium = geen overschrijdingskost.
//   • Opstelling 2 houdt de aansluiting en laat de batterij het opvangen. Schiet die
//     tekort, dan verhoogt simulator.py ZELF de aansluiting (v1.8.10) en meldt dat via
//     laadplein.toegangsvermogen_verhoogd_kw. Dat is het bewijs dat de batterij te klein
//     is — de LP lost intussen probleemloos op.
// Zonder deze check zou de lus bij iteratie 0 stoppen en een te kleine opstelling
// doorrekenen: lage factuur, want er werd minder geladen of stilletjes verzwaard.
function _opstellingHaalbaar(sim, opst) {
  const verloren = _verlorenDagen(sim);
  if (verloren > 0) return { ok: false, reden: `${verloren} verloren dagen (dispatch kon niet oplossen)` };
  const lp = (sim && sim.laadplein) || {};
  if (opst === 'verhogen') {
    const jf = (sim && (sim.jaarfactuur || sim.factuur)) || {};
    const gr = jf.groepen || {};
    const B = gr.B_netgebruik_afname || gr.B || {};
    const over = parseFloat(B.overschrijding_toegangsvermogen) || 0;
    if (over > 1) return { ok: false, reden: `overschrijdingskost € ${Math.round(over)} — aansluiting nog te klein voor ongestuurd laden` };
    return { ok: true };
  }
  const geforceerd = parseFloat(lp.toegangsvermogen_verhoogd_kw) || 0;
  if (geforceerd > 0) return { ok: false, reden: `simulator moest de aansluiting met ${geforceerd} kW verhogen — batterij te klein` };
  return { ok: true };
}
// ─── SIM-JOBS: voortgang van lange simulaties (v15.20) ───────────────────────
// Met DRIE opstellingen x tot 7 sim-runs elk loopt /api/nominatie-sim-3 op tot
// enkele minuten. Dat is te lang voor een blokkerende POST (Railway/proxy kapt af)
// en de verkoper zit al die tijd naar een dood scherm te kijken.
// Daarom: POST met _async:true geeft direct een job_id terug en draait door in de
// achtergrond; de UI pollt /api/sim-voortgang/:id en toont het log live.
// Het synchrone pad blijft bestaan (geen _async) zodat oude clients niet breken.
//
// Bewust in-memory: een job leeft hooguit enkele minuten en een herstart van de
// service is zeldzaam. Gaat een job toch verloren, dan krijgt de UI 404 en kan ze
// gewoon opnieuw starten. Een DB erbij halen voor 5 minuten state is overkill.
const SIM_JOBS = new Map();
const JOB_TTL_MS = 15 * 60 * 1000;
function _jobNieuw() {
  for (const [k, v] of SIM_JOBS) if (Date.now() - v.gestart > JOB_TTL_MS) SIM_JOBS.delete(k);
  const id = 'job_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 8);
  const job = { id, status: 'bezig', log: [], resultaat: null, fout: null,
                gestart: Date.now(), runs: 0, runs_verwacht: 0 };
  SIM_JOBS.set(id, job);
  return job;
}
// Eén logregel = één ding dat de verkoper snapt. Geen debug-spam: dit scherm is
// verkoop-zichtbaar. `fase` stuurt het icoon in de UI.
function _jlog(job, fase, tekst, extra) {
  const r = Object.assign({ t: job ? Date.now() - job.gestart : 0, fase, tekst }, extra || {});
  if (job) job.log.push(r);
  console.log(`[sim-3] ${fase}: ${tekst}`);
  return r;
}
const OPSTELLING_LABEL = {
  verhogen:    'Opstelling 1 — toegangsvermogen verhogen',
  batterij:    'Opstelling 2 — batterij, aansluiting blijft',
  mix:         'Opstelling 3 — mix: deels verzwaren, kleinere batterij',
};

// v15.20.4 — DE DERDE OPSTELLING IS ALTIJD 'mix'.
// Vroeger: op LS werd de derde opstelling 'ms_batterij' (LS-met-batterij vs
// MS-met-batterij — de tariefkaart-vraag). Vervallen (overdracht §4 + §4bis.B): de
// LS/MS-keuze is een POORT die je één keer vooraf beslist; daarna draaien ALLE
// ontwerpscenario's op die ene tariefkaart. We vergelijken LS niet met MS in de sim.
//
// De zinvolle derde weg blijft de 'mix'. Opstelling 1 en 2 zijn de twee UITERSTEN:
// alles oplossen met de aansluiting, of alles met de batterij. Het optimum ligt
// bijna altijd ertussen: een beetje verzwaren maakt de batterij fors kleiner, en
// batterij-kWh is duur (350 EUR/kWh) terwijl kVA relatief goedkoop is (100 EUR/kVA).
// Dat binnengebied IS Johans batterij-sweep. Werkt identiek op LS en MS.
function _derdeOpstelling(input) {
  return 'mix';
}
// Mengverhoudingen: aandeel van de VOLLEDIGE verzwaring uit opstelling 1.
// Drie punten in het binnengebied — genoeg om de vorm van de afweging te zien,
// zonder de looptijd te verdrievoudigen. Geen bewezen optimum, wel de beste van drie.
const MIX_FRACTIES = [0.33, 0.50, 0.67];

const DIM_MAX_ITER  = 4;      // cap: elke iteratie is een volle sim-run (~20-30s)
const DIM_GROEI     = 1.30;   // 30% per stap — grof, maar convergeert snel genoeg
const DIM_FIJN_ITER = 2;      // binaire verfijning tussen de laatste faal/succes-stap
// v15.22.0 — DISCRETE BATTERIJ-EENHEDEN. Johan (§4.3): de batterij groeit in fysieke
// eenheden van 120 kW / 260 kWh, niet continu in kWh. De zoeklus mag blijven groeien/
// krimpen met DIM_GROEI, maar _dimZet snapt de maat altijd op een geheel aantal eenheden.
// Zo is 'aantal batterijen' een echt geheel getal en klopt de capex per eenheid.
const BATT_UNIT_KW  = 120;
const BATT_UNIT_KWH = 260;
// De zoeklus is opstelling-agnostisch: voor 'batterij' én 'mix' is de bepalende maat
// de batterij-kWh. De tariefkaart ligt vast in de config (de LS/MS-poort, vooraf), niet
// in de zoeklogica.
function _isBatterijOpstelling(opst) {
  return opst === 'batterij' || opst === 'mix';
}
// Maat uitlezen/zetten per opstelling — zo blijft de zoeklus opstelling-agnostisch.
function _dimMaat(cfg, opst) {
  return (opst === 'verhogen') ? (cfg.aansluiting_kva || 0)
                               : ((cfg.batterijCustom && cfg.batterijCustom.kwh) || 0);
}
function _dimZet(cfg, opst, maat) {
  if (opst === 'verhogen') {
    const n = Math.ceil(maat / 5) * 5;
    cfg.aansluiting_kva = n; cfg.aansluitingKva = n; cfg.toegangsvermogen_kw = n;
    // v15.18: de LS/MS-grens opnieuw toetsen — groeien kan hem alsnog overschrijden.
    if (n > LS_MAX_KVA && cfg.spanning !== 'MS') {
      cfg._spanning_origineel = cfg.spanning || 'LS';
      cfg.spanning = 'MS'; cfg._spanning_omgezet = true;
    }
    return n;
  }
  // Batterij-opstellingen ('batterij' en 'mix'): de kWh is de bepalende maat. De spanning
  // ligt vooraf vast via de LS/MS-poort en mag hier NIET wijzigen. v15.22.0: snap de maat
  // op een geheel aantal fysieke eenheden (120 kW / 260 kWh) — minimaal 1 zodra er een
  // batterij nodig is. kw en kwh volgen het aantal eenheden, zodat de C-rate en de capex
  // per eenheid consistent blijven.
  const eenheden = Math.max(1, Math.ceil(maat / BATT_UNIT_KWH));
  const kwh = eenheden * BATT_UNIT_KWH;
  const kw  = eenheden * BATT_UNIT_KW;
  cfg.batterijCustom = Object.assign({}, cfg.batterijCustom || {}, { kwh, kw, aantal_batterijen: eenheden });
  return kwh;
}
function _dimEenheid(opst) { return (opst === 'verhogen') ? 'kVA' : 'kWh'; }

// Groeit de bepalende parameter tot de dispatch 0 verloren dagen meldt.
//  - 'verhogen': het toegangsvermogen (opstelling 1 wordt beoordeeld ZONDER sturing,
//                want dát is het basisscenario: verzwaren en verder niets slims doen)
//  - 'batterij': de batterijcapaciteit (beoordeeld MET sturing 2, want zo wordt ze ingezet)
async function _dimensioneerTotHaalbaar(cfg0, opstelling, cap, job) {
  const variant = (opstelling === 'verhogen') ? 'geen' : 'sturing';
  const eenh = _dimEenheid(opstelling);
  let cfg = JSON.parse(JSON.stringify(cfg0));
  let resultaat = null, stappen = [], runs = 0;
  let laatsteFaal = null;              // grootste maat die NIET volstond
  let okMaat = null, okCfg = null, okRes = null;

  // ── Fase 0: past de startmaat meteen? Dan KRIMPEN, niet groeien ──
  // v15.20.1: de zoeklus groeide alleen. Voor opstelling 2 klopt dat meestal (de
  // vuistregel is te klein), maar bij een mix met een ruimere aansluiting volstaat de
  // startbatterij vaak meteen — en dan accepteerden we die, terwijl de helft ook had
  // gekund. Dat maakte juist de mixen met veel kVA onterecht duur in de TCO, dus
  // vertekende het exact de vergelijking waarvoor de mix bestaat.
  {
    const _m0 = _dimMaat(cfg, opstelling);
    _jlog(job, 'run', `${OPSTELLING_LABEL[opstelling] || opstelling}: proefdraai op ${_m0} ${eenh}…`,
          { opstelling, maat: _m0, eenheid: eenh });
    const r0 = await _runSimulatorOnce(buildSimInput(_variantUi(cfg, variant))); runs++;
    if (job) job.runs = (job.runs || 0) + 1;
    const h0 = _opstellingHaalbaar(r0, opstelling);
    stappen.push({ fase: 'start', maat: _m0, eenheid: eenh, ok: h0.ok, reden: h0.reden || null });
    if (h0.ok) {
      _jlog(job, 'ok', `${_m0} ${eenh} volstaat meteen — kijken of het kleiner kan`,
            { opstelling, maat: _m0, eenheid: eenh });
      okMaat = _m0; okCfg = JSON.parse(JSON.stringify(cfg)); okRes = r0; resultaat = r0;
      // Krimpen tot het NIET meer past; dat punt wordt de ondergrens van de verfijning.
      let krimp = JSON.parse(JSON.stringify(cfg));
      for (let k = 0; k < DIM_MAX_ITER; k++) {
        const kleiner = _dimZet(krimp, opstelling, _dimMaat(krimp, opstelling) / DIM_GROEI);
        if (kleiner <= 0 || kleiner >= okMaat) break;
        _jlog(job, 'run', `Kan het met ${kleiner} ${eenh}?`, { opstelling, maat: kleiner, eenheid: eenh });
        const rk = await _runSimulatorOnce(buildSimInput(_variantUi(krimp, variant))); runs++;
        if (job) job.runs = (job.runs || 0) + 1;
        const hk = _opstellingHaalbaar(rk, opstelling);
        stappen.push({ fase: 'krimp', maat: kleiner, eenheid: eenh, ok: hk.ok, reden: hk.reden || null });
        if (hk.ok) {
          _jlog(job, 'ok', `Ja — ${kleiner} ${eenh} volstaat ook`, { opstelling, maat: kleiner, eenheid: eenh });
          okMaat = kleiner; okCfg = JSON.parse(JSON.stringify(krimp)); okRes = rk; resultaat = rk;
        } else {
          _jlog(job, 'faal', `Nee — ${kleiner} ${eenh} is te krap`, { opstelling, maat: kleiner, eenheid: eenh });
          laatsteFaal = kleiner; break;
        }
      }
      // Door naar fase 2 (binair verfijnen tussen laatsteFaal en okMaat).
      cfg = okCfg;
    } else {
      laatsteFaal = _m0;
      _jlog(job, 'faal', `${_m0} ${eenh} volstaat niet: ${h0.reden}`, { opstelling, maat: _m0, eenheid: eenh });
      const _nw = _dimZet(cfg, opstelling, _m0 * DIM_GROEI);
      _jlog(job, 'groei', `Te klein — opschalen naar ${_nw} ${eenh} en opnieuw proberen`,
            { opstelling, maat: _nw, eenheid: eenh });
    }
  }

  // ── Fase 1: grof groeien tot het past (overgeslagen als fase 0 al slaagde) ──
  for (let i = 0; okMaat === null && i <= DIM_MAX_ITER; i++) {
    const _maat0 = _dimMaat(cfg, opstelling);
    _jlog(job, 'run', `${OPSTELLING_LABEL[opstelling] || opstelling}: proefdraai op ${_maat0} ${eenh}…`,
          { opstelling, maat: _maat0, eenheid: eenh });
    resultaat = await _runSimulatorOnce(buildSimInput(_variantUi(cfg, variant))); runs++;
    if (job) job.runs = (job.runs || 0) + 1;
    const h = _opstellingHaalbaar(resultaat, opstelling);
    const maat = _dimMaat(cfg, opstelling);
    stappen.push({ fase: 'groei', maat, eenheid: eenh, ok: h.ok, reden: h.reden || null });
    _jlog(job, h.ok ? 'ok' : 'faal',
          h.ok ? `${maat} ${eenh} volstaat — alle laaddagen opgelost`
               : `${maat} ${eenh} volstaat niet: ${h.reden}`,
          { opstelling, maat, eenheid: eenh });
    if (h.ok) { okMaat = maat; okCfg = JSON.parse(JSON.stringify(cfg)); okRes = resultaat; break; }
    laatsteFaal = maat;
    if (i === DIM_MAX_ITER) {
      _jlog(job, 'waarschuwing',
            `${OPSTELLING_LABEL[opstelling] || opstelling}: niet haalbaar na ${DIM_MAX_ITER} groeistappen (${h.reden})`,
            { opstelling });
      return { cfg, resultaat, variant, haalbaar: false, iteraties: runs, stappen,
               reden: h.reden, verloren_dagen: _verlorenDagen(resultaat), totaal_dagen: _totaalDagen(resultaat) };
    }
    const _nw = _dimZet(cfg, opstelling, maat * DIM_GROEI);
    _jlog(job, 'groei', `Te klein — opschalen naar ${_nw} ${eenh} en opnieuw proberen`,
          { opstelling, maat: _nw, eenheid: eenh });
  }

  // ── Fase 2: binair verfijnen tussen de laatste faal en het eerste succes ──
  // Zonder dit weet je enkel dat (bv.) 490 kWh werkt en 370 niet — je koopt dan tot
  // 30% te veel batterij. Elke stap is een sim-run, dus streng gecapt.
  if (laatsteFaal !== null && okMaat !== null) {
    let lo = laatsteFaal, hi = okMaat;
    for (let j = 0; j < DIM_FIJN_ITER; j++) {
      const mid = _dimZet(JSON.parse(JSON.stringify(cfg)), opstelling, (lo + hi) / 2);
      if (mid <= lo || mid >= hi) break;          // geen ruimte meer binnen de afronding
      const probe = JSON.parse(JSON.stringify(okCfg));
      _dimZet(probe, opstelling, mid);
      _jlog(job, 'run', `Verfijnen: past ${mid} ${eenh} ook nog? (tussen ${lo} en ${hi})`,
            { opstelling, maat: mid, eenheid: eenh });
      const r = await _runSimulatorOnce(buildSimInput(_variantUi(probe, variant))); runs++;
      if (job) job.runs = (job.runs || 0) + 1;
      const h2 = _opstellingHaalbaar(r, opstelling);
      stappen.push({ fase: 'verfijn', maat: mid, eenheid: eenh, ok: h2.ok, reden: h2.reden || null });
      _jlog(job, h2.ok ? 'ok' : 'faal',
            h2.ok ? `${mid} ${eenh} volstaat ook — dat scheelt ${hi - mid} ${eenh}`
                  : `${mid} ${eenh} is net te krap`,
            { opstelling, maat: mid, eenheid: eenh });
      if (h2.ok) { hi = mid; okMaat = mid; okCfg = probe; okRes = r; }
      else { lo = mid; }
    }
  }
  _jlog(job, 'klaar', `${OPSTELLING_LABEL[opstelling] || opstelling}: gedimensioneerd op ${okMaat} ${eenh} (${runs} proefdraaien)`,
        { opstelling, maat: okMaat, eenheid: eenh });
  return { cfg: okCfg || cfg, resultaat: okRes || resultaat, variant, haalbaar: true,
           iteraties: runs, stappen, gekozen_maat: okMaat, eenheid: eenh,
           start_maat: (stappen[0] || {}).maat };
}

// v15.20.1: doorloop de mengverhoudingen, dimensioneer per punt de batterij, en kies
// op TOTALE EIGENDOMSKOST (investering + factuur x horizon). Kiezen op factuurkost
// alleen zou altijd de grootste aansluiting winnen — die verlaagt de factuur maar
// kost kapitaal. Kiezen op investering alleen zou altijd de kleinste winnen.
async function _dimensioneerMix(input, cap, job) {
  const inv = input._investering || null;
  const huidig = Number(input.aansluiting_kva || input.aansluitingKva || 0) || 0;
  const _sub = r => (r && r.jaarfactuur) ? (r.jaarfactuur.subtotaal_excl_btw || 0) : 0;
  const punten = [];
  for (const f of MIX_FRACTIES) {
    const cfg0 = _mixCfg(input, cap, f);
    if (cfg0._mix_kva <= huidig) {                      // niets te verzwaren op dit punt
      _jlog(job, 'groei', `Mengverhouding ${Math.round(f*100)}% valt samen met de huidige aansluiting — overgeslagen`);
      continue;
    }
    _jlog(job, 'opstelling',
          `Mix ${Math.round(f*100)}%: aansluiting ${cfg0._mix_kva} kVA — hoe klein mag de batterij dan?`,
          { opstelling: 'mix', maat: cfg0._mix_kva, eenheid: 'kVA' });
    const dim = await _dimensioneerTotHaalbaar(cfg0, 'mix', cap, job);
    if (!dim.haalbaar) {
      _jlog(job, 'faal', `Mix ${Math.round(f*100)}%: geen werkende batterijmaat gevonden — overgeslagen`);
      continue;
    }
    const kwh = dim.gekozen_maat || 0;
    const jaarkost = _sub(dim.resultaat);
    const tco = inv ? _mixTco(cfg0._mix_kva, kwh, !!dim.cfg._spanning_omgezet, inv, jaarkost, huidig) : null;
    punten.push({ fractie: f, kva: cfg0._mix_kva, kwh, jaarkost,
                  capex: tco ? tco.capex : null, tco: tco ? tco.tco : null, dim, cfg: dim.cfg });
    _jlog(job, 'resultaat',
          `Mix ${Math.round(f*100)}%: ${cfg0._mix_kva} kVA + ${kwh} kWh batterij` +
          (tco ? ` — investering € ${Math.round(tco.capex).toLocaleString('nl-BE')}, ` +
                 `factuur € ${Math.round(jaarkost).toLocaleString('nl-BE')}/jaar` : ''),
          { opstelling: 'mix', kva: cfg0._mix_kva, kwh });
  }
  if (!punten.length) return null;
  // Zonder investeringsconstanten kunnen we niet eerlijk kiezen -> middelste punt,
  // en dat zeggen we ook zo in het log i.p.v. een optimum te suggereren.
  let beste;
  if (punten.every(p => p.tco != null)) {
    beste = punten.reduce((a, b) => (b.tco < a.tco ? b : a));
    _jlog(job, 'ok',
          `Beste mengverhouding: ${beste.kva} kVA + ${beste.kwh} kWh ` +
          `(laagste totale kost over ${(input._investering.horizon_jaar||15)} jaar van ${punten.length} onderzochte verhoudingen)`,
          { opstelling: 'mix' });
  } else {
    beste = punten[Math.floor(punten.length / 2)];
    _jlog(job, 'waarschuwing',
          'Geen investeringsconstanten meegegeven — middelste mengverhouding gekozen, niet de goedkoopste.',
          { opstelling: 'mix' });
  }
  beste.alternatieven = punten.map(p => ({ fractie: p.fractie, kva: p.kva, kwh: p.kwh,
                                           jaarkost: Math.round(p.jaarkost),
                                           capex: p.capex != null ? Math.round(p.capex) : null,
                                           tco: p.tco != null ? Math.round(p.tco) : null,
                                           gekozen: p === beste }));
  return beste;
}

function _opstellingUi(ui, opstelling, cap) {
  const v = JSON.parse(JSON.stringify(ui || {}));
  if (opstelling === 'verhogen') {
    v.aansluiting_kva = cap.benodigd_toegangsvermogen_kw;
    v.aansluitingKva = cap.benodigd_toegangsvermogen_kw;
    // v15.15.7: bij verhogen wordt óók het gecontracteerde toegangsvermogen opgetrokken
    // → dat is de nieuwe facturatiebasis (anders bleef de sunk 35 kW staan).
    v.toegangsvermogen_kw = cap.benodigd_toegangsvermogen_kw;
    // v15.18: moet de aansluiting boven de LS-grens, dan rekent opstelling 1 op MS.
    // Opstelling 2 (batterij) blijft op de spanning zoals in stap 9 gedefinieerd —
    // die vermijdt de verzwaring net.
    if (cap.benodigd_toegangsvermogen_kw > LS_MAX_KVA && v.spanning !== 'MS') {
      v._spanning_origineel = v.spanning || 'LS';
      v.spanning = 'MS';
      v._spanning_omgezet = true;
      console.log(`[sim-3] opstelling 'verhogen': ${cap.benodigd_toegangsvermogen_kw} kVA > ${LS_MAX_KVA} → tariefkaart MS i.p.v. ${v._spanning_origineel}`);
    }
  } else { // 'batterij' en 'mix'
    v.batterijId = 'CUSTOM';
    // v15.22.0: start meteen op een geheel aantal fysieke eenheden (120 kW / 260 kWh),
    // zodat de eerste proefdraai — en dus ook een direct geaccepteerde startmaat — al
    // op hele batterijen valt en 'aantal_batterijen' overal een integer is.
    const _startEenheden = Math.max(1, Math.ceil((cap.advies_batterij_kwh || 0) / BATT_UNIT_KWH));
    v.batterijCustom = {
      naam: 'Advies-batterij',
      kw: _startEenheden * BATT_UNIT_KW, kwh: _startEenheden * BATT_UNIT_KWH,
      aantal_batterijen: _startEenheden,
      dod_pct: 90, rte_pct: 92, capex_eur: 0, max_cycli: 8000,
    };
    // v15.20.4: de tariefkaart (LS/MS) ligt vooraf vast via de poort en wordt hier NIET
    // meer omgezet. De verdwenen 'ms_batterij'-tak zette hier v.spanning='MS' om
    // LS-met-batterij tegen MS-met-batterij te zetten; die vergelijking is vervallen
    // (overdracht §4). 'mix': deels verzwaren EN een batterij — de aansluiting wordt
    // gezet door _mixCfg() (per mengverhouding); dit is enkel de batterij-basis.
  }
  return v;
}

// v15.20.1: één mengpunt. `fractie` = aandeel van de volledige verzwaring uit
// opstelling 1. De batterij wordt daarna door de gewone zoeklus gedimensioneerd,
// dus de mix is per constructie ook haalbaar — net als 1 en 2.
function _mixCfg(input, cap, fractie) {
  const v = _opstellingUi(input, 'batterij', cap);
  const huidig = Number(input.aansluiting_kva || input.aansluitingKva || 0) || 0;
  const volledig = Number(cap.benodigd_toegangsvermogen_kw || 0) || huidig;
  const kva = Math.ceil((huidig + Math.max(0, volledig - huidig) * fractie) / 5) * 5;
  v.aansluiting_kva = kva; v.aansluitingKva = kva; v.toegangsvermogen_kw = kva;
  v._mix_fractie = fractie; v._mix_kva = kva; v._mix_huidig_kva = huidig;
  if (kva > LS_MAX_KVA && v.spanning !== 'MS') {
    v._spanning_origineel = v.spanning || 'LS';
    v.spanning = 'MS'; v._spanning_omgezet = true; v._cabine_nodig = true;
  }
  return v;
}
// Totale eigendomskost van één mix: de factuur + de geannualiseerde investering.
// De investeringsconstanten komen UIT DE FRONTEND (input._investering) zodat ze op
// één plek staan; zonder die constanten kunnen we niet kiezen en valt de mix terug
// op de middelste fractie.
function _mixTco(kva, kwh, cabine, inv, jaarkost, huidigKva) {
  const jaren = Number(inv.horizon_jaar) || 15;
  const capex = (Math.max(0, kva - huidigKva) * (Number(inv.eur_per_kva) || 0))
              + (kwh * (Number(inv.eur_per_kwh) || 0))
              + (cabine ? (Number(inv.cabine_eur) || 0) : 0);
  const metKabel = capex * (1 + (Number(inv.kabel_pct) || 0));
  return { capex: metKabel, jaarkost, tco: metKabel + jaarkost * jaren };
}

app.post('/api/nominatie-sim-3', async (req, res) => {
  const input = req.body;
  if (!input || typeof input !== 'object')
    return res.status(400).json({ error:'body is verplicht' });
  if (!MARKT) {
    // Zelfde 503-semantiek als /api/nominatie-sim zodat de UI-retry-ladder werkt.
    return res.status(503).json({
      error: 'Marktdata nog niet geladen — probeer over 30 seconden opnieuw',
      status: MARKT_STATUS, pogingen: MARKT_POGINGEN,
    });
  }
  const simulatorPath = path.join(__dirname, 'simulator.py');
  if (!fs.existsSync(simulatorPath))
    return res.status(500).json({ error:'simulator.py niet gevonden' });

  // v15.20: async-modus. De UI zet _async:true, krijgt meteen een job_id en pollt
  // /api/sim-voortgang/:id. Zonder _async blijft alles exact zoals vroeger — oude
  // clients en de retry-ladder merken niets.
  if (input._async) {
    const job = _jobNieuw();
    res.json({ ok: true, async: true, job_id: job.id });
    _draaiSim3(input, job)
      .then(r => { job.resultaat = r; job.status = 'klaar';
                   _jlog(job, 'klaar', 'Simulatie afgerond.'); })
      .catch(e => { job.fout = e.message; job.status = 'fout';
                    _jlog(job, 'fout', 'Simulatie gefaald: ' + e.message);
                    console.error('[sim-3] async fout:', e.message); });
    return;
  }
  try {
    const r = await _draaiSim3(input, null);
    return res.json(r);
  } catch (e) {
    console.error('[sim-3] fout:', e.message);
    return res.status(500).json({ error: 'Simulatie-3 gefaald: ' + e.message });
  }
});

// v15.20: voortgang van een async job. De UI pollt dit elke ~1,5s en toont het log.
// Geen auth, consistent met /api/nominatie-sim-3 zelf. Een job_id is niet te raden
// en bevat geen klantdata — enkel maten en statusregels.
app.get('/api/sim-voortgang/:id', (req, res) => {
  const job = SIM_JOBS.get(req.params.id);
  if (!job) return res.status(404).json({ error: 'job onbekend of verlopen' });
  return res.json({
    ok: true, status: job.status, log: job.log,
    runs: job.runs || 0, runs_verwacht: job.runs_verwacht || 0,
    elapsed_ms: Date.now() - job.gestart,
    resultaat: job.status === 'klaar' ? job.resultaat : null,
    fout: job.fout || null,
  });
});

async function _draaiSim3(input, job) {
  const startTime = Date.now();
  {
    const _sub = r => (r && r.jaarfactuur) ? (r.jaarfactuur.subtotaal_excl_btw || 0) : 0;
    const _kpi = (v, onbalansNvt) => {
      const kg = _sub(v.geen), ks = _sub(v.sturing), ko = _sub(v.onbalans);
      return { kost_geen_excl_btw: kg, kost_sturing_excl_btw: ks, kost_onbalans_excl_btw: ko,
        meerwaarde_sturing_excl_btw: kg - ks, meerwaarde_onbalans_excl_btw: ks - ko,
        onbalans_niet_van_toepassing: !!onbalansNvt };
    };
    // Flex-detectie (zie ook onbalans-gate): zonder batterij én PV geen stuurbare asset.
    const _heeftPv = Number(input.pv_kwp || input.pvKwp || 0) > 0;
    let _heeftBatt = false;
    if (input.batterijId === 'CUSTOM') { const _c = input.batterijCustom || {}; _heeftBatt = Number(_c.kwh) > 0 && Number(_c.kw) > 0; }
    else { _heeftBatt = !!(input.batterijId); }
    const heeftFlex = _heeftPv || _heeftBatt;
    const heeftLaadplein = Array.isArray(input.laadpleinen) && input.laadpleinen.length > 0;

    // Probe: één 'geen'-run op de originele config → capaciteits-oordeel (uit simulator.py).
    const probe = await _runSimulatorOnce(buildSimInput(_variantUi(input, 'geen')));
    const cap = (probe.laadplein && probe.laadplein.capaciteit) || { voldoende: true };

    // ── Geen laadplein OF aansluiting voldoende → normale 3-sturingen (probe = 'geen') ──
    if (!heeftLaadplein || cap.voldoende) {
      const varianten = { geen: probe };
      varianten.sturing = await _runSimulatorOnce(buildSimInput(_variantUi(input, 'sturing')));
      varianten.onbalans = heeftFlex
        ? await _runSimulatorOnce(buildSimInput(_variantUi(input, 'onbalans')))
        : varianten.sturing;
      const kpi_sturing = _kpi(varianten, !heeftFlex);
      _jlog(job, 'klaar', `Aansluiting volstaat — geen opstellingen nodig (${Math.round((Date.now()-startTime)/1000)}s)`);
      return { ok: true, modus: 'enkel', varianten, kpi_sturing, capaciteit: cap,
        _meta: { elapsed_ms: Date.now() - startTime, server_version: '15.20.0', heeftFlex } };
    }

    // ── Aansluiting ontoereikend voor de laadvraag → 3 opstellingen × 3 sturingen ──
    // Opstelling 1 = toegangsvermogen verhogen (LS, of MS als het >100 kVA moet)
    // Opstelling 2 = geadviseerde batterij, aansluiting blijft, tariefkaart blijft gelijk
    // Opstelling 3 = 'mix': deels verzwaren EN een kleinere batterij (v15.20.4)
    //
    // v15.20.4 — de derde opstelling is altijd 'mix'. De vroegere 'ms_batterij' (LS vs
    // MS tariefkaart) is vervallen: de LS/MS-keuze is een POORT die je vooraf beslist,
    // geen scenario-as (overdracht §4). Opstelling 1 en 2 zijn de twee UITERSTEN — alles
    // met de aansluiting, of alles met de batterij. De zinvolle derde weg is het
    // BINNENGEBIED: deels verzwaren maakt de batterij fors kleiner, en kWh is duur
    // (350 EUR) terwijl kVA goedkoop is (100 EUR). Dat is Johans batterij-sweep, en werkt
    // identiek op LS en MS (de tariefkaart ligt vast, we vergelijken hem niet).
    const derde = _derdeOpstelling(input);
    const OPSTELLINGEN = ['verhogen', 'batterij', derde];
    _jlog(job, 'start', `Aansluiting te klein: ${cap.tekort_mwh} MWh raakt niet geladen. ` +
                        `Drie opstellingen doorrekenen…`, { tekort_mwh: cap.tekort_mwh });
    if (derde === 'mix')
      _jlog(job, 'start', 'Naast verzwaren (opstelling 1) en de volledige batterij (opstelling 2) ' +
                          'zoeken we de beste mix ertussen: deels verzwaren met een kleinere batterij, ' +
                          'op de huidige tariefkaart.');
    if (job) job.runs_verwacht = 22;
    const opstellingen = {};
    for (const opst of OPSTELLINGEN) {
      _jlog(job, 'opstelling', OPSTELLING_LABEL[opst], { opstelling: opst });
      // ── mix: eigen zoeklus over de mengverhoudingen ──
      if (opst === 'mix') {
        const m = await _dimensioneerMix(input, cap, job);
        if (!m) { _jlog(job, 'waarschuwing', 'Geen werkende mix gevonden — opstelling 3 overgeslagen.'); continue; }
        const cfgM = m.cfg;
        const vm = {};
        _jlog(job, 'run', `${OPSTELLING_LABEL.mix}: de drie sturingen doorrekenen…`, { opstelling: 'mix' });
        vm.geen     = await _runSimulatorOnce(buildSimInput(_variantUi(cfgM, 'geen')));
        vm.sturing  = m.dim.resultaat;
        vm.onbalans = await _runSimulatorOnce(buildSimInput(_variantUi(cfgM, 'onbalans')));
        if (job) job.runs = (job.runs || 0) + 2;
        opstellingen.mix = {
          varianten: vm, kpi_sturing: _kpi(vm, false),
          config: {
            spanning: cfgM.spanning || input.spanning || 'LS',
            spanning_omgezet: !!cfgM._spanning_omgezet,
            spanning_origineel: cfgM._spanning_origineel || null,
            aansluiting_kva: m.kva, toegangsvermogen_kw: m.kva,
            batterij: cfgM.batterijCustom || null,
            cabine_nodig: !!cfgM._spanning_omgezet,
            mix_fractie: m.fractie, mix_huidig_kva: cfgM._mix_huidig_kva || null,
          },
          dimensionering: {
            haalbaar: true, iteraties: m.dim.iteraties, beoordeeld_op: 'sturing',
            stappen: m.dim.stappen, start_maat: m.dim.start_maat || null,
            gekozen_maat: m.kwh, eenheid: 'kWh', verloren_dagen: 0, totaal_dagen: null,
          },
          mix: { fractie: m.fractie, kva: m.kva, kwh: m.kwh,
                 capex: m.capex != null ? Math.round(m.capex) : null,
                 alternatieven: m.alternatieven },
        };
        _jlog(job, 'resultaat',
              `${OPSTELLING_LABEL.mix}: € ${Math.round(_sub(vm.sturing)).toLocaleString('nl-BE')}/jaar ` +
              `op sturing 2 (${m.kva} kVA + ${m.kwh} kWh)`,
              { opstelling: 'mix', subtotaal: Math.round(_sub(vm.sturing)) });
        continue;
      }
      let cfg = _opstellingUi(input, opst, cap);
      // v15.19: ITERATIEVE DIMENSIONERING — voor BEIDE opstellingen.
      // De maten die _laadplein_capaciteit aanlevert (verhoging_kw, advies_batterij_*)
      // zijn vuistregels die op gemiddelden rekenen. De LP-dispatch rekent per kwartier
      // met het echte profiel, de laadpuntlimieten en de SoC — en vindt dan geregeld dat
      // het NIET past (bv. 260/365 dagen verloren). Een vergelijking tussen twee
      // opstellingen die allebei laadvraag laten liggen is waardeloos, en een opstelling
      // die faalt betaalt overschrijding → haar business case lijkt onterecht slecht.
      // Daarom: groeien tot de dispatch zelf 0 verloren dagen meldt.
      const dim = await _dimensioneerTotHaalbaar(cfg, opst, cap, job);
      cfg = dim.cfg;
      const v = {};
      _jlog(job, 'run', `${OPSTELLING_LABEL[opst]}: de drie sturingen doorrekenen…`, { opstelling: opst });
      v.geen     = dim.variant === 'geen'    ? dim.resultaat : await _runSimulatorOnce(buildSimInput(_variantUi(cfg, 'geen')));
      v.sturing  = dim.variant === 'sturing' ? dim.resultaat : await _runSimulatorOnce(buildSimInput(_variantUi(cfg, 'sturing')));
      v.onbalans = await _runSimulatorOnce(buildSimInput(_variantUi(cfg, 'onbalans')));
      if (job) job.runs = (job.runs || 0) + 2;
      // v15.18: geef de gebruikte configuratie mee terug — vooral de spanning, want
      // opstelling 1 kan naar MS zijn omgezet (>100 kVA). Zonder dit ziet de verkoper
      // niet dat hij twee verschillende tariefkaarten vergelijkt.
      opstellingen[opst] = {
        varianten: v, kpi_sturing: _kpi(v, false),
        config: {
          spanning: cfg.spanning || input.spanning || 'LS',
          spanning_omgezet: !!cfg._spanning_omgezet,
          spanning_origineel: cfg._spanning_origineel || null,
          aansluiting_kva: cfg.aansluiting_kva || input.aansluiting_kva || null,
          toegangsvermogen_kw: cfg.toegangsvermogen_kw || null,
          batterij: cfg.batterijCustom || null,
          // v15.20: een cabine is nodig zodra deze opstelling van LS naar MS gaat.
          // Zowel opstelling 1 (>100 kVA) als opstelling 3 (bewuste keuze) kunnen dat.
          cabine_nodig: !!cfg._spanning_omgezet,
        },
        // v15.19: bewijs dat deze opstelling de laadvraag écht aankan (of niet).
        dimensionering: {
          haalbaar: dim.haalbaar, iteraties: dim.iteraties, beoordeeld_op: dim.variant,
          stappen: dim.stappen, start_maat: dim.start_maat || null,
          gekozen_maat: dim.gekozen_maat || null, eenheid: dim.eenheid || null,
          verloren_dagen: dim.verloren_dagen || 0, totaal_dagen: dim.totaal_dagen || null,
        },
      };
      _jlog(job, 'resultaat',
            `${OPSTELLING_LABEL[opst]}: € ${Math.round(_sub(v.sturing)).toLocaleString('nl-BE')}/jaar ` +
            `op sturing 2 (${cfg.spanning || 'LS'})`,
            { opstelling: opst, subtotaal: Math.round(_sub(v.sturing)), spanning: cfg.spanning || 'LS' });
    }
    // Besparing t.o.v. opstelling 1 (verzwaren = het ijkpunt), per sturing.
    // v15.20: modus heet nog 'twee_opstellingen' voor backwards-compat met de UI-check;
    // het aantal opstellingen lees je uit Object.keys(opstellingen).
    const vergelijking = {};
    ['geen', 'sturing', 'onbalans'].forEach(s => {
      vergelijking['besparing_batterij_' + s + '_excl_btw'] =
        _sub(opstellingen.verhogen.varianten[s]) - _sub(opstellingen.batterij.varianten[s]);
      if (opstellingen[derde]) {
        vergelijking['besparing_' + derde + '_' + s + '_excl_btw'] =
          _sub(opstellingen.verhogen.varianten[s]) - _sub(opstellingen[derde].varianten[s]);
        // derde is altijd 'mix' (v15.20.4): 3 t.o.v. 2 — hier verschilt óók de
        // aansluiting, dus géén zuiver tariefkaart-effect (dat was de vervallen
        // 'ms_batterij'-vergelijking).
        vergelijking['mix_vs_batterij_' + s + '_excl_btw'] =
          _sub(opstellingen.batterij.varianten[s]) - _sub(opstellingen[derde].varianten[s]);
      }
    });
    _jlog(job, 'klaar',
          `Drie opstellingen doorgerekend in ${Math.round((Date.now() - startTime) / 1000)}s ` +
          `(${job ? job.runs : '?'} sim-runs).`);
    console.log(`[sim-3] drie opstellingen — ${Date.now() - startTime}ms (tekort ${cap.tekort_mwh} MWh)`);
    return { ok: true, modus: 'twee_opstellingen', capaciteit: cap, opstellingen, vergelijking,
      _meta: { elapsed_ms: Date.now() - startTime, server_version: '15.20.4',
               opstellingen: Object.keys(opstellingen), derde_opstelling: derde,
               sim_runs: job ? job.runs : null } };
  }
}

// ─── BUILD SIM INPUT ─────────────────────────────────────────────────────────
function buildSimInput(ui) {
  const grd     = ui.grd || 'Fluvius West';
  const spanning = ui.spanning || 'LS';
  const jaarverbruik = ui.jaarverbruik_mwh || ui.jaarverbruik || 200;

  // Contract staffel
  const staffel = (ui.contract && ui.contract.staffel && ui.contract.staffel.length > 0)
    ? ui.contract.staffel : CONTRACT_STAFFEL;

  // Batterij
  // v15.12 sessie 5b: BESS-CUSTOM detectie. Wanneer ui.batterijId === 'CUSTOM'
  // gebruikt de verkoper de stap-5 Custom-mode (vrij ingegeven kw/kwh/RTE/...).
  // In dat geval slaan we de catalogus-lookup over en bouwen we batt direct
  // uit ui.batterijCustom. Bij ontbrekende velden vallen we terug op
  // realistische defaults (350 €/kWh CAPEX, 90% DoD, 92% RTE, 8000 cycli).
  // Anti-regressie: bij batterijId !== 'CUSTOM' is dit pad inert; oude flow
  // blijft 1-op-1 hetzelfde.
  let batt = { kw:0, kwh:0, dod_pct:90, rte_pct:85, capex_eur:0, max_cycli:8000 };
  if (ui.batterijId === 'CUSTOM' && ui.batterijCustom) {
    const c = ui.batterijCustom;
    batt = {
      kw:        Number(c.kw) || 0,
      kwh:       Number(c.kwh) || 0,
      dod_pct:   Number(c.dod_pct) || 90,
      rte_pct:   Number(c.rte_pct) || 92,
      capex_eur: Number(c.capex_eur) || (350 * (Number(c.kwh) || 0)),
      max_cycli: Number(c.max_cycli) || 8000
    };
    console.log(`[sim] BESS-CUSTOM: ${c.naam || 'unnamed'} — ${batt.kw} kW / ${batt.kwh} kWh / RTE ${batt.rte_pct}% / CAPEX ${batt.capex_eur} €`);
  } else if (ui.batterijId) {
    const b = BATTERIJEN.find(x => x.id===ui.batterijId || x.naam===ui.batterijId);
    if (b) batt = { kw:b.kw, kwh:b.kwh, dod_pct:Math.round((b.dod||0.90)*100),
                    rte_pct:Math.round((b.eta||0.85)*100), capex_eur:b.capex||0, max_cycli:b.max_cycli||8000 };
  }

  const pvKwp    = ui.pv_kwp || ui.pvKwp || 0;
  const aanslKw  = ui.aansluiting_kva || ui.aansluitingKva || 80;
  // v15.15.7: GECONTRACTEERD toegangsvermogen (uit de klantfactuur) is de
  // facturatiebasis voor Groep B/D — LOS van het fysieke aansluitvermogen (aanslKw,
  // = dispatch-hard-cap). Vroeger factureerde simulator.py het toegangsvermogen op
  // aanslKw, waardoor een klant met bv. 100 kVA aansluiting maar 35 kW gecontracteerd
  // toegangsvermogen te hoge netkosten kreeg in de 'betere' factuur. Zonder factuur-
  // waarde (ui.toegangsvermogen_kw) valt het terug op aanslKw → ongewijzigd gedrag.
  const toegangsKw = Number(ui.toegangsvermogen_kw || ui.toegangsvermogenKw || 0) || aanslKw;
  const stacked  = batt.kwh > 0;
  const bspActief    = !!(ui.bsp && ui.bsp.actief);
  const curtailActief = !!(ui.pv_curtailment && ui.pv_curtailment.actief);

  // v1.6 / v15.13: asymmetrie injectie ≠ afname.
  // PV-omvormer is meestal kleiner dan piek-kWp (clipping). De _invTabel encodeert
  // de meest voorkomende defaults uit de Fluctus-catalogus voor populaire kWp's.
  // Bij geen match: 0.77 × kWp (fabriekstypisch).
  const _invTabel = { 125: 96, 150: 115, 200: 153 };
  const pvInverterKw = Number(ui.pv_inverter_kw || ui.pvInverterKw || 0) ||
                       (pvKwp > 0 ? (_invTabel[pvKwp] || Math.round(pvKwp * 0.77)) : 0);
  // Injectie-cap = som van fysieke injectie-vermogens (PV-omvormer + BESS-omvormer).
  // UI kan dit overschrijven via ui.max_injectie_kw. Default = pvInverterKw + batt.kw.
  // De afname-cap (aanslKw) blijft het contractueel toegangsvermogen — onafhankelijk
  // van fysieke injectie-capaciteit (Belgisch tarief: Groep B/D wegen op afname-piek).
  const maxInjectieKw = Number(ui.max_injectie_kw || ui.maxInjectieKw || 0) ||
                        Math.max(1, pvInverterKw + (batt.kw || 0));

  // Gebruik de dynamisch bepaalde marktperiode als rolling12 gevraagd wordt
  // v15.11 sessie 4: nieuwe modus 'specifiek' — STATE.jaar='specifiek' met
  // expliciete periodeVan/periodeTot uit base-case-factuur.
  let simPeriode = ui.simulatieperiode || {};
  if (ui.jaar === 'specifiek' && ui.periodeVan && ui.periodeTot) {
    // Base-case-pad: gebruik exacte factuurperiode + type-vlag voor simulator.py.
    // Factuurperiode komt typisch met INCLUSIEVE einddatum (bv. 2026-01-31 = "t/m
    // 31 januari"). Simulator.py loopt `while cur < tot` (= EXCLUSIEF tot),
    // dus we moeten +1 dag toevoegen aan periodeTot.
    // Heuristiek: als periodeTot dezelfde maand is als periodeVan (= maand-factuur),
    // dan is het 99% zeker inclusief. We converteren altijd via +1 dag —
    // dat is veilig want simulator.py simuleert in kwartieren, niet hele dagen.
    const periodeTotExcl = new Date(ui.periodeTot + 'T00:00:00Z');
    periodeTotExcl.setUTCDate(periodeTotExcl.getUTCDate() + 1);
    const periodeTotStr = periodeTotExcl.toISOString().slice(0, 10);
    simPeriode = {
      van: ui.periodeVan,
      tot: periodeTotStr,
      type: 'specifiek',
    };
    console.log(`[sim] specifiek-periode: ${ui.periodeVan} → ${ui.periodeTot} (incl) → tot=${periodeTotStr} (excl)`);
  } else if (!simPeriode.van || ui.jaar === 'rolling12') {
    // Gebruik de periode uit MARKT (bepaald door prebuild op basis van laatste cache-dag)
    // MARKT.van/tot zijn inclusieve datums uit prebuild
    // Simulator verwacht exclusieve tot (dag erna)
    const marktTot = (MARKT && MARKT.tot) ? MARKT.tot : '2026-04-27';
    const marktTotExcl = new Date(marktTot + 'T00:00:00Z');
    marktTotExcl.setUTCDate(marktTotExcl.getUTCDate() + 1);
    const marktTotStr = marktTotExcl.toISOString().slice(0, 10);
    simPeriode = {
      van: (MARKT && MARKT.van) ? MARKT.van : simPeriode.van || '2025-04-28',
      tot: marktTotStr,
    };
  }

  // v15.11 sessie 4: slice marktdata op simPeriode VOOR doorgave aan simulator.
  // Fixt ook latente bug bij kalenderjaar-pad (was [:N] simple truncate vanaf
  // MARKT.van, niet vanaf simPeriode.van).
  // Voor rolling12 met simPeriode.van == MARKT.van: identiek aan v15.10.
  const _marktSlice = _sliceMarktVoorPeriode(MARKT, simPeriode);
  if (_marktSlice.mode !== 'binnen-markt') {
    console.log(`[sim] markt-slice: mode=${_marktSlice.mode}, offset=${_marktSlice.offset}, n=${_marktSlice.n}`);
  }

  // PV solar vorm — gebruik pre-built solar_norm als UI geen vorm stuurt
  // pvVorm: solar reeks hernormaliseerd voor de exacte simulatieperiode (van→tot)
  // Simulator verwacht N waarden genormaliseerd op 1, waarbij N = aantal kwartieren in periode
  let pvVorm = [];
  if (pvKwp > 0 && MARKT && MARKT.solar_norm && MARKT.solar_norm.length === 35040) {
    // Bouw periode-specifieke solar reeks vanuit de 2025-kalender solar_norm
    // via quarter_index: zelfde logica als simulator's quarter_index_in_year_2025
    const van = new Date(simPeriode.van + 'T00:00:00');
    const tot = new Date(simPeriode.tot + 'T00:00:00');
    const solarNorm2025 = MARKT.solar_norm;
    const jan2025 = Date.UTC(2025, 0, 1); // ms timestamp van 1 jan 2025
    const periodeSolar = [];
    for (let d = new Date(van); d < tot; d = new Date(d.getTime() + 15*60*1000)) {
      // Bereken de corresponderende index in 2025
      const maand = d.getUTCMonth();
      const dag = d.getUTCDate() - 1;
      const kwartier = Math.floor((d.getUTCHours() * 60 + d.getUTCMinutes()) / 15);
      // Schat index in 2025 via maand/dag/kwartier (geen weekdag-alignment nodig voor solar)
      const maandDagen2025 = [0,31,59,90,120,151,181,212,243,273,304,334];
      const idx2025 = (maandDagen2025[maand] + dag) * 96 + kwartier;
      periodeSolar.push(idx2025 < solarNorm2025.length ? solarNorm2025[idx2025] : 0);
    }
    const solarSum = periodeSolar.reduce((a,b) => a+b, 0);
    pvVorm = solarSum > 0 ? periodeSolar.map(v => v/solarSum) : periodeSolar;
    console.log('[sim] pvVorm gebouwd:', pvVorm.length, 'kwartieren, niet-nul:', pvVorm.filter(v=>v>0).length);
  } else if (pvKwp > 0) {
    console.warn('[sim] solar_norm niet beschikbaar, pvVorm=[]. PV-productie = 0.');
  }

  // v15.13.1 sessie 6 optie 2: bereken profielpiek voor max_afname_kw_zacht heuristiek.
  // Doel: geef LP een zachte penalty voor grid_in boven natuurlijke profielpiek + 20% buffer.
  // Voorkomt dat BSP-modus de aansluitingscap volledig benut voor BESS-laden, wat onnodig
  // de Groep B (maandpiek) kost de hoogte injaagt — zie SMARTUNIT_v10 Sc4 cijfers
  // (gem maandpiek 126 kW i.p.v. profielpiek 92 kW → +€3.578/jaar onterechte capaciteit).
  // Buffer 20% dekt (a) aanvullingen (laadinfra/elektrificatie niet meegenomen in basisprofiel),
  // (b) profiel-variabiliteit per kwartier, (c) sporadische werkdag-pieken.
  // Hard cap blijft aanslKw — alleen zacht-penalty triggert eerder.
  // Sessie 7 (v1.7) voegt monthly_peak-constraint toe aan BSP-LP objective met
  // c_per_maand_kw uit netbeheer.tarieven; deze profielpiek-heuristiek (zachte band)
  // blijft als bovengrens voor de LP staan zodat ZEER hoge BSP-laad-pieken alsnog
  // worden afgeremd. De combinatie pakt de meeste maandpiek-shaving op.
  const profielKwartier = (() => {
    const pNaam = ui.profielNaam || ui.profiel_naam || 'Slager';
    const profielDir = path.join(__dirname, 'data', 'profielen');
    if (fs.existsSync(profielDir)) {
      const files = fs.readdirSync(profielDir);
      const match = files.find(f => f.toLowerCase() === pNaam.toLowerCase() + '.json');
      if (match) {
        const d = JSON.parse(fs.readFileSync(path.join(profielDir, match), 'utf8'));
        return Array.isArray(d) ? d : d.profiel_kwartier || [];
      }
    }
    return (MARKT && MARKT.profiel) || [];
  })();
  let profielMax = 0;
  for (let i = 0; i < profielKwartier.length; i++) {
    if (profielKwartier[i] > profielMax) profielMax = profielKwartier[i];
  }
  // profielMax is genormaliseerd (profielKwartier som = 1.0).
  // profielMax × jaarverbruik_MWh × 1000 kWh/MWh / 0.25 h/kwartier = kW.
  const profielpiekKw = profielMax * jaarverbruik * 1000 / 0.25;
  // UI-override voor zachte cap (voor sales-tuning): ui.max_afname_zacht_kw.
  const zachtAfnameKw = Number(ui.max_afname_zacht_kw || ui.maxAfnameZachtKw || 0) ||
                        Math.max(1, Math.min(aanslKw, Math.ceil(profielpiekKw * 1.20)));
  console.log(`[sim] profielpiek=${profielpiekKw.toFixed(1)} kW → max_afname_kw_zacht=${zachtAfnameKw} kW (aanslKw=${aanslKw} hard)`);

  // v15.15.5: kies de tariefkaart één keer (grd + spanning) en hergebruik in
  // netbeheer + aansluiting (overschrijdingstarief).
  const _kaart = _kiesTarieven(grd, spanning);

  return {
    profiel_kwartier: profielKwartier,
    jaarverbruik_mwh: jaarverbruik,
    aanvullingen: {},
    pv: {
      kwp: pvKwp,
      specifiek_rendement_kwh_per_kwp: 900,
      vorm_kwartier: pvVorm,
      capex_eur: pvKwp > 0 ? (pvKwp <= 125 ? 71875 : pvKwp <= 150 ? 86250 : 115000) : 0,
      // v15.13: expliciete inverter_kw doorgeven (simulator gebruikt dit voor
      // PV-clipping; default fallback in simulator.py is 0.77 × kWp).
      inverter_kw: pvInverterKw,
    },
    pv_curtailment: {
      actief: curtailActief,
      trigger_eur_mwh: (ui.pv_curtailment && ui.pv_curtailment.trigger_eur_mwh) || 0,
      strategie: 'cap_op_verbruik',
    },
    batterij: batt,
    aansluiting: {
      // v15.13: asymmetrie afname ≠ injectie.
      // afname-cap = contractueel toegangsvermogen (aanslKw).
      // injectie-cap = som fysieke inverter-vermogens (PV-omvormer + BESS-omvormer),
      // tenzij UI expliciet maxInjectieKw zet.
      // v15.13.1: max_afname_kw_zacht = profielpiek × 1.20 (i.p.v. aanslKw) zodat
      // LP een penalty krijgt voor BSP-laden boven natuurlijke profielpiek.
      // v15.15.7: gecontracteerd toegangsvermogen = facturatiebasis (sunk), los van
      // het fysieke aansluitvermogen (max_afname_kw_hard = dispatch-cap).
      toegangsvermogen_kw:  toegangsKw,
      max_afname_kw_zacht:  zachtAfnameKw,   max_afname_kw_hard:  aanslKw,
      max_injectie_kw_zacht: maxInjectieKw,  max_injectie_kw_hard: maxInjectieKw,
      tarief_overschrijding_afname_eur_per_kw_jaar: _kaart.overschrijding_toegangsvermogen_eur_kw_jaar,
      tarief_overschrijding_injectie_eur_per_kw_jaar: 1.0,
    },
    contract: {
      // Modus bepaalt of de klant nomineert (passthrough) of niet (forfaitair)
      // Bij geen sturing of curtailment: geen nominatie → forfaitair (IMB markdown op injectie)
      // Bij BSP-modus: wel nominatie → passthrough
      modus: ui.pvInjStrategie === 'bsp_actief'
        ? 'passthrough'
        : (ui.pvInjStrategie === 'geen' || ui.pvInjStrategie === 'curtail_neg')
          ? 'forfaitair'
          : (ui.contract && ui.contract.modus) || 'passthrough',
      staffel,
      gsc_eur_mwh:  (ui.contract && ui.contract.gsc_eur_mwh)  || 11.0,
      wkk_eur_mwh:  (ui.contract && ui.contract.wkk_eur_mwh)  || 4.20,
      vergroening_eur_per_mwh: (ui.contract && ui.contract.vergroening_eur_per_mwh) || 0,
      vaste_kost_eur_maand: (ui.contract && ui.contract.vaste_kost_eur_maand) || 10.0,
      injectie_toegelaten: true,
      jaarverbruik_mwh: jaarverbruik,
    },
    netbeheer: { grd, spanning, tarieven: _kaart },
    forecast:  { sigma_da:0, sigma_imb:0, sigma_volume_verbruik_pct:0, sigma_volume_pv_pct:0 },
    markt: {
      // v15.11 sessie 4: gesliceerde arrays die exact mappen op simPeriode.
      // Voor rolling12 met simPeriode.van == MARKT.van: identiek aan v15.10
      // (full MARKT.spot_q / imb_q). Voor specifiek + kalenderjaar: correcte
      // tijdsuitlijning.
      spot_kwartier: _marktSlice.spot_q,
      imb_kwartier:  _marktSlice.imb_q,
    },
    simulatieperiode: simPeriode,
    random_seed: 42,
    bsp: {
      actief: bspActief,
      paper_capture_rate: 0.018,
      forecast_modus: (ui.bsp && ui.bsp.forecast_modus) || 'realistic',
      pv_curtailment_allowed: curtailActief,
      stacked,
    },
    // v15.15.3: 3-sturingen variant 1 — batterij enkel zelfconsumptie +
    // piekshaving, geen arbitrage (simulator.py v1.7.1 leest deze vlag).
    geen_arbitrage: !!ui.geen_arbitrage,
    // v15.15.4: laadpleinen (flexibele EV-laadvraag). simulator.py v1.8 leest
    // deze lijst; normalisatie + laadpunt-kW gebeurt daar. Zonder lijst = inert.
    laadpleinen: Array.isArray(ui.laadpleinen) ? ui.laadpleinen : [],
  };
}


// ─── MARKTDATA DASHBOARD ROUTES ──────────────────────────────────────────────
// GitHub market-data repo configuratie
const MARKET_DATA_OWNER = process.env.GITHUB_OWNER || 'JohanMMK';
const MARKET_DATA_REPO  = process.env.GITHUB_REPO  || 'market-data';
// GITHUB_PATH is het volledige pad van het primaire cachebestand (bv. 'data/fluctus-cache.json')
// De map wordt daaruit afgeleid ('data')
const _GITHUB_PATH_RAW  = process.env.GITHUB_PATH  || 'data/fluctus-cache.json';
const MARKET_DATA_PATH  = _GITHUB_PATH_RAW.includes('/') ? _GITHUB_PATH_RAW.split('/').slice(0,-1).join('/') : _GITHUB_PATH_RAW;
const GITHUB_TOKEN      = process.env.GITHUB_TOKEN || '';

// Helper: lees bestand van GitHub
async function githubRead(filename) {
  // Stap 1: haal de sha op via de Contents API (werkt altijd, klein antwoord)
  const apiUrl = `https://api.github.com/repos/${MARKET_DATA_OWNER}/${MARKET_DATA_REPO}/contents/${MARKET_DATA_PATH}/${filename}`;
  const headers = { 'User-Agent': 'fluctus-proxy', 'Accept': 'application/vnd.github.v3+json' };
  if (GITHUB_TOKEN) headers['Authorization'] = `token ${GITHUB_TOKEN}`;
  const metaResp = await fetch(apiUrl, { headers });
  if (!metaResp.ok) throw new Error(`GitHub read ${filename}: HTTP ${metaResp.status}`);
  const meta = await metaResp.json();
  const sha = meta.sha;

  // Stap 2: lees de inhoud via de raw URL (geen groottelimiet)
  const rawUrl = `https://raw.githubusercontent.com/${MARKET_DATA_OWNER}/${MARKET_DATA_REPO}/main/${MARKET_DATA_PATH}/${filename}`;
  const rawHeaders = { 'User-Agent': 'fluctus-proxy' };
  if (GITHUB_TOKEN) rawHeaders['Authorization'] = `token ${GITHUB_TOKEN}`;
  const rawResp = await fetch(rawUrl, { headers: rawHeaders });
  if (!rawResp.ok) throw new Error(`GitHub raw read ${filename}: HTTP ${rawResp.status}`);
  const content = await rawResp.text();
  return { data: JSON.parse(content), sha };
}

// Helper: schrijf bestand naar GitHub
async function githubWrite(filename, data, sha) {
  const url = `https://api.github.com/repos/${MARKET_DATA_OWNER}/${MARKET_DATA_REPO}/contents/${MARKET_DATA_PATH}/${filename}`;
  const content = Buffer.from(JSON.stringify(data)).toString('base64');
  const headers = { 'User-Agent': 'fluctus-proxy', 'Content-Type': 'application/json' };
  if (GITHUB_TOKEN) headers['Authorization'] = `token ${GITHUB_TOKEN}`;
  const body = { message: `auto: ${filename.replace('.json','')} ${new Date().toISOString().slice(0,10)}`, content };
  if (sha) body.sha = sha;
  const r = await fetch(url, { method: 'PUT', headers, body: JSON.stringify(body) });
  if (!r.ok) { const t = await r.text(); throw new Error(`GitHub write ${filename}: HTTP ${r.status} ${t.slice(0,200)}`); }
  return r.json();
}

// Dataset → bestandsnaam mapping
const DATASET_FILES = {
  meta:  'fluctus-cache-meta.json',
  spot:  'fluctus-cache-spot.json',
  imb:   'fluctus-cache-imb.json',
  wind:  'fluctus-cache-wind.json',
  solar: 'fluctus-cache-solar.json',
};


// ── GET /cache-read?dataset=<meta|spot|imb|wind|solar> ───────────────────────
// Leest een dataset uit de GitHub market-data repo en geeft die terug als JSON
app.get('/cache-read', async (req, res) => {
  const ds = req.query.dataset;
  if (!DATASET_FILES[ds]) return res.status(400).json({ error: `Onbekende dataset: ${ds}` });
  try {
    console.log(`[cache-read] ${ds} → ${DATASET_FILES[ds]} (owner=${MARKET_DATA_OWNER}, repo=${MARKET_DATA_REPO}, path=${MARKET_DATA_PATH})`);
    const { data } = await githubRead(DATASET_FILES[ds]);
    console.log(`[cache-read] ${ds} OK — type=${Array.isArray(data)?'array['+data.length+']':typeof data}`);
    res.json(data);
  } catch (e) {
    console.error(`[cache-read] ${ds} FOUT:`, e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── POST /cache-update?dataset=<...> ─────────────────────────────────────────
// Schrijft data naar de GitHub market-data repo
app.post('/cache-update', async (req, res) => {
  const ds = req.query.dataset;
  if (!DATASET_FILES[ds]) return res.status(400).json({ error: `Onbekende dataset: ${ds}` });
  try {
    let sha;
    try { const existing = await githubRead(DATASET_FILES[ds]); sha = existing.sha; } catch {}
    await githubWrite(DATASET_FILES[ds], req.body, sha);
    const size_kb = Math.round(JSON.stringify(req.body).length / 1024);
    res.json({ ok: true, dataset: ds, size_kb });
  } catch (e) {
    console.error(`[cache-update] ${ds}:`, e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ── GET /elia-data?from=YYYY-MM-DD&to=YYYY-MM-DD ─────────────────────────────
// Haalt Elia imbalance SI-prijzen op (kwartierlijks)
// Splitst automatisch in segmenten van 30 dagen
app.get('/elia-data', async (req, res) => {
  const { from, to, debug } = req.query;
  if (!from || !to) return res.status(400).json({ error: 'from en to verplicht' });
  try {
    const fromDate = new Date(from);
    const toDate   = new Date(to);

    // Splits in segmenten van 30 dagen
    const segDays  = 30;
    const segments = [];
    let segStart   = new Date(fromDate);
    while (segStart < toDate) {
      const segEnd = new Date(Math.min(toDate.getTime(), segStart.getTime() + segDays * 86400000));
      segments.push({ from: segStart.toISOString().slice(0,10), to: segEnd.toISOString().slice(0,10) });
      segStart = new Date(segEnd.getTime() + 86400000);
    }

    console.log(`[elia-data] ${segments.length} segmenten (${from} → ${to})`);

    const seen = new Map();
    let totalFetched = 0;
    let debugDone = false;

    for (const seg of segments) {
      // ods134 = Elia System Imbalance prijzen
      const baseUrl = `https://opendata.elia.be/api/explore/v2.1/catalog/datasets/ods134/records?where=datetime%3E%3D'${seg.from}'%20AND%20datetime%3C%3D'${seg.to}T23%3A45%3A00'&order_by=datetime%20asc&timezone=UTC&include_links=false&include_app_metas=false`;

      let offset = 0;
      while (true) {
        const pageUrl = baseUrl + `&limit=100&offset=${offset}`;
        const r = await fetch(pageUrl, { headers: { 'Accept': 'application/json', 'User-Agent': 'fluctus-proxy/1.0' } });
        if (!r.ok) { const t = await r.text(); throw new Error(`Elia imb HTTP ${r.status}: ${t.slice(0,100)}`); }
        const json = await r.json();
        const results = json.results || [];
        if (results.length === 0) break;

        // Debug: toon veldnamen
        if (debug && !debugDone) {
          return res.json({ debug_fields: Object.keys(results[0]), sample: results[0] });
        }
        debugDone = true;

        results.forEach(row => {
          const t = new Date(row.datetime).getTime();
          const v = parseFloat(row.imbalanceprice ?? 0);
          if (!isNaN(v) && !seen.has(t)) seen.set(t, v);
        });

        totalFetched += results.length;
        if (results.length < 100) break;
        offset += 100;
      }
    }

    const imb = Array.from(seen.entries())
      .map(([t,v]) => ({t,v}))
      .sort((a,b) => a.t - b.t);

    console.log(`[elia-data] ${imb.length} kwartieren uit ${totalFetched} records`);
    res.json({ imb });

  } catch (e) {
    console.error('[elia-data]', e.message);
    res.status(500).json({ error: e.message });
  }
});


// ── GET /entsoe-dayahead?from=YYYY-MM-DD&to=YYYY-MM-DD ───────────────────────
// Haalt ENTSO-E BELPEX day-ahead spotprijzen op (uurlijks)
// Splitst in segmenten van 30 dagen om timeout te vermijden
app.get('/entsoe-dayahead', async (req, res) => {
  const { from, to } = req.query;
  if (!from || !to) return res.status(400).json({ error: 'from en to verplicht' });
  try {
    const fromDate = new Date(from);
    const toDate   = new Date(to);

    // Splits in segmenten van 30 dagen
    const segDays  = 30;
    const segments = [];
    let segStart   = new Date(fromDate);
    while (segStart < toDate) {
      const segEnd = new Date(Math.min(toDate.getTime(), segStart.getTime() + segDays * 86400000));
      segments.push({ from: segStart.toISOString().slice(0,10), to: segEnd.toISOString().slice(0,10) });
      segStart = new Date(segEnd.getTime() + 86400000);
    }

    console.log(`[entsoe] ${segments.length} segmenten (${from} → ${to})`);

    // Debug: stuur ruwe XML terug voor eerste segment
    if (req.query.debug) {
      const seg0 = segments[0];
      const p0 = seg0.from.replace(/-/g,'') + '0000';
      const p1 = seg0.to.replace(/-/g,'') + '2300';
      const debugUrl = `https://web-api.tp.entsoe.eu/api?securityToken=${process.env.ENTSOE_TOKEN||''}&documentType=A44&in_Domain=10YBE----------2&out_Domain=10YBE----------2&periodStart=${p0}&periodEnd=${p1}`;
      const dr = await fetch(debugUrl);
      const xml = await dr.text();
      return res.send(xml.slice(0, 3000));
    }

    // Gebruik Map om eerste waarde per timestamp te bewaren
    // ENTSO-E A44 voor BE→BE geeft 1 prijs per uur/kwartier
    // Meerdere TimeSeries zijn verschillende periodes in hetzelfde XML-document
    const byTime = new Map();

    for (const seg of segments) {
      const periodStart = seg.from.replace(/-/g,'') + '0000';
      const periodEnd   = seg.to.replace(/-/g,'')   + '2300';
      const url = `https://web-api.tp.entsoe.eu/api?securityToken=${process.env.ENTSOE_TOKEN||''}&documentType=A44&in_Domain=10YBE----------2&out_Domain=10YBE----------2&periodStart=${periodStart}&periodEnd=${periodEnd}`;

      // Haal XML op met retry
      let xml = null;
      for (let attempt = 1; attempt <= 3; attempt++) {
        try {
          const r = await fetch(url);
          if (!r.ok) {
            const errBody = await r.text();
            throw new Error(`HTTP ${r.status}: ${errBody.slice(0,200)}`);
          }
          xml = await r.text();
          break;
        } catch (e) {
          console.warn(`[entsoe] segment ${seg.from}→${seg.to} poging ${attempt}/3: ${e.message}`);
          if (attempt === 3) throw new Error(`Segment ${seg.from}→${seg.to} gefaald na 3 pogingen: ${e.message}`);
          await new Promise(resolve => setTimeout(resolve, 2000 * attempt));
        }
      }
      if (xml) {

        // Parse XML TimeSeries
        const tsMatches = [...xml.matchAll(/<TimeSeries>[\s\S]*?<\/TimeSeries>/g)];
        tsMatches.forEach(tsM => {
          const tsBlock = tsM[0];
          const startM  = tsBlock.match(/<start>(.*?)<\/start>/);
          const resM    = tsBlock.match(/<resolution>(.*?)<\/resolution>/);
          if (!startM || !resM) return;
          const start   = new Date(startM[1]);
          const res_min = resM[1] === 'PT60M' ? 60 : 15;
          const ptMs    = [...tsBlock.matchAll(/<Point>[\s\S]*?<position>(\d+)<\/position>[\s\S]*?<price\.amount>(-?[\d.]+)<\/price\.amount>[\s\S]*?<\/Point>/g)];
          ptMs.forEach(m => {
            const pos = parseInt(m[1]) - 1;
            const t   = start.getTime() + pos * res_min * 60000;
            const v   = parseFloat(m[2]);
            // Eerste waarde per timestamp bewaren (niet gemiddelde)
            if (!byTime.has(t)) byTime.set(t, v);
          });
        });

        console.log(`[entsoe] segment ${seg.from}→${seg.to}: ${tsMatches.length} TimeSeries`);
      }
    }

    const points = Array.from(byTime.entries())
      .map(([t, v]) => ({ t, v: Math.round(v * 100) / 100 }))
      .sort((a, b) => a.t - b.t);

    console.log(`[entsoe] totaal ${points.length} punten`);
    res.json({ spot: points, data: points });

  } catch (e) {
    console.error('[entsoe]', e.message);
    res.status(500).json({ error: e.message });
  }
});


// ── GET /elia-renewable?dataset=wind|solar&from=YYYY-MM-DD&to=YYYY-MM-DD ─────
// Haalt Elia hernieuwbare productievolumes op
// Splitst automatisch in segmenten van 30 dagen om timeout te vermijden
app.get('/elia-renewable', async (req, res) => {
  const { dataset, from, to } = req.query;
  const dsIdMap = { wind: 'ods031', solar: 'ods032', ods031: 'ods031', ods032: 'ods032' };
  if (!dsIdMap[dataset]) return res.status(400).json({ error: `Onbekende dataset: ${dataset}` });

  try {
    const dsId     = dsIdMap[dataset];
    const fromDate = new Date(from);
    const toDate   = new Date(to);

    // Splits in segmenten van 30 dagen
    const segDays  = 30;
    const segments = [];
    let segStart   = new Date(fromDate);
    while (segStart < toDate) {
      const segEnd = new Date(Math.min(toDate.getTime(), segStart.getTime() + segDays * 86400000));
      segments.push({ from: segStart.toISOString().slice(0,10), to: segEnd.toISOString().slice(0,10) });
      segStart = new Date(segEnd.getTime() + 86400000);
    }

    console.log(`[elia-renewable/${dataset}] ${segments.length} segmenten (${from} → ${to})`);

    const byTime = new Map();
    let totalFetched = 0;

    for (const seg of segments) {
      // group_by datetime geeft 1 record per kwartier = totaal België
      const url = `https://opendata.elia.be/api/explore/v2.1/catalog/datasets/${dsId}/records?where=datetime%3E%3D'${seg.from}'%20AND%20datetime%3C%3D'${seg.to}T23%3A45%3A00'&group_by=datetime&select=datetime,sum(measured)%20as%20measured&order_by=datetime%20asc&timezone=UTC&include_links=false&include_app_metas=false&limit=100&offset=0`;

      // Debug
      if (req.query.debug) {
        const r = await fetch(url, { headers: { 'Accept': 'application/json', 'User-Agent': 'fluctus-proxy/1.0' } });
        const json = await r.json();
        return res.json({ debug_fields: Object.keys((json.results||[{}])[0]), sample: (json.results||[])[0] });
      }

      // Pagineer over segment (max 100 per pagina, 30 dagen × 96 = 2880 records → 29 pagina's)
      let offset = 0;
      while (true) {
        const pageUrl = url.replace('offset=0', `offset=${offset}`);
        const r = await fetch(pageUrl, { headers: { 'Accept': 'application/json', 'User-Agent': 'fluctus-proxy/1.0' } });
        if (!r.ok) { const t = await r.text(); throw new Error(`Elia ${dataset} HTTP ${r.status}: ${t.slice(0,100)}`); }
        const json = await r.json();
        const results = json.results || [];
        if (results.length === 0) break;

        results.forEach(row => {
          const t = new Date(row.datetime).getTime();
          const v = parseFloat(row.measured ?? 0) || 0;
          byTime.set(t, v); // group_by geeft al gesommeerde waarde
        });

        totalFetched += results.length;
        if (results.length < 100) break;
        offset += 100;
      }
    }

    const data = Array.from(byTime.entries())
      .map(([t,v]) => ({t, v: Math.round(v * 10) / 10}))
      .sort((a,b) => a.t - b.t);

    console.log(`[elia-renewable/${dataset}] ${data.length} kwartieren uit ${totalFetched} records`);
    res.json({ data });

  } catch (e) {
    console.error(`[elia-renewable/${dataset}]`, e.message);
    res.status(500).json({ error: e.message });
  }
});


// ── GET /explanation?chartId=<id> ────────────────────────────────────────────
// Levert dagelijks gecachede AI-uitleg per grafiek
app.get('/explanation', async (req, res) => {
  const { chartId } = req.query;
  if (!chartId) return res.status(400).json({ error: 'chartId verplicht' });
  try {
    const { data } = await githubRead(`fluctus-explanation-${chartId}.json`);
    // Cache geldig voor 6 uur
    const now = Date.now();
    const savedAt = data.savedAt ? new Date(data.savedAt).getTime() : 0;
    const cached = (now - savedAt) < 6 * 3600 * 1000;
    res.json({ cached, date: data.date, text: data.text, reason: cached ? null : 'ouder dan 6u' });
  } catch (e) {
    res.json({ cached: false, reason: 'niet gevonden', text: null });
  }
});

// ── GET /claude-explain-refresh?chartId=<id>&context=<tekst> ────────────────
// Genereert nieuwe AI-uitleg via Claude en slaat op in GitHub
app.all('/claude-explain-refresh', async (req, res) => {
  // Accepteer chartId uit query string OF request body (POST)
  const chartId = req.query.chartId || req.body?.chartId;
  const context = req.query.context || req.body?.context || req.body?.prompt;
  if (!chartId) return res.status(400).json({ error: 'chartId verplicht' });
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) return res.status(503).json({ error: 'ANTHROPIC_API_KEY niet geconfigureerd' });

  try {
    const prompt = context
      ? `Je bent een energiemarkt-expert voor Fluctus.net CVSO (België). ` +
        `De volgende marktdata is HISTORISCHE data uit het VERLEDEN — reeds voorbije periodes, NIET de toekomst. ` +
        `Datumnotatie in de data: DD/MM/JJJJ (dag/maand/jaar). ` +
        `\n\nSchrijf een UITGEBREIDE analyse (minimum 200 woorden) in het Nederlands met deze drie secties:\n` +
        `\n**1) Algemeen beeld**\n` +
        `Beschrijf de prijsniveaus, volatiliteit, spreads en het gedrag van spot vs onbalans in de getoonde periode. Wees concreet met cijfers uit de data.\n` +
        `\n**2) Trends**\n` +
        `Beschrijf duidelijke patronen: dag/nacht cycli, weekenddips, zonne-energie injectie (negatieve prijzen), windpieken, seizoenspatronen. Leg uit wat de spread en ratio betekenen voor batterij- en flexibiliteitsopbrengsten.\n` +
        `\n**3) Belangrijkste gebeurtenissen**\n` +
        `Gebruik de web_search tool om gericht te zoeken naar nieuws en events in de Belgische/Europese energiemarkt in de SPECIFIEKE periode uit de data. ` +
        `Zoek naar: nucleaire beschikbaarheid Doel/Tihange, gasprijs TTF, windproductie België, Elia systeemstoringen, Europese interconnectie-events. ` +
        `Koppel wat je vindt aan de zichtbare pieken en dalen in de grafiek. Als er geen relevante events gevonden worden, zeg dat dan eerlijk.\n` +
        `\nGrafiek: ${chartId}. Data: ${context}`
      : `Je bent een energiemarkt-expert voor Fluctus.net. Geef een algemene uitleg (3-5 zinnen, Nederlands) van wat grafiek "${chartId}" toont in het Fluctus marktdata dashboard.`;

    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'x-api-key': apiKey,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json'
      },
      body: JSON.stringify({
        model: 'claude-haiku-4-5-20251001',
        max_tokens: 1000,
        messages: [{ role: 'user', content: prompt }]
      })
    });

    if (!r.ok) throw new Error(`Anthropic HTTP ${r.status}`);
    const json = await r.json();
    let text = json.content?.[0]?.text || '';
    // Verwijder interne zoekprocessen - bewaar alleen de analyse vanaf "1)"
    const match = text.match(/(\*\*1\)|^1\))/m);
    if (match) text = text.slice(text.indexOf(match[0]));
    // Verwijder <search> blokken en --- lijnen
    text = text.replace(/<search>[\s\S]*?<\/search>/g, '');
    text = text.replace(/^-{3,}$/gm, '');
    text = text.trim();
    const today = new Date().toISOString().slice(0, 10);
    const data = { date: today, chartId, text, savedAt: new Date().toISOString() };

    // Sla op in GitHub
    try {
      let sha;
      try { const ex = await githubRead(`fluctus-explanation-${chartId}.json`); sha = ex.sha; } catch {}
      await githubWrite(`fluctus-explanation-${chartId}.json`, data, sha);
    } catch (e) {
      console.warn('[explanation] GitHub write mislukt:', e.message);
    }

    res.json({ ok: true, date: today, text });
  } catch (e) {
    console.error('[claude-explain-refresh]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─── START ────────────────────────────────────────────────────────────────────
laadMarktdata();  // laad marktdata synchroon bij startup

app.listen(PORT, () => {
  console.log(`Fluctus proxy v15.15.2 luistert op poort ${PORT}`);
  console.log(`simulator.py: ${fs.existsSync(path.join(__dirname,'simulator.py')) ? 'aanwezig':'ONTBREEKT'}`);
  console.log(`Markt status: ${MARKT_STATUS}${MARKT ? ' ('+MARKT.n_kwartieren+' kwartieren)' : ''}`);
});
