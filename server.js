'use strict';
// ============================================================================
// FLUCTUS PROXY SERVER
// Versie:        v15.121.0 (2026-08-31, Fase 5c — NBB-WINST): winstcijfers uit de neergelegde jaarrekening via de
//                GRATIS NBB-Balanscentrale-webservice "Authentic Data Query" (_bedrijfsWinst — 2 calls: references
//                + accountingData, headers NBB-CBSO-Subscription-Key/X-Request-Id, codes 9904/9903/9900/70).
//                Env NBB_CBSO_KEY. Toegevoegd aan de locatiescan (scan.financieel) + los testbaar via
//                GET /api/bedrijfswinst?btw=. Voedt de nota-noemer "besparing = X% van uw winst". Zonder key → null.
// Versie:        v15.120.0 (2026-08-31, Fase 5c — KBO-ADAPTER): _scanKbo is nu een provider-tolerante CBE/KBO-
//                REST-adapter (KBO_API = basis-URL, KBO_API_KEY = bearer-token) die werkt met cbeapi.be én de
//                Crossroads/kbodata.app-API. Leest NACE + ondernemingen-op-adres + (indien de provider ze levert)
//                de bestuurders, tolerant over veldvormen. cbeapi.be geeft GEEN bestuurders; kbodata.app wel
//                (EnterpriseRoles, groter plan). Zonder key/bron → null, scan valt terug op heuristiek. Geen regressie.
// Versie:        v15.119.0 (2026-08-31, Fase 5c — LEADS DUURZAAM): leads persisteren nu ook naar de Supabase-
//                bucket (leads/<token>.json, fire-and-forget in _leadOpslaan) en worden bij opstart in het
//                geheugen gehydrateerd (_leadsHydrate, gepagineerd, gated op SUPABASE_OK). /api/leads leest uit
//                dat gehydrateerde geheugen samengevoegd met de lokale cache → warme leads en gemailde nota-links
//                overleven een Railway-redeploy. Scans blijven kortlevend (bewust niet duurzaam gemaakt).
// Versie:        v15.118.0 (2026-08-31, Fase 5b — HARDWARE-BRUG + DESTINATION + VISION): /api/hardware-voorstel
//                (KMO-batterijstaffel §14.8 + Jacops-palen/PV → shoppinglist "welke PV/batterij/palen" + payback);
//                /api/destination-raming (spoor 2: capture/dwell §12.3, drempel = functie van de kostprijs §13.1,
//                sessieraming 0/50/100/150%); vision-pass in de locatiescan (Claude-vision op de Mapbox-tile →
//                panelen/parkeervakken, gated op keys, kruiscontrole tegen de factuur).
// Versie:        v15.117.0 (2026-08-31, Fase 5 — LOCATIESCAN): async POST/GET /api/locatiescan die uit het
//                factuuradres dak/sectorprofiel/laadpotentieel/LS-MS inschat en stap 8 vooringevuld aanlevert.
//                Pluggable, env/data-gated bronnen (Mapbox · GRB · KBO/NACE · Places · OpenChargeMap · Fluvius-
//                cabines), niet-blokkerend, graceful degradation. Lead: groeistap_aanvaard +28, scan-events.
// Versie:        v15.116.0 (2026-08-31, Fase 4 — VOORSCHOTFACTUUR): /api/lead neemt factuur_type
//                ('voorschot'|'afrekening'); _leadScore dempt de marge-bijdrage bij een voorschot (raming, niet
//                kunstmatig warm); /api/leads geeft factuur_type mee. Detectie zelf in factuur/extract.js v1.4.7.
// Versie:        v15.115.0 (2026-08-31, Fase 4 — SELF-SERVICE MANDAAT-INTAKE): POST /api/mandaat/self-identiteit
//                (lead-token + rol klant/adviseur), self-aanvraag (EAN → losse wachtrij met aanvrager+factuuradres),
//                self-status, self-bevestig-adres (lead-variant), los-patch (manager: rol/partner corrigeren).
//                wachtrij/sync dragen aanvrager/rol/partner/factuur_adres/aangevraagd_via/kwartierdata_aanwezig.
// Versie:        v15.114.0 (2026-08-31, Fase 4 — MANDAAT-PARALLEL + DOWNLOAD-GATING): POST /api/lead-mandaat
//                (akkoord + back-office-melding + score +20), /api/lead-herbereken (manager-only, sector nu /
//                echte Fluvius-data later → klant gemaild). GET /api/lead/:token geeft tier/mag_download.
// Versie:        v15.113.0 (2026-08-30, Fase 4 — SELF-SERVICE na HOE): e-mailverificatie via OTP-code
//                (/api/lead-verify-send + -check, code per Brevo-mail, 15 min, 6 pogingen) + /api/lead-update
//                (rapport-tier view|later|direct · laadplein-definitie · engagement-events). GET /api/lead/:token
//                logt 'nota_opened' (time-to-open). Warmte-score (_leadScore 0–100) + MANAGER-ONLY GET /api/leads
//                (overzicht + score + time-to-open) voor de supermanager/accountmanager-routing. ── v15.112.0 (2026-08-30, Fase 4 — SCHIL-LEAD → KAMINO-PROJECT): /api/lead seedt (service-role, best-
//                effort, enkel bij factuur-baseCase) een studieklaar Kamino-project onder kamino/<FLX-id>.json met de
//                baseCase + profiel + PV + klant + partner + samenvatting (bron:'energiekompas-schil'). Zo wordt elke
//                warme lead een voorgevuld project waarop de volledige studie (nominatie-sim → laadplein-rapport) kan
//                draaien; project_id komt in het lead-antwoord én de lead-notificatiemail. ── v15.111.0 (2026-08-30, Fase 4 — EnergieKompas €/km ALL-IN): /api/energiekompas/kpi rekent de
//                kilometerkost nu ALL-IN achter de meter = energiecomponent (goedkoopste ~30% spot) + netkosten/
//                heffingen (NET_HEFFING 90 €/MWh), i.p.v. enkel de commodity (die gaf ~0,006 €/km). De energie-
//                component blijft apart terug (kilometerkost_energie_eur_per_km); markt: laadkost_allin_eur_mwh +
//                net_heffing_eur_mwh toegevoegd. ── v15.110.0 (2026-08-30, Fase 4 — EnergieKompas LEAD-CAPTURE): nieuwe publieke endpoints /api/lead
//                (bewaart de onderhandelingsnota-data onder een token, mailt de klant een persoonlijke nota-link via
//                Brevo + notificeert de lead-mailbox), /api/lead-interesse (verrijkt de lead met de diepte-analyse-
//                interesses → "WARME LEAD"-mail) en GET /api/lead/:token (nota-pagina haalt D op via ?lead=<token>).
//                ENV: LEAD_MAIL_TO (default = AUDIT_MAIL_TO), WEB_BASE (default https://fluctus.net). Geen wijziging
//                aan bestaande endpoints. ── v15.109.0 (2026-08-28, Fase 4 — COHERENTIE onderhandel): de contract-heatmaps in /api/kamino/onderhandel
//                schalen nu op het GEPROJECTEERDE JAARvolume (geprojecteerd_mwh, fallback volumeMwh×365/dagen) i.p.v.
//                het factuur-maandvolume — anders stond de afname-heatmap ~12× te laag bij een maandfactuur. Zo geeft
//                de EnergieKompas-nota, gevoed met de factuur-baseCase, identieke cijfers als Kamino tegel 1.
// Versie:        v15.108.0 (2026-08-27, Fase 4 — GENERATOR twee-bakjes-netting): referentie bij een generator =
//                gebouw-elektriciteit ZONDER de generator-last (draait vandaag op diesel) + de vermeden dieselkost;
//                de sweep-cellen draaien mét de generator-last (batterij dekt de shots) → besparing = ref − cel nettot
//                vermeden diesel én bijgekomen stroomkost. Batterij-as start op de vervang-batterij P_gen (§3.2).
// Versie:        v15.107.0 (2026-08-27, Fase 4 — GENERATOR-component): de generator wordt in de bedrijf-sweep
//                gemodelleerd als een synthetische 'plein'-last (§3): shots op de kVA-piek, genset 3,3 kWh_e/L,
//                C-factor 2 (batterij-seed P_gen / 2u). De echte laadplein-engine waardeert zo de batterij+PV die
//                de diesel vervangt; diesel_vermeden = liter × €/L. + eigen 24×365 generator-heatmap. Additief/opt-in.
// Versie:        v15.106.0 (2026-08-27, Fase 4 slice D — onderhandelingsnota-heatmaps): /api/kamino/onderhandel
//                geeft OPT-IN (b.heatmaps=true) drie 24×365-kalenderheatmaps voor "huidige situatie": afname-profiel
//                (kW), dynamische prijs (spot €/MWh), afnamekost (afname×spot). Helper _contractHeatmaps, zelfde
//                vorm/index als de injectie-heatmaps. Tegel-1-respons blijft licht zonder de vlag. Additief.
// Versie:        v15.105.0 (2026-08-27, Fase 4 slice B/C): bedrijf-sweep PV-as fijner — 0/25/50/75/100% (≤5×3=15
//                cellen). Het 75%-PV-anker halveert het brede interieur-gat 50→100%, waar de bilineaire interpolatie
//                in de schil het meest afweek (~13% → ~3-5%). Batterij-as en schil ongewijzigd.
// Versie:        v15.104.0 (2026-08-27, Fase 4 slice B/C): GROVE bedrijf-sweep (≤4×3) + nieuw PUBLIEK
//                POST /api/energiekompas/bedrijf-cel — één echte dispatch voor een gekozen (pv_kwp, batt_kw). De
//                schil interpoleert tussen de sweep-ankers en roept bedrijf-cel aan bij klik voor de EXACTE cel.
// Versie:        v15.103.0 (2026-08-27, Fase 4 slice B/C BEDRIJF — PV×BATTERIJ-SWEEP): nieuw PUBLIEK
//                POST /api/energiekompas/bedrijf-sweep — echte laadplein-engine (buildSimInput → _runSimulatorOnce)
//                over een PV×batterij-rooster op de bedrijfslast + laadpleinen. Assen (Johan): PV 0→1,25×afname (kWp);
//                batterij-kW 0→(generator-kVA + Σ laadplein-vermogen + toegangsvermogen). Anchors+grid zoals
//                /api/thuisladen → de schil tekent de heatmap. Standaardprofiel, geen PII. Additief.
// Versie:        v15.102.0 (2026-08-27, Fase 4 — EnergieKompas publieke KPI's): nieuw PUBLIEK (geen login)
//                POST /api/energiekompas/kpi — 4 eerste-feedback-KPI's (kost afname · waarde injectie ·
//                restcapaciteit · kilometerkost) op de LIVE markt (MARKT.spot_q, goedkoopste ~30%% voor slim
//                laden) + de door de gebruiker ingegeven factuurwaarden. Geen PII/opslag. Additief.
// Versie:        v15.101.0 (2026-08-27, Fase 3 — KOPPEL LOSSE BIJ AANVRAAG): POST /api/mandaat/aanvraag checkt
//                nu eerst of de EAN al in de LOSSE lijst staat (= onze Fluvius-kennis na sync); zo ja → die
//                bestaande mandaat KOPPELEN aan het project (status/referentienr behouden) i.p.v. opnieuw aan te
//                vragen, en uit de losse lijst halen. Antwoord bevat {nieuw, gekoppeld}. Additief + geguard.
// Versie:        v15.100.0 (2026-08-27, Fase 3 — FLUVIUS-SYNC + LOSSE LIJST): nieuw POST /api/mandaat/sync
//                (MANAGER) — de skill schrijft de live Fluvius-status in bulk terug; EAN's zonder overeenkomstig
//                project komen in de LOSSE lijst (`mandaat_los/los.json`) die ook in GET /api/mandaat/wachtrij +
//                de Mandaten-app verschijnt (project_id 'LOS'). Additief + geguard.
// Versie:        v15.99.0 (2026-08-27, Fase 3 — ADRES-BEVESTIGING): nieuw POST /api/mandaat/bevestig-adres —
//                wie het mandaat startte (klant/adviseur, projecttoegang) bevestigt/weigert een adres-mismatch;
//                akkoord → EAN terug in de wachtrij met `adres_bevestigd:true` (skill dient in → mail buiten),
//                weiger → 'geannuleerd'. bevestigd_door/op gelogd op het record (manuele controle). Overall-
//                status negeert geannuleerde EAN's. Manager-overzicht: `mandaten.html` (web) leest de wachtrij.
// Versie:        v15.98.0 (2026-08-27, Fase 3 — MANDAAT-WACHTRIJ): mandaat-aanvragen worden nu op het
//                projectrecord bewaard (`rec.mandaat`) zodat ze NIET verloren gaan als de PC/Chrome/Fluvius-
//                sessie niet live is (nog geen always-on VPS). Nieuw: POST /api/mandaat/aanvraag (enqueue,
//                toegang = wie het project mag openen), GET /api/mandaat/status?project_id (per project),
//                GET /api/mandaat/wachtrij?status= (MANAGER — alle openstaande EAN's over alle projecten, wat
//                de Fluvius-skill ophaalt), POST /api/mandaat/status (MANAGER — skill schrijft referentienr/
//                gemaskeerd mail/adres-match/status per EAN terug = de controlelijst). Statussen: wachtrij →
//                aangevraagd → actief → geleverd (+ adres_mismatch/fout). Additief + geguard.
// Versie:        v15.97.0 (2026-08-27, Fase 3 — profiel-ladder OPEN): het gemeten kwartierprofiel
//                (`_opgeladenProfiel` → `_pasOpgeladenAfnameToe/_InjectieToe`) wordt niet langer enkel voor
//                MANAGERS toegepast maar voor ALLE gebruikers zodra er een profiel bij het project staat
//                (Johan-keuze). Enkel `project_id` + opgeladen profiel vereist; `_mgr_ok` wordt niet meer
//                gecheckt in de twee helpers + de toegangsvermogen-check. Fallback = standaard-SLP (ongewijzigd).
//                Opladen/aanvragen van meetdata blijft een managerhandeling (Fluvius-itsme + profiel-upload).
// Versie:        v15.96.0 (2026-08-26, Fase 2c — klant-onboarding & scoped toegang): (1) resolveUser matcht
//                nu ook op E-MAIL als er geen auth_uid-profiel is (uitgenodigd/klant-profiel) en self-healt
//                auth_uid → de rol (bv. 'klant') klopt meteen bij de eerste OTP-login. (2) POST /api/kamino/
//                project maakt best-effort een AUTO klant-account (profiles role 'klant', status 'invited')
//                als rec.klant.email gezet is — non-blocking. (3) GET /api/kamino/rapport-open kreeg de
//                `_magProjectOpenen`-gate (externe klant-accounts mogen niet elk rapport lezen). Additief +
//                geguard; klant opent via /apps/klant.html?project=<code> enkel zijn eigen project (OTP-login).
// Versie:        v15.95.0 (2026-08-26, Fase 2b — Johan-correctie): een **partnermanager** ziet AUTOMATISCH
//                enkel de **EnergieKompas**-schil (app_id 'energiekompas'), niet de losse interne tools.
//                `_PARTNER_APPS` = {'energiekompas'}. Zowel de portal-launcher als de in-app toegangsgate
//                volgen dit. Fluctus-interne apps (energiemarkt, gemeenteplan) + Gebruikers blijven manager-
//                only; adviseurs blijven grant-gebaseerd (zoals sellers). Additief + geguard.
// Versie:        v15.94.0 (2026-08-26, Fase 2b — RBAC nieuwe rollen): een **partnermanager** kreeg
//                automatisch de klant-toolset (simulator/betaalplein/kamino/thuisladen). Vervangen door
//                v15.95.0 (enkel EnergieKompas) — zie boven.
// Versie:        v15.93.0 (2026-08-26, Fase 2): rollen & bedrijf beheerbaar in de Gebruikers-app. EPC/bedrijf-
//                sleutel = `profiles.company`. `action:'role'` aanvaardt nu manager/seller/adviseur/partnermanager/
//                klant (+ zet optioneel company mee); nieuw `action:'company'`; nieuw GET /api/manager/partners
//                (distinct bedrijven). Partnermanager ziet alle projecten met rec.partner === zijn company.
// Versie:        v15.92.0 (2026-08-26, Fase 2 rollen & toegang — FUNDERING): resolveUser leest nu `partner`
//                (EPC-id) uit profiles; `_magProjectOpenen` geeft een **partnermanager** toegang tot alle
//                projecten van zijn eigen EPC (rec.partner === u.partner); elke save registreert de opslaande
//                adviseur (als er nog geen is) + het partner-id (uit de ?partner=-schil). Additief — zonder
//                partner-data (schil komt in Fase 4) verandert er niets aan het huidige gedrag.
// Versie:        v15.91.1 (2026-08-26, Fase 1): PERF /api/kamino/projecten — records nu PARALLEL gedownload
//                (Promise.all) + 20s in-memory cache (bust bij elke save). Was 50× sequentieel ≈ 3,1s → de
//                terughaal-dropdown vulde te traag ("2× openen"). Nu < 0,5s (koud) / instant (warm).
// Versie:        v15.91.0 (2026-08-26, Fase 1): MEERDERE invoer-scenario's per project. Het kamino-record
//                krijgt een `scenarios`-map (<key> → {scenario,input,baseCase,bijgewerkt}); elke save accumuleert
//                onder de scenario-naam i.p.v. te overschrijven. GET /api/kamino/projecten geeft één rij per
//                scenario; GET /api/kamino/project-get?...&scenario=<key> overlayt dat scenario op input/baseCase.
//                Volledig additief — records zonder scenarios-map vallen terug op het oude gedrag.
// Versie:        v15.90.1 (2026-08-26, Fase 1): + `scenario`-veld in het kamino-record en in
//                GET /api/kamino/projecten, zodat de terughaal-dropdown "naam · scenario · datum · code" toont.
// Versie:        v15.90.0 (2026-08-26, Fase 1 kern-persistentie): (1) /api/kamino/project bewaart nu ook `input`
//                (volledige invoer-snapshot per flow — universele save). (2) /api/kamino/project-get is niet langer
//                manager-only: toegang = manager OF eigenaar/adviseur/klant (helper `_magProjectOpenen`). (3) NIEUW
//                GET /api/kamino/projecten: rol-gefilterde lijst van kamino-records voor de terughaal-dropdown
//                (naast het bestaande /api/projecten met scenario-namen). Volledig additief + geguard.
// Versie:        v15.89.0 (2026-08-22 Europe/Brussels, Johan): DRIFT-FIX 100%-factuur. scaleBez rondde de sessies
//                op hele getallen af (Math.round), maar de AC-default is 1,5 sessies/paal → 4,5 werd 5, óók bij 100%
//                → de 100%-scenariofactuur lag ~€529 hoger dan de hoofdsim. Nu op 0,1 sessie afgerond → 100% = hoofdsim.
// Versie:        v15.88.0 (2026-08-22 Europe/Brussels, Johan): bezoekers-scenario's — als er GEEN betalende pleinen
//                zijn (of client stuurt alleen_basis), enkel scenario 1 (geen batterij) + 2 (batterij), telkens MET
//                de gewone (wagenpark) pleinen; de 50–200%-sessieschaling wordt overgeslagen (niets te schalen → 4
//                zware sims minder). Response draagt heeft_betalend + alleen_basis.
// Versie:        v15.87.0 (2026-08-21 Europe/Brussels, Johan): CREG-besparing gewoon plein nu ÉÉN CONSTANTE (netto).
//                Besparing = geleverd_wag × (CREG-forfait − all-in laadkost van de BASIS geen_batt). Voorheen per rij
//                variërend (marg. vs all-in) → €1036–1416 voor hetzelfde plein. Nu constant + gelijk aan scherm/rapport;
//                nog steeds netto (geen bruto geleverd×CREG, dat zou dubbeltellen met de factuur). Response: extra
//                velden besparing_wag_const_eur + basis_energieprijs_eur_mwh.
// Versie:        v15.86.0 (2026-08-21 Europe/Brussels, Johan): COHERENTIE bezoekers-scenario's. (1) Omzet betalend
//                plein = Σ GELEVERD MWh per plein × de VERKOOPPRIJS van DÁT plein (opbrengst_eur_mwh), niet één
//                globaal tarief. (2) km nu expliciet gevraagd én geleverd (bezoekers), plus km_wag + geleverd_wag_mwh
//                zodat de levering van de niet-betalende pleinen in ELKE rij zichtbaar is, met besparing_wag_eur.
// Versie:        v15.85.0 (2026-08-21 Europe/Brussels, Johan): MIX betalend + gewoon plein. De basisvarianten
//                (geen batt / batt) strippen ENKEL de betalende (bezoekers) pleinen — het gewone (wagenpark) plein
//                blijft vast in álle rijen, zodat de sensitiviteit (50–200%) puur over de betalende pleinen gaat.
//                Winst (marginaal) = opbrengst(bezoekers × verkoopprijs) − factuur + factuur_basis (de wagenpark-
//                CREG-besparing zit al in de referentie → niet dubbel geteld). Per rij komt de wagenpark-besparing
//                (geleverd × CREG-forfait req.creg_eur_mwh) apart mee als besparing_wag_eur (informatief). Puur-
//                bezoekers = identiek aan v15.84.
// Versie:        v15.84.0 (2026-08-20 Europe/Brussels, Johan): BEZOEKERS-SCENARIO'S — extra kolom "laadprijs
//                marginaal" = (jaarfactuur_scenario − jaarfactuur_batterij) / geladen_mwh (kost van ENKEL de
//                laadenergie; marge = tarief − dit). rows geven nu laadprijs_marg_eur_mwh terug. De bestaande
//                energieprijs_eur_mwh blijft de all-in afnameprijs (factuur / totale afname).
// Versie:        v15.83.0 (2026-08-20 Europe/Brussels, Johan): BEZOEKERS-SCENARIO'S — DRIFT-FIX. Elke variant
//                wordt nu op de sturing EXCL. onbalans gepind (_variantUi 'sturing'), exact dezelfde dispatch
//                als de hoofdsim-tegel "factuur sturing zonder onbalans". Voorheen erfde de scenario-run de
//                rauwe sturing-vlaggen uit STATE.lastSimInput → de 100%-rij kon een andere sturing draaien dan
//                de tegel en week de jaarfactuur af. Nu reproduceert de 100%-rij exact de hoofdsim-factuur.
// Versie:        v15.82.0 (2026-08-20 Europe/Brussels, Johan): BEZOEKERS-SCENARIO'S — tabel per scenario nu met
//                % geleverd (i.p.v. absolute geleverde km), TOTALE jaarfactuur van dat scenario, en winst t.o.v.
//                scenario 1 (opbrengst − factuur_scenario + factuur_basis). De batterij-rij zonder sessies toont
//                zo het verbruiksvoordeel (piekshaving + spot-arbitrage). rows.map geeft is_basis/pct_geleverd/
//                jaarfactuur_eur/winst_eur voor ELKE rij terug.
// Versie:        v15.81.0 (2026-08-20 Europe/Brussels, Johan): BEZOEKERS-SCENARIO'S. Nieuwe route
//                POST /api/bezoekers-scenarios draait 6 varianten parallel (geen batt / batt / batt+plein @
//                50/100/150/200% sessies) op dezelfde aansluiting → energieprijs, km gevraagd/geleverd,
//                opbrengst (geleverd × tarief) en winst (opbrengst − factuur + factuur_basis).
// Versie:        v15.80.0 (2026-08-18 Europe/Brussels, Johan): POSTCODE hoofdgemeente-override. Sommige postcodes
//                werden met een deelgemeente gelabeld i.p.v. de hoofdgemeente (3800 → "Aalst (Limb.)" i.p.v.
//                "Sint-Truiden"). HOOFDGEMEENTE_OVERRIDE zet de hoofdgemeente vooraan in PC_GEMEENTE_INDEX (additief).
// Versie:        v15.79.0 (2026-08-17 14:26 Europe/Brussels, Johan): THUISLADEN — /api/thuisladen geeft nu netbeheer
//                {grd, spanning, regio, default} terug. Zonder postcode valt de input stil terug op Fluvius West|LS
//                (Vlaanderen); de app toont dit expliciet zodat de klant de veronderstelde regio/tariefkaart ziet.
// Versie:        v15.78.0 (2026-08-17 12:29 Europe/Brussels, Johan): THUISLADEN PV-FIX. De base werd met pv_kwp=0
//                gebouwd → buildSimInput bouwde GEEN zonvorm → de extra PV per anker produceerde NIETS (€/MWh vlak,
//                rendement daalde met panelen). Base nu met max-extra-PV gebouwd (vorm + omvormer) en pv.kwp gereset
//                naar enkel de bestaande PV; run_thuisladen telt per anker de panelen bovenop mét echte zonvorm.
// Versie:        v15.77.0 (2026-08-17 11:58 Europe/Brussels, Johan): THUISLADEN — omvormer-sizing C-rate (par.BATT_CRATE,
//                default 2) doorgegeven aan de dispatch (kW = kWh/C → grotere batterij shaaft hógere piek).
// Versie:        v15.76.0 (2026-08-17 11:49 Europe/Brussels, Johan): THUISLADEN — nieuwe route POST /api/thuisladen-cel
//                (één echte dispatch voor de aangeklikte cel) zodat "klik = exact simuleren" in server-modus de ECHTE
//                cijfers geeft i.p.v. het lokale benaderingsmodel (dat inconsistente heatmap-uitschieters gaf).
//                Input-opbouw gedeeld via _thuisladenInput().
// Versie:        v15.75.0 (2026-08-17 11:25 Europe/Brussels, Johan): THUISLADEN PARALLEL. /api/thuisladen spawnt de
//                30 ankers nu PARALLEL (per cel _tl_cell via _runSimulatorOnce, gecapt op SIM_MAX_PARALLEL) i.p.v.
//                één trage sequentiële Python-lus die de client-time-out (150s) haalde. Besparing/ref hier berekend.
// Versie:        v15.74.0 (2026-08-17 11:11 Europe/Brussels, Johan): THUISLADEN — /api/thuisladen geeft nu ook de
//                weekend-vensters (we_start/we_eind) per wagen door en kapt de PV-as bij 2 fasen op 20 panelen.
// Versie:        v15.73.0 (2026-08-16, Johan): THUISLADEN-TEGEL. Nieuwe route POST /api/thuisladen — bouwt een
//                residentiële base-input (buildSimInput) en spawnt simulator.py in de geïsoleerde _modus:'thuisladen'
//                (30 anker-dispatches, 6 batterij-kWh × 5 PV-panelen, ALLE zonder onbalans). Frontend interpoleert +
//                simuleert losse cellen bij. Onbalans blijft buiten de zoektocht (extra winst apart). VOLLEDIG
//                geïsoleerd: geen enkele bestaande route/flow raakt gewijzigd. Vereist simulator.py met run_thuisladen.
// Versie:        v15.72.0 (2026-08-15 20:11, Johan): SIMULEER-KNOP. _draaiSim3 kent nu de vlag input._simuleer_enkel →
//                forceert EXACT de opgegeven installatie in 3 sturingen (modus 'enkel'), zonder opstellingen-vergelijking,
//                batterij-/PV-sweep of groeipad. Geguard: enkel de nieuwe Simuleer-knop zet de vlag; alle bestaande
//                flows (Ontwerp/nominatie-sim-3, autopilot) ONgewijzigd. Vereist simulator.html v1.63.73+.
// Versie:        v15.71.0 (09-08, Johan): GEEN AUDIT-MAIL BIJ VERWIJDEREN. action='delete' verstuurt geen Brevo-mail meer;
//                de export blijft lokaal downloadbaar via de UI. (_verzendAuditMail blijft bestaan maar wordt niet meer aangeroepen.)
// Versie:        v15.70.0 (09-08, Johan): NO-FACTUUR-FLOW + LABEL. /api/kamino/onderhandel werkt nu ook ZONDER
//                factuur-energiepost: op een referentieprijs (referentie_eur_mwh, default 90) × verbruik → geen_factuur:true.
//                /api/nominatie-sim is async + past het opgeladen afname-profiel toe (tegel 1 op echt profiel) en draagt
//                profiel_bron als label. /api/injectie-optimalisatie draagt profiel_bron+injectie_bron. Fallback overal ongewijzigd.
// Versie:        v15.69.0 (09-08, Johan): FASE 4 — TEGEL 2 OP HET ECHTE INJECTIEPROFIEL. _analyseerInjectieOptimalisatie
//                aanvaardt nu een opgeladen injectie-kwartierprofiel (p.injectie_kwartier, 35040, som=1): de curtailment/
//                onbalans-waardering draait op de ECHTE gemeten injectievorm i.p.v. de gemodelleerde zonvorm. Nieuwe helper
//                _pasOpgeladenInjectieToe (manager-only, project_id). Fallback = ONGEWIJZIGD (zonvorm) bij geen manager/geen
//                profiel/fout. kamino/productie wired + label out.injectie_bron. GEEN wijziging aan het afname-pad.
// Versie:        v15.68.0 (08-08, Johan): PROFIEL-ANALYSES MANAGER-ONLY. Het opgeladen Fluvius-profiel wordt nu enkel
//                in de analyse gebruikt wanneer de aanvrager een MANAGER is (_isManagerReq → input._mgr_ok). Sellers/
//                adviseurs blijven altijd op het standaardprofiel (eenvoud + geen verwarring). Toegepast op
//                nominatie-sim-3, groeipad, pv-sweep, opstelling, kamino/onderhandel (+ toegangsvermogen_advies),
//                kamino/productie, kamino/aansluiting. Fallback ongewijzigd: geen manager / geen profiel / fout →
//                standaardprofiel. (Upload was al manager-only sinds v15.66.)
// Versie:        v15.67.0 (08-08, Johan): OPGELADEN PROFIEL IN DE ANALYSE (FASE 2/3/5). Een opgeladen Fluvius-afname-
//                profiel (fase 1) wordt nu, wanneer het request een geldig FLX-project_id draagt, in de analyse gebruikt
//                i.p.v. het standaardprofiel: _opgeladenProfiel() laadt het CSV uit de bucket, normaliseert het naar de
//                35040-vorm (som=1, 2025-kalenderindex, dubbele kwartieren gemiddeld) en buildSimInput geeft het
//                voorrang (ui._opgeladen_profiel_kwartier) — fallback = standaard bij eender welke fout (geen regressie).
//                Toegepast op nominatie-sim-3, groeipad, pv-sweep, opstelling, kamino/onderhandel, kamino/productie,
//                kamino/aansluiting. FASE 3: kamino/onderhandel geeft nu een toegangsvermogen_advies (maandpiek 12 mnd
//                < gecontracteerd toegangsvermogen → verlaagbaar + indicatieve besparing). Resultaten dragen
//                profiel_bron/_meta.profiel als label (fase 5). Nog te doen: tegel-2 injectie-curtailment/onbalans op
//                het injectieprofiel + labels in de simulator-rapporten.
// Versie:        v15.66.0 (08-08, Johan): FLUVIUS-PROFIEL UPLOAD (FASE 1). Nieuwe manager-only endpoint
//                POST /api/kamino/profiel-upload slaat een door de converter gemaakt kwartier-CSV (afname/injectie)
//                op in de bucket als profielen/<projectID>_<EAN>_<klant>_<type>.csv en registreert de meta in het
//                projectrecord (rec.profielen[type]) — zo gedeeld over de 4 tegels. /api/kamino/project whitelist nu
//                'profielen' zodat het bij elke save behouden blijft. FASE 2 (gebruik in de analyse i.p.v. het
//                standaardprofiel, toegangsvermogen-check, tegel-2 injectie-curtailment/onbalans, rapport-labels) volgt.
// Versie:        v15.65.0 (08-08, Johan): PARALLELLE-DISPATCH-ZICHTBAARHEID. GET / (en /health) tonen nu
//                sim_max_parallel (effectieve bovengrens), piek_gelijktijdig (hoeveel runs er ECHT tegelijk liepen
//                sinds start = wat er effectief benut werd), runs_totaal en cpus_gerapporteerd. Startup logt dit ook
//                altijd (ook bij =1). LET OP: Node's os.cpus() rapporteert meestal het HOST-aantal cores, niet je
//                Railway-plan-quota; het echte vCPU-quota lees je in Railway → service → Metrics/Settings.
// Versie:        v15.64.0 (08-08, Johan): PREVIEW (DRY-RUN) VOOR VERWIJDEREN. POST /api/manager/user action='delete'
//                met dry_run:true verzamelt de info + telt wat er ZOU herlinken/verwijderen en geeft diagnostiek terug
//                (doel gevonden?, GitHub-token aanwezig?, Brevo geconfigureerd?, bucket-list OK?), MAAR wijzigt, mailt
//                en verwijdert niets. Zo kan je de hele keten op een bestaand account testen zonder een wegwerp-account
//                te maken. Gebruikers.html v1.5 heeft een "Preview"-knop per rij die de preview-export ook downloadt.
// Versie:        v15.63.0 (08-08, Johan): PROJECTEN HERLINKEN BIJ VERWIJDEREN. Bij action='delete' worden de gelinkte
//                projecten niet langer verweesd achtergelaten maar herstempeld naar AUDIT_MAIL_TO (oekene@gmail.com):
//                scenario's in fluctus-scenarios (owner_uid → oekene's auth_uid) en Kamino-projectrecords in de bucket
//                (adviseur-email → oekene). Gebeurt vóór het verwijderen; aantallen staan in de export/mail en in het
//                antwoord (herlink.scenarios / herlink.kamino). Zo blijven de projecten in het systeem en zichtbaar bij
//                de manager, en komt het e-mailadres tóch vrij voor een verse onboarding.
// Versie:        v15.62.0 (08-08, Johan): VERKOPER VERWIJDEREN + AUDIT-EXPORT. POST /api/manager/user action='delete'
//                verzamelt eerst alle gelogde info van de gebruiker (profiel + app-toegangen + activiteitslog +
//                certificaten) in een .txt, mailt die naar AUDIT_MAIL_TO (default oekene@gmail.com) met onderwerp
//                "<naam> verwijderd op YYYYMMDD uu:mm" via Brevo (gated op BREVO_API_KEY), en verwijdert dan logs →
//                grants → profiel → auth-user. Zo komt het e-mailadres weer vrij voor een verse onboarding. De UI
//                (gebruikers.html v1.3) downloadt de .txt óók lokaal als vangnet, ook als de mail niet verstuurd is.
//                Zelf-verwijderen is geblokkeerd. NIEUWE ENV (optioneel): BREVO_API_KEY, BREVO_SENDER, AUDIT_MAIL_TO.
// Versie:        v15.61.0 (08-08, Johan): APP-CATALOGUS AUTO-PROVISION. POST /api/manager/app-access maakt een
//                ontbrekende app-rij nu automatisch aan in `apps` (naam meegestuurd vanuit de UI, fallback = id),
//                vóór de grant. Oorzaak was een FK-violation (23503): kamino/jacops/gemeenteplan stonden in de UI
//                maar niet als rij in `apps` (enkel simulator/congestie/energiemarkt geseed in 9a). Nu heelt de
//                endpoint dat zelf → een nieuwe app toekennen werkt zonder handmatige SQL-seed. Gebruikers.html v1.2
//                stuurt app_naam mee. (Blijft compatibel: zónder app_naam valt de naam terug op het id.)
// Versie:        v15.60.0 (08-08, Johan): APP-TOEGANG TOEKENNEN FIX. POST /api/manager/app-access gaf HTTP 500 bij
//                het aanzetten van een app voor een actieve verkoper (bv. supervision@directmarket.energy): de insert
//                gebruikte `on_conflict=user_id,app_id`, wat een unique-constraint op (user_id,app_id) in
//                user_app_access vereist — ontbreekt die, dan faalt PostgREST (42P10) en springt het vinkje in de UI
//                terug. Nu idempotent zónder on_conflict (bestaanscheck → INSERT indien nodig): geen constraint meer
//                nodig, dubbel toekennen faalt niet. Gebruikers.html (v1.1) toont de exacte serverfout nu in een alert.
// Versie:        v15.59.0 (08-08, Johan): PARALLELLE DISPATCH-RUNS (snelheid sim-3/groeipad/pv-sweep). De sim-3-
//                zoektocht bestaat uit ONAFHANKELIJKE _runSimulatorOnce-spawns: de mix-zoeklus (k=1..N), de
//                batterij-sweep, de drie opstellingen (verhogen/batterij/mix), elke opstelling z'n drie sturingen,
//                en de groeipad/pv-sweep-stappen. Die draaien nu via _pmap parallel, begrensd door een globale
//                semafoor SIM_MAX_PARALLEL (env, default 1 = EXACT het oude sequentiële gedrag → geen regressie).
//                Cap = min(env, #cores) zodat CBC z'n per-solve timeLimit niet mist (oversubscriptie → time-out →
//                suboptimaal → cent-drift). Elke run is deterministisch en leest enkel z'n stdin ⇒ parallel geeft
//                IDENTIEKE cijfers, enkel sneller. Zet SIM_MAX_PARALLEL=4 (of #cores) op fluctus-proxy om aan te
//                zetten; terug op 1 = instant rollback. VEREIST een live smoke-test na het verhogen van de flag.
// Versie:        v15.58.0 (07-08, Johan): PER-DAG-VERMOGEN VANGNET. Sommige leveranciers (bevestigd YUSO; wellicht
//                Luminus) tonen de kW-posten (toegangsvermogen/maandpiek/overschrijding) als "kW × aantal dagen"
//                met eenheidsprijs = maandtarief/dagen. Het bedrag klopt, maar de afgelezen kW is ~dagen× te hoog
//                (bv. 7.500 "kW" op 30 dagen = 250 kW echt). /api/factuur-extract normaliseert nu bc.aansluitVermogenKva
//                via de load factor over de factuurperiode (LF < 2% én na ÷dagen weer plausibel → per-dag → ÷dagen),
//                met flag bc._perDagVermogenCorrectie. Beschermt ALLE consumers (kamino-aansluiting leidt toegangs-
//                vermogen af uit aansluitVermogenKva×0.9). Bron-fix (Methode A, tariefkaart-ratio, incl. maandpiek
//                als apart veld) hoort in factuur/extract.js naast de CAPACITEIT_DUBBEL-correctie. apply_base_case.js
//                v1.18 houdt hetzelfde vangnet als tweede net op het wizard-pad.
// Versie:        v15.57.0 (03-08, Johan): CARRYOVER-FIX. /api/kamino/project whitelist nu het 'profiel'-veld in het
//                projectrecord, zodat het gekozen verbruiksprofiel bewaard blijft en correct terugkomt bij de manager-
//                open in de interactieve simulator (voorheen ontbrak het → default-profiel → verkeerde dispatch/cijfers).
//                Alleen deze whitelist-regel; geen sim-logica gewijzigd.
// Versie:        v15.56.0 (01-08, Johan): GROEIPAD STAPT MET DE GEBRUIKTE MODULE. /api/groeipad hardcodede nog k×120/260
//                (over het hoofd gezien in v15.54); nu leest het input.batt_module via _buKw/_buKwh (fallback 120/260).
//                Een kleine site groeit dus in 5/10- of 30/60-stappen i.p.v. altijd 120/260 — consistent met de sizing,
//                de pv-sweep en /api/opstelling. Client stuurt batt_module al mee (simulator ≥ v1.63.34).
// Versie:        v15.55.0 (01-08, Johan): KABELTRACÉ per batterij-module (KABEL_BATT_TRACE 5→1.500/30→4.000/120→15.000,
//                was vast €15.000). /api/kamino/aansluiting rekent het batterij-kabeltracé nu op de gekozen module. De
//                client (_kpi_capex_vast via _kabeltrace) doet hetzelfde voor de nominatie-sim-3-sweep. Zo wordt een
//                kleine batterij (bv. 30/60) rendabel i.p.v. door een vast €15k-tracé onder de 10% gedrukt te worden.
// Versie:        v15.54.0 (01-08, Johan): BATTERIJ-MODULE OP MAAT. Drie modules (5/10, 30/60, 120/260); de sizing-
//                functies (_dimZet, _mixZoekVerzwaring, _dimensioneerMix, _opstellingUi, _batterijSweepGebouw, pv-sweep,
//                /api/opstelling, /api/kamino/aansluiting) lezen de module via _buKw/_buKwh uit input.batt_module met
//                FALLBACK op 120/260 (geen regressie). De client kiest de module op de trigger (tegel 3 = toegangs-
//                vermogen; tegel 4 = +50% laadpleinvermogen) en stuurt ze mee; gebouw-sweep = veelvouden tot ≈ basis, max 6.
// Versie:        v15.53.0 (01-08, Johan): /api/kamino/project-open geeft nu ook `rapporten.studies` mee (lijst tegels
//                met een bewaard rapport) → Kamino toont per tegel een "bekijk vorig rapport"-knop bij een heropend
//                project (i.p.v. enkel herrekenen). Rapport zelf komt uit /api/kamino/rapport-open.
// Versie:        v15.52.0 (01-08, Johan): MARGINALE zelfconsumptie voor de extra-PV-dimensionering. /api/pv-sweep en
//                /api/kamino/aansluiting nemen nu een NULPUNT-baseline (pv=0 = enkel bestaande PV) en geven/kiezen op
//                `marginale_zelfconsumptie_pct` = (Δzelfverbruik)/(Δproductie) van de NIEUWE PV, i.p.v. het geblende %
//                (dat door bestaande PV te gunstig oogde). Zelfverbruik-drempel is nu een parameter (b.pvDrempel),
//                default 80% (was 90). Zonder bestaande PV: baseline 0 → marginaal == geblend (geen regressie).
// Versie:        v15.50.0 (31-07): /api/groeipad geeft per stap `afname_mwh` (grid-afname uit r.kpi.totaal_afname_mwh)
//                mee → de simulator berekent de loadfactor (KPI3) per groeistap client-side = afname/(8760×aansluiting),
//                zelfde conventie als _kpiEngine. Voordien enkel de optimale stap een loadfactor.
// Versie:        v15.49.0 (Kamino T3 /aansluiting: kern = VOLLEDIGE besparing vs vandaag (_kpiEngine-headline),
//                niet de groeipad-marginale (Johan-keuze A 28-07). NB: sinds het launcher-model rekent de
//                Kamino-kaart niet meer via dit endpoint — de simulator is de enige rekenmachine.)
// Versie:        v15.48.0 (Kamino T3: /api/kamino/aansluiting — async (job): optimale opstelling →
//                PV@90%-sweep → _draaiSim3 (batterij_gebouw) → groeistap 1, met exacte investeringsconstanten.)
// Versie:        v15.47.0 (Kamino T1 vergroening→0 (= simulator-factuuranalyse, €6.361) +
//                T2 /api/kamino/productie = _analyseerInjectieOptimalisatie (zelfde functie als de simulator).)
// Versie:        v15.51.0 (Manager-ondersteuning: /api/kamino/project-get (manager-only, zonder e-mail →
//                projectrecord + rapportenlijst met signed PDF-URLs) zodat de manager een project in de
//                simulator kan openen, bestaande rapporten bekijken en nieuwe scenario's maken.)
// Versie:        v15.50.0 (Kamino auto-rapport: /api/kamino/rapport-bewaar zet het rapportartefact na élke
//                tegel-berekening in de bucket (rapporten/<pid>/kamino-<tegel>.json) + /api/kamino/rapport-open
//                voor recall. Geen manuele PDF-upload meer nodig. + project-open matcht klant/adviseur (trim),
//                projectrecord bewaart PV (kWp+injectie).)
// Versie:        v15.46.0 (Kamino studie 1: /api/kamino/onderhandel — echte onderhandelingsmarge via
//                dezelfde buildSimInput → _runSimulatorOnce als /api/nominatie-sim, drift-vrij.)
// Versie:        v15.45.0 (Kamino-toegangspoort: /api/kamino/project (record bewaren) +
//                /api/kamino/project-open (project-ID + e-mail → record; e-mail moet matchen met
//                klant of adviseur). Zo heropenen klant + adviseur een project voor een volgende studie.
// Versie:        v15.44.0 (project-ID + rapport-opslag: /api/project-id (stabiel FLX-nummer per project),
//                          /api/rapport-opslaan (PDF → rapporten/<id>/ in de facturen-bucket + meta-JSON),
//                          /api/rapporten (lijst per project). Voor het ontwerp-rapport én de andere rapporten.)
// Versie:        v15.43.0 (bestaande PV fysiek in de LP-run: buildSimInput zet pv.kwp = nieuw + bestaand,
//                          injectie-cap += bestaande omvormer, capex enkel nieuw, en levert
//                          aanvullingen['pv_zelfverbruik'] (netto-afname gross-up op de zonvorm) →
//                          simulator.py v1.10.3. Facturatie neutraal; batterij ziet het bestaande surplus.)
// Versie:        v15.42.1 (SolarActive-heatmap-x-as = kalenderdag 1 jan→31 dec i.p.v. sim-index; bij een
//                          rolling-12-maand-venster viel de winter voorheen in het midden van de heatmap)
// Versie:        v15.42.0 (uniform _ijk-blok uit elke sim-engine — fase 1 imby-ijkinfrastructuur)
// Wijziging v15.42.0 vs v15.41.0: nominatie-sim-3, /api/opstelling én /api/injectie-optimalisatie geven
//   nu een identiek gestructureerd `_ijk`-blok terug (schema fluctus-ijk/1): engine, soort (kost/opbrengst),
//   input-signatuur, gebruikte parameters, niveaus (basis/sturing/onbalans/plafond) en meerwaarde. Puur
//   additief (geen LP-wijziging) — klaar om via de webhook per simulatie een paar (eigen, imby) te loggen.
// Versie:        v15.41.0 (injectie-optimalisatie = ECHTE predict-nominate-steer-simulatie + imby-ijkhaak)
// Wijziging v15.41.0 vs v15.40.0: eigen simulatie die voorspelt (day-ahead/onbalans + ruis), nomineert,
//   bijstuurt en afrekent op de gerealiseerde prijzen (seeded → reproduceerbaar). Rapporteert een
//   capture-aandeel van het theoretische plafond (perfecte vooruitzichten) met de forecast-simulatie als
//   ondergrens. Vrije parameters (sigma_da/imb/prod, thr_factor, capture, kalibratie) zijn ijkbaar via de
//   toekomstige imby-webhook zodat onze studies systematisch naar imby convergeren.
// Versie:        v15.40.0 (injectie-optimalisatie GEKALIBREERD op de imby SolarActive-methode)
// Wijziging v15.40.0 vs v15.39.3: SolarActive = fysieke productie-curtailment (niet de 1,8% batterij-
//   capture). Onbalans-meerwaarde = injectie gesetteld op de betere van day-ahead/onbalans × forecast-
//   efficiëntie (conservatief 0,60 / realistisch 0,80 / optimistisch 1,00). Investering €3.500,
//   beheerkost €6,6/kVA/jaar (imby-basis). Komt in dezelfde grootteorde als de imby-studies uit.
// Versie:        v15.39.3 (injectie-optimalisatie: schaal/payback-drempel — vanaf welke kVA een payback haalbaar is)
// Wijziging v15.39.3 vs v15.39.2: analyse geeft nu een `schaal`-blok. Meerwaarde én beheerkost schalen
//   met kVA; enkel als de meerwaarde > 8,64 €/kVA/jaar bestaat er een drempel-kVA voor 3/5/7 jaar payback.
// Versie:        v15.39.2 (injectie-optimalisatie: injectievolume = opgegeven jaarvolume, op de productievorm)
// Wijziging v15.39.2 vs v15.39.1: het gevaloriseerde injectievolume is nu het OPGEGEVEN jaarvolume
//   (van de factuur), verdeeld over de productievorm — i.p.v. een klein gereconstrueerd overschot bij
//   demand >> productie (dat gaf een ~10× te lage opbrengst). Zelfconsumptie = productie − injectie.
// Versie:        v15.39.1 (bestaande PV — injectie-optimalisatie SolarActive: /api/injectie-optimalisatie)
// Wijziging v15.39.1 vs v15.39.0: onbalans-meerwaarde via forecast-nominatie + capture-rate (0,018 ×
//   modus-multiplier 0,67/1,0/1,5), zelfde methode als de flex-nominatie — GEEN perfecte vooruitzichten.
// Wijziging v15.39.0 vs v15.38.0: nieuwe endpoint POST /api/injectie-optimalisatie + functie
//   _analyseerInjectieOptimalisatie (demand-reconstructie bestaande PV + 3-niveau injectiewaardering + heatmaps).
// Versie:        v15.38.0 (/api/pv-sweep geeft zelfconsumptie-splitsing + % t.o.v. productie per PV-stap)
// Wijziging v15.38.0 vs v15.37.0: /api/pv-sweep geeft per stap pv_direct_mwh, pv_via_batterij_mwh,
//   pv_injectie_mwh en zelfconsumptie_pct (= (direct + via batterij) / bruto productie) mee, zodat de
//   frontend een zelfconsumptie-%-kolom en het max-zelfconsumptie-scenario kan tonen.
// Versie:        v15.37.0 (POST /api/opstelling — één volledige opstelling op vaste config, voor "Herbereken groeistap 1")
// Wijziging v15.37.0 vs v15.36.0: nieuwe endpoint /api/opstelling draait de drie sturingen op een vaste
//   aansluiting + vast batterij-aantal en geeft één opstellings-object terug (varianten + kpi_sturing +
//   config + dimensionering), zodat de frontend "Groeistap 1" als extra opstelling naast Optimaal toont.
// Versie:        v15.36.0 (PV-suggestie — POST /api/pv-sweep: 5 PV-stappen 0→PVmax op de vaste instap-config)
// Wijziging v15.36.0 vs v15.35.0: nieuwe endpoint /api/pv-sweep. Sweept een lijst PV-vermogens (kWp) op
//   een VASTE aansluiting + VAST batterij-aantal (de instap/Optimaal-config) en geeft per stap factuur +
//   netkosten + injectie terug. De frontend toont de marginale PV-waarde (besparing t.o.v. 0 PV) en laat
//   de klant één PV-systeem kiezen dat in Optimaal én groeistap 1 wordt gebakken (via her-run).
// Versie:        v15.35.0 (Battery-only Kamino — gebouw zonder laadplein: batterij-sweep op bestaand verbruik)
// Wijziging v15.35.0 vs v15.34.0: een gebouw ZONDER laadplein krijgt nu ook een volwaardige
//   Kamino-analyse. _batterijSweepGebouw() sweept 1…Nmax batterijen (Nmax = ceil((toegangsvermogen+120)/120),
//   d.w.z. tot 120 kW batterijvermogen boven de bestaande aansluiting) op het bestaande verbruik
//   (piekshaving + arbitrage + PV-zelfconsumptie + onbalans), kiest de HOOGSTE NPV als Optimaal en geeft
//   het volledige groeipad terug (modus 'batterij_gebouw', groeipad_gebouw.alternatieven), met stap 1 =
//   aanbevolen instap. De aansluiting blijft vast (geen verzwaring/cabine).
// Versie:        v15.34.0 (Kamino-analyse óók als de laadvraag past — optie a: enkel de batterij-opstelling)
// Wijziging v15.34.0 vs v15.33.0: de `modus 'enkel'`-poort is gesplitst. Geen laadplein (of laadplein
//   zonder batterij) → 'enkel' zoals vroeger. Laadplein + aansluiting VOLDOENDE + batterij → nu
//   'twee_opstellingen' met ENKEL opstellingen.batterij (geen verzwaring nodig ⇒ Batterij = Optimaal),
//   zodat de volle Kamino-analyse (Vandaag vs Batterij + groeipad + KPI's) óók verschijnt als het past.
// Versie:        v15.33.0 (groeipad geeft factuur-componenten per stap → detailfactuur-vergelijking)
// Wijziging v15.33.0 vs v15.32.0: /api/groeipad geeft per stap factuur_detail mee (energie /
//   distributie+transport / capaciteit / heffingen / subtotaal, via _frCompJF) zodat de frontend een
//   gedetailleerde factuurvergelijking van een gekozen groeipad-stap vs. opstelling 0 kan tonen.
// Versie:        v15.32.0 (groeipad rekent op MEERDERE vaste aansluitingen — O2 én O3 — per stap)
// Wijziging v15.32.0 vs v15.31.0: /api/groeipad accepteert aansluitingen_kva[] en draait elke
//   batterijstap op ELKE meegegeven aansluiting (die van opstelling 2 = geen verzwaring én die van
//   opstelling 3 = verzwaard voor 100%). Elke stap krijgt aansluiting_kva mee terug; de frontend kiest
//   per stap de aansluiting met de hoogste NPV op de geleverde km. Back-compat: enkel aansluiting_kva.
// Versie:        v15.31.0 (KEUZEMAATSTAF = hoogste NPV @ cost of capital i.p.v. laagste TCO)
// Wijziging v15.31.0 vs v15.30.0: _dimensioneerMix kiest de mix nu op de HOOGSTE NPV (contante waarde
//   van de netto besparingen − investering, verdisconteerd aan input._tco.disconto). besparingNet =
//   base_net − netdeel per kandidaat. Terugval op laagste TCO als base_net/disconto ontbreken.
// Versie:        v15.30.0 (groeipad geeft distributie_eur per stap door → cumulatieve besparing in de frontend)
// Versie:        v15.29.0 (groeipad: aansluiting VAST — geen auto-verhoging, wél clip → echt % geladen)
// Wijziging v15.29.0 vs v15.28.1: /api/groeipad zet cfg.geen_aansluiting_verhoging=true en buildSimInput
//   geeft de vlag door aan simulator.py (v1.8.11), zodat de aansluiting op de vaste (volgroeide) waarde
//   blijft en de dispatch clipt i.p.v. de aansluiting te verhogen. Zo toont geladen_mwh per stap wat er
//   écht onder die aansluiting past (dalend % met minder batterijen).
// Versie:        v15.28.1 (mix-log rendement net van opex; keuze-TCO ongewijzigd)
// Wijziging v15.28.1 vs v15.28.0: het rendement in de mix-log is nu net van opex (onderhoud +
//   verzekering jaar 1), consistent met de frontend. De keuze blijft op de laagste TCO.
// Versie:        v15.28.0 (mix-TCO afgestemd op financieel rapport: onderhoud + verzekering + realistisch netkosten-schema)
// Wijziging v15.28.0 vs v15.27.0: de meerjarige TCO waarop _dimensioneerMix kiest is nu IDENTIEK
//   aan het financieel rapport (rapport_generator AAN-blok): capex + Σ(factuur_y + opex_y) over de
//   horizon, met de factuur gesplitst in niet-net (energie/heffingen, groeit met inflatie) en
//   netkosten (groeit met inflatie+net_extra, sprong vanaf net_sprong_jaar). Opex = onderhoud×inflatie
//   + verzekering (‰ capex), omvormer-vervanging in jaar 10. Parameters komen via input._tco uit de
//   frontend (realistisch scenario). Zonder _tco: terugval op de vlakke TCO (capex + jaarkost × 15).
// Wijziging v15.27.0 vs v15.26.0: _dimensioneerMix kiest de beste mix niet meer op het hoogste
//   rendement (dat verkoos altijd de kleinste batterij, waardoor opstelling 3 naar opstelling 1
//   samenviel) maar op de LAAGSTE TCO incl. cabine = capex (incl cabine bij MS) + jaarfactuur
//   sturing 2 × horizon (15 j). Onbalans blijft uit de keuze (kers op de taart, ná de selectie).
//   Elke kandidaat draagt nu ook tco + cabine mee in alternatieven (voor het groeipad).
// Wijziging v15.26.0 vs v15.25.0: elke mix-kandidaat (batterij-count-sweep) geeft nu ook
//   afname_mwh (voor loadfactor/KPI3) en distributie_eur (netkosten×2-blootstelling) mee in
//   alternatieven. De frontend toont daarmee het groeipad als een KPI1/2/3-evolutie over het
//   aantal batterijen — zonder extra sim-runs. Geen wijziging aan de keuzelogica.
// Versie:        v15.25.0 (opstelling 3 = batterij-count-sweep, keuze op KPI2/rendement)
// Wijziging v15.25.0 vs v15.24.0: _dimensioneerMix zoekt niet langer over verzwarings-fracties
//   maar over BATTERIJ-AANTALLEN 1..N. Per k batterijen zoekt _mixZoekVerzwaring de minimale
//   aansluiting die de volle laadvraag levert (sturing 2, GEEN onbalans). Keuze op hoogste
//   KPI2 = (E_base+CREG − factuur)/capex_excl_cabine; E_base+CREG en het vaste capex-deel komen
//   uit de frontend (_kpi_base_plus_creg / _kpi_capex_vast). Terugval op laagste factuur zonder
//   die basis. ⚠ raakt de dispatch-zoeklus — VEREIST een live smoke-test.
// Versie:        v15.24.0 (fix: opstelling 1 = puur verzwaren, batterij uit config gewist)
// Wijziging v15.24.0 vs v15.23.0: _opstellingUi('verhogen') wiste de batterij uit de
//   meegegeven config niet, waardoor opstelling 1 stiekem 'verzwaren + batterij' werd en een
//   onterecht goed rendement kreeg. Nu batterijId='' en batterijCustom=null in de verhoog-tak.
// Versie:        v15.23.0 (groeipad-sweep: POST /api/groeipad — batterij 1→N op vaste aansluiting)
// Wijziging v15.23.0 vs v15.22.0: nieuwe endpoint /api/groeipad voor blok 10 van het
//   resultaatscherm. Houdt de aansluiting vast op de optimale opstelling en draait de sim
//   voor 1..N batterijen (120/260), met per stap het % geleverde laadenergie + of de sim de
//   aansluiting zelf moest optrekken. ⚠ elke stap is een echte dispatch-run — VEREIST een
//   live smoke-test; geen wijziging aan bestaande endpoints.
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
const os          = require('os');
const factuurExtract = require('./factuur/extract');
const { projectJaarverbruik } = require('./project_jaarverbruik.js');

// ─── v15.59.0 — OPTIONELE PARALLELLE DISPATCH-RUNS (flag-gated, default = oud gedrag) ─────────
// De sim-3-zoektocht (mix-zoeklus k=1..N, batterij-sweep, groeipad/pv-sweep, de drie opstellingen)
// bestaat uit ONAFHANKELIJKE _runSimulatorOnce-spawns. Elke spawn is een verse, deterministische
// python-run die enkel z'n eigen stdin leest → ze parallel draaien geeft IDENTIEKE cijfers, enkel
// sneller. SIM_MAX_PARALLEL (env, default 1) is de globale bovengrens op gelijktijdige python-
// processen. Cap op #cores: oversubscriptie zou CBC z'n per-solve timeLimit doen missen
// (time-out → suboptimaal → cent-drift). SIM_MAX_PARALLEL=1 ⇒ alle _pmap-lussen serialiseren in
// submissievolgorde → byte-identiek aan de sequentiële versie. Rollback = env terug op 1 (of weg).
const SIM_MAX_PARALLEL = Math.max(1, Math.min(
  parseInt(process.env.SIM_MAX_PARALLEL || '1', 10) || 1,
  Math.max(1, (os.cpus() || [{}]).length)
));
let _simBezet = 0;
let _simPiek = 0;                 // v15.65.0: hoogste aantal gelijktijdige runs ooit bereikt (zichtbaar in GET /)
let _simRunsTotaal = 0;           // v15.65.0: totaal aantal spawns sinds start
const _simWachtrij = [];
function _simSlot() {
  return new Promise(res => {
    if (_simBezet < SIM_MAX_PARALLEL) { _simBezet++; _simRunsTotaal++; if (_simBezet > _simPiek) _simPiek = _simBezet; res(); }
    else _simWachtrij.push(res);
  });
}
function _simSlotVrij() {
  _simBezet--;
  const volgende = _simWachtrij.shift();
  if (volgende) { _simBezet++; _simRunsTotaal++; if (_simBezet > _simPiek) _simPiek = _simBezet; volgende(); }
}
// Bounded-parallel map met FIFO-slots. Bij SIM_MAX_PARALLEL=1 draait alles strikt op volgorde
// (identiek aan de oude for-await-lus); >1 laat tot N runs overlappen. Resultaat = oorspronkelijke
// itemvolgorde, zodat de assemblage downstream ongewijzigd blijft.
function _pmap(items, fn) {
  return Promise.all(items.map((it, i) => fn(it, i)));
}
// v15.65.0: altijd loggen (ook bij =1) zodat je in de Railway-logs ziet wat effectief actief is.
console.log(`[sim] parallelle dispatch: SIM_MAX_PARALLEL=${SIM_MAX_PARALLEL} · env=${process.env.SIM_MAX_PARALLEL || '(niet gezet → 1)'} · cores gerapporteerd door Node=${(os.cpus() || []).length} (let op: dit is meestal het HOST-aantal, niet je Railway-plan-quota)`);

const app  = express();
const PORT = process.env.PORT || 3000;

// ─── MIDDLEWARE ───────────────────────────────────────────────────────────────
app.use(compression());
app.use(express.json({ limit: '20mb' }));
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
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

// ─── HOOFDGEMEENTE-OVERRIDE (18-08) ─────────────────────────────────────────
// De bron-data labelt sommige postcodes met een DEELGEMEENTE i.p.v. de
// hoofdgemeente (bv. 3800 → "Aalst (Limb.)" i.p.v. "Sint-Truiden"). Deze
// override zet de correcte hoofdgemeente VOORAAN in de gemeenten-lijst, zodat
// de UI die als default toont. Deelgemeenten blijven als extra keuze staan.
// Additief: alleen herordenen/prepend, geen data verwijderd. Uitbreidbaar.
const HOOFDGEMEENTE_OVERRIDE = {
  '3800': 'Sint-Truiden',
};
for (const [pc, hoofd] of Object.entries(HOOFDGEMEENTE_OVERRIDE)) {
  const cur = PC_GEMEENTE_INDEX[pc] || [];
  PC_GEMEENTE_INDEX[pc] = [hoofd, ...cur.filter(g => g !== hoofd)];
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

// ─── v15.39: BESTAANDE PV — injectie-optimalisatie (SolarActive) ──────────────
// Zuiver JS, hergebruikt MARKT.solar_norm (900 kWh/kWp-vorm) + spot_q + imb_q + het
// verbruiksprofiel. Reconstrueert de gebouw-demand per kwartier uit afname + zelfconsumptie
// (= productie − injectie), leidt daaruit het injectieprofiel af en waardeert dat op drie
// niveaus: vandaag (spot), + curtailen bij negatieve spot, + nomineren/sturen op de
// onbalansmarkt (imbalance settlement, uitsluitend passieve respons — geen reservediensten).
const _MAAND_DAGEN_2025 = [0,31,59,90,120,151,181,212,243,273,304,334];
function _idx2025(d){
  const maand=d.getUTCMonth(), dag=d.getUTCDate()-1;
  const kwartier=Math.floor((d.getUTCHours()*60+d.getUTCMinutes())/15);
  return (_MAAND_DAGEN_2025[maand]+dag)*96+kwartier;
}
// Seeded PRNG (mulberry32) + Gaussiaanse ruis (Box-Muller) — reproduceerbaar (zelfde input → zelfde output).
function _mulberry32(a){ return function(){ a|=0; a=a+0x6D2B79F5|0; let t=Math.imul(a^a>>>15,1|a); t=t+Math.imul(t^t>>>7,61|t)^t; return ((t^t>>>14)>>>0)/4294967296; }; }
function _gauss(rng){ let u=0,v=0; while(u===0)u=rng(); while(v===0)v=rng(); return Math.sqrt(-2*Math.log(u))*Math.cos(2*Math.PI*v); }

// ─── v15.42: uniform IJK-blok (fase 1) ───────────────────────────────────────
// Identiek gestructureerde output uit ELKE sim-engine (batterij-BSP, opstelling, injectie), zodat we
// straks via de webhook per simulatie een paar (eigen output, imby output) kunnen loggen en de vrije
// parameters systematisch ijken. Puur ADDITIEF: raakt geen bestaande velden of de LP aan.
const SERVER_VERSIE = '15.121.0'; // v15.121.0 (31-08, Fase 5c): NBB-WINST — _bedrijfsWinst haalt de winst uit de neergelegde jaarrekening via de gratis NBB Authentic Data Query (references + accountingData, codes 9904/9903/9900/70, env NBB_CBSO_KEY); in de locatiescan (scan.financieel) + los testbaar via GET /api/bedrijfswinst?btw=. ── v15.120.0 (31-08, Fase 5c): KBO-ADAPTER — _scanKbo is nu een provider-tolerante CBE/KBO-REST-adapter (KBO_API=basis-URL + KBO_API_KEY=bearer), werkt met cbeapi.be én kbodata.app; leest NACE + ondernemingen-op-adres + bestuurders (indien geleverd), tolerant over veldvormen; zonder key/bron → null (heuristiek). ── v15.119.0 (31-08, Fase 5c): LEADS DUURZAAM — _leadOpslaan spiegelt naar Supabase-bucket (leads/<token>.json, fire-and-forget), _leadsHydrate laadt ze bij opstart gepagineerd in het geheugen (gated op SUPABASE_OK), /api/leads leest uit geheugen+lokale cache → warme leads en gemailde nota-links overleven een Railway-redeploy. Scans blijven bewust kortlevend. ── v15.118.0 (31-08, Fase 5b): HARDWARE-BRUG (/api/hardware-voorstel — KMO-batterijstaffel §14.8 + Jacops-palen/PV → shoppinglist + payback), DESTINATION-LUIK spoor 2 (/api/destination-raming — capture/dwell §12.3, drempel=functie kostprijs §13.1, sessieraming), VISION-PASS fase 2 (locatiescan: Claude-vision op de Mapbox-tile → panelen/parkeervakken, gated + kruiscontrole factuur). ── v15.117.0 (31-08, Fase 5): LOCATIESCAN — async POST/GET /api/locatiescan (pluggable bronnen: Mapbox-luchtfoto/geocode, GRB-dak, KBO/NACE, Places, OpenChargeMap, Fluvius-cabines LS/MS), niet-blokkerend + graceful degradation. Lead-scoring: groeistap_aanvaard +28 (§14.6), scan-engagement +8. ── v15.116.0 (31-08, Fase 4): VOORSCHOTFACTUUR — /api/lead neemt factuur_type ('voorschot'|'afrekening'), lead-scoring dempt de marge-bijdrage bij voorschot (raming, niet kunstmatig warm), /api/leads geeft factuur_type mee. Detectie zelf zit in factuur/extract.js v1.4.7 (is_voorschot). ── v15.115.0 (31-08, Fase 4): SELF-SERVICE MANDAAT-INTAKE — POST /api/mandaat/self-aanvraag (geverifieerde lead → EAN in losse wachtrij met aanvrager+factuuradres), GET /api/mandaat/self-status, POST /api/mandaat/self-bevestig-adres (lead-variant adres-mismatch). wachtrij/sync dragen nu aanvrager/factuur_adres/aangevraagd_via/kwartierdata_aanwezig. LET OP: gelijk houden aan de Versie-header.
function _bouwIjk(engine, soort, input, parameters, niveaus){
  // soort: 'kost' (lager = beter, batterij/opstelling) of 'opbrengst' (hoger = beter, injectie).
  const n = niveaus || {};
  const basis = +n.basis||0, sturing = +n.sturing||0, onbalans = +n.onbalans||0;
  let mSturing, mOnbalans, mTotaal;
  if(soort==='kost'){ mSturing = basis - sturing; mOnbalans = sturing - onbalans; mTotaal = basis - onbalans; }
  else            { mSturing = sturing - basis; mOnbalans = onbalans - sturing; mTotaal = onbalans - basis; }
  return {
    schema: 'fluctus-ijk/1', engine: engine, soort: soort, versie: SERVER_VERSIE, ts: new Date().toISOString(),
    input: input||{}, parameters: parameters||{},
    niveaus_eur: { basis:Math.round(basis), sturing:Math.round(sturing), onbalans:Math.round(onbalans),
                   plafond: (n.plafond!=null?Math.round(n.plafond):null) },
    meerwaarde_eur: { sturing:Math.round(mSturing), onbalans:Math.round(mOnbalans), totaal:Math.round(mTotaal) }
  };
}
// Verrijkt een nominatie-sim-3-resultaat met een _ijk-blok (batterij-BSP). Additief; faalt stil.
function _verrijkIjk(r, input){
  try{
    if(!r || typeof r!=='object') return r;
    input = input || {};
    let kpi = r.kpi_sturing, perOpst = null;
    if(!kpi && r.opstellingen){
      perOpst = {};
      Object.keys(r.opstellingen).forEach(function(k){ const o=r.opstellingen[k]; if(o&&o.kpi_sturing){
        perOpst[k] = { basis:Math.round(o.kpi_sturing.kost_geen_excl_btw||0),
                       sturing:Math.round(o.kpi_sturing.kost_sturing_excl_btw||0),
                       onbalans:Math.round(o.kpi_sturing.kost_onbalans_excl_btw||0) }; } });
      const prim = r.opstellingen.batterij || r.opstellingen.mix || r.opstellingen.verhogen
                   || r.opstellingen[Object.keys(r.opstellingen)[0]];
      kpi = prim && prim.kpi_sturing;
    }
    const niveaus = kpi
      ? { basis:kpi.kost_geen_excl_btw, sturing:kpi.kost_sturing_excl_btw, onbalans:kpi.kost_onbalans_excl_btw, plafond:null }
      : { basis:0, sturing:0, onbalans:0, plafond:null };
    let lpEnergie=0; (input.laadpleinen||[]).forEach(function(p){ (p.laadpunten||[]).forEach(function(x){}); });
    const inSig = {
      aansluiting_kva: +(input.aansluiting_kva||input.aansluitingKva||0)||null,
      batterij_kwh: (input.batterijCustom && +input.batterijCustom.kwh)||null,
      pv_kwp: +(input.pv_kwp||input.pvKwp||0)||null,
      profiel: input.profielNaam||input.profiel_naam||null,
      jaarverbruik_mwh: +(input.jaarverbruik||input.jaarverbruik_mwh||0)||null,
      laadpleinen: (input.laadpleinen||[]).length,
      modus: r.modus||null,
    };
    const params = { paper_capture_rate:0.018,
                     forecast_modus:(input.bsp && input.bsp.forecast_modus) || 'realistic',
                     kalibratie:1.0 };
    r._ijk = _bouwIjk('batterij-bsp','kost', inSig, params, niveaus);
    if(perOpst) r._ijk.per_opstelling = perOpst;
  }catch(e){ /* stil: ijk is additief, mag nooit de sim breken */ }
  return r;
}
function _analyseerInjectieOptimalisatie(MARKT, p){
  if(!MARKT || !Array.isArray(MARKT.spot_q) || !MARKT.spot_q.length) throw new Error('geen marktdata');
  const spot = MARKT.spot_q;
  const imb  = (MARKT.imb_q && MARKT.imb_q.length===spot.length) ? MARKT.imb_q : spot;
  const N = spot.length;
  const solar = (MARKT.solar_norm && MARKT.solar_norm.length===35040) ? MARKT.solar_norm : null;
  const profielRaw = (Array.isArray(p.profiel_kwartier)&&p.profiel_kwartier.length===35040) ? p.profiel_kwartier
                    : (MARKT.profiel && MARKT.profiel.length===35040 ? MARKT.profiel : null);
  // kWp / kVA — punt 3: piekvermogen (kW) op de injectiefactuur → kVA-omvormer; kWp ≈ 1,3 × kVA.
  let kwp = +p.pv_kwp||0;
  let kva = +p.inverter_kva||0;
  const piek = +p.piek_kw||0;
  if(piek>0){ kva = piek; if(kwp<=0) kwp = 1.3*piek; }
  if(kwp<=0 && kva>0) kwp = 1.3*kva;
  if(kva<=0 && kwp>0) kva = kwp/1.3;
  if(kwp<=0) throw new Error('geen PV-vermogen (kWp of kVA)');
  const YIELD = 900; // kWh/kWp/jaar (Johan)

  // vorm projecteren op de MARKT-timeline (seizoen uitgelijnd met spot/imb)
  const van = new Date(MARKT.van + 'T00:00:00Z');
  const solarFrac = new Array(N), profFrac = new Array(N), maandVan = new Array(N), dagVanJaar = new Array(N);
  let sSum=0, pSum=0;
  for(let i=0;i<N;i++){
    const d = new Date(van.getTime()+i*15*60*1000);
    const idx = _idx2025(d);
    const sv = solar ? (idx>=0&&idx<solar.length?solar[idx]:0) : 0;
    const pv = profielRaw ? (idx>=0&&idx<profielRaw.length?profielRaw[idx]:0) : 0;
    solarFrac[i]=sv; profFrac[i]=pv; maandVan[i]=d.getUTCMonth()+1; sSum+=sv; pSum+=pv;
    // kalender-dag-van-het-jaar (0..364) — de heatmap-x-as loopt zo altijd 1 jan → 31 dec,
    // ongeacht of de sim-periode een rolling-12-maand-venster is (anders viel de winter in het midden).
    const yStart = Date.UTC(d.getUTCFullYear(),0,1);
    dagVanJaar[i] = Math.min(364, Math.max(0, Math.floor((d.getTime()-yStart)/86400000)));
  }
  if(sSum>0) for(let i=0;i<N;i++) solarFrac[i]/=sSum;
  if(pSum>0) for(let i=0;i<N;i++) profFrac[i]/=pSum;

  const periodeJaarFractie = N/35040;
  const productiePeriode = YIELD*kwp*periodeJaarFractie;   // kWh over de periode
  const afnameJaarMwh = +p.afname_mwh_jaar||0;
  const afnamePeriodeKwh = afnameJaarMwh*1000*periodeJaarFractie;

  // injectie-totaal (kWh) over de periode
  let injGegevenPeriode = 0;
  if(+p.injectie_mwh_jaar>0){
    injGegevenPeriode = (+p.injectie_mwh_jaar)*1000*periodeJaarFractie;
  } else if(+p.injectie_mwh_maand>0){
    // extrapoleren maand→jaar via het aandeel van die maand in de zon (injectie volgt productie)
    const m = Math.min(12, Math.max(1, +p.injectie_maand||0));
    let maandSolar=0; for(let i=0;i<N;i++) if(maandVan[i]===m) maandSolar+=solarFrac[i];
    const injJaar = maandSolar>0 ? (+p.injectie_mwh_maand)*1000/maandSolar : (+p.injectie_mwh_maand)*1000*12;
    injGegevenPeriode = injJaar*periodeJaarFractie;
  }
  // Injectieprofiel: we VERTROUWEN de opgegeven injectie (factuur) als jaarvolume en verdelen ze over de
  // PRODUCTIEVORM (injectie gebeurt tijdens productie, met het zwaartepunt rond de middag — net waar de
  // spot negatief kan zijn en de onbalans-kans zit). Zo blijft het gevaloriseerde injectievolume gelijk
  // aan wat de klant écht injecteert, i.p.v. een klein gereconstrueerd overschot wanneer de demand (bv.
  // 200 MWh) veel groter is dan de productie (bv. 27 MWh) — dat gaf voorheen een 10× te lage opbrengst.
  let prodTot=0; const prodArr=new Array(N);
  for(let i=0;i<N;i++){ const prod=productiePeriode*solarFrac[i]; prodArr[i]=prod; prodTot+=prod; }
  // ── Fase 4 (v15.69): opgeladen INJECTIE-profiel → waardering (curtailment/onbalans) op de ECHTE gemeten
  // injectievorm i.p.v. de gemodelleerde zonvorm. Fallback = ONGEWIJZIGD (zonvorm) bij ontbrekend/ongeldig
  // profiel. Uitgelijnd op de MARKT-timeline via _idx2025 (identiek aan solar/prof), genormaliseerd som=1.
  const injProfiel = (Array.isArray(p.injectie_kwartier)&&p.injectie_kwartier.length===35040)?p.injectie_kwartier:null;
  let injRealFrac=null;
  if(injProfiel){
    injRealFrac=new Array(N); let iSum=0;
    for(let i=0;i<N;i++){ const d=new Date(van.getTime()+i*15*60*1000); const idx=_idx2025(d); const v=(idx>=0&&idx<injProfiel.length)?(+injProfiel[idx]||0):0; injRealFrac[i]=v; iSum+=v; }
    if(iSum>0){ for(let i=0;i<N;i++) injRealFrac[i]/=iSum; } else { injRealFrac=null; }
  }
  if(injRealFrac && injGegevenPeriode<=0 && +p.injectie_profiel_mwh>0){
    injGegevenPeriode = (+p.injectie_profiel_mwh)*1000*periodeJaarFractie;   // geen factuurvolume → val terug op het gemeten profielvolume
  }
  const _injBron = injRealFrac ? 'opgeladen_injectie' : 'gemodelleerd';
  const injFrac = prodTot>0 ? Math.min(1, injGegevenPeriode/prodTot) : 0;   // aandeel van de productie dat geïnjecteerd wordt (fallback-pad)
  let injTotReco=0, demTot=0;
  const inj = new Array(N);
  for(let i=0;i<N;i++){
    inj[i] = injRealFrac ? (injGegevenPeriode*injRealFrac[i])   // Fase 4: ECHTE gemeten vorm, geschaald op het opgegeven jaarvolume
                         : (prodArr[i]*injFrac);                 // fallback: injectie ∝ gemodelleerde productie (ongewijzigd)
    const selfc = Math.max(0, prodArr[i]-inj[i]);              // zelfconsumptie dat kwartier (≥0; no-op voor het fallback-pad)
    const dem = afnamePeriodeKwh*profFrac[i] + selfc;          // gebouw-demand = afname + zelfconsumptie
    injTotReco+=inj[i]; demTot+=dem;
  }
  const zelfconsumptie = Math.max(0, prodTot - injTotReco);   // = productie − injectie (voor rapportage)

  // ── SIMULATIE: VOORSPELLEN → NOMINEREN → BIJSTUREN → AFREKENEN (v15.41) ──
  // Een echte predict-nominate-steer-lus (geen vaste efficiëntie-haircut meer). De beslissingen worden op
  // FORECASTS genomen (= gerealiseerde prijs/productie + Gaussiaanse ruis), maar de afrekening gebeurt op de
  // GEREALISEERDE prijzen. De forecast-fout bepaalt zo organisch hoeveel van de theoretische waarde je
  // effectief capteert: een slechtere forecast → soms fout gegokt → minder opbrengst. De forecast-modus
  // schaalt de ruis (skill). Seeded PRNG → reproduceerbaar. Een kalibratiefactor (default 1,0) laat toe om
  // later, met de imby-webhookparen, systematisch naar imby te convergeren.
  //   1) VANDAAG: alles injecteren aan de day-ahead (geen sturing) — kan negatief bij negatieve DA.
  //   2) + CURTAILEN: geen injectie bij negatieve day-ahead (die is de dag vooraf ZEKER gekend).
  //   3) + NOMINEREN/BIJSTUREN: dag vooraf nomineren op de forecast, real-time bijsturen op de
  //      onbalans-forecast, en afrekenen op de gerealiseerde prijzen (nominatie aan DA + deviatie aan onbalans).
  // Vrije parameters (ijkbaar via de imby-webhook): forecast-ruis per modus, handeldrempel en kalibratie.
  const SKILL = { conservatief:1.5, realistic:1.0, optimistisch:0.6 };   // ruis-schaal (hogere skill = lagere ruis)
  const modus = (p.forecast_modus && SKILL[p.forecast_modus]) ? p.forecast_modus : 'realistic';
  const kf = SKILL[modus];
  const SIG_DA   = (p.sigma_da   != null ? +p.sigma_da   : 12)  * kf;    // €/MWh, day-ahead forecast-fout
  const SIG_IMB  = (p.sigma_imb  != null ? +p.sigma_imb  : 45)  * kf;    // €/MWh, onbalans forecast-fout (moeilijker)
  const SIG_PROD = (p.sigma_prod != null ? +p.sigma_prod : 0.15)* kf;    // relatieve productie-forecast-fout
  const thrFactor= (p.thr_factor != null && +p.thr_factor>0) ? +p.thr_factor : 1.3;   // handeldrempel × σ_imb
  const kalibratie = (p.kalibratie != null && +p.kalibratie>0) ? +p.kalibratie : 1.0; // lineaire imby-ijking
  // Capture-ratio: welk aandeel van het theoretische onbalans-plafond (perfecte vooruitzichten) we in de
  // praktijk als haalbaar rapporteren. De echte predict-nominate-steer-simulatie draait ernaast en toont
  // wat een forecast-gedreven operator ZEKER haalt (ondergrens); imby rapporteert dicht bij het plafond.
  // Dit is de vrije parameter die de imby-webhook per modus zal bijstellen tot onze studies imby benaderen.
  const CAP = { conservatief:0.55, realistic:0.72, optimistisch:0.90 };
  const capture = (p.capture != null && +p.capture>=0) ? +p.capture : (CAP[modus] || 0.72);
  const rng = _mulberry32(42);

  let euroBaseline=0, euroCurtail=0, euroBeide=0, euroPotentie=0, simOnbSum=0;
  const HR = 24, DG = 365;   // heatmap-x-as = kalenderdag (1 jan → 31 dec), niet de sim-index
  const hm1 = new Array(HR*DG).fill(0);   // netto injectieopbrengst €/kwartier (zonder sturing)
  const hm2 = new Array(HR*DG).fill(0);   // meerwaarde curtailment €/kwartier
  const hm3 = new Array(HR*DG).fill(0);   // meerwaarde onbalans-sturing €/kwartier
  const thr = thrFactor*SIG_IMB;                              // handeldrempel > forecast-onzekerheid (voorzichtig)
  for(let i=0;i<N;i++){
    const avail = inj[i]; if(avail<=0){ continue; }           // werkelijk beschikbare injectie (kWh)
    const da = spot[i], ib = imb[i];                          // GEREALISEERDE prijzen (€/MWh)
    // forecasts (= beslissingsbasis) = gerealiseerd + Gaussiaanse ruis
    const daFc    = da + _gauss(rng)*SIG_DA;
    const ibFc    = ib + _gauss(rng)*SIG_IMB;
    const availFc = Math.max(0, avail*(1 + _gauss(rng)*SIG_PROD));
    // 1) VANDAAG
    const eBase = avail*da/1000;
    // 2) CURTAILEN (op de zekere day-ahead)
    const eCurt = (da>=0 ? avail*da : 0)/1000;
    // 3) NOMINEREN (day-ahead, op forecast) + BIJSTUREN (real-time, op onbalans-forecast), settlement op realisatie.
    //    Een rationele operator deviaeert ALLEEN bij een onbalans-signaal dat de forecast-onzekerheid
    //    overstijgt (|ibFc| > drempel) — anders handel je op ruis en verlies je. Bij onzekerheid blijf je bij
    //    de nominatie. iAct is fysiek begrensd op de beschikbare injectie.
    const iNom = daFc>=0 ? availFc : 0;                       // dag vooraf genomineerde injectie
    let iAct;
    if(ibFc >  thr) iAct = avail;                             // sterk positieve onbalans verwacht → maximaal injecteren
    else if(ibFc < -thr) iAct = 0;                            // sterk negatieve onbalans verwacht → curtailen
    else iAct = Math.min(iNom, avail);                        // onzeker → bij de nominatie blijven (fysiek begrensd)
    const eBeide = (iNom*da + (iAct - iNom)*ib)/1000;         // nominatie aan DA + deviatie aan onbalans (gerealiseerd)
    // theoretisch plafond (perfecte vooruitzichten): settle elke MWh aan de betere van DA/onbalans
    const ePot = avail*Math.max(0, Math.max(da,ib))/1000;
    euroBaseline+=eBase; euroCurtail+=eCurt; euroBeide+=eBeide; euroPotentie+=ePot;
    simOnbSum += (eBeide-eCurt);                              // wat de forecast-simulatie ZEKER haalt (diagnostiek)
    const dag=dagVanJaar[i], uur=Math.floor((i%96)/4), cel=uur*DG+dag;
    // heatmap-3 = het (gekalibreerde) onbalans-plafond per kwartier; consistent met de gerapporteerde meerwaarde.
    if(cel>=0&&cel<hm1.length){ hm1[cel]+=eBase; hm2[cel]+=(eCurt-eBase); hm3[cel]+=Math.max(0,(ePot-eCurt)); }
  }
  // RAW niveaus (loop-sommen, périodebedragen):
  const rawBaseline = euroBaseline;
  const rawCurtail  = euroCurtail;                            // curtail-niveau
  const rawOnbPot   = Math.max(0, euroPotentie - rawCurtail); // onbalans-plafond (perfecte vooruitzichten)
  const rawOnbSim   = Math.max(0, simOnbSum);                 // wat de forecast-simulatie zeker haalt (ondergrens)
  // Curtailment = zekere day-ahead-actie (× kalibratie). Onbalans-meerwaarde = capture-aandeel van het
  // plafond (× kalibratie), met een ondergrens op de forecast-simulatie. De heatmap wordt mee geschaald.
  const bespaardCurtail  = (rawCurtail - rawBaseline) * kalibratie;
  const rawOnb           = Math.max(rawOnbSim, capture * rawOnbPot);
  const verdiendOnbalans = rawOnb * kalibratie;
  const hm3Scale = rawOnbPot>0 ? (verdiendOnbalans/rawOnbPot) : 0;   // schaal de plafond-heatmap naar de gerapporteerde meerwaarde
  for(let k=0;k<hm2.length;k++){ hm2[k]*=kalibratie; }
  for(let k=0;k<hm3.length;k++){ hm3[k]*=hm3Scale; }
  euroBaseline = rawBaseline;
  euroCurtail  = rawBaseline + bespaardCurtail;
  euroBeide    = euroCurtail + verdiendOnbalans;
  const jaarFx = periodeJaarFractie>0 ? 1/periodeJaarFractie : 1;
  const onbalansPotentieJaar = rawOnbPot * jaarFx;            // plafond (referentie/ijking)
  const onbalansSimJaar      = rawOnbSim * jaarFx;            // forecast-ondergrens (diagnostiek)
  // opschalen naar vol jaar voor de kerncijfers
  const jaarF = periodeJaarFractie>0 ? 1/periodeJaarFractie : 1;
  const baselineJaar = euroBaseline*jaarF;
  const curtailJaar  = euroCurtail*jaarF;
  const beideJaar    = euroBeide*jaarF;
  const meerCurtailJaar = bespaardCurtail*jaarF;
  const meerOnbalansJaar= verdiendOnbalans*jaarF;

  // payback: eenmalige gateway-investering €3.500 + beheerkost €6,6/kVA/jaar (imby SolarActive-basis;
  // ≈ €0,55/kVA/maand — de effectieve kost uit de imby-studies, waar 0,72 €/kVA/maand nominaal ~0,55
  // aangerekend wordt).
  const INVEST=3500, MGMT_KVA_JAAR=6.6;
  const mgmtJaar = MGMT_KVA_JAAR*kva;
  const nettoCurtail = meerCurtailJaar - mgmtJaar;
  const nettoBeide   = (meerCurtailJaar+meerOnbalansJaar) - mgmtJaar;
  const paybackCurtailJaar = nettoCurtail>0 ? INVEST/nettoCurtail : null;
  const paybackBeideJaar   = nettoBeide>0   ? INVEST/nettoBeide   : null;

  // ── schaal: vanaf welke systeemgrootte een payback haalbaar is ──
  // Zowel de meerwaarde (curtail + onbalans) ALS de beheerkost schalen ~lineair met kVA. Netto per kVA =
  // meerwaarde/kVA − 6,6 €/kVA/jaar. Enkel als dat POSITIEF is helpt groeien: dan spreidt de vaste €3.500
  // zich uit en bestaat er een drempel-kVA voor 3/5/7 jaar payback. Is netto/kVA ≤ 0, dan is er bij GEEN
  // enkele grootte een payback (groter = evenredig meer beheerkost).
  const mgmtPerKva = MGMT_KVA_JAAR;                          // 6,6 €/kVA/jaar (imby SolarActive-basis)
  const meerTotJaar = meerCurtailJaar + meerOnbalansJaar;
  const meerPerKva = kva>0 ? meerTotJaar/kva : 0;
  const nettoPerKva = meerPerKva - mgmtPerKva;
  function drempelKva(jaren){ return nettoPerKva>0 ? Math.ceil((INVEST/jaren)/nettoPerKva) : null; }
  function drempelKwp(jaren){ const k=drempelKva(jaren); return k!=null ? Math.round(k*1.3) : null; }

  return {
    invoer:{ pv_kwp:Math.round(kwp*10)/10, inverter_kva:Math.round(kva*10)/10, piek_kw:piek||null,
             afname_mwh_jaar:afnameJaarMwh, injectie_gegeven_mwh_periode:Math.round(injGegevenPeriode/100)/10,
             injectie_bron:_injBron },
    periode:{ van:MARKT.van, tot:MARKT.tot, n_kwartieren:N, jaar_fractie:Math.round(periodeJaarFractie*1000)/1000 },
    sturing:{ forecast_modus:modus, methode:'predict-nominate-steer (seeded) + capture van het plafond',
              sigma_da_eur_mwh:Math.round(SIG_DA*10)/10, sigma_imb_eur_mwh:Math.round(SIG_IMB*10)/10,
              sigma_prod_pct:Math.round(SIG_PROD*100), thr_factor:thrFactor, capture:capture, kalibratie:kalibratie,
              onbalans_gerapporteerd_eur:Math.round(meerOnbalansJaar), onbalans_gesimuleerd_eur:Math.round(onbalansSimJaar),
              onbalans_potentie_eur:Math.round(onbalansPotentieJaar) },
    schaal:{ meerwaarde_per_kva_eur:Math.round(meerPerKva*100)/100, mgmt_per_kva_eur:mgmtPerKva,
             netto_per_kva_eur:Math.round(nettoPerKva*100)/100, haalbaar:nettoPerKva>0,
             drempel_kva_3jaar:drempelKva(3), drempel_kwp_3jaar:drempelKwp(3),
             drempel_kva_5jaar:drempelKva(5), drempel_kwp_5jaar:drempelKwp(5),
             drempel_kva_7jaar:drempelKva(7), drempel_kwp_7jaar:drempelKwp(7) },
    energie_jaar:{ productie_mwh:Math.round(YIELD*kwp/1000*10)/10,
                   zelfconsumptie_mwh:Math.round(zelfconsumptie*jaarF/1000*10)/10,
                   injectie_mwh:Math.round(injTotReco*jaarF/1000*10)/10,
                   demand_mwh:Math.round(demTot*jaarF/1000*10)/10,
                   afname_mwh:afnameJaarMwh },
    opbrengst_jaar:{ vandaag_spot_eur:Math.round(baselineJaar),
                     met_curtail_eur:Math.round(curtailJaar),
                     met_curtail_onbalans_eur:Math.round(beideJaar),
                     meerwaarde_curtail_eur:Math.round(meerCurtailJaar),
                     meerwaarde_onbalans_eur:Math.round(meerOnbalansJaar),
                     meerwaarde_totaal_eur:Math.round(meerCurtailJaar+meerOnbalansJaar) },
    payback:{ investering_eur:INVEST, management_eur_per_kva_jaar:MGMT_KVA_JAAR, management_jaar_eur:Math.round(mgmtJaar),
              netto_curtail_jaar_eur:Math.round(nettoCurtail), netto_beide_jaar_eur:Math.round(nettoBeide),
              payback_curtail_jaar:paybackCurtailJaar!=null?Math.round(paybackCurtailJaar*10)/10:null,
              payback_beide_jaar:paybackBeideJaar!=null?Math.round(paybackBeideJaar*10)/10:null },
    heatmaps:{ uren:HR, dagen:DG,
               netto_injectie_eur:hm1.map(v=>Math.round(v*100)/100),
               bespaard_curtail_eur:hm2.map(v=>Math.round(v*100)/100),
               verdiend_onbalans_eur:hm3.map(v=>Math.round(v*100)/100) }
  };
}

// ─── CONTRACT-HEATMAPS (Kamino-tegel 1 / EnergieKompas onderhandelingsnota) ──────
// Drie 24×365-kalenderheatmaps voor "huidige situatie" uit het standaard/opgeladen afnameprofiel
// × de marktdata: (1) afname-profiel (kW gem./uur), (2) dynamische prijs (spot €/MWh), (3) afnamekost
// (afname × spot, € dat uur = waar je geld verliest/wint). Zelfde vorm/index als de injectie-heatmaps
// hierboven (platte array, cel = uur*365 + dag, uur 0-23 · dag 0-364 = 1 jan→31 dec). Render = heatmapSvg.
function _contractHeatmaps(MARKT, profielNaam, volumeMwh){
  if(!MARKT || !Array.isArray(MARKT.spot_q) || !MARKT.spot_q.length) return null;
  const spot = MARKT.spot_q, N = spot.length;
  const pk = _laadProfielKwartier(profielNaam);
  if(!(pk && pk.length===35040)) return null;
  const van = new Date(MARKT.van + 'T00:00:00Z');
  const HR=24, DG=365, LEN=HR*DG;
  // afnameprofiel-fracties op de MARKT-timeline, genormaliseerd (som=1 over de window)
  const pf = new Array(N); let pSum=0;
  for(let i=0;i<N;i++){ const d=new Date(van.getTime()+i*15*60*1000); const idx=_idx2025(d);
    const v=(idx>=0&&idx<pk.length)?(+pk[idx]||0):0; pf[i]=v; pSum+=v; }
  if(pSum>0) for(let i=0;i<N;i++) pf[i]/=pSum;
  const volKwh = (+volumeMwh||0)*1000;
  const afnKwh = new Array(LEN).fill(0), spSum = new Array(LEN).fill(0), spCnt = new Array(LEN).fill(0);
  for(let i=0;i<N;i++){
    const d=new Date(van.getTime()+i*15*60*1000);
    const yStart=Date.UTC(d.getUTCFullYear(),0,1);
    const dag=Math.min(364,Math.max(0,Math.floor((d.getTime()-yStart)/86400000)));
    const uur=Math.floor((i%96)/4), cel=uur*DG+dag;
    afnKwh[cel]+=pf[i]*volKwh;   // kWh dat kwartier → som over het uur ≈ kW gem.
    spSum[cel]+=spot[i]; spCnt[cel]++;
  }
  const afname_kw=new Array(LEN), spot_eur_mwh=new Array(LEN), afnamekost_eur=new Array(LEN);
  for(let c=0;c<LEN;c++){
    const sp = spCnt[c]>0 ? spSum[c]/spCnt[c] : 0;
    afname_kw[c]=Math.round(afnKwh[c]*10)/10;                   // ≈ kW gem. dat uur
    spot_eur_mwh[c]=Math.round(sp*10)/10;                       // €/MWh gem. dat uur
    afnamekost_eur[c]=Math.round(afnKwh[c]*sp/1000*100)/100;    // € dat uur (kWh × €/MWh /1000)
  }
  return { uren:HR, dagen:DG, afname_kw, spot_eur_mwh, afnamekost_eur };
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
// Academy-backend (fluctus-academy-backend) voor de uitnodigingsmail (Brevo).
// De proxy maakt zelf het profiel aan (service-role) en TRIGGERT enkel de mail
// bij de academy; Brevo-config blijft ongewijzigd op de academy-backend.
// Zonder deze env wordt het profiel wél aangemaakt maar de mail overgeslagen.
const ACADEMY_BACKEND_URL  = (process.env.ACADEMY_BACKEND_URL || '').replace(/\/+$/, '');

// ─── v15.62.0 — VERKOPER VERWIJDEREN + AUDIT-EXPORT PER MAIL ──────────────────────────────────
// Bij het verwijderen van een gebruiker verzamelen we eerst al z'n gelogde info (profiel + app-
// toegangen + activiteitslog + certificaten) in een .txt en mailen die naar de audit-mailbox, met
// als onderwerp "<naam> verwijderd op YYYYMMDD uu:mm". Mail loopt via Brevo (transactioneel), gated
// op BREVO_API_KEY op de proxy; zonder key wordt de mail overgeslagen (de UI downloadt de .txt dan
// als vangnet). AUDIT_MAIL_TO default oekene@gmail.com; BREVO_SENDER = een in Brevo geverifieerde
// afzender (zelfde als de Academy gebruikt).
const BREVO_API_KEY = process.env.BREVO_API_KEY || '';
const BREVO_SENDER  = process.env.BREVO_SENDER  || 'no-reply@fluctus.net';
const AUDIT_MAIL_TO = process.env.AUDIT_MAIL_TO || 'oekene@gmail.com';
function _nuBrussel() {
  const d = new Date();
  try {
    const fmt = new Intl.DateTimeFormat('nl-BE', { timeZone: 'Europe/Brussels',
      year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false });
    const p = {}; for (const x of fmt.formatToParts(d)) p[x.type] = x.value;
    return { stempel: `${p.year}${p.month}${p.day} ${p.hour}:${p.minute}`, iso: d.toISOString() };
  } catch (e) {
    const iso = d.toISOString();
    return { stempel: iso.slice(0, 16).replace('T', ' ').replace(/-/g, '').replace(' ', ' '), iso };
  }
}
async function _verzendAuditMail(onderwerp, txtInhoud, bestandsnaam) {
  if (!BREVO_API_KEY) return { sent: false, reden: 'BREVO_API_KEY niet gezet op de proxy' };
  try {
    const r = await fetch('https://api.brevo.com/v3/smtp/email', {
      method: 'POST',
      headers: { 'api-key': BREVO_API_KEY, 'Content-Type': 'application/json', 'accept': 'application/json' },
      body: JSON.stringify({
        sender: { name: 'Fluctus Gebruikersbeheer', email: BREVO_SENDER },
        to: [{ email: AUDIT_MAIL_TO }],
        subject: onderwerp,
        textContent: `Bijgevoegd de volledige export van de verwijderde gebruiker.\n\n${onderwerp}`,
        attachment: [{ content: Buffer.from(txtInhoud, 'utf8').toString('base64'), name: bestandsnaam }],
      }),
    });
    if (!r.ok) { const t = await r.text(); return { sent: false, reden: `Brevo HTTP ${r.status} ${t.slice(0, 200)}` }; }
    return { sent: true, naar: AUDIT_MAIL_TO };
  } catch (e) { return { sent: false, reden: e.message }; }
}
function _bouwVerwijderExport(prof, authUid, grants, logs, certs, nu, mgr, herlink) {
  const L = [];
  L.push('FLUCTUS — EXPORT BIJ VERWIJDERING VAN GEBRUIKER');
  L.push('='.repeat(62));
  L.push(`Verwijderd op   : ${nu.stempel} (Europe/Brussels)  [${nu.iso}]`);
  L.push(`Uitgevoerd door : ${(mgr && (mgr.email || mgr.id)) || '?'}`);
  L.push('');
  L.push('PROFIEL'); L.push('-'.repeat(62));
  L.push(`E-mail       : ${prof.email || ''}`);
  L.push(`Naam         : ${prof.name || prof.naam || prof.full_name || ''}`);
  L.push(`Bedrijf      : ${prof.company || ''}`);
  L.push(`Rol          : ${prof.role || ''}`);
  L.push(`Status       : ${prof.status || ''}`);
  L.push(`auth_uid     : ${authUid || '(nooit ingelogd)'}`);
  L.push(`Aangemaakt   : ${prof.created_at || prof.aangemaakt_op || '?'}`);
  L.push('');
  L.push(`APP-TOEGANGEN (${grants.length})`); L.push('-'.repeat(62));
  if (!grants.length) L.push('(geen)');
  else grants.forEach(g => L.push(`- ${g.app_id}${g.toegekend_op ? ('  · toegekend ' + g.toegekend_op) : ''}${g.toegekend_door ? (' door ' + g.toegekend_door) : ''}`));
  L.push('');
  L.push(`ACTIVITEITSLOG (${logs.length})`); L.push('-'.repeat(62));
  if (!logs.length) L.push('(geen)');
  else logs.forEach(r => {
    const det = (r.details && typeof r.details === 'object' && Object.keys(r.details).length) ? ('  ' + JSON.stringify(r.details)) : '';
    L.push(`[${r.ts || '?'}] ${r.app_id || '?'} · ${r.actie || '?'}${r.klant_naam ? (' · klant: ' + r.klant_naam) : ''}${r.klant_btw ? (' (' + r.klant_btw + ')') : ''}${det}`);
  });
  L.push('');
  L.push(`CERTIFICATEN (${certs.length})`); L.push('-'.repeat(62));
  if (!certs.length) L.push('(geen)');
  else certs.forEach(c => L.push(`- ${c.training_title || c.training_id || '?'} · score ${c.score != null ? c.score : '?'}/${c.total != null ? c.total : '?'} · ${c.issued_at || ''}${c.revoked ? ' (INGETROKKEN)' : ''}`));
  if (herlink) {
    L.push(''); L.push('HERLINK PROJECTEN'); L.push('-'.repeat(62));
    L.push(`Doel                : ${herlink.doel || '?'}`);
    const s = herlink.scenarios || {}, k = herlink.kamino || {};
    L.push(`Scenario's herlinkt : ${s.herlinkt || 0} van ${s.onderzocht || 0} onderzocht${s.fout ? (' — ' + s.fout) : ''}${s.fouten ? (' (' + s.fouten + ' fouten)') : ''}`);
    L.push(`Kamino herlinkt     : ${k.herlinkt || 0} van ${k.onderzocht || 0} onderzocht${k.fout ? (' — ' + k.fout) : ''}${k.fouten ? (' (' + k.fouten + ' fouten)') : ''}`);
  }
  L.push(''); L.push('EINDE EXPORT');
  return L.join('\n');
}

// v15.63.0: herlink alle scenario's (fluctus-scenarios repo) van owner_uid vanUid → naarUid.
// v15.64.0: dryRun=true telt enkel wat er ZOU herlinken (geen schrijfactie).
async function _herlinkScenarios(vanUid, naarUid, naarNaam, dryRun) {
  const res = { onderzocht: 0, herlinkt: 0, fouten: 0, dry: !!dryRun };
  if (!vanUid || !naarUid) { res.fout = 'uid ontbreekt'; return res; }
  try {
    const apiUrl = `https://api.github.com/repos/${SCENARIOS_REPO_OWNER}/${SCENARIOS_REPO_NAME}/contents/${SCENARIOS_PATH_PREFIX}`;
    const headers = { 'User-Agent': 'fluctus-proxy', 'Accept': 'application/vnd.github.v3+json' };
    if (GITHUB_TOKEN) headers['Authorization'] = `token ${GITHUB_TOKEN}`;
    const r = await fetch(apiUrl, { headers });
    if (r.status === 404) return res;
    if (!r.ok) throw new Error(`projecten-lijst HTTP ${r.status}`);
    const entries = await r.json();
    const projecten = (entries || []).filter(e => e.type === 'dir').map(e => e.name);
    for (const project of projecten) {
      let namen = [];
      try { namen = await _scenariosGithubListProject(project); } catch (e) { continue; }
      for (const naam of namen) {
        res.onderzocht++;
        const pad = _scenarioPad(project, naam);
        try {
          const { data, sha } = await _scenariosGithubRead(pad);
          if (data && data.owner_uid === vanUid) {
            if (!dryRun) {
              const nieuw = Object.assign({}, data, {
                owner_uid: naarUid, owner_naam: naarNaam || data.owner_naam,
                _owner_herlinkt_op: new Date().toISOString(), _owner_herlinkt_van: vanUid,
              });
              await _scenariosGithubWrite(pad, nieuw, sha);
            }
            res.herlinkt++;
          }
        } catch (e) { res.fouten++; }
      }
    }
  } catch (e) { res.fout = e.message; }
  return res;
}

// v15.63.0: herlink alle Kamino-projectrecords (bucket kamino/*.json) van adviseur-email vanEmail → naarEmail.
// v15.64.0: dryRun=true telt enkel wat er ZOU herlinken (geen schrijfactie).
async function _herlinkKamino(vanEmail, naarNaam, naarEmail, dryRun) {
  const res = { onderzocht: 0, herlinkt: 0, fouten: 0, dry: !!dryRun };
  const doel = String(vanEmail || '').toLowerCase();
  if (!doel) { res.fout = 'email ontbreekt'; return res; }
  try {
    const url = `${SUPABASE_URL}/storage/v1/object/list/${FACTUREN_BUCKET}`;
    const r = await fetch(url, {
      method: 'POST',
      headers: { 'apikey': SUPABASE_SERVICE_KEY, 'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ prefix: 'kamino/', limit: 1000, sortBy: { column: 'name', order: 'asc' } }),
    });
    if (!r.ok) { res.fout = `list HTTP ${r.status}`; return res; }
    const objs = await r.json();
    for (const o of (objs || [])) {
      const naam = o && o.name;
      if (!naam || !/\.json$/i.test(naam)) continue;
      res.onderzocht++;
      const pad = `kamino/${naam}`;
      try {
        const rec = JSON.parse(await _factuurDownload(pad));
        const adv = (rec && rec.adviseur) || {};
        if (String(adv.email || '').toLowerCase() === doel) {
          if (!dryRun) {
            rec.adviseur = Object.assign({}, adv, { naam: naarNaam || adv.naam, email: naarEmail });
            rec._adviseur_herlinkt_op = new Date().toISOString();
            rec._adviseur_herlinkt_van = doel;
            rec.bijgewerkt = new Date().toISOString();
            await _factuurUpload(Buffer.from(JSON.stringify(rec), 'utf8').toString('base64'), 'application/json', pad);
          }
          res.herlinkt++;
        }
      } catch (e) { res.fouten++; }
    }
  } catch (e) { res.fout = e.message; }
  return res;
}
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
    // v15.96 (Fase 2c): fallback op E-MAIL. Een uitgenodigd/klant-profiel wordt aangemaakt met enkel
    // een e-mail (nog géén auth_uid — die ontstaat pas bij de eerste OTP-login). Match dan op e-mail
    // zodat de rol (bv. 'klant') meteen klopt, en self-heal auth_uid zodat volgende lookups direct raak zijn.
    if (!profiel) {
      try {
        const em = String(user.email || '').trim().toLowerCase();
        if (em) {
          const rows = await _sbRest(`profiles?email=eq.${encodeURIComponent(em)}&select=*`);
          profiel = Array.isArray(rows) && rows.length ? rows[0] : null;
          if (profiel && !profiel.auth_uid) {
            try {
              await _sbRest(`profiles?email=eq.${encodeURIComponent(em)}`, {
                method: 'PATCH', headers: { 'Prefer': 'return=minimal' }, body: { auth_uid: user.id },
              });
              profiel.auth_uid = user.id;
            } catch (e) { /* self-heal niet-blokkerend */ }
          }
        }
      } catch (e) { /* e-mail-fallback faalde — ok, val terug op default-rol */ }
    }
    const val = {
      id: user.id,
      email: user.email || (profiel && profiel.email) || '',
      naam: (profiel && (profiel.name || profiel.naam || profiel.full_name)) || user.email || user.id,
      role: (profiel && profiel.role) || 'seller',
      partner: (profiel && (profiel.company || profiel.partner || profiel.epc)) || null,   // v15.92/93 (Fase 2): EPC/bedrijf-sleutel = 'company' (partnermanager-scoping)
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

// v15.90 (Fase 1): mag deze gebruiker het kamino-projectrecord openen?
// Manager = altijd; anders enkel eigenaar (door=auth-uid), adviseur- of klant-e-mail.
function _magProjectOpenen(u, rec) {
  if (!u || !rec) return false;
  if (_isManager(u)) return true;
  // v15.92 (Fase 2): partnermanager ziet alle projecten van zijn eigen EPC (zelfde partner-id).
  if (u.role === 'partnermanager' && u.partner && rec.partner && u.partner === rec.partner) return true;
  if (rec.door && (rec.door === u.id || rec.door === u.naam)) return true;
  const em = String(u.email || '').toLowerCase();
  if (!em) return false;
  const adv = String((rec.adviseur && rec.adviseur.email) || '').toLowerCase();
  const kl = String((rec.klant && rec.klant.email) || '').toLowerCase();
  return em === adv || em === kl;
}

// v15.95 (Fase 2b, Johan-correctie): een partnermanager ziet AUTOMATISCH enkel de
// EnergieKompas-schil — niet de losse interne tools. EnergieKompas is de partner-
// gerichte ingang (particulier|bedrijf, thema per partner); de onderliggende tools
// (simulator, kamino, thuisladen …) blijven Fluctus-intern of grant-gebaseerd.
// Fluctus-interne apps (energiemarkt, gemeenteplan) en Gebruikers blijven manager-only.
// Adviseurs blijven grant-gebaseerd (zoals sellers) — zij werken via de ingebedde schil.
const _PARTNER_APPS = new Set(['energiekompas']);

async function _heeftAppToegang(u, appId) {
  // Enkel een gedeactiveerde gebruiker wordt geblokkeerd. 'invited' (uitgenodigd,
  // nog niet geactiveerd) én 'active' krijgen toegang volgens hun toekenningen —
  // zo werkt een toegekende tool meteen zodra de verkoper voor het eerst inlogt.
  // 'Deactiveren' (status inactive) is de blokkeer-schakelaar.
  if (!u || u.status === 'inactive') return false;
  if (_isManager(u)) return true; // managers impliciet alle apps
  // v15.95 (Fase 2b): partnermanager = automatische toegang tot ENKEL EnergieKompas.
  if (u.role === 'partnermanager' && _PARTNER_APPS.has(appId)) return true;
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

// ════════════════════════════════════════════════════════════════════════════
//  USER MANAGEMENT (manager-only) — toegang tot de portal-tools beheren.
//  Bouwt voort op het bestaande 9a-model (profiles + user_app_access). Geen
//  nieuwe tabellen. Alle endpoints: manager-only via resolveUser + _isManager.
//  user_app_access.user_id == profiles.auth_uid == auth.users.id.
// ════════════════════════════════════════════════════════════════════════════

// Kleine guard: retourneert manager-user of null (response al verstuurd bij null).
async function _managerGuard(req, res) {
  if (!SUPABASE_OK) { res.status(503).json({ error: 'auth_niet_geconfigureerd' }); return null; }
  const u = await resolveUser(req);
  if (!u) { res.status(401).json({ error: 'niet ingelogd' }); return null; }
  if (!_isManager(u)) { res.status(403).json({ error: 'alleen voor managers' }); return null; }
  return u;
}

// GET /api/manager/users
// → { users:[{ auth_uid, email, name, company, role, status, apps:[app_id] }] }
// Toont alle profielen + de per-gebruiker toegekende apps. Gebruikers zonder
// auth_uid (uitgenodigd maar nog nooit ingelogd) hebben auth_uid=null → app-
// toegang kan pas toegekend worden zodra ze één keer ingelogd zijn.
app.get('/api/manager/users', async (req, res) => {
  const u = await _managerGuard(req, res); if (!u) return;
  try {
    const profs = await _sbRest('profiles?select=auth_uid,email,name,company,role,status&order=email.asc');
    let grants = [];
    try { grants = await _sbRest('user_app_access?select=user_id,app_id'); }
    catch (e) { console.warn(`[manager/users] grants-lookup faalde: ${e.message}`); }
    const perUser = {};
    for (const g of (grants || [])) {
      if (!g.user_id) continue;
      (perUser[g.user_id] = perUser[g.user_id] || []).push(g.app_id);
    }
    const users = (profs || []).map(p => ({
      auth_uid: p.auth_uid || null,
      email: p.email || '',
      name: p.name || p.naam || p.full_name || '',
      company: p.company || '',
      role: p.role || 'seller',
      status: p.status || 'active',
      apps: p.auth_uid ? (perUser[p.auth_uid] || []) : [],
    }));
    return res.json({ users });
  } catch (e) {
    console.error(`[manager/users] fout: ${e.message}`);
    return res.status(500).json({ error: e.message });
  }
});

// POST /api/manager/app-access   { user_id, app_id }   → toegang toekennen
// GET /api/manager/partners → distinct bedrijven/EPC's (uit profiles.company) voor de partner-datalists. v15.93.
app.get('/api/manager/partners', async (req, res) => {
  const u = await _managerGuard(req, res); if (!u) return;
  try {
    const rows = await _sbRest('profiles?select=company');
    const set = new Set();
    (rows || []).forEach(r => { const c = String(r.company || '').trim(); if (c) set.add(c); });
    return res.json({ partners: [...set].sort() });
  } catch (e) {
    console.error('[manager/partners] faalde:', e.message);
    return res.status(500).json({ error: e.message });
  }
});

app.post('/api/manager/app-access', async (req, res) => {
  const u = await _managerGuard(req, res); if (!u) return;
  const b = req.body || {};
  const userId = b.user_id, appId = b.app_id;
  if (!userId || !appId) return res.status(400).json({ error: 'user_id en app_id verplicht' });
  try {
    // v15.61.0: AUTO-PROVISION van de app in de catalogus. user_app_access.app_id heeft een FK naar
    // apps.id; staat een app wél in de UI maar nog niet als rij in `apps` (bv. kamino/jacops die na de
    // 9a-seed zijn toegevoegd), dan faalt toekennen met 23503. We maken de ontbrekende app-rij nu
    // idempotent aan met de naam die de UI meestuurt (fallback = id), zodat een nieuwe app nooit meer
    // handmatig geseed hoeft te worden. Bestaanscheck → INSERT (geen on_conflict nodig).
    const appNaam = (b.app_naam && String(b.app_naam).trim()) || appId;
    const appBestaat = await _sbRest(`apps?select=id&id=eq.${encodeURIComponent(appId)}&limit=1`);
    if (!Array.isArray(appBestaat) || appBestaat.length === 0) {
      await _sbRest('apps', {
        method: 'POST',
        headers: { 'Prefer': 'return=minimal' },
        body: { id: appId, naam: appNaam, beschrijving: appNaam, actief: true },
      });
      console.log(`[manager/app-access] app '${appId}' ontbrak in de catalogus → aangemaakt ('${appNaam}')`);
    }
    // v15.60.0: IDEMPOTENT ZONDER on_conflict. De vorige versie gebruikte
    // `on_conflict=user_id,app_id` + merge-duplicates; dat VEREIST een unique-constraint op
    // (user_id, app_id) in user_app_access. Ontbreekt die, dan geeft PostgREST HTTP 500 (42P10:
    // "no unique or exclusion constraint matching the ON CONFLICT specification") en springt het
    // vinkje in de UI terug. We doen nu eerst een bestaanscheck en INSERTen enkel indien nodig →
    // geen constraint meer nodig, en dubbel toekennen faalt niet meer.
    const bestaand = await _sbRest(
      `user_app_access?select=app_id&user_id=eq.${encodeURIComponent(userId)}&app_id=eq.${encodeURIComponent(appId)}&limit=1`);
    if (!Array.isArray(bestaand) || bestaand.length === 0) {
      await _sbRest('user_app_access', {
        method: 'POST',
        headers: { 'Prefer': 'return=minimal' },
        body: { user_id: userId, app_id: appId },
      });
    }
    return res.json({ ok: true, user_id: userId, app_id: appId, toegang: true });
  } catch (e) {
    console.error(`[manager/app-access grant] fout: ${e.message}`);
    return res.status(500).json({ error: e.message });
  }
});

// DELETE /api/manager/app-access  { user_id, app_id }  (of ?user_id=&app_id=)
app.delete('/api/manager/app-access', async (req, res) => {
  const u = await _managerGuard(req, res); if (!u) return;
  const src = Object.assign({}, req.query || {}, req.body || {});
  const userId = src.user_id, appId = src.app_id;
  if (!userId || !appId) return res.status(400).json({ error: 'user_id en app_id verplicht' });
  try {
    await _sbRest(
      `user_app_access?user_id=eq.${encodeURIComponent(userId)}&app_id=eq.${encodeURIComponent(appId)}`,
      { method: 'DELETE', headers: { 'Prefer': 'return=minimal' } });
    return res.json({ ok: true, user_id: userId, app_id: appId, toegang: false });
  } catch (e) {
    console.error(`[manager/app-access revoke] fout: ${e.message}`);
    return res.status(500).json({ error: e.message });
  }
});

// POST /api/manager/user   { action, ... }
//   action='invite'  { email, name?, company? } → profiel (role seller,
//                      status invited) aanmaken + academy-mail triggeren.
//                      Bestaat het profiel al → 409 (gebruik rol/status/toegang).
//   action='role'    { email, role: 'manager'|'seller' }
//   action='status'  { email, status: 'active'|'inactive'|'invited' }
app.post('/api/manager/user', async (req, res) => {
  const u = await _managerGuard(req, res); if (!u) return;
  const b = req.body || {};
  const action = String(b.action || '').toLowerCase();
  try {
    if (action === 'invite') {
      const email = String(b.email || '').trim().toLowerCase();
      if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return res.status(400).json({ error: 'geldig e-mailadres verplicht' });
      // Bestaat het profiel al? Dan NIET overschrijven (geen downgrade van rol/status).
      const bestaand = await _sbRest(`profiles?email=eq.${encodeURIComponent(email)}&select=email`);
      if (Array.isArray(bestaand) && bestaand.length) {
        return res.status(409).json({ error: 'bestaat_al', melding: 'Deze gebruiker bestaat al. Gebruik rol/status/toegang.' });
      }
      // 1) Profiel aanmaken (service-role) — gegarandeerd, ook als de mail faalt.
      await _sbRest('profiles', {
        method: 'POST',
        headers: { 'Prefer': 'return=minimal' },
        body: { email, name: b.name || '', company: b.company || '', role: 'seller', status: 'invited' },
      });
      // 2) Academy-mail triggeren (Brevo blijft op de academy-backend).
      let mail = { sent: false, reden: 'academy_url_niet_geconfigureerd' };
      if (ACADEMY_BACKEND_URL) {
        try {
          const r = await fetch(`${ACADEMY_BACKEND_URL}/api/invite`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': req.headers['authorization'] || '' },
            body: JSON.stringify({ email, name: b.name || '', company: b.company || '' }),
          });
          const j = await r.json().catch(() => ({}));
          mail = r.ok ? { sent: true, queued: !!j.emailQueued } : { sent: false, reden: (j.error || ('http_' + r.status)) };
        } catch (e) { mail = { sent: false, reden: e.message }; }
      }
      return res.json({ ok: true, email, profiel: 'aangemaakt', mail });
    }

    if (action === 'role') {
      const email = String(b.email || '').trim().toLowerCase();
      const role = String(b.role || '').toLowerCase();
      const geldig = ['manager', 'seller', 'adviseur', 'partnermanager', 'klant'];   // v15.93 (Fase 2): 4 rollen (adviseur = seller-gedrag)
      if (!email) return res.status(400).json({ error: 'email verplicht' });
      if (!geldig.includes(role)) return res.status(400).json({ error: 'ongeldige rol' });
      const patch = { role };
      if (b.company !== undefined) patch.company = String(b.company || '');   // v15.93: bedrijf/EPC in één keer mee zetten
      await _sbRest(`profiles?email=eq.${encodeURIComponent(email)}`, {
        method: 'PATCH', headers: { 'Prefer': 'return=minimal' }, body: patch,
      });
      _AUTH_CACHE.clear(); // rol-wijziging meteen laten doorwerken
      return res.json({ ok: true, email, role, company: patch.company });
    }
    if (action === 'company') {   // v15.93 (Fase 2): bedrijf/EPC van een gebruiker instellen (scopingsleutel)
      const email = String(b.email || '').trim().toLowerCase();
      if (!email) return res.status(400).json({ error: 'email verplicht' });
      await _sbRest(`profiles?email=eq.${encodeURIComponent(email)}`, {
        method: 'PATCH', headers: { 'Prefer': 'return=minimal' }, body: { company: String(b.company || '') },
      });
      _AUTH_CACHE.clear();
      return res.json({ ok: true, email, company: String(b.company || '') });
    }

    if (action === 'status') {
      const email = String(b.email || '').trim().toLowerCase();
      const status = String(b.status || '').toLowerCase();
      if (!email) return res.status(400).json({ error: 'email verplicht' });
      if (!['active', 'inactive', 'invited'].includes(status)) return res.status(400).json({ error: "status moet 'active', 'inactive' of 'invited' zijn" });
      await _sbRest(`profiles?email=eq.${encodeURIComponent(email)}`, {
        method: 'PATCH', headers: { 'Prefer': 'return=minimal' }, body: { status },
      });
      _AUTH_CACHE.clear(); // status-wijziging meteen laten doorwerken
      return res.json({ ok: true, email, status });
    }

    if (action === 'delete') {
      const email = String(b.email || '').trim().toLowerCase();
      const dryRun = !!(b.dry_run || b.preview);   // v15.64.0: preview → verzamelt + telt herlink, MAAR wijzigt/verwijdert/maildt niets
      if (!email) return res.status(400).json({ error: 'email verplicht' });
      // Zelf-verwijderen blokkeren (bij een echte delete; preview mag altijd).
      if (!dryRun && u.email && u.email.toLowerCase() === email) return res.status(400).json({ error: 'Je kan je eigen account niet verwijderen.' });
      // Doelprofiel ophalen
      const profs = await _sbRest(`profiles?email=eq.${encodeURIComponent(email)}&select=*`);
      const prof = (Array.isArray(profs) && profs.length) ? profs[0] : null;
      if (!prof) return res.status(404).json({ error: 'gebruiker niet gevonden' });
      const authUid = prof.auth_uid || null;
      const naam = prof.name || prof.naam || prof.full_name || email;

      // 1) Alle gelogde info verzamelen (best-effort per bron)
      let grants = [], logs = [], certs = [];
      if (authUid) {
        try { grants = (await _sbRest(`user_app_access?user_id=eq.${encodeURIComponent(authUid)}&select=*`)) || []; } catch (e) {}
        try { logs = (await _sbRest(`app_activity_log?user_id=eq.${encodeURIComponent(authUid)}&select=*&order=ts.asc&limit=10000`)) || []; } catch (e) {}
      }
      try { certs = (await _sbRest(`certificates?email=eq.${encodeURIComponent(email)}&select=*`)) || []; } catch (e) {}

      // 2) Projecten HERLINKEN naar oekene@gmail.com (= AUDIT_MAIL_TO) i.p.v. verweesd achterlaten.
      //    Doel opgezocht via profiles; scenario's (owner_uid) + Kamino-records (adviseur-email) herstempeld.
      let doelProf = null;
      try {
        const dp = await _sbRest(`profiles?email=eq.${encodeURIComponent(AUDIT_MAIL_TO)}&select=auth_uid,email,name,naam,full_name`);
        if (Array.isArray(dp) && dp.length) doelProf = dp[0];
      } catch (e) {}
      const doelUid = (doelProf && doelProf.auth_uid) || null;
      const doelNaam = (doelProf && (doelProf.name || doelProf.naam || doelProf.full_name)) || AUDIT_MAIL_TO;
      const doelEmail = (doelProf && doelProf.email) || AUDIT_MAIL_TO;
      const scenarioHerlink = doelUid ? await _herlinkScenarios(authUid, doelUid, doelNaam, dryRun)
                                      : { onderzocht: 0, herlinkt: 0, fouten: 0, fout: `doel-uid voor ${AUDIT_MAIL_TO} niet gevonden` };
      const kaminoHerlink = await _herlinkKamino(email, doelNaam, doelEmail, dryRun);
      const herlink = { doel: doelEmail, scenarios: scenarioHerlink, kamino: kaminoHerlink };

      const nu = _nuBrussel();

      // v15.64.0: PREVIEW (dry-run) → toon wat er zou gebeuren, wijzig/verwijder/mail NIETS.
      if (dryRun) {
        const onderwerpP = `[PREVIEW] ${naam} — zou verwijderd worden op ${nu.stempel}`;
        const txtP = _bouwVerwijderExport(prof, authUid, grants, logs, certs, nu, u, herlink);
        const bestandsnaamP = `preview_${email.replace(/[^a-z0-9]+/gi, '_')}_${nu.stempel.replace(/[^0-9]/g, '')}.txt`;
        return res.json({ ok: true, dry_run: true, email, naam, onderwerp: onderwerpP, bestandsnaam: bestandsnaamP, txt: txtP, herlink,
          diagnostiek: {
            doel_gevonden: !!doelUid, doel_email: doelEmail,
            github_token: !!GITHUB_TOKEN, brevo_key_gezet: !!BREVO_API_KEY, brevo_sender: BREVO_SENDER, audit_naar: AUDIT_MAIL_TO,
          },
          zou_verwijderen: { grants: grants.length, logs: logs.length, certificaten: certs.length, auth_user: !!authUid } });
      }

      const onderwerp = `${naam} verwijderd op ${nu.stempel}`;
      const txt = _bouwVerwijderExport(prof, authUid, grants, logs, certs, nu, u, herlink);
      const bestandsnaam = `verwijderd_${email.replace(/[^a-z0-9]+/gi, '_')}_${nu.stempel.replace(/[^0-9]/g, '')}.txt`;

      // 3) v15.71 (Johan 09-08): GEEN mail bij verwijderen — de audit-export wordt enkel lokaal gedownload (vangnet).
      const mail = { sent: false, reden: 'uitgeschakeld' };

      // 4) Verwijderen: logs → grants → profiel → auth-user (in deze volgorde i.v.m. FK's)
      if (authUid) {
        try { await _sbRest(`app_activity_log?user_id=eq.${encodeURIComponent(authUid)}`, { method: 'DELETE', headers: { 'Prefer': 'return=minimal' } }); } catch (e) {}
        try { await _sbRest(`user_app_access?user_id=eq.${encodeURIComponent(authUid)}`, { method: 'DELETE', headers: { 'Prefer': 'return=minimal' } }); } catch (e) {}
      }
      await _sbRest(`profiles?email=eq.${encodeURIComponent(email)}`, { method: 'DELETE', headers: { 'Prefer': 'return=minimal' } });
      let authVerwijderd = false, authReden = null;
      if (authUid) {
        try {
          const r = await fetch(`${SUPABASE_URL}/auth/v1/admin/users/${encodeURIComponent(authUid)}`, {
            method: 'DELETE', headers: { 'apikey': SUPABASE_SERVICE_KEY, 'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}` },
          });
          authVerwijderd = r.ok; if (!r.ok) authReden = `HTTP ${r.status} ${(await r.text()).slice(0, 150)}`;
        } catch (e) { authReden = e.message; }
      }
      _AUTH_CACHE.clear();
      console.log(`[manager/user delete] ${email} verwijderd (auth:${authVerwijderd}) mail:${mail.sent} · herlink scen:${scenarioHerlink.herlinkt} kamino:${kaminoHerlink.herlinkt} → ${doelEmail}`);
      return res.json({ ok: true, email, naam, onderwerp, bestandsnaam, txt, mail, herlink,
        verwijderd: { profiel: true, grants: grants.length, logs: logs.length, certificaten: certs.length, auth_user: authVerwijderd, auth_reden: authReden } });
    }

    return res.status(400).json({ error: "onbekende action (invite|role|status|delete)" });
  } catch (e) {
    console.error(`[manager/user ${action}] fout: ${e.message}`);
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
  status:'ok', version: SERVER_VERSIE, ts:new Date().toISOString(), markt_geladen: !!MARKT,
  markt_status: MARKT_STATUS, markt_pogingen: MARKT_POGINGEN,
  markt_laatste_fout: MARKT_LAATSTE_FOUT,
  markt_periode: MARKT ? { van: MARKT.van, tot: MARKT.tot, n_kwartieren: MARKT.n_kwartieren } : null,
  market_config: {
    owner: MARKET_DATA_OWNER,
    repo: MARKET_DATA_REPO,
    path: MARKET_DATA_PATH,
    has_token: !!GITHUB_TOKEN
  },
  // v15.65.0: parallelle-dispatch-status. sim_max_parallel = effectieve bovengrens op gelijktijdige
  // sim-runs; piek_gelijktijdig = hoogste aantal dat sinds start ECHT tegelijk liep (dus hoeveel er
  // effectief benut werd); cpus_gerapporteerd = wat Node ziet (meestal HOST-cores, NIET je plan-quota).
  sim_parallel: {
    sim_max_parallel: SIM_MAX_PARALLEL,
    env: process.env.SIM_MAX_PARALLEL || '(niet gezet → 1)',
    piek_gelijktijdig: _simPiek,
    runs_totaal: _simRunsTotaal,
    nu_bezig: _simBezet,
    cpus_gerapporteerd: (os.cpus() || []).length
  }
}));
app.get('/health', (req, res) => res.json({ status:'ok', markt_status: MARKT_STATUS,
  sim_max_parallel: SIM_MAX_PARALLEL, sim_piek_gelijktijdig: _simPiek })); // v15.65.0

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

// ─── v15.102 (Fase 4 — EnergieKompas publieke KPI's) ─────────────────────────────────────────
// PUBLIEK (geen login): eerste-feedback-KPI's voor de EnergieKompas-schil, op de LIVE markt (MARKT.spot_q,
// €/MWh) + de door de gebruiker ingegeven factuurwaarden. Geen PII, geen opslag — enkel 4 aggregaten terug.
// De PRECIEZE studie (op het werkelijke kwartierprofiel na mandaat) draait later via de bestaande sim.
app.post('/api/energiekompas/kpi', (req, res) => {
  try {
    if (!MARKT || !Array.isArray(MARKT.spot_q) || !MARKT.spot_q.length) {
      return res.status(503).json({ error: 'Marktdata nog niet geladen — probeer over 30 s opnieuw', markt_status: MARKT_STATUS });
    }
    const b = req.body || {};
    const _n = (v, mx) => { let x = Number(v); if (!isFinite(x) || x < 0) x = 0; if (mx != null && x > mx) x = mx; return x; };
    const afname = _n(b.afname_mwh, 100000);
    const injectie = _n(b.injectie_mwh, 100000);
    const kva = _n(b.aansluiting_kva, 100000);
    const huidigeKost = _n(b.huidige_kost, 100000000);
    const wagens = Math.max(0, Math.round(_n(b.wagens, 100000)));
    const km = _n(b.km, 1000000);
    const KWH_PER_KM = 0.16;
    // Marktankers uit spot_q (€/MWh)
    const spot = MARKT.spot_q.filter(x => isFinite(x));
    const avgSpot = spot.reduce((a, c) => a + c, 0) / spot.length;
    const sorted = spot.slice().sort((a, c) => a - c);
    const goedkoopN = Math.max(1, Math.round(sorted.length * 0.30));   // slim laden mikt op de goedkoopste ~30%
    const dynLaad = sorted.slice(0, goedkoopN).reduce((a, c) => a + c, 0) / goedkoopN;
    const injWaarde = Math.max(0, avgSpot * 0.9);   // injectievergoeding ≈ spot (indicatief)
    const NET_HEFFING = 90;   // €/MWh all-in netkosten/heffingen bovenop energie (indicatief) als er geen factuurkost is
    // KPI's
    const kostAfname = huidigeKost > 0 ? huidigeKost : afname * (avgSpot + NET_HEFFING);
    const waardeInjectie = injectie * injWaarde;
    const toegangKw = kva * 0.9;
    const gemKw = afname * 1000 / 8760;
    const restPct = toegangKw > 0 ? Math.max(0, Math.min(95, Math.round((1 - gemKw / toegangKw) * 100))) : null;
    const laadMwh = wagens * km * KWH_PER_KM / 1000;
    // €/km bij dynamisch laden op de goedkoopste uren. ALL-IN achter de meter = energiecomponent (dynLaad)
    // + netkosten/heffingen (NET_HEFFING). De energiecomponent apart voor transparantie.
    const laadAllIn = dynLaad + NET_HEFFING;                       // €/MWh all-in aan de laadpaal
    const kmKost = (laadAllIn / 1000) * KWH_PER_KM;                // €/km all-in
    const kmKostEnergie = (dynLaad / 1000) * KWH_PER_KM;          // €/km enkel commodity
    return res.json({
      ok: true,
      kpi: {
        kost_afname_eur: Math.round(kostAfname),
        waarde_injectie_eur: Math.round(waardeInjectie),
        restcapaciteit_pct: restPct,
        kilometerkost_eur_per_km: Math.round(kmKost * 1000) / 1000,          // all-in achter de meter
        kilometerkost_energie_eur_per_km: Math.round(kmKostEnergie * 1000) / 1000,   // enkel energiecomponent
        laadvraag_mwh: Math.round(laadMwh * 10) / 10,
      },
      markt: {
        avg_spot_eur_mwh: Math.round(avgSpot * 10) / 10,
        dyn_laadkost_eur_mwh: Math.round(dynLaad * 10) / 10,          // enkel commodity (goedkoopste ~30%)
        laadkost_allin_eur_mwh: Math.round((dynLaad + NET_HEFFING) * 10) / 10,   // all-in achter de meter
        net_heffing_eur_mwh: NET_HEFFING,
        injectiewaarde_eur_mwh: Math.round(injWaarde * 10) / 10,
        van: MARKT.van, tot: MARKT.tot,
      },
      indicatief: true,
    });
  } catch (e) {
    console.error('[energiekompas/kpi] faalde:', e.message);
    return res.status(500).json({ error: e.message });
  }
});

// ── v15.103 (Fase 4 — slice B/C BEDRIJF): PV×batterij-sweep op de bedrijfslast (echte laadplein-engine) ──
// PUBLIEK. Dimensioneringsassen (Johan 27-08):
//   PV-as        : 0 → 1,25 × jaarafname (kWp).
//   Batterij-kW-as: 0 → (generator-kVA + Σ laadplein-vermogen + toegangsvermogen).
// Per cel één ECHTE dispatch (variant 'sturing') via buildSimInput → _runSimulatorOnce, op het gekozen
// STANDAARDprofiel + de laadpleinen. Referentie = cel (0 PV, 0 batterij). besparing = ref − kost;
// rendement = besparing/capex. Zelfde vorm als /api/thuisladen (anchors + grid) → de schil tekent er de
// heatmap mee. De PRECIEZE run komt later op het werkelijke kwartierprofiel (na mandaat). Additief.
// ─── GENERATOR-heatmap (24×365): wanneer de shots vallen / de last die de batterij moet leveren ──────
// Zelfde vorm/index als de andere heatmaps (platte array, cel = uur*365 + dag). De dagelijkse generator-
// energie wordt gelijkmatig over het draaivenster gespreid, op de werkdagen (regime dagen/week, ma-eerst).
function _ekGeneratorHeatmap(genStarts, egenKwh, venS, venE, dagenWeek) {
  const HR = 24, DG = 365, LEN = HR * DG; const hm = new Array(LEN).fill(0);
  const wS = Math.max(0, Math.min(23, Math.round(venS))), wE = Math.max(wS + 1, Math.min(24, Math.round(venE)));
  const uren = wE - wS, dw = Math.max(1, Math.min(7, Math.round(dagenWeek)));
  if (!(uren > 0) || !(egenKwh > 0)) return null;
  const dagEgen = egenKwh / (dw * 52);          // energie op één werkdag
  const perUur = dagEgen / uren;
  for (let dag = 0; dag < DG; dag++) {
    const js = new Date(Date.UTC(2025, 0, 1 + dag)).getUTCDay();   // 0=zo..6=za
    const iso = ((js + 6) % 7) + 1;                                 // 1=ma..7=zo
    if (iso > dw) continue;
    for (let u = wS; u < wE; u++) hm[u * DG + dag] = Math.round(perUur * 100) / 100;
  }
  return { uren: HR, dagen: DG, last_kwh: hm };
}

// ─── EnergieKompas bedrijf — gedeelde context (coarse sweep + losse cel) ──────
// De sweep berekent nu een GROVE ankergrid (≤4×3). De schil interpoleert tussen
// de ankers voor een fijnere heatmap; bij klik op een cel roept ze /bedrijf-cel
// aan voor de ECHTE berekening van die ene combinatie. Zo blijft de sweep snel.
// GENERATOR (§3, Johan): opt-in, gemodelleerd als een synthetische 'plein'-last (shots op kVA-piek),
// zodat de bestaande laadplein-engine de batterij+PV-vervanging waardeert. GENSET 3,3 kWh_e/L, C-factor 2.
function _ekBedrijfCtx(b) {
  const _n = (v) => { const x = Number(v); return (isFinite(x) && x > 0) ? x : 0; };
  const afname = _n(b.afname_mwh);
  const kva = _n(b.aansluiting_kva) || 100;
  const spanning = (b.spanning === 'MS' || b.spanning === 'LS') ? b.spanning : (kva >= 100 ? 'MS' : 'LS');
  const toegang = _n(b.toegangsvermogen_kw) || Math.round(kva * 0.9);
  const profielNaam = String(b.profielNaam || 'kantoor');
  const grd = b.grd || '', postcode = String(b.postcode || '');
  const pleinen = Array.isArray(b.pleinen) ? b.pleinen : [];
  const genKva = _n((b.generator || {}).kva);
  // Σ laadplein-vermogen (kW): betalend = palen × paal_kw; wagenpark = aantal × 11 kW (nominaal AC).
  let lpKw = 0;
  pleinen.forEach(p => {
    if (String(p.type_plein) === 'bezoekers') lpKw += (_n(p.aantal_palen) || 1) * (_n(p.paal_kw) || (p.paaltype === 'DC160' ? 160 : 22));
    else lpKw += _n(p.aantal) * 11;
  });
  const bessMaxKw = Math.max(0, Math.round(genKva + lpKw + toegang));
  const pvMaxKwp = Math.max(0, Math.round(1.25 * afname));   // PV tot 125% van de jaarafname
  const PV_KOST = 600, BATT_KWH_KOST = 350, CRATE = 2;   // €/kWp · €/kWh · uur (kWh = kW × C-rate)
  // ── GENERATOR → synthetische last (§3) ──
  const gen = b.generator || {};
  const genStarts = _n(gen.starts || gen.starts_per_dag || gen.n_dag) || 1;
  const genLiter = _n(gen.liter_jaar || gen.l_per_jaar || gen.brandstof_l_jaar);
  const GENSET_KWH_PER_L = _n(gen.kwh_per_l) || 3.3;          // elektrische genset-opbrengst (Johan)
  const DIESEL_EUR_L = _n(gen.diesel_eur_l) || 1.58;          // professionele diesel (€/L)
  const genVenS = Number.isFinite(+gen.venster_start) ? +gen.venster_start : 7;
  const genVenE = Number.isFinite(+gen.venster_eind) ? +gen.venster_eind : 18;
  const genDagen = _n(gen.dagen_per_week) || 5;
  const genEgenKwh = genLiter * GENSET_KWH_PER_L;             // elektrische output kWh/jaar
  const genMomenten = genStarts * genDagen * 52;
  const genKwhPerMoment = genMomenten > 0 ? genEgenKwh / genMomenten : 0;
  let genPlein = null, generator = null;
  if (genKva > 0 && genEgenKwh > 0) {
    // shots op de kVA-piek; per-sessie-energie op 365-dagenbasis zodat de engine ≈ het jaarvolume egen levert.
    const sessieKwh = genEgenKwh / (genStarts * 365);
    const duurMin = Math.max(1, Math.round(sessieKwh / genKva * 60));
    genPlein = { naam: 'Generator (vervanging)', type_plein: 'bezoekers', betalend: false, paaltype: 'GEN',
      paal_kw: genKva, aantal_palen: 1, duur_min: duurMin, opbrengst_eur_mwh: 0, sessies_paal_dag: genStarts,
      vensters: [{ start: genVenS, eind: genVenE, sessies: genStarts }], _generator: true };
    generator = {
      kva: genKva, liter_jaar: Math.round(genLiter), kwh_per_l: GENSET_KWH_PER_L, egen_kwh_jaar: Math.round(genEgenKwh),
      starts_per_dag: genStarts, momenten_jaar: genMomenten, kwh_per_moment: Math.round(genKwhPerMoment * 10) / 10,
      venster: [genVenS, genVenE], dagen_per_week: genDagen, diesel_eur_l: DIESEL_EUR_L,
      diesel_vermeden_eur_jaar: Math.round(genLiter * DIESEL_EUR_L),
      batterij_seed_kw: genKva, batterij_seed_kwh: Math.round(2 * genKva),   // vervang-batterij (§3.2: P_gen / 2u)
      heatmap: _ekGeneratorHeatmap(genStarts, genEgenKwh, genVenS, genVenE, genDagen),
    };
  }
  const pleinenSim = genPlein ? pleinen.concat([genPlein]) : pleinen;
  const _mkBaseUi = (pl) => ({
    grd, postcode, spanning, profielNaam, jaarverbruik_mwh: afname,
    aansluiting_kva: kva, toegangsvermogen_kw: toegang,
    laadpleinen: pl, jaar: 'rolling12', geen_aansluiting_verhoging: true,
    pv_curtailment: { actief: false }, bsp: { actief: false },
    contract: { leverancier: (CONTRACT_RAW && CONTRACT_RAW.leverancier) || 'Enwyse', modus: 'passthrough',
      staffel: CONTRACT_STAFFEL || [], vergroening_eur_per_mwh: 0,
      vaste_kost_eur_maand: (CONTRACT_RAW && CONTRACT_RAW.vast_eur_per_maand) || 10.00, injectie_toegelaten: true, gsc_eur_mwh: 0, wkk_eur_mwh: 0 },
  });
  const baseUi = () => _mkBaseUi(pleinenSim);        // mét generator-last (de cellen)
  const baseUiNoGen = () => _mkBaseUi(pleinen);      // zónder generator-last (de diesel-referentie)
  return { afname, kva, spanning, toegang, profielNaam, grd, postcode, pleinen, pleinenSim, genKva, generator, lpKw, bessMaxKw, pvMaxKwp, PV_KOST, BATT_KWH_KOST, CRATE, baseUi, baseUiNoGen };
}
// Eén echte dispatch voor combinatie (pv kWp, bkw kW-batterij) → {kost, capex, …}.
async function _ekBedrijfCel(ctx, pv, bkw) {
  const ui = ctx.baseUi(); ui.pv_kwp = pv;
  if (bkw > 0) { ui.batterijId = 'CUSTOM'; ui.batterijCustom = { naam: 'ek-bedrijf', kw: bkw, kwh: bkw * ctx.CRATE, aantal_batterijen: 1, dod_pct: 90, rte_pct: 92, capex_eur: 0, max_cycli: 8000 }; }
  else { ui.batterijId = ''; ui.batterijCustom = null; }
  const r = await _runSimulatorOnce(buildSimInput(_variantUi(ui, 'sturing')));
  const kost = Number((r.jaarfactuur || r.factuur || {}).subtotaal_excl_btw) || 0;
  const capex = pv * ctx.PV_KOST + bkw * ctx.CRATE * ctx.BATT_KWH_KOST;
  return { battKw: bkw, battKwh: bkw * ctx.CRATE, pvKwp: pv, kost: Math.round(kost), capex: Math.round(capex) };
}

app.post('/api/energiekompas/bedrijf-sweep', async (req, res) => {
  try {
    if (!MARKT) return res.status(503).json({ error: 'Marktdata nog niet geladen — probeer over 30 s opnieuw', markt_status: MARKT_STATUS });
    const ctx = _ekBedrijfCtx(req.body || {});
    if (!(ctx.afname > 0)) return res.status(400).json({ error: 'afname_mwh (jaarverbruik) verplicht' });
    // GROVE ankergrid: PV op 0/25/50/75/100% van max (≤5), batterij op 0/50/100% van max (≤3) = ≤15 cellen.
    // Het 75%-PV-anker (v15.105) halveert het brede interieur-gat 50→100%: de bilineaire interpolatie in de
    // schil zakt daar van ~13% naar ~3-5% afwijking. De schil interpoleert de tussencellen; klik = echte cel.
    const pvAs = [0]; [0.25, 0.5, 0.75, 1].forEach(f => { const v = Math.round(ctx.pvMaxKwp * f); if (v > (pvAs[pvAs.length - 1] || 0)) pvAs.push(v); });
    // Batterij-as: normaal 0→bessMax. Bij een generator start ze op de vervang-batterij P_gen (§3.2) — kleiner
    // vervangt de generator fysiek niet, dus die cellen zijn geen geldige replace-scenario's.
    const bkwFloor = ctx.generator ? (ctx.generator.batterij_seed_kw || 0) : 0;
    const bkwAs = [bkwFloor]; [0.5, 1].forEach(f => { const v = Math.round(bkwFloor + (ctx.bessMaxKw - bkwFloor) * f); if (v > (bkwAs[bkwAs.length - 1] || 0)) bkwAs.push(v); });
    const cellDefs = []; for (const bkw of bkwAs) for (const pv of pvAs) cellDefs.push({ bkw, pv });
    const t0 = Date.now();
    const raw = await _pmap(cellDefs, ({ bkw, pv }) => _ekBedrijfCel(ctx, pv, bkw));
    // Referentie. Twee-bakjes (dieselbasis-doc) bij een generator: de HUIDIGE situatie is gebouw-elektriciteit
    // ZONDER de generator-last (die draait vandaag op diesel) PLUS de vermeden dieselkost. De sweep-cellen draaien
    // mét de generator-last (de batterij dekt de shots), zodat besparing = ref − cel zowel de vermeden diesel als
    // de bijgekomen stroomkost correct nettot. Zonder generator = gewoon de (0,0)-cel.
    let refKost;
    if (ctx.generator) {
      const uiRef = ctx.baseUiNoGen(); uiRef.pv_kwp = 0; uiRef.batterijId = ''; uiRef.batterijCustom = null;
      const rRef = await _runSimulatorOnce(buildSimInput(_variantUi(uiRef, 'sturing')));
      const elekRef = Number((rRef.jaarfactuur || rRef.factuur || {}).subtotaal_excl_btw) || 0;
      refKost = Math.round(elekRef + (ctx.generator.diesel_vermeden_eur_jaar || 0));
    } else {
      const ref = raw.find(c => c.battKw === 0 && c.pvKwp === 0); refKost = ref ? ref.kost : null;
    }
    const anchors = raw.map(c => { const besp = (refKost != null) ? (refKost - c.kost) : null;
      const rend = (besp != null && c.capex > 0) ? (besp / c.capex * 100) : null;
      return Object.assign({}, c, { ref_kost: refKost, besparing: (besp != null ? Math.round(besp) : null), rendement: (rend != null ? Math.round(rend * 100) / 100 : null) }); });
    console.log(`[bedrijf-sweep] ${anchors.length} cellen in ${Date.now() - t0}ms (pvMax ${ctx.pvMaxKwp} kWp, bessMax ${ctx.bessMaxKw} kW, profiel ${ctx.profielNaam})`);
    return res.json({ ok: true, anchors, grid: { batt_kw_as: bkwAs, pv_kwp_as: pvAs }, referentie_kost: refKost,
      dimensionering: { pv_max_kwp: ctx.pvMaxKwp, bess_max_kw: ctx.bessMaxKw, generator_kva: ctx.genKva, laadplein_kw: Math.round(ctx.lpKw), toegangsvermogen_kw: ctx.toegang, profiel: ctx.profielNaam },
      generator: ctx.generator || null,
      indicatief: true, _meta: { server_version: SERVER_VERSIE, cellen: anchors.length, elapsed_ms: Date.now() - t0 } });
  } catch (e) { console.error('[bedrijf-sweep] faalde:', e.message); return res.status(500).json({ error: e.message }); }
});

// Losse cel: één echte dispatch voor een door de klant gekozen (pv_kwp, batt_kw)-combinatie.
// De schil berekent besparing/rendement zelf met de referentiekost uit de sweep.
app.post('/api/energiekompas/bedrijf-cel', async (req, res) => {
  try {
    if (!MARKT) return res.status(503).json({ error: 'Marktdata nog niet geladen — probeer over 30 s opnieuw', markt_status: MARKT_STATUS });
    const b = req.body || {};
    const ctx = _ekBedrijfCtx(b);
    if (!(ctx.afname > 0)) return res.status(400).json({ error: 'afname_mwh (jaarverbruik) verplicht' });
    const _n0 = (v) => { const x = Number(v); return (isFinite(x) && x > 0) ? x : 0; };
    const pv = Math.round(_n0(b.pv_kwp));
    const bkw = Math.round(_n0(b.batt_kw));
    const t0 = Date.now();
    const cel = await _ekBedrijfCel(ctx, pv, bkw);
    console.log(`[bedrijf-cel] pv ${pv} kWp · batt ${bkw} kW → kost ${cel.kost} in ${Date.now() - t0}ms`);
    return res.json({ ok: true, cel, _meta: { server_version: SERVER_VERSIE, elapsed_ms: Date.now() - t0 } });
  } catch (e) { console.error('[bedrijf-cel] faalde:', e.message); return res.status(500).json({ error: e.message }); }
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

    // v15.58.0: PER-DAG-VERMOGEN VANGNET (YUSO/Luminus). Reken een "kW × dagen"-gepresenteerd
    // toegangsvermogen terug naar de echte kW vóór het naar de wizard/simulator gaat. Detectie via
    // de load factor: onmogelijk laag (<2%) én na ÷dagen weer plausibel (≤100%) ⇒ per-dag ⇒ ÷dagen.
    // Enkel op de kW-vermogenspost; de €-bedragen en kWh-posten blijven ongemoeid.
    try {
      const _pdDate = (s) => { if (!s) return null;
        const m = String(s).match(/(\d{1,2})[\/-](\d{1,2})[\/-](\d{4})/);
        if (m) return new Date(+m[3], +m[2]-1, +m[1]);
        const d = new Date(s); return isNaN(d.getTime()) ? null : d; };
      const _a = _pdDate(_bc.periodeVan), _b = _pdDate(_bc.periodeTot);
      const dgn = (_a && _b) ? Math.max(1, Math.round((_b - _a)/86400000) + 1) : null;
      const kw = +_bc.aansluitVermogenKva || 0, afn = +_bc.afnameKwh || 0;
      if (kw > 0 && dgn && dgn > 1 && afn > 0) {
        const lfRaw = afn / (kw * dgn * 24);
        if (lfRaw < 0.02 && (lfRaw * dgn) <= 1.0) {
          const echt = Math.round(kw / dgn);
          _bc._perDagVermogenCorrectie = {
            rauw_kva: Math.round(kw), dagen: dgn, gecorrigeerd_kva: echt,
            load_factor_rauw_pct: Math.round(lfRaw * 10000) / 100,
            load_factor_gecorrigeerd_pct: Math.round(lfRaw * dgn * 10000) / 100
          };
          _bc.aansluitVermogenKva = echt;
          console.warn(`[factuur-extract] per-dag-vermogen: ${Math.round(kw)} kW → ${echt} kW (${dgn} dagen, LF ${(lfRaw*100).toFixed(2)}%)`);
        }
      }
    } catch (e) { console.warn('[factuur-extract] per-dag-vermogen-check faalde (niet-blokkerend):', e.message); }

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

// ─── v15.44: PROJECT-ID + RAPPORT-OPSLAG (PDF) in dezelfde private bucket als de factuur ──
// Elk project krijgt een STABIEL identificatienummer (deterministisch uit de projectnaam → idempotent,
// zelfde project = zelfde nummer, geen teller nodig). Dat nummer komt op elk rapport (titel + voettekst)
// en in het opslagpad. Gegenereerde rapporten worden als PDF bewaard onder rapporten/<project-id>/ in de
// bestaande 'facturen'-bucket, zodat ze samen met de factuur oproepbaar zijn (o.a. voor de rapport-chatbox).
function _projectId(project){
  const s = String(project||'').trim().toLowerCase();
  let h = 0x811c9dc5 >>> 0;
  for(let i=0;i<s.length;i++){ h ^= s.charCodeAt(i); h = Math.imul(h, 0x01000193) >>> 0; }
  const code = h.toString(36).toUpperCase().padStart(7,'0').slice(-7);
  return 'FLX-' + code.slice(0,3) + '-' + code.slice(3);
}
async function _bucketList(prefix){
  if (!SUPABASE_OK) throw new Error('Supabase niet geconfigureerd');
  const url = `${SUPABASE_URL}/storage/v1/object/list/${FACTUREN_BUCKET}`;
  const r = await fetch(url, { method:'POST',
    headers:{ 'Authorization':`Bearer ${SUPABASE_SERVICE_KEY}`, 'Content-Type':'application/json' },
    body: JSON.stringify({ prefix, limit:100, sortBy:{ column:'name', order:'desc' } }) });
  if (!r.ok) throw new Error(`storage list ${prefix}: HTTP ${r.status}`);
  return r.json();
}

// POST /api/project-id  { project }  → { id }  (+ registratie in de bucket onder projecten/)
app.post('/api/project-id', async (req, res) => {
  try {
    const u = await resolveUser(req);
    if (!u) return res.status(401).json({ error: 'Niet ingelogd' });
    const project = String((req.body||{}).project || '').trim();
    if (!project) return res.status(400).json({ error: 'project verplicht' });
    const id = _projectId(project);
    if (SUPABASE_OK) {
      try {
        const veilig = project.replace(/[^A-Za-z0-9._-]/g,'_').slice(0,60);
        const reg = JSON.stringify({ id, project, aangemaakt: new Date().toISOString(), door: u.name || u.id || null });
        await _factuurUpload(Buffer.from(reg,'utf8').toString('base64'), 'application/json', `projecten/${veilig}.json`);
      } catch(e){ console.warn(`[project-id] registratie faalde (niet blokkerend): ${e.message}`); }
    }
    return res.json({ id, project });
  } catch (e) {
    console.error('[project-id] fout:', e.message);
    return res.status(500).json({ error: e.message });
  }
});

// POST /api/rapport-opslaan  { project, project_id, type, filenaam, pdf_base64, meta? }
//   → bewaart de PDF onder rapporten/<project-id>/<type>-<ts>.pdf in de private bucket
app.post('/api/rapport-opslaan', async (req, res) => {
  try {
    if (!SUPABASE_OK) return res.status(503).json({ error: 'Opslag niet geconfigureerd' });
    const u = await resolveUser(req);
    if (!u) return res.status(401).json({ error: 'Niet ingelogd' });
    const b = req.body || {};
    const project = String(b.project || '').trim();
    const pid = String(b.project_id || (project ? _projectId(project) : '')).trim();
    if (!pid) return res.status(400).json({ error: 'project of project_id verplicht' });
    if (!b.pdf_base64 || typeof b.pdf_base64 !== 'string') return res.status(400).json({ error: 'pdf_base64 (string) verplicht' });
    const type = String(b.type || 'rapport').replace(/[^A-Za-z0-9._-]/g,'_').slice(0,40);
    // grootte-check (base64 → bytes ≈ ×0,75)
    const bytes = Math.floor(b.pdf_base64.length * 0.75);
    if (bytes > 20 * 1024 * 1024) return res.status(413).json({ error: 'PDF te groot (>20 MB)' });
    const veiligPid = pid.replace(/[^A-Za-z0-9._-]/g,'_').slice(0,40);
    const pad = `rapporten/${veiligPid}/${type}-${Date.now()}.pdf`;
    await _factuurUpload(b.pdf_base64, 'application/pdf', pad);
    // metadata-zijkaartje (JSON) naast de PDF, handig voor de latere chatbox/recall
    try {
      const meta = JSON.stringify({ project, project_id:pid, type, pad, filenaam:b.filenaam||null,
        bewaard: new Date().toISOString(), door: u.name||u.id||null, bytes, extra: b.meta||null });
      await _factuurUpload(Buffer.from(meta,'utf8').toString('base64'), 'application/json', pad.replace(/\.pdf$/, '.json'));
    } catch(e){ /* niet blokkerend */ }
    let signed = null; try { signed = await _factuurSignedUrl(pad, 600); } catch(e){}
    console.log(`[rapport-opslaan] ${FACTUREN_BUCKET}/${pad} (${(bytes/1024).toFixed(0)} KB) type=${type} pid=${pid}`);
    return res.json({ ok:true, bucket:FACTUREN_BUCKET, pad, project_id:pid, bytes, signed_url:signed });
  } catch (e) {
    console.error('[rapport-opslaan] faalde:', e.message);
    return res.status(500).json({ error: e.message });
  }
});

// GET /api/rapporten?project_id=FLX-XXX-XXX  → lijst van bewaarde rapporten voor een project
app.get('/api/rapporten', async (req, res) => {
  try {
    if (!SUPABASE_OK) return res.status(503).json({ error: 'Opslag niet geconfigureerd' });
    const u = await resolveUser(req);
    if (!u) return res.status(401).json({ error: 'Niet ingelogd' });
    const pid = String(req.query.project_id || '').replace(/[^A-Za-z0-9._-]/g,'_').slice(0,40);
    if (!pid) return res.status(400).json({ error: 'project_id verplicht' });
    const lijst = await _bucketList(`rapporten/${pid}/`);
    const pdfs = (Array.isArray(lijst)?lijst:[]).filter(o => o.name && /\.pdf$/.test(o.name))
      .map(o => ({ pad:`rapporten/${pid}/${o.name}`, naam:o.name, bijgewerkt:o.updated_at||o.created_at||null }));
    return res.json({ project_id:pid, rapporten:pdfs });
  } catch (e) {
    console.error('[rapporten] lijst faalde:', e.message);
    return res.status(500).json({ error: e.message });
  }
});

// ─── v15.45: KAMINO-projectrecord + toegangspoort (project-ID + e-mail) ───────────
// Zodat klant én adviseur een bestaand project later heropenen om een volgende studie te doen —
// zonder mailverkeer. Record = kamino/<id>.json in de private bucket: id, naam, klant/adviseur (incl.
// e-mail), factuurref en de reeds gedane studies. Bewaren vereist login; heropenen mag zonder login
// maar de e-mail moet matchen met klant of adviseur (tweede factor naast het ondoorzichtige FLX-id).
let _kaminoLijstCache = { ts: 0, records: [] };   // v15.91.1: in-memory cache voor /api/kamino/projecten (was 50× sequentieel → ~3s)
app.post('/api/kamino/project', async (req, res) => {
  try {
    if (!SUPABASE_OK) return res.status(503).json({ error: 'Opslag niet geconfigureerd' });
    const u = await resolveUser(req);
    if (!u) return res.status(401).json({ error: 'Niet ingelogd' });
    const b = req.body || {};
    const id = String(b.id || '').trim().toUpperCase();
    if (!/^FLX-[A-Z0-9]{3}-[A-Z0-9]{3,4}$/.test(id)) return res.status(400).json({ error: 'geldig project-id verplicht' });
    const veilig = id.replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 40);
    let bestaand = {};
    try { bestaand = JSON.parse(await _factuurDownload(`kamino/${veilig}.json`)); } catch (e) {}
    // v15.91 (Fase 1): meerdere invoer-scenario's náást elkaar onder één project.
    const _scNaam = String(b.scenario || (b.input && b.input.scenario) || '').trim();
    const _scKey = (_scNaam ? _scNaam.toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '') : 'standaard').slice(0, 60) || 'standaard';
    const scenarios = Object.assign({}, bestaand.scenarios || {});
    if (b.input || b.baseCase) {
      scenarios[_scKey] = {
        scenario: _scNaam || 'standaard',
        input: b.input || (scenarios[_scKey] && scenarios[_scKey].input) || null,
        baseCase: b.baseCase || (scenarios[_scKey] && scenarios[_scKey].baseCase) || null,
        bijgewerkt: new Date().toISOString(), door: u.name || u.id || null
      };
    }
    const rec = {
      id, naam: b.naam || bestaand.naam || '',
      klant: b.klant || bestaand.klant || {},
      adviseur: b.adviseur || bestaand.adviseur || { id: u.id, naam: u.naam, email: u.email },   // v15.92 (Fase 2): registreer de opslaande adviseur als er nog geen is
      partner: b.partner || bestaand.partner || null,                        // v15.92 (Fase 2): EPC/partner-id (uit de ?partner=-schil) voor partnermanager-scoping
      factuur: b.factuur || bestaand.factuur || '',
      baseCase: b.baseCase || bestaand.baseCase || null,                     // factuurgegevens voor een volgende studie
      input: b.input || bestaand.input || null,                              // v15.90 (Fase 1): volledige invoer-snapshot per flow (universele save)
      scenario: b.scenario || (b.input && b.input.scenario) || bestaand.scenario || '',   // v15.90.1: scenario-label voor de terughaal-dropdown (laatst bewaarde)
      scenarios,                                                             // v15.91: map <scenarioKey> → {scenario,input,baseCase,bijgewerkt} — meerdere scenario's per project
      profiel: b.profiel || bestaand.profiel || null,                        // v15.57 (Johan 03-08): gekozen verbruiksprofiel — nodig voor carryover naar de interactieve simulator (manager-open)
      pv: b.pv || bestaand.pv || null,                                       // bestaande-PV (kWp + injectie MWh/jr) voor SolarActive
      studies: Object.assign({}, bestaand.studies || {}, b.studies || {}),   // gedane studies accumuleren
      profielen: Object.assign({}, bestaand.profielen || {}, b.profielen || {}),   // v15.66: opgeladen Fluvius-profielen (afname/injectie) — behouden bij elke save
      aangemaakt: bestaand.aangemaakt || new Date().toISOString(),
      bijgewerkt: new Date().toISOString(), door: u.name || u.id || null
    };
    await _factuurUpload(Buffer.from(JSON.stringify(rec), 'utf8').toString('base64'), 'application/json', `kamino/${veilig}.json`);
    try { _kaminoLijstCache.ts = 0; } catch (e) {}   // v15.91.1: dropdown-cache verversen zodat het nieuwe project meteen verschijnt
    // v15.96 (Fase 2c): AUTO klant-account (best-effort, non-blocking). Maakt enkel een profiles-rij
    // (role 'klant') als er nog geen profiel is met dat e-mail; de auth.users-rij ontstaat vanzelf bij
    // de eerste OTP-login (shouldCreateUser). De klant kan zo via /apps/klant.html enkel dit project
    // openen (_magProjectOpenen matcht op rec.klant.email). Blokkeert de save NOOIT.
    try {
      const kEmail = String((rec.klant && rec.klant.email) || '').trim().toLowerCase();
      if (kEmail && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(kEmail)) {
        const bestaat = await _sbRest(`profiles?email=eq.${encodeURIComponent(kEmail)}&select=email`);
        if (!(Array.isArray(bestaat) && bestaat.length)) {
          await _sbRest('profiles', {
            method: 'POST', headers: { 'Prefer': 'return=minimal' },
            body: { email: kEmail, name: (rec.klant && (rec.klant.naam || rec.klant.name)) || '', company: rec.partner || '', role: 'klant', status: 'invited' },
          });
          console.log(`[kamino/project] auto klant-account aangemaakt: ${kEmail} (${id})`);
        }
      }
    } catch (e) { console.warn('[kamino/project] auto klant-account faalde (niet-blokkerend):', e.message); }
    return res.json({ ok: true, id });
  } catch (e) {
    console.error('[kamino/project] bewaren faalde:', e.message);
    return res.status(500).json({ error: e.message });
  }
});

// ─── v15.66.0 — FLUVIUS-PROFIEL UPLOAD (MANAGER-ONLY) ────────────────────────────────────────
// POST /api/kamino/profiel-upload { project_id, type:'afname'|'injectie', csv, ean?, klant?, meta? }
// Slaat het door de converter gemaakte kwartier-CSV op in de factuur-bucket onder de naam
// profielen/<projectID>_<EAN>_<klant>_<type>.csv en registreert de meta in het projectrecord
// (rec.profielen[type]). Enkel voor managers. Het profiel is zo gedeeld over de 4 tegels van
// hetzelfde project. (FASE 1: opslaan + delen; het gebruik in de analyse volgt in fase 2.)
app.post('/api/kamino/profiel-upload', async (req, res) => {
  const u = await _managerGuard(req, res); if (!u) return;   // manager-only
  const b = req.body || {};
  const pid = String(b.project_id || '').trim().toUpperCase();
  const type = String(b.type || '').toLowerCase();
  if (!/^FLX-[A-Z0-9]{3}-[A-Z0-9]{3,4}$/.test(pid)) return res.status(400).json({ error: 'geldig project-id verplicht' });
  if (type !== 'afname' && type !== 'injectie') return res.status(400).json({ error: "type moet 'afname' of 'injectie' zijn" });
  if (!b.csv || typeof b.csv !== 'string') return res.status(400).json({ error: 'csv verplicht' });
  if (b.csv.length > 12 * 1024 * 1024) return res.status(413).json({ error: 'CSV te groot (max ~12 MB)' });
  try {
    const veilig = (s, fb) => String(s || fb).replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 60);
    const ean = veilig(b.ean, 'geenEAN');
    const klant = veilig(b.klant, 'klant');
    const veiligPid = pid.replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 40);
    const bestand = `profielen/${veiligPid}_${ean}_${klant}_${type}.csv`;
    await _factuurUpload(Buffer.from(b.csv, 'utf8').toString('base64'), 'text/csv', bestand);

    // Projectrecord bijwerken met de profiel-meta (behouden via de whitelist in /api/kamino/project).
    let rec = {};
    try { rec = JSON.parse(await _factuurDownload(`kamino/${veiligPid}.json`)); } catch (e) {}
    rec.id = rec.id || pid;
    rec.profielen = rec.profielen || {};
    const m = b.meta || {};
    rec.profielen[type] = {
      bestand, ean: b.ean || null, klant: b.klant || null,
      mwh: (m.mwh != null ? Number(m.mwh) : null),
      piek_kw: (m.piek_kw != null ? Number(m.piek_kw) : null),
      van: m.van || null, tot: m.tot || null, n_kwartier: (m.n != null ? Number(m.n) : null),
      door: u.email || u.id || null, op: new Date().toISOString(),
    };
    rec.bijgewerkt = new Date().toISOString();
    try { await _factuurUpload(Buffer.from(JSON.stringify(rec), 'utf8').toString('base64'), 'application/json', `kamino/${veiligPid}.json`); } catch (e) {
      console.warn('[kamino/profiel-upload] projectrecord bijwerken faalde (niet-blokkerend):', e.message);
    }
    console.log(`[kamino/profiel-upload] ${type}-profiel bewaard: ${bestand} (${rec.profielen[type].mwh} MWh, piek ${rec.profielen[type].piek_kw} kW)`);
    return res.json({ ok: true, type, bestand, profielen: rec.profielen });
  } catch (e) {
    console.error('[kamino/profiel-upload] fout:', e.message);
    return res.status(500).json({ error: e.message });
  }
});

// POST /api/kamino/project-open  { id, email }  → projectrecord (email moet matchen met klant of adviseur)
app.post('/api/kamino/project-open', async (req, res) => {
  try {
    if (!SUPABASE_OK) return res.status(503).json({ error: 'Opslag niet geconfigureerd' });
    const b = req.body || {};
    const id = String(b.id || '').trim().toUpperCase();
    const email = String(b.email || '').trim().toLowerCase();
    if (!/^FLX-[A-Z0-9]{3}-[A-Z0-9]{3,4}$/.test(id) || !/\S+@\S+\.\S+/.test(email))
      return res.status(400).json({ error: 'project-id en e-mail verplicht' });
    const veilig = id.replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 40);
    let rec;
    try { rec = JSON.parse(await _factuurDownload(`kamino/${veilig}.json`)); }
    catch (e) { return res.status(404).json({ error: 'Geen project gevonden met dit nummer.' }); }
    const mails = [ (rec.klant && rec.klant.email) || '', (rec.adviseur && rec.adviseur.email) || '' ].map(function (s) { return String(s).trim().toLowerCase(); }).filter(Boolean);
    if (mails.indexOf(email) < 0) return res.status(403).json({ error: 'Dit e-mailadres hoort niet bij dit project.' });
    // v15.53: geef mee welke tegels een BEWAARD rapport hebben (rapporten/<pid>/kamino-<tegel>.json), zodat Kamino
    // per tegel een "bekijk vorig rapport"-knop kan tonen bij een heropend project (niet enkel "herrekenen").
    let studies = [];
    try {
      const lijst = await _bucketList(`rapporten/${veilig}/`);
      for (const o of (Array.isArray(lijst) ? lijst : [])) {
        if (o.name && /^kamino-.+\.json$/i.test(o.name)) studies.push(o.name.replace(/^kamino-/, '').replace(/\.json$/, ''));
      }
    } catch (e) { /* niet-blokkerend */ }
    console.log(`[kamino/project-open] ${id} geopend door ${email}`);
    return res.json({ ok: true, project: rec, rapporten: { studies } });
  } catch (e) {
    console.error('[kamino/project-open] faalde:', e.message);
    return res.status(500).json({ error: e.message });
  }
});

// ─── v15.50: KAMINO auto-rapport in de bucket ────────────────────────────────
// Na élke tegel-berekening heeft Kamino het rapport al in handen (kern + reportData/HTML). Dit endpoint
// zet dat artefact meteen in de bucket (rapporten/<pid>/kamino-<tegel>.json), nog vóór het geopend wordt.
// Zo hoeft niemand het rapport nog manueel (PDF) naar Supabase te laden. Overschrijft = één actueel
// rapport per tegel. De losse PDF-opslag (/api/rapport-opslaan) blijft bestaan voor een downloadbare PDF.
app.post('/api/kamino/rapport-bewaar', async (req, res) => {
  try {
    if (!SUPABASE_OK) return res.status(503).json({ error: 'Opslag niet geconfigureerd' });
    const u = await resolveUser(req);
    if (!u) return res.status(401).json({ error: 'Niet ingelogd' });
    const b = req.body || {};
    const pid = String(b.project_id || '').replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 40);
    const tegel = String(b.tegel || '').replace(/[^a-z]/gi, '').slice(0, 20);
    if (!pid || !tegel) return res.status(400).json({ error: 'project_id en tegel verplicht' });
    const artefact = {
      project_id: pid, tegel,
      kern: b.kern || null, reportKey: b.reportKey || null,
      reportData: b.reportData || null, reportHtml: b.reportHtml || null,
      bewaard: new Date().toISOString(), door: u.name || u.id || null
    };
    const json = JSON.stringify(artefact);
    if (json.length > 8 * 1024 * 1024) return res.status(413).json({ error: 'rapport te groot (>8 MB)' });
    const pad = `rapporten/${pid}/kamino-${tegel}.json`;
    await _factuurUpload(Buffer.from(json, 'utf8').toString('base64'), 'application/json', pad);
    console.log(`[kamino/rapport-bewaar] ${pad} (${(json.length/1024).toFixed(0)} KB) tegel=${tegel}`);
    return res.json({ ok: true, pad });
  } catch (e) {
    console.error('[kamino/rapport-bewaar] faalde:', e.message);
    return res.status(500).json({ error: e.message });
  }
});

// GET /api/kamino/rapport-open?project_id=&tegel=  → het bewaarde Kamino-rapportartefact (voor recall)
app.get('/api/kamino/rapport-open', async (req, res) => {
  try {
    if (!SUPABASE_OK) return res.status(503).json({ error: 'Opslag niet geconfigureerd' });
    const u = await resolveUser(req);
    if (!u) return res.status(401).json({ error: 'Niet ingelogd' });
    const pid = String(req.query.project_id || '').replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 40);
    const tegel = String(req.query.tegel || '').replace(/[^a-z]/gi, '').slice(0, 20);
    if (!pid || !tegel) return res.status(400).json({ error: 'project_id en tegel verplicht' });
    // v15.96 (Fase 2c): toegangsgate. Nu er externe klant-accounts zijn, mag niet elke ingelogde
    // gebruiker elk rapport-artefact opvragen — enkel wie het project mag openen (manager/partnermanager/
    // eigenaar/adviseur/klant). Additief: legitieme callers (Kamino/simulator manager-open) passeren gewoon.
    try {
      const rec = JSON.parse(await _factuurDownload(`kamino/${pid}.json`));
      if (!_magProjectOpenen(u, rec)) return res.status(403).json({ error: 'Geen toegang tot dit rapport.' });
    } catch (e) { /* geen projectrecord (los rapport) → val terug op login-only, zoals voorheen */ }
    let art;
    try { art = JSON.parse(await _factuurDownload(`rapporten/${pid}/kamino-${tegel}.json`)); }
    catch (e) { return res.status(404).json({ error: 'geen bewaard rapport' }); }
    return res.json({ ok: true, rapport: art });
  } catch (e) {
    console.error('[kamino/rapport-open] faalde:', e.message);
    return res.status(500).json({ error: e.message });
  }
});

// GET /api/kamino/project-get?id=FLX-...  → MANAGER-ONLY: haalt het projectrecord op ZONDER e-mail-check.
// De manager mag elk project inzien om te ondersteunen (data bekijken, bestaande rapporten openen, nieuwe
// scenario's maken in de simulator). Geeft ook de bestaande rapporten mee: PDF's (met signed URL) + de
// per-tegel Kamino-artefacten.
app.get('/api/kamino/project-get', async (req, res) => {
  try {
    if (!SUPABASE_OK) return res.status(503).json({ error: 'Opslag niet geconfigureerd' });
    const u = await resolveUser(req);
    if (!u) return res.status(401).json({ error: 'Niet ingelogd' });
    const id = String(req.query.id || '').trim().toUpperCase();
    if (!/^FLX-[A-Z0-9]{3}-[A-Z0-9]{3,4}$/.test(id)) return res.status(400).json({ error: 'geldig project-id verplicht' });
    const veilig = id.replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 40);
    let rec;
    try { rec = JSON.parse(await _factuurDownload(`kamino/${veilig}.json`)); }
    catch (e) { return res.status(404).json({ error: 'Geen project gevonden met dit nummer.' }); }
    // v15.90 (Fase 1): toegang = manager OF eigenaar/adviseur/klant (niet langer manager-only).
    if (!_magProjectOpenen(u, rec)) return res.status(403).json({ error: 'Geen toegang tot dit project.' });
    // v15.91 (Fase 1): optioneel een specifiek invoer-scenario overlayen op input/baseCase.
    const scq = String(req.query.scenario || '').trim();
    if (scq && rec.scenarios && rec.scenarios[scq]) {
      rec.input = rec.scenarios[scq].input || rec.input;
      rec.baseCase = rec.scenarios[scq].baseCase || rec.baseCase;
      rec.scenario = rec.scenarios[scq].scenario || rec.scenario;
    }
    let pdfs = [], studies = [];
    try {
      const lijst = await _bucketList(`rapporten/${veilig}/`);
      for (const o of (Array.isArray(lijst) ? lijst : [])) {
        if (!o.name) continue;
        const pad = `rapporten/${veilig}/${o.name}`;
        if (/\.pdf$/i.test(o.name)) {
          let url = null; try { url = await _factuurSignedUrl(pad, 3600); } catch (e) {}
          pdfs.push({ naam: o.name, pad, url, bijgewerkt: o.updated_at || o.created_at || null });
        } else if (/^kamino-.+\.json$/i.test(o.name)) {
          studies.push({ tegel: o.name.replace(/^kamino-/, '').replace(/\.json$/, ''), pad, bijgewerkt: o.updated_at || o.created_at || null });
        }
      }
    } catch (e) { /* niet-blokkerend */ }
    console.log(`[kamino/project-get] ${id} geopend door ${u.role || 'user'} ${u.naam || u.id}`);
    return res.json({ ok: true, project: rec, rapporten: { pdfs, studies } });
  } catch (e) {
    console.error('[kamino/project-get] faalde:', e.message);
    return res.status(500).json({ error: e.message });
  }
});

// ─── v15.90 (Fase 1) — GET /api/kamino/projecten ────────────────────────────────────────────
// Rol-gefilterde lijst van kamino-projectrecords voor de dropdown "project terughalen".
// Manager: alle projecten; anders enkel projecten waar de gebruiker eigenaar/adviseur/klant is.
// Additief — laat het bestaande /api/projecten (scenario-namen) ongemoeid.
app.get('/api/kamino/projecten', async (req, res) => {
  try {
    if (!SUPABASE_OK) return res.status(503).json({ error: 'Opslag niet geconfigureerd' });
    const u = await resolveUser(req);
    if (!u) return res.status(401).json({ error: 'Niet ingelogd' });
    // v15.91.1: parallelle download + korte in-memory cache (was 50× sequentieel → ~3s).
    let records = _kaminoLijstCache.records, afgekapt = false;
    if (Date.now() - _kaminoLijstCache.ts > 20000 || !records.length) {
      let lijst = [];
      try { lijst = await _bucketList('kamino/'); } catch (e) { lijst = []; }
      const jsons = (Array.isArray(lijst) ? lijst : []).filter(o => o.name && /\.json$/i.test(o.name));
      afgekapt = jsons.length >= 100;
      const downloaded = await Promise.all(jsons.map(o =>
        _factuurDownload(`kamino/${o.name}`).then(t => { try { return JSON.parse(t); } catch (e) { return null; } }).catch(() => null)));
      records = downloaded.filter(r => r && r.id);
      _kaminoLijstCache = { ts: Date.now(), records };
    }
    const out = [];
    for (const rec of records) {
      if (!_magProjectOpenen(u, rec)) continue;
      const naam = rec.naam || (rec.klant && (rec.klant.naam || rec.klant.name)) || rec.id;
      const klant = (rec.klant && (rec.klant.naam || rec.klant.name)) || '';
      const adviseur = (rec.adviseur && (rec.adviseur.naam || rec.adviseur.name || rec.adviseur.email)) || '';
      const scKeys = rec.scenarios ? Object.keys(rec.scenarios) : [];
      if (scKeys.length) {
        // v15.91: één rij per bewaard invoer-scenario
        for (const k of scKeys) {
          const sc = rec.scenarios[k] || {};
          out.push({ id: rec.id, scenarioKey: k, naam, klant, adviseur,
            scenario: sc.scenario || k,
            bijgewerkt: sc.bijgewerkt || rec.bijgewerkt || rec.aangemaakt || null,
            heeftInput: !!sc.input, heeftBaseCase: !!sc.baseCase });
        }
      } else {
        out.push({ id: rec.id, scenarioKey: '', naam, klant, adviseur,
          scenario: rec.scenario || (rec.input && rec.input.scenario) || '',
          bijgewerkt: rec.bijgewerkt || rec.aangemaakt || null,
          heeftInput: !!rec.input, heeftBaseCase: !!rec.baseCase });
      }
    }
    out.sort((a, b) => String(b.bijgewerkt || '').localeCompare(String(a.bijgewerkt || '')));
    if (afgekapt) console.warn('[kamino/projecten] bucket-list op limiet 100 — mogelijk afgekapt');
    return res.json({ projecten: out, afgekapt });
  } catch (e) {
    console.error('[kamino/projecten] faalde:', e.message);
    return res.status(500).json({ error: e.message });
  }
});

// ─── v15.98 (Fase 3) — MANDAAT-WACHTRIJ ──────────────────────────────────────────
// De mandaat-aanvraag wordt op het projectrecord bewaard (rec.mandaat) zodat een aanvraag NIET verloren gaat
// wanneer de PC/Chrome/Fluvius-sessie niet live is (er is nog geen always-on VPS — zie KB Fase 3). De Fluvius-
// flow (skill/watchdog) haalt de wachtrij op zodra hij ingelogd is, verwerkt EAN per EAN en schrijft de status
// terug. Additief + geguard. Statussen: wachtrij → aangevraagd → actief → geleverd (+ adres_mismatch / fout).
const _MANDAAT_STATUSSEN = ['wachtrij', 'aangevraagd', 'actief', 'geleverd', 'adres_mismatch', 'geannuleerd', 'fout'];
async function _kaminoRecLaden(id) {
  const veilig = String(id).replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 40);
  try { return { veilig, rec: JSON.parse(await _factuurDownload(`kamino/${veilig}.json`)) }; }
  catch (e) { return { veilig, rec: null }; }
}
function _mandaatOverallStatus(eans) {
  const actief = (eans || []).filter(x => x.status !== 'geannuleerd');   // geannuleerde EAN's tellen niet mee
  if (!actief.length) return (eans && eans.length) ? 'geannuleerd' : 'wachtrij';
  if (actief.some(x => x.status === 'wachtrij')) return 'wachtrij';
  if (actief.some(x => x.status === 'adres_mismatch')) return 'adres_mismatch';
  if (actief.some(x => x.status === 'aangevraagd')) return 'aangevraagd';
  if (actief.some(x => x.status === 'actief')) return 'actief';
  if (actief.every(x => x.status === 'geleverd')) return 'geleverd';
  return 'aangevraagd';
}

// POST /api/mandaat/aanvraag  { project_id, ean | eans:[{ean,richting?}], meter_type?, factuuradres? }
// Zet één of meer EAN's in de wachtrij op het projectrecord. Toegang = wie het project mag openen.
app.post('/api/mandaat/aanvraag', async (req, res) => {
  try {
    if (!SUPABASE_OK) return res.status(503).json({ error: 'Opslag niet geconfigureerd' });
    const u = await resolveUser(req); if (!u) return res.status(401).json({ error: 'Niet ingelogd' });
    const b = req.body || {};
    const id = String(b.project_id || '').trim().toUpperCase();
    if (!/^FLX-[A-Z0-9]{3}-[A-Z0-9]{3,4}$/.test(id)) return res.status(400).json({ error: 'geldig project-id verplicht' });
    let items = Array.isArray(b.eans) ? b.eans : (b.ean ? [{ ean: b.ean, richting: b.richting }] : []);
    items = items.map(x => ({ ean: String((x && x.ean) || '').replace(/\s/g, ''), richting: (x && x.richting) || null }))
                 .filter(x => /^\d{18}$/.test(x.ean));
    if (!items.length) return res.status(400).json({ error: 'minstens één geldig EAN (18 cijfers) verplicht' });
    const { veilig, rec } = await _kaminoRecLaden(id);
    if (!rec) return res.status(404).json({ error: 'Geen project gevonden met dit nummer.' });
    if (!_magProjectOpenen(u, rec)) return res.status(403).json({ error: 'Geen toegang tot dit project.' });
    const m = rec.mandaat || { status: 'wachtrij', meter_type: null, factuuradres: null, eans: [], aangemaakt: new Date().toISOString() };
    if (b.meter_type) m.meter_type = (String(b.meter_type).toUpperCase() === 'AMR' ? 'AMR' : 'SMR');
    if (b.factuuradres) m.factuuradres = String(b.factuuradres).slice(0, 300);
    else if (!m.factuuradres && rec.input && rec.input.adres) m.factuuradres = String(rec.input.adres).slice(0, 300);
    m.aangevraagd_door = m.aangevraagd_door || { id: u.id, naam: u.naam, email: u.email, rol: u.role };
    m.eans = m.eans || [];
    // v15.101 (Johan 27-08): vóór een NIEUWE aanvraag checken of we die EAN al kennen in de LOSSE lijst
    // (= onze Fluvius-kennis na sync). Zo ja → die bestaande mandaat KOPPELEN aan dit project (status/
    // referentienr behouden) i.p.v. opnieuw aan te vragen, en uit de losse lijst halen.
    let losRec = null; let losGewijzigd = false;
    try { losRec = JSON.parse(await _factuurDownload('mandaat_los/los.json')); } catch (e) { losRec = null; }
    let gekoppeld = 0, nieuw = 0;
    for (const it of items) {
      const best = m.eans.find(x => x.ean === it.ean);
      if (best) { if (it.richting && !best.richting) best.richting = it.richting; continue; }   // niet dupliceren
      let losHit = null;
      if (losRec && Array.isArray(losRec.eans)) {
        const idx = losRec.eans.findIndex(x => x.ean === it.ean);
        if (idx >= 0) { losHit = losRec.eans[idx]; losRec.eans.splice(idx, 1); losGewijzigd = true; }
      }
      if (losHit) {   // bestaande (bv. actieve) mandaat koppelen — geen her-aanvraag, status blijft
        m.eans.push(Object.assign({}, losHit, { richting: losHit.richting || it.richting || null }));
        gekoppeld++;
      } else {
        m.eans.push({ ean: it.ean, richting: it.richting || null, status: 'wachtrij',
          referentienummer: null, titularis_mail_masked: null, fluvius_adres: null, adres_match: null,
          adres_bevestigd: false, bevestigd_door: null, bevestigd_op: null, opmerking: null,
          in_wachtrij_sinds: new Date().toISOString(), aangevraagd_op: null, actief_op: null, geleverd_op: null });
        nieuw++;
      }
    }
    m.status = _mandaatOverallStatus(m.eans);
    m.bijgewerkt = new Date().toISOString();
    rec.mandaat = m; rec.bijgewerkt = new Date().toISOString();
    await _factuurUpload(Buffer.from(JSON.stringify(rec), 'utf8').toString('base64'), 'application/json', `kamino/${veilig}.json`);
    if (losGewijzigd) {   // gekoppelde EAN's uit de losse lijst verwijderen
      try { await _factuurUpload(Buffer.from(JSON.stringify(losRec), 'utf8').toString('base64'), 'application/json', 'mandaat_los/los.json'); }
      catch (e) { console.warn('[mandaat/aanvraag] losse lijst bijwerken faalde (niet-blokkerend):', e.message); }
    }
    try { _kaminoLijstCache.ts = 0; } catch (e) {}
    console.log(`[mandaat/aanvraag] ${id}: ${nieuw} nieuw in wachtrij + ${gekoppeld} gekoppeld uit losse lijst (door ${u.role} ${u.naam || u.id})`);
    return res.json({ ok: true, project_id: id, nieuw, gekoppeld, mandaat: m });
  } catch (e) { console.error('[mandaat/aanvraag] faalde:', e.message); return res.status(500).json({ error: e.message }); }
});

// GET /api/mandaat/status?project_id=FLX-...  → mandaatblok van dit project (toegang = wie het mag openen).
app.get('/api/mandaat/status', async (req, res) => {
  try {
    if (!SUPABASE_OK) return res.status(503).json({ error: 'Opslag niet geconfigureerd' });
    const u = await resolveUser(req); if (!u) return res.status(401).json({ error: 'Niet ingelogd' });
    const id = String(req.query.project_id || '').trim().toUpperCase();
    if (!/^FLX-[A-Z0-9]{3}-[A-Z0-9]{3,4}$/.test(id)) return res.status(400).json({ error: 'geldig project-id verplicht' });
    const { rec } = await _kaminoRecLaden(id);
    if (!rec) return res.status(404).json({ error: 'Geen project gevonden.' });
    if (!_magProjectOpenen(u, rec)) return res.status(403).json({ error: 'Geen toegang tot dit project.' });
    return res.json({ ok: true, project_id: id, mandaat: rec.mandaat || null });
  } catch (e) { console.error('[mandaat/status] faalde:', e.message); return res.status(500).json({ error: e.message }); }
});

// GET /api/mandaat/wachtrij?status=wachtrij,aangevraagd,actief  → MANAGER-ONLY. Alle mandaat-EAN's over alle
// projecten met een van de gevraagde statussen. Dit is wat de Fluvius-skill/watchdog ophaalt om te verwerken.
app.get('/api/mandaat/wachtrij', async (req, res) => {
  const u = await _managerGuard(req, res); if (!u) return;
  try {
    const gevraagd = String(req.query.status || 'wachtrij').split(',').map(s => s.trim()).filter(Boolean);
    let lijst = [];
    try { lijst = await _bucketList('kamino/'); } catch (e) { lijst = []; }
    const jsons = (Array.isArray(lijst) ? lijst : []).filter(o => o.name && /\.json$/i.test(o.name));
    const recs = await Promise.all(jsons.map(o =>
      _factuurDownload(`kamino/${o.name}`).then(t => { try { return JSON.parse(t); } catch (e) { return null; } }).catch(() => null)));
    const out = [];
    for (const rec of recs) {
      if (!rec || !rec.id || !rec.mandaat || !Array.isArray(rec.mandaat.eans)) continue;
      for (const en of rec.mandaat.eans) {
        if (gevraagd.indexOf(en.status) < 0) continue;
        out.push({ project_id: rec.id, project_naam: rec.naam || (rec.klant && (rec.klant.naam || rec.klant.name)) || rec.id,
          klant: (rec.klant && (rec.klant.naam || rec.klant.name)) || null,
          factuuradres: rec.mandaat.factuuradres || (rec.input && rec.input.adres) || null,
          meter_type: rec.mandaat.meter_type || null,
          ean: en.ean, richting: en.richting || null, status: en.status,
          referentienummer: en.referentienummer || null, fluvius_adres: en.fluvius_adres || null,
          adres_match: en.adres_match, adres_bevestigd: !!en.adres_bevestigd,
          bevestigd_door: en.bevestigd_door || null, bevestigd_op: en.bevestigd_op || null,
          titularis_mail_masked: en.titularis_mail_masked || null, opmerking: en.opmerking || null,
          in_wachtrij_sinds: en.in_wachtrij_sinds || null, aangevraagd_op: en.aangevraagd_op || null,
          actief_op: en.actief_op || null, geleverd_op: en.geleverd_op || null });
      }
    }
    // v15.100 (Fase 3): LOSSE Fluvius-mandaten (via sync ontdekt, geen overeenkomstig project) meenemen.
    try {
      const los = JSON.parse(await _factuurDownload('mandaat_los/los.json'));
      for (const en of (los && los.eans) || []) {
        if (gevraagd.indexOf(en.status) < 0) continue;
        out.push({ project_id: 'LOS', project_naam: 'Fluvius — los (geen project)', los: true,
          klant: (en.aanvrager && en.aanvrager.naam) || null,
          factuuradres: en.factuur_adres || null, meter_type: en.meter_type || null,
          ean: en.ean, richting: en.richting || null, status: en.status,
          aanvrager: en.aanvrager || null, rol: (en.aanvrager && en.aanvrager.rol) || null,
          partner: en.partner || null, aangevraagd_via: en.aangevraagd_via || null, lead_token: en.lead_token || null,
          kwartierdata_aanwezig: !!en.kwartierdata_aanwezig,
          referentienummer: en.referentienummer || null, fluvius_adres: en.fluvius_adres || null,
          adres_match: en.adres_match, adres_bevestigd: !!en.adres_bevestigd,
          bevestigd_door: en.bevestigd_door || null, bevestigd_op: en.bevestigd_op || null,
          titularis_mail_masked: en.titularis_mail_masked || null, opmerking: en.opmerking || null,
          in_wachtrij_sinds: en.in_wachtrij_sinds || null, aangevraagd_op: en.aangevraagd_op || null,
          actief_op: en.actief_op || null, geleverd_op: en.geleverd_op || null });
      }
    } catch (e) { /* geen losse lijst — ok */ }
    out.sort((a, b) => String(a.in_wachtrij_sinds || '').localeCompare(String(b.in_wachtrij_sinds || '')));
    return res.json({ ok: true, statussen: gevraagd, aantal: out.length, items: out });
  } catch (e) { console.error('[mandaat/wachtrij] faalde:', e.message); return res.status(500).json({ error: e.message }); }
});

// POST /api/mandaat/status  { project_id, ean, patch:{status?,referentienummer?,titularis_mail_masked?,fluvius_adres?,adres_match?,bevestigd_door?,opmerking?} }
// MANAGER-ONLY. De Fluvius-skill schrijft de uitkomst per EAN terug → rec.mandaat groeit mee = de controlelijst.
app.post('/api/mandaat/status', async (req, res) => {
  const u = await _managerGuard(req, res); if (!u) return;
  try {
    const b = req.body || {};
    const id = String(b.project_id || '').trim().toUpperCase();
    const ean = String(b.ean || '').replace(/\s/g, '');
    if (!/^FLX-[A-Z0-9]{3}-[A-Z0-9]{3,4}$/.test(id)) return res.status(400).json({ error: 'geldig project-id verplicht' });
    if (!/^\d{18}$/.test(ean)) return res.status(400).json({ error: 'geldig EAN verplicht' });
    const patch = b.patch || {};
    const { veilig, rec } = await _kaminoRecLaden(id);
    if (!rec || !rec.mandaat || !Array.isArray(rec.mandaat.eans)) return res.status(404).json({ error: 'Geen mandaat-wachtrij voor dit project.' });
    const ent = rec.mandaat.eans.find(x => x.ean === ean);
    if (!ent) return res.status(404).json({ error: 'EAN niet in de wachtrij van dit project.' });
    ['referentienummer', 'titularis_mail_masked', 'fluvius_adres', 'bevestigd_door', 'opmerking'].forEach(k => {
      if (patch[k] !== undefined) ent[k] = (patch[k] === null ? null : String(patch[k]).slice(0, 300));
    });
    if (patch.adres_match !== undefined) ent.adres_match = (patch.adres_match === null ? null : !!patch.adres_match);
    if (patch.status !== undefined) {
      const st = String(patch.status);
      if (_MANDAAT_STATUSSEN.indexOf(st) < 0) return res.status(400).json({ error: 'ongeldige status' });
      ent.status = st;
      const nu = new Date().toISOString();
      if (st === 'aangevraagd' && !ent.aangevraagd_op) ent.aangevraagd_op = nu;
      if (st === 'actief' && !ent.actief_op) ent.actief_op = nu;
      if (st === 'geleverd' && !ent.geleverd_op) ent.geleverd_op = nu;
    }
    rec.mandaat.status = _mandaatOverallStatus(rec.mandaat.eans);
    rec.mandaat.bijgewerkt = new Date().toISOString(); rec.bijgewerkt = new Date().toISOString();
    await _factuurUpload(Buffer.from(JSON.stringify(rec), 'utf8').toString('base64'), 'application/json', `kamino/${veilig}.json`);
    try { _kaminoLijstCache.ts = 0; } catch (e2) {}
    console.log(`[mandaat/status] ${id} ${ean} → ${ent.status} (door ${u.naam || u.id})`);
    return res.json({ ok: true, project_id: id, ean, eanStatus: ent, mandaat: rec.mandaat });
  } catch (e) { console.error('[mandaat/status] faalde:', e.message); return res.status(500).json({ error: e.message }); }
});

// POST /api/mandaat/bevestig-adres  { project_id, ean, akkoord:true|false, opmerking? }
// Wie het mandaat startte (klant/adviseur) — of iemand met projecttoegang — bevestigt of weigert een adres-
// MISMATCH (factuur ↔ Fluvius). Akkoord → EAN terug in de wachtrij, gemarkeerd `adres_bevestigd` (de skill dient
// dan in zonder her-check → de bevestigingsmail naar de titularis mag buiten). Weiger → 'geannuleerd'. In beide
// gevallen wordt bevestigd_door + bevestigd_op op het record gelogd (manuele controle nadien). Toegang = wie het
// project mag openen (NIET manager-only — de aanvrager moet zelf kunnen bevestigen).
app.post('/api/mandaat/bevestig-adres', async (req, res) => {
  try {
    if (!SUPABASE_OK) return res.status(503).json({ error: 'Opslag niet geconfigureerd' });
    const u = await resolveUser(req); if (!u) return res.status(401).json({ error: 'Niet ingelogd' });
    const b = req.body || {};
    const id = String(b.project_id || '').trim().toUpperCase();
    const ean = String(b.ean || '').replace(/\s/g, '');
    if (!/^FLX-[A-Z0-9]{3}-[A-Z0-9]{3,4}$/.test(id)) return res.status(400).json({ error: 'geldig project-id verplicht' });
    if (!/^\d{18}$/.test(ean)) return res.status(400).json({ error: 'geldig EAN verplicht' });
    if (typeof b.akkoord !== 'boolean') return res.status(400).json({ error: 'akkoord (true/false) verplicht' });
    const { veilig, rec } = await _kaminoRecLaden(id);
    if (!rec) return res.status(404).json({ error: 'Geen project gevonden.' });
    if (!_magProjectOpenen(u, rec)) return res.status(403).json({ error: 'Geen toegang tot dit project.' });
    if (!rec.mandaat || !Array.isArray(rec.mandaat.eans)) return res.status(404).json({ error: 'Geen mandaat-wachtrij voor dit project.' });
    const ent = rec.mandaat.eans.find(x => x.ean === ean);
    if (!ent) return res.status(404).json({ error: 'EAN niet in de wachtrij van dit project.' });
    if (ent.status !== 'adres_mismatch') return res.status(409).json({ error: "Deze EAN staat niet op 'adres_mismatch'." });
    const nu = new Date().toISOString();
    ent.bevestigd_door = { id: u.id, naam: u.naam, email: u.email, rol: u.role };
    ent.bevestigd_op = nu;
    if (b.opmerking) ent.opmerking = String(b.opmerking).slice(0, 300);
    if (b.akkoord) { ent.adres_bevestigd = true; ent.status = 'wachtrij'; }   // terug in de wachtrij, gemarkeerd bevestigd
    else { ent.adres_bevestigd = false; ent.status = 'geannuleerd'; }
    rec.mandaat.status = _mandaatOverallStatus(rec.mandaat.eans);
    rec.mandaat.bijgewerkt = nu; rec.bijgewerkt = nu;
    await _factuurUpload(Buffer.from(JSON.stringify(rec), 'utf8').toString('base64'), 'application/json', `kamino/${veilig}.json`);
    try { _kaminoLijstCache.ts = 0; } catch (e2) {}
    console.log(`[mandaat/bevestig-adres] ${id} ${ean} → ${b.akkoord ? 'BEVESTIGD (terug in wachtrij)' : 'GEWEIGERD (geannuleerd)'} door ${u.role} ${u.naam || u.id}`);
    return res.json({ ok: true, project_id: id, ean, akkoord: !!b.akkoord, eanStatus: ent, mandaat: rec.mandaat });
  } catch (e) { console.error('[mandaat/bevestig-adres] faalde:', e.message); return res.status(500).json({ error: e.message }); }
});

// POST /api/mandaat/sync  { items:[{ ean, status?, referentienummer?, titularis_mail_masked?, fluvius_adres?, adres_match?, richting?, meter_type? }] }
// MANAGER-ONLY. De Fluvius-skill (statuscheck) schrijft hiermee de LIVE Fluvius-status in bulk terug:
//  - EAN die al bij een project in `rec.mandaat` staat → dat projectrecord bijwerken.
//  - EAN die nergens in een project staat (bestaand/lopend mandaat bij Fluvius, geen app-project) → in de
//    LOSSE lijst (`mandaat_los/los.json`), zodat hij tóch in de Mandaten-app verschijnt.
app.post('/api/mandaat/sync', async (req, res) => {
  const u = await _managerGuard(req, res); if (!u) return;
  try {
    const items = Array.isArray((req.body || {}).items) ? req.body.items : [];
    const genorm = items.map(x => ({
      ean: String((x && x.ean) || '').replace(/\s/g, ''),
      status: (x && x.status && _MANDAAT_STATUSSEN.indexOf(String(x.status)) >= 0) ? String(x.status) : 'aangevraagd',
      referentienummer: (x && x.referentienummer != null) ? String(x.referentienummer).slice(0, 120) : null,
      titularis_mail_masked: (x && x.titularis_mail_masked != null) ? String(x.titularis_mail_masked).slice(0, 160) : null,
      fluvius_adres: (x && x.fluvius_adres != null) ? String(x.fluvius_adres).slice(0, 300) : null,
      adres_match: (x && x.adres_match != null) ? !!x.adres_match : null,
      richting: (x && x.richting) || null, meter_type: (x && x.meter_type) || null,
      kwartierdata_aanwezig: (x && x.kwartierdata_aanwezig != null) ? !!x.kwartierdata_aanwezig : null,
    })).filter(x => /^\d{18}$/.test(x.ean));
    if (!genorm.length) return res.status(400).json({ error: 'geen geldige items (elk met een 18-cijferig EAN)' });
    // Alle projecten laden + EAN-index (ean → projectrecord + entry)
    let lijst = []; try { lijst = await _bucketList('kamino/'); } catch (e) { lijst = []; }
    const jsons = (Array.isArray(lijst) ? lijst : []).filter(o => o.name && /\.json$/i.test(o.name));
    const paren = await Promise.all(jsons.map(o =>
      _factuurDownload(`kamino/${o.name}`).then(t => { try { return { veilig: o.name.replace(/\.json$/i, ''), rec: JSON.parse(t) }; } catch (e) { return null; } }).catch(() => null)));
    const index = {};
    for (const p of paren) {
      if (!p || !p.rec || !p.rec.mandaat || !Array.isArray(p.rec.mandaat.eans)) continue;
      for (const en of p.rec.mandaat.eans) index[en.ean] = { veilig: p.veilig, rec: p.rec, en };
    }
    const nu = new Date().toISOString();
    function _patchEntry(en, it) {
      if (it.referentienummer != null) en.referentienummer = it.referentienummer;
      if (it.titularis_mail_masked != null) en.titularis_mail_masked = it.titularis_mail_masked;
      if (it.fluvius_adres != null) en.fluvius_adres = it.fluvius_adres;
      if (it.adres_match != null) en.adres_match = it.adres_match;
      if (it.kwartierdata_aanwezig != null) en.kwartierdata_aanwezig = it.kwartierdata_aanwezig;
      if (it.richting && !en.richting) en.richting = it.richting;
      en.status = it.status;
      if (it.status === 'aangevraagd' && !en.aangevraagd_op) en.aangevraagd_op = nu;
      if (it.status === 'actief' && !en.actief_op) en.actief_op = nu;
      if (it.status === 'geleverd' && !en.geleverd_op) en.geleverd_op = nu;
    }
    const teBewaren = {}; const los = [];
    for (const it of genorm) {
      const hit = index[it.ean];
      if (hit) { _patchEntry(hit.en, it); teBewaren[hit.veilig] = hit.rec; }
      else los.push(it);
    }
    let bijgewerkt = 0;
    for (const veilig of Object.keys(teBewaren)) {
      const rec = teBewaren[veilig];
      rec.mandaat.status = _mandaatOverallStatus(rec.mandaat.eans);
      rec.mandaat.bijgewerkt = nu; rec.bijgewerkt = nu;
      await _factuurUpload(Buffer.from(JSON.stringify(rec), 'utf8').toString('base64'), 'application/json', `kamino/${veilig}.json`);
      bijgewerkt++;
    }
    let losToegevoegd = 0;
    if (los.length) {
      let losRec = { eans: [] };
      try { losRec = JSON.parse(await _factuurDownload('mandaat_los/los.json')); } catch (e) {}
      losRec.eans = losRec.eans || [];
      for (const it of los) {
        let en = losRec.eans.find(x => x.ean === it.ean);
        if (!en) { en = { ean: it.ean, in_wachtrij_sinds: nu, adres_bevestigd: false, bevestigd_door: null, bevestigd_op: null, opmerking: null, aangevraagd_op: null, actief_op: null, geleverd_op: null }; losRec.eans.push(en); losToegevoegd++; }
        _patchEntry(en, it);
        if (it.meter_type) en.meter_type = it.meter_type;
      }
      losRec.bijgewerkt = nu;
      await _factuurUpload(Buffer.from(JSON.stringify(losRec), 'utf8').toString('base64'), 'application/json', 'mandaat_los/los.json');
    }
    try { _kaminoLijstCache.ts = 0; } catch (e) {}
    console.log(`[mandaat/sync] ${genorm.length} items → ${bijgewerkt} project(en) bijgewerkt, ${losToegevoegd} los toegevoegd (door ${u.naam || u.id})`);
    return res.json({ ok: true, ontvangen: genorm.length, projecten_bijgewerkt: bijgewerkt, los_toegevoegd: losToegevoegd });
  } catch (e) { console.error('[mandaat/sync] faalde:', e.message); return res.status(500).json({ error: e.message }); }
});

// ─── v15.46: KAMINO studie 1 — onderhandelingsmarge (echte run, drift-vrij) ──────
// Draait de kale factuur-sim (profiel × spot + Enwyse-staffel) via DEZELFDE weg als /api/nominatie-sim
// (buildSimInput → _runSimulatorOnce), zodat het cijfer identiek is aan de simulator. Marge = huidige
// energiepost (uit de factuur) − gesimuleerde dynamische energiepost. Enkel de energiepost telt; netkosten,
// aansluiting en toegangsvermogen raken de marge niet, dus die mogen benaderd zijn.
app.post('/api/kamino/onderhandel', async (req, res) => {
  try {
    if (!MARKT) return res.status(503).json({ error: 'Marktdata nog niet geladen — probeer over 30 s opnieuw' });
    const b = req.body || {}; const bc = b.baseCase || {};
    const profielNaam = String(b.profielNaam || b.profiel || '').trim();
    if (!profielNaam) return res.status(400).json({ error: 'profielNaam verplicht' });
    let energie = +bc.totaalEnergieExclBtw || 0;
    // v15.70 (no-factuur-flow): zonder factuur-energiepost rekenen we op een referentieprijs × volume,
    // zodat een Kamino-project zonder PDF (EAN/profiel/verbruik) tóch een onderhandelingsmarge krijgt.
    let _geenFactuur = false, _refEurMwh = 0;
    if (!(energie > 0)) {
      const _volMwhChk = (+bc.afnameKwh || 0) / 1000;
      if (!(_volMwhChk > 0)) return res.status(400).json({ error: 'zonder factuur: afnameKwh (verbruik) verplicht' });
      _refEurMwh = +b.referentie_eur_mwh || +bc.referentieEurMwh || (CONTRACT_RAW && +CONTRACT_RAW.referentie_eur_mwh) || 90;
      energie = _refEurMwh * _volMwhChk;   // indicatieve "huidige" energiepost
      _geenFactuur = true;
    }
    const distributie = +bc.totaalDistributieExclBtw || 0, heffingen = +bc.totaalHeffingenExclBtw || 0;
    const subtot = (+bc.totaalExclBtw || 0) || (energie + distributie + heffingen);
    const pVan = bc.periodeVan, pTot = bc.periodeTot;
    // dagen IDENTIEK aan de simulator (_periodeDagen): span + 1, met DD-MM-YYYY of ISO-parsing.
    const _pd = (s) => { if (!s) return null; const m = String(s).match(/(\d{1,2})[\/-](\d{1,2})[\/-](\d{4})/);
      if (m) return new Date(+m[3], +m[2]-1, +m[1]); const d = new Date(s); return isNaN(d) ? null : d; };
    let dagen = 30; try { const a=_pd(pVan), b=_pd(pTot); if (a && b) dagen = Math.max(1, Math.round((b-a)/86400000)+1); } catch (e) {}
    const volumeMwh = (+bc.afnameKwh || 0) / 1000;
    const kva = +bc.aansluitVermogenKva || 100;
    const spanning = (bc.spanningsniveau === 'MS' || bc.spanningsniveau === 'LS') ? bc.spanningsniveau : (kva >= 100 ? 'MS' : 'LS');
    const ui = {
      project: bc.klantNaam || 'kamino', scenario: 'kamino_onderhandel',
      postcode: String(bc.postcode || '').trim(), grd: bc.dnb || '', spanning,
      profielNaam, jaarverbruik_mwh: volumeMwh,
      pv_kwp: 0, pv_curtailment: { actief: false }, bsp: { actief: false },
      batterijId: null, batterijCustom: null, laadpleinen: [],
      contract: { leverancier: (CONTRACT_RAW && CONTRACT_RAW.leverancier) || 'Enwyse', modus: 'passthrough',
        // v15.47: vergroening ANCHORED op 0 — identiek aan de simulator-factuuranalyse.
        // De simulator draait _bareSimInput('specifiek') met STATE.contractVergroening, dat default 0 is
        // en pas 2,50 wordt als het staffel-paneel geopend is (niet tijdens een factuuranalyse). Kamino
        // gebruikte de contract-fallback 2,50 → dynamische energiepost ~€40/maand hoger → marge ~€471/j lager
        // (€5.890 i.p.v. €6.361). Johan-beslissing 28-07: vergroening UIT de factuur-energiepost. Zo matcht
        // Kamino de simulator deterministisch. gsc/wkk blijven 0 (passthrough, niet in de energiepost).
        staffel: CONTRACT_STAFFEL || [], vergroening_eur_per_mwh: 0,
        vaste_kost_eur_maand: (CONTRACT_RAW && CONTRACT_RAW.vast_eur_per_maand) || 10.00, injectie_toegelaten: true, gsc_eur_mwh: 0, wkk_eur_mwh: 0 },
      aansluiting_kva: kva, toegangsvermogen_kw: Math.max(1, Math.round(kva * 0.9)),
      max_injectie_kw: 0, jaar: 'specifiek', periodeVan: pVan, periodeTot: pTot,
      simulatieperiode: { van: pVan, tot: pTot, type: 'specifiek' },
      project_id: b.project_id || null,   // v15.67: voor het opgeladen afname-profiel
      _mgr_ok: await _isManagerReq(req)    // v15.68: profiel-analyse enkel voor managers
    };
    const _pInfo = await _pasOpgeladenAfnameToe(ui);   // v15.67: opgeladen profiel (fallback = standaard)
    const simInput = buildSimInput(ui);
    const result = await _runSimulatorOnce(simInput);
    const jf = result.jaarfactuur || result.factuur || {}; const gr = jf.groepen || {};
    const A = gr.A_energiekost || gr.A || {};
    const energie_dyn = (A._subtotaal != null) ? (+A._subtotaal || 0) : null;
    if (energie_dyn == null) return res.status(500).json({ error: 'geen energiepost in de simulatie-output' });
    const factor = 365 / dagen, marge_maand = energie - energie_dyn;
    const out = {
      marge_jaar: Math.round(marge_maand * factor), marge_maand: Math.round(marge_maand),
      energie_nu: Math.round(energie), energie_dyn: Math.round(energie_dyn),
      besparing_pct: subtot > 0 ? +(marge_maand / subtot * 100).toFixed(1) : 0,
      energiekost_nu_mwh: volumeMwh > 0 ? Math.round(energie / volumeMwh) : null,
      energiekost_dyn_mwh: volumeMwh > 0 ? Math.round(energie_dyn / volumeMwh) : null,
      volume_mwh: +volumeMwh.toFixed(1), dagen, profiel: profielNaam
    };
    // v15.67 (fase 2): label of met het opgeladen profiel gerekend is.
    if (_pInfo) out.profiel_bron = { bron: 'opgeladen_afname', ean: _pInfo.ean, mwh: _pInfo.mwh, maanden: _pInfo.maanden, van: _pInfo.van, tot: _pInfo.tot };
    // v15.67 (fase 3): TOEGANGSVERMOGEN-CHECK. Ligt de echte maandpiek over 12 maanden onder het
    // gecontracteerde toegangsvermogen, dan kan dat in het nieuwe/optimale contract lager → extra besparing.
    try {
      const op = await _opgeladenProfiel(b.project_id, 'afname');   // v15.97: toegangsvermogen-advies ook voor alle gebruikers
      const toegangKw = Number(bc.toegangsvermogenKw || bc.toegangsvermogen_kw || 0) || Math.round((+bc.aansluitVermogenKva || 0) * 0.9);
      if (op && op.maandpiek_kw > 0 && toegangKw > 0 && (op.maanden || 0) >= 6) {
        const piek = op.maandpiek_kw;
        const kaart = _kiesTarieven(bc.dnb || '', spanning) || {};
        const eurKwJaar = Number(kaart.maandpiek_eur_kw_jaar) || Number(kaart.toegangsvermogen_eur_kw_jaar) || Number(kaart.capaciteit_eur_kw_jaar) || 0;
        if (piek < toegangKw * 0.95) {
          const nieuw = Math.max(5, Math.ceil(piek / 5) * 5);   // ronde marge net boven de echte piek
          out.toegangsvermogen_advies = {
            verlaagbaar: true, huidig_kw: toegangKw, maandpiek_12m_kw: piek, verlaagbaar_naar_kw: nieuw,
            besparing_indicatie_eur_jaar: eurKwJaar > 0 ? Math.round((toegangKw - nieuw) * eurKwJaar) : null,
            maanden: op.maanden, bron: 'opgeladen_afname',
          };
        } else {
          out.toegangsvermogen_advies = { verlaagbaar: false, huidig_kw: toegangKw, maandpiek_12m_kw: piek, maanden: op.maanden, bron: 'opgeladen_afname' };
        }
      }
    } catch (e) { console.warn('[kamino/onderhandel] toegangsvermogen-check faalde (niet-blokkerend):', e.message); }
    // diagnose: welk geprojecteerd jaarverbruik + staffelschijf gebruikte de sim? (zo is een profiel-mismatch zichtbaar)
    try {
      const pk = _laadProfielKwartier(profielNaam);
      if (pk && pk.length === 35040) {
        const iso = (d) => d ? new Date(d.getTime() - d.getTimezoneOffset()*60000).toISOString().slice(0,10) : null;
        const sr = projectJaarverbruik({ profielNaam, profielKwartier: pk, afnameKwh: (+bc.afnameKwh||0),
          periodeVan: iso(_pd(pVan)), periodeTot: iso(_pd(pTot)), staffel: CONTRACT_STAFFEL });
        if (sr) { out.geprojecteerd_mwh = sr.geprojecteerdJaarverbruikMWh!=null ? Math.round(sr.geprojecteerdJaarverbruikMWh*10)/10 : null;
          out.schijf = (sr.tier && (sr.tier.label || sr.tier.code)) || null;
          out.markup = (sr.tier && sr.tier.consumption_dam_markup) != null ? sr.tier.consumption_dam_markup : null; }
      }
    } catch (e) { console.warn('[kamino/onderhandel] diagnose-projectie faalde (niet-blokkerend):', e.message); }
    // DEBUG: exacte sim-input + energiepost-opbouw + toegepaste pricing, om Kamino ↔ simulator te vergelijken.
    out._debug = {
      in: { volume_mwh: ui.jaarverbruik_mwh, profiel: profielNaam, periode: [pVan, pTot], dagen, spanning,
            aansluiting_kva: kva, vergroening: ui.contract.vergroening_eur_per_mwh, vast: ui.contract.vaste_kost_eur_maand,
            staffel_n: (ui.contract.staffel || []).length },
      A_energiekost: A,
      jf_keys: Object.keys(jf || {}),
      pricing: result.pricing || jf.pricing || result._pricing || null,
      effectief_jaarverbruik_mwh: result.effectief_jaarverbruik_mwh || jf.effectief_jaarverbruik_mwh || result.effectief_jaarverbruik || null
    };
    out.geen_factuur = _geenFactuur; if (_geenFactuur) out.referentie_eur_mwh = Math.round(_refEurMwh);   // v15.70
    // v15.106: OPT-IN contract-heatmaps (huidige situatie) — enkel als de caller ze vraagt (b.heatmaps),
    // zodat de Kamino-tegel-1-respons licht blijft. De EnergieKompas-onderhandelingsnota zet b.heatmaps=true.
    // v15.109: heatmaps schalen op het GEPROJECTEERDE JAARvolume (niet het factuur-maandvolume) — anders staat de
    // afname-heatmap ~12× te laag bij een maandfactuur. geprojecteerd_mwh valt terug op volumeMwh × (365/dagen).
    if (b.heatmaps) { try { const hmVol = (out.geprojecteerd_mwh > 0) ? out.geprojecteerd_mwh : (volumeMwh * (365 / (dagen || 365)));
      const ch = _contractHeatmaps(MARKT, profielNaam, hmVol); if (ch) out.heatmaps = ch; }
      catch (e) { console.warn('[kamino/onderhandel] contract-heatmaps faalde (niet-blokkerend):', e.message); } }
    console.log(`[kamino/onderhandel] marge/jaar=${out.marge_jaar} (energie ${out.energie_nu}→${out.energie_dyn} · ${dagen}d · profiel=${profielNaam} · vergroening=${ui.contract.vergroening_eur_per_mwh} · geproj=${out.geprojecteerd_mwh} · schijf=${out.schijf})`);
    return res.json(out);
  } catch (e) {
    console.error('[kamino/onderhandel] fout:', e.message);
    return res.status(500).json({ error: e.message });
  }
});

// ─── KAMINO TEGEL 2 — SolarActive / injectie-optimalisatie ──────────────────────
// v15.47: exact dezelfde valuatie als de simulator. We roepen NIET een eigen
// berekening aan, maar HERGEBRUIKEN _analyseerInjectieOptimalisatie() — precies de
// functie achter /api/injectie-optimalisatie die de simulator zelf aanroept. Zo is
// het cijfer per constructie identiek voor dezelfde invoer (pv_kwp, injectie, profiel).
// De 'eerste rapport'-standaardaannames (inverter ≈ PV-vermogen, piek uit de aansluiting)
// worden teruggegeven zodat de gebruiker ze 1-op-1 in de simulator kan reproduceren.
app.post('/api/kamino/productie', async (req, res) => {
  try {
    if (!MARKT) return res.status(503).json({ error: 'Marktdata nog niet geladen — probeer over 30 s opnieuw' });
    const b = req.body || {}; const bc = b.baseCase || {};
    const profielNaam = String(b.profielNaam || b.profiel || '').trim();
    if (!profielNaam) return res.status(400).json({ error: 'profielNaam verplicht' });
    const pv_kwp = +b.pv_kwp || 0;
    const injectie_mwh_jaar = +b.injectie_mwh_jaar || +b.pv_inj || 0;
    if (!(pv_kwp > 0)) return res.status(400).json({ error: 'pv_kwp verplicht (bestaande PV)' });
    // dagen + geprojecteerd jaarverbruik IDENTIEK aan de onderhandel-tegel (zelfde projectie als de simulator).
    const _pd = (s) => { if (!s) return null; const m = String(s).match(/(\d{1,2})[\/-](\d{1,2})[\/-](\d{4})/);
      if (m) return new Date(+m[3], +m[2]-1, +m[1]); const d = new Date(s); return isNaN(d) ? null : d; };
    const pVan = bc.periodeVan, pTot = bc.periodeTot;
    let dagen = 30; try { const a = _pd(pVan), b2 = _pd(pTot); if (a && b2) dagen = Math.max(1, Math.round((b2-a)/86400000)+1); } catch (e) {}
    const volumeMwh = (+bc.afnameKwh || 0) / 1000;
    let afnameJaarMwh = dagen > 0 ? volumeMwh * (365 / dagen) : volumeMwh;
    try {
      const pk = _laadProfielKwartier(profielNaam);
      if (pk && pk.length === 35040) {
        const iso = (d) => d ? new Date(d.getTime() - d.getTimezoneOffset()*60000).toISOString().slice(0,10) : null;
        const sr = projectJaarverbruik({ profielNaam, profielKwartier: pk, afnameKwh: (+bc.afnameKwh||0),
          periodeVan: iso(_pd(pVan)), periodeTot: iso(_pd(pTot)), staffel: CONTRACT_STAFFEL });
        if (sr && sr.geprojecteerdJaarverbruikMWh != null) afnameJaarMwh = sr.geprojecteerdJaarverbruikMWh;
      }
    } catch (e) { console.warn('[kamino/productie] projectie faalde (niet-blokkerend):', e.message); }
    const kva = +bc.aansluitVermogenKva || 100;
    const inverter_kva = +b.inverter_kva || pv_kwp;              // standaard: inverter ≈ PV-vermogen
    const piek_kw = +b.piek_kw || Math.max(1, Math.round(kva * 0.9)); // standaard: uit de aansluiting
    let profiel_kwartier = _laadProfielKwartier(profielNaam);
    // v15.67 (fase 2): opgeladen afname-profiel gebruiken voor de zelfconsumptie-berekening (fallback = standaard).
    let _pbron = null;
    const _mgrOk = await _isManagerReq(req);
    try { const op = _mgrOk ? await _opgeladenProfiel(b.project_id, 'afname') : null; if (op && Array.isArray(op.kwartier) && op.kwartier.length === 35040) { profiel_kwartier = op.kwartier; _pbron = { bron: 'opgeladen_afname', ean: op.ean, mwh: op.mwh, maanden: op.maanden }; } } catch (e) {}
    // v15.69 (fase 4): opgeladen INJECTIE-profiel → curtailment/onbalans op de ECHTE gemeten vorm (fallback = zonvorm).
    const _iparams = { pv_kwp, inverter_kva, piek_kw, afname_mwh_jaar: afnameJaarMwh,
      injectie_mwh_jaar, injectie_mwh_maand: 0, injectie_maand: 0,
      forecast_modus: 'realistic', profiel_kwartier,
      project_id: b.project_id, _mgr_ok: _mgrOk };
    const _ibron = await _pasOpgeladenInjectieToe(_iparams);
    if (_ibron && !(injectie_mwh_jaar > 0) && _iparams.injectie_profiel_mwh > 0) _iparams.injectie_mwh_jaar = _iparams.injectie_profiel_mwh;   // geen factuurvolume → gemeten profielvolume
    const a = _analyseerInjectieOptimalisatie(MARKT, _iparams);
    const o = a.opbrengst_jaar || {}, pb = a.payback || {}, e = a.energie_jaar || {};
    const out = {
      extra_opbrengst_jaar: Math.round(+o.meerwaarde_totaal_eur || 0),
      netto_jaar: Math.round(+pb.netto_beide_jaar_eur || 0),
      terugverdientijd_jaar: (pb.payback_beide_jaar != null ? pb.payback_beide_jaar : null),
      injectie_mwh: (e.injectie_mwh != null ? e.injectie_mwh : null),
      pv_kwp, inverter_kva, piek_kw, afname_mwh_jaar: Math.round(afnameJaarMwh*10)/10, profiel: profielNaam,
      _debug: { opbrengst_jaar: o, payback: pb, energie_jaar: e, sturing: a.sturing }
    };
    if (_pbron) out.profiel_bron = _pbron;   // v15.67 (fase 2): label
    if (_ibron) out.injectie_bron = _ibron;   // v15.69 (fase 4): label opgeladen injectieprofiel
    console.log(`[kamino/productie] extra=${out.extra_opbrengst_jaar} netto=${out.netto_jaar} tvt=${out.terugverdientijd_jaar} (pv=${pv_kwp} inj=${injectie_mwh_jaar} profiel=${profielNaam}${_pbron?' · opgeladen':''})`);
    return res.json(out);
  } catch (e) {
    console.error('[kamino/productie] fout:', e.message);
    return res.status(500).json({ error: e.message });
  }
});

// ─── KAMINO TEGEL 3 — BatteryActive / Mijn aansluiting ──────────────────────────
// v15.47: het "eerste rapport op standaardprofiel". ORCHESTREERT bestaande, al-geverifieerde
// server-logica — GEEN herrekening:
//   (1) extra PV dimensioneren op ≥90% zelfverbruik via een PV-sweep (zelfde zelfconsumptie-%
//       als /api/pv-sweep: (direct + via batterij)/bruto), met de instap-batterij (1 eenheid) vast;
//   (2) batterij-groeipad via _draaiSim3 (batterij_gebouw-pad) → per stap besparing/rendement/NPV
//       server-side berekend met _batterijSweepGebouw, IDENTIEK aan de simulator omdat we exact
//       dezelfde investeringsconstanten meesturen (_investering/_tco/_kpi_*) als simulator.html:9437-9469.
// Kerncijfers = de AANBEVOLEN instap (1 batterij / 120 kW / 260 kWh, hard-verankerd = "eerste groeistap");
// de OPTIMALE opstelling (hoogste NPV) gaat mee als advies. Async (job + /api/sim-voortgang) want de
// sweep + het groeipad zijn meerdere dispatch-runs. VERGROENING = 0 (Johan 28-07, = de andere tegels).
// LET OP: geen lokale MARKT-data in de dev-sandbox → live smoke-test vereist tegen de simulator.
app.post('/api/kamino/aansluiting', async (req, res) => {
  try {
    if (!MARKT) return res.status(503).json({ error: 'Marktdata nog niet geladen — probeer over 30 s opnieuw' });
    const b = req.body || {}; const bc = b.baseCase || {};
    const profielNaam = String(b.profielNaam || b.profiel || '').trim();
    if (!profielNaam) return res.status(400).json({ error: 'profielNaam verplicht' });
    if (!((+bc.totaalEnergieExclBtw || 0) > 0)) return res.status(400).json({ error: 'geen energiepost in de factuurgegevens' });
    const _pd = (s) => { if (!s) return null; const m = String(s).match(/(\d{1,2})[\/-](\d{1,2})[\/-](\d{4})/);
      if (m) return new Date(+m[3], +m[2]-1, +m[1]); const d = new Date(s); return isNaN(d) ? null : d; };
    const pVan = bc.periodeVan, pTot = bc.periodeTot;
    let dagen = 30; try { const a = _pd(pVan), b2 = _pd(pTot); if (a && b2) dagen = Math.max(1, Math.round((b2-a)/86400000)+1); } catch (e) {}
    const annf = dagen > 0 ? 365 / dagen : 1;
    const volumeMwh = (+bc.afnameKwh || 0) / 1000;
    let afnameJaarMwh = dagen > 0 ? volumeMwh * (365 / dagen) : volumeMwh;
    try {
      const pk0 = _laadProfielKwartier(profielNaam);
      if (pk0 && pk0.length === 35040) {
        const iso = (d) => d ? new Date(d.getTime() - d.getTimezoneOffset()*60000).toISOString().slice(0,10) : null;
        const sr = projectJaarverbruik({ profielNaam, profielKwartier: pk0, afnameKwh: (+bc.afnameKwh||0),
          periodeVan: iso(_pd(pVan)), periodeTot: iso(_pd(pTot)), staffel: CONTRACT_STAFFEL });
        if (sr && sr.geprojecteerdJaarverbruikMWh != null) afnameJaarMwh = sr.geprojecteerdJaarverbruikMWh;
      }
    } catch (e) { console.warn('[kamino/aansluiting] projectie faalde (niet-blokkerend):', e.message); }
    const kva = +bc.aansluitVermogenKva || 100;
    const spanning = (bc.spanningsniveau === 'MS' || bc.spanningsniveau === 'LS') ? bc.spanningsniveau : (kva >= 100 ? 'MS' : 'LS');
    const postcode = String(bc.postcode || '').trim(), grd = bc.dnb || '';
    // v15.54 (Johan): tegel 3 = trigger toegangsvermogen (geen laadplein) → batterij-module die kleiner is dan de
    // aansluiting. Zo krijgt een kleine site een passende (kleine) batterij i.p.v. de vaste 120/260.
    const _kaModule = _kiesBattModule(kva);
    // v15.67 (fase 2): opgeladen afname-profiel één keer resolven; elke ui/cfg uit baseUi erft het (fallback = standaard).
    // v15.68: enkel voor managers.
    const _kaMgrOk = await _isManagerReq(req);
    const _kaProfiel = _kaMgrOk ? await _opgeladenProfiel(b.project_id, 'afname').catch(function(){ return null; }) : null;
    const baseUi = () => Object.assign({
      project: bc.klantNaam || 'kamino', scenario: 'kamino_aansluiting',
      postcode, grd, spanning, profielNaam, jaarverbruik_mwh: volumeMwh,
      pv_curtailment: { actief: false }, bsp: { actief: false }, laadpleinen: [],
      batterijId: null, batterijCustom: null, batt_module: _kaModule,
      contract: { leverancier: (CONTRACT_RAW && CONTRACT_RAW.leverancier) || 'Enwyse', modus: 'passthrough',
        staffel: CONTRACT_STAFFEL || [], vergroening_eur_per_mwh: 0,
        vaste_kost_eur_maand: (CONTRACT_RAW && CONTRACT_RAW.vast_eur_per_maand) || 10.00, injectie_toegelaten: true, gsc_eur_mwh: 0, wkk_eur_mwh: 0 },
      aansluiting_kva: kva, toegangsvermogen_kw: Math.max(1, Math.round(kva * 0.9)),
      max_injectie_kw: 0, jaar: 'specifiek', periodeVan: pVan, periodeTot: pTot,
      simulatieperiode: { van: pVan, tot: pTot, type: 'specifiek' },
      project_id: b.project_id || null
    }, (_kaProfiel && Array.isArray(_kaProfiel.kwartier) && _kaProfiel.kwartier.length === 35040)
        ? { _opgeladen_profiel_kwartier: _kaProfiel.kwartier, _profiel_bron: 'opgeladen_afname' } : {});

    const job = _jobNieuw();
    res.json({ ok: true, async: true, job_id: job.id });

    (async () => {
      try {
        // v15.51: zelfverbruik-drempel is nu een PARAMETER (b.pvDrempel), default 80% (was 90%). En de keuze rekent
        // op de MARGINALE zelfconsumptie van de nieuwe PV (t.o.v. bestaand-only), niet op het geblende %.
        const DREMPEL = Number(b.pvDrempel) > 0 ? Number(b.pvDrempel) : 80;
        if (job) job.runs_verwacht = 13;
        // Investeringsconstanten voor een gegeven PV — IDENTIEK aan simulator.html:9437-9469.
        const _consts = (pvKwp) => {
          const kabeltrace = _kabelBattTrace(_kaModule) + (pvKwp > 0 ? 10000 : 0);   // v15.55: batterij-kabeltracé per module (5→1.500,30→4.000,120→15.000) + PV

          const baseNet = dagen > 0 ? (+bc.totaalDistributieExclBtw || 0) * (365 / dagen) : null;
          return {
            _investering: { cabine_eur: 90000, eur_per_kva: 100, eur_per_kwh: 350, kabel_pct: 0.20, horizon_jaar: 15 },
            _tco: { scenario: 'realistisch', inflatie: 0.02, net_extra: 0.04, net_sprong_jaar: 2029, net_sprong: 0.30,
              startjaar: 2026, horizon: 15, verzekering_promille: 3.4, omvormer_vervang_kwp: 50, pv_kwp: pvKwp,
              onderhoud: { pv_kwp: 5, batterij_kw: 5, laadpaal: 50 }, onderhoud_vast: pvKwp * 5,
              besparing_energie_deel: 0.72, base_net: baseNet, disconto: 0.05 },
            _kpi_capex_vast: pvKwp * 450 + kabeltrace,
            _kpi_base_plus_creg: dagen > 0 ? (+bc.totaalExclBtw || 0) * (365 / dagen) : (+bc.totaalExclBtw || 0),
            _kpi_annfactor: annf, _kabeltrace: kabeltrace
          };
        };
        const _pas = (ui, pvKwp) => { const c = _consts(pvKwp); const kt = c._kabeltrace; delete c._kabeltrace; Object.assign(ui, c); return kt; };

        // STAP 1 — OPTIMALE OPSTELLING berekenen (zonder extra PV) → kOpt (hoogste NPV). (= simulator-volgorde.)
        _jlog(job, 'start', 'Optimale opstelling berekenen (batterij-groeipad)…', {});
        const ui0 = baseUi(); ui0.pv_kwp = 0; ui0.pvKwp = 0; _pas(ui0, 0);
        const sim0 = await _draaiSim3(ui0, job);
        const gp0 = sim0 && sim0.groeipad_gebouw;
        if (!gp0) throw new Error('geen groeipad_gebouw (optimale opstelling)');
        const kOpt = Math.max(1, gp0.optimaal_k || (gp0.optimaal && gp0.optimaal.aantal_batterijen) || 1);
        _jlog(job, 'ok', `Optimale opstelling: ${kOpt} batterij${kOpt>1?'en':''} (hoogste NPV).`, {});

        // STAP 2 — EXTRA PV @ ≥90% zelfverbruik, met de OPTIMALE batterij (kOpt) VAST (= _pvSweepConfig).
        const pvMax = Math.max(0, Math.round(afnameJaarMwh));   // ~1 kWp per MWh verbruik (= _pvMaxKwp zonder laadplein)
        const pvKandidaten = []; for (let i = 1; i <= 4; i++) { const v = Math.round(pvMax * i / 4); if (v > 0 && pvKandidaten.indexOf(v) < 0) pvKandidaten.push(v); }
        _jlog(job, 'start', `Extra PV dimensioneren op ≥${DREMPEL}% MARGINAAL zelfverbruik (${pvKandidaten.length} stappen, ${kOpt}× batterij vast)…`, {});
        // v15.51: NULPUNT-baseline = bestaande PV only (pv=0), zodat we de nieuwe PV op zijn marginale zelfconsumptie
        // beoordelen i.p.v. het geblende % (dat door de bestaande PV te gunstig oogt).
        const _cb = baseUi(); _cb.pv_kwp = 0; _cb.pvKwp = 0; _cb.geen_aansluiting_verhoging = true;
        _cb.batterijId = 'CUSTOM'; _cb.batterijCustom = { naam: 'kamino-optimaal', kw: kOpt * _kaModule.kw, kwh: kOpt * _kaModule.kwh, aantal_batterijen: kOpt, dod_pct: 90, rte_pct: 92, capex_eur: 0, max_cycli: 8000 };
        const _rb = await _runSimulatorOnce(buildSimInput(_variantUi(_cb, 'sturing')));
        if (job) job.runs = (job.runs || 0) + 1;
        const _kb = (_rb && _rb.kpi) || {};
        const _prod0 = Number(_kb.pv_potentiele_productie_mwh) || 0;
        const _zelf0 = (Number(_kb.pv_direct_zelfverbruik_mwh) || 0) + (Number(_kb.pv_naar_batterij_mwh) || 0);
        let pvKwp = 0, pvZelf = null; const sweep = [];
        for (const pv of pvKandidaten) {
          const cfg = baseUi(); cfg.pv_kwp = pv; cfg.pvKwp = pv; cfg.geen_aansluiting_verhoging = true;
          cfg.batterijId = 'CUSTOM'; cfg.batterijCustom = { naam: 'kamino-optimaal', kw: kOpt * _kaModule.kw, kwh: kOpt * _kaModule.kwh, aantal_batterijen: kOpt, dod_pct: 90, rte_pct: 92, capex_eur: 0, max_cycli: 8000 };
          const r = await _runSimulatorOnce(buildSimInput(_variantUi(cfg, 'sturing')));
          if (job) job.runs = (job.runs || 0) + 1;
          const kpi = (r && r.kpi) || {};
          const prodBruto = Number(kpi.pv_potentiele_productie_mwh) || (pv * 0.95);
          const zelf = (Number(kpi.pv_direct_zelfverbruik_mwh) || 0) + (Number(kpi.pv_naar_batterij_mwh) || 0);
          const dProd = prodBruto - _prod0, dZelf = zelf - _zelf0;                       // nieuwe PV op eigen merites
          const zc = (dProd > 0 && pv > 0) ? Math.round(dZelf / dProd * 1000) / 10 : null;   // MARGINAAL %
          sweep.push({ pv_kwp: pv, zelfconsumptie_pct: zc, marginale_zelfconsumptie_pct: zc });
          _jlog(job, 'opstelling', `PV ${pv} kWp → marginaal zelfverbruik ${zc != null ? zc + '%' : '?'}`, {});
          if (zc != null && zc >= DREMPEL && pv > pvKwp) { pvKwp = pv; pvZelf = zc; }
        }
        if (pvKwp === 0) { let best = null; sweep.forEach(s => { if (s.zelfconsumptie_pct != null && (!best || s.zelfconsumptie_pct > best.zelfconsumptie_pct)) best = s; });
          if (best) { pvKwp = best.pv_kwp; pvZelf = best.zelfconsumptie_pct; } }
        _jlog(job, 'ok', `Extra PV gekozen: ${pvKwp} kWp (zelfverbruik ${pvZelf != null ? pvZelf + '%' : '?'}).`, {});

        // STAP 3 — GROEISTAPPEN mét die PV → stap 1 (aanbevolen instap) weerhouden; optimaal = advies.
        const ui = baseUi(); ui.pv_kwp = pvKwp; ui.pvKwp = pvKwp;
        const kabeltrace = _pas(ui, pvKwp);
        const sim = await _draaiSim3(ui, job);
        const gp = sim && sim.groeipad_gebouw;
        if (!gp) throw new Error('geen groeipad_gebouw (groeistappen)');
        const alts = gp.alternatieven || [];
        const stap1 = alts.find(a => a.aanbevolen) || alts.find(a => a.aantal_batterijen === 1) || alts[0] || {};
        const opt = gp.optimaal || {};
        const optAlt = alts.find(a => a.aantal_batterijen === opt.aantal_batterijen) || stap1;
        // v15.49 (Johan-keuze A, 28-07): VOLLEDIGE besparing vs vandaag = de headline van het simulator-ontwerp-rapport
        // (_kpiEngine-methodiek), NIET de groeipad-marginale (die crediteert de PV-winst niet en oogt onterecht op ~3%).
        //   besparing = E_base − E ; E_base = factuur vandaag geannualiseerd (bc.totaalExclBtw × 365/dagen) ; E = factuur van de config.
        //   rendement = (besparing − opex1)/capex×100 ; opex1 = pv·5 + battKw·5 + capex·3,4‰. (C=0 zonder laadplein.)
        const E_base = dagen > 0 ? (+bc.totaalExclBtw || 0) * (365 / dagen) : (+bc.totaalExclBtw || 0);
        const _volleFR = (a) => {
          const capex = +a.capex || 0, E = +a.jaarkost || 0;
          const battKw = +a.kw || (_kaModule.kw * (a.aantal_batterijen || 1));
          const oh1 = pvKwp * 5 + battKw * 5, opex1 = oh1 + capex * 3.4 / 1000;
          const besparing = E_base - E, besparingNetto = besparing - opex1;
          const rend = capex > 0 ? (besparingNetto / capex * 100) : null;
          const tvt = (besparingNetto > 0 && capex > 0) ? (capex / besparingNetto) : null;
          return { E: Math.round(E), besparing: Math.round(besparing), besparing_netto: Math.round(besparingNetto),
                   rendement: rend != null ? Math.round(rend * 10) / 10 : null, tvt: tvt != null ? Math.round(tvt * 10) / 10 : null, capex: Math.round(capex) };
        };
        const fr1 = _volleFR(stap1), frOpt = _volleFR(optAlt);
        const out = {
          besparing_jaar: fr1.besparing, rendement_pct: fr1.rendement, terugverdientijd_jaar: fr1.tvt,
          instap: { batterijen: stap1.aantal_batterijen || 1, kw: stap1.kw || _kaModule.kw, kwh: stap1.kwh || _kaModule.kwh,
                    capex: fr1.capex, npv: (stap1.npv != null ? stap1.npv : null), factuur_jaar: fr1.E, vandaag_jaar: Math.round(E_base) },
          optimaal: { batterijen: opt.aantal_batterijen, kw: opt.kw, kwh: opt.kwh, capex: opt.capex, npv: opt.npv,
                      besparing_jaar: frOpt.besparing, rendement_pct: frOpt.rendement, terugverdientijd_jaar: frOpt.tvt },
          pv_kwp: pvKwp, pv_zelfconsumptie_pct: pvZelf, profiel: profielNaam, afname_mwh_jaar: Math.round(afnameJaarMwh*10)/10,
          _debug: { pv_sweep: sweep, drempel: DREMPEL, kOpt, kabeltrace, capex_vast: ui._kpi_capex_vast, annf, nmax: gp.nmax, optimaal_k: gp.optimaal_k,
                    vandaag_jaar: Math.round(E_base), marginaal: { besparing_jaar: stap1.besparing_jaar, rendement: stap1.rendement }, volledig: fr1 }
        };
        job.resultaat = out; job.status = 'klaar';
        _jlog(job, 'klaar', `Aansluiting-studie klaar — instap ${stap1.aantal_batterijen||1} batterij: VOLLE besparing € ${Math.round(fr1.besparing||0).toLocaleString('nl-BE')}/j (vandaag € ${Math.round(E_base).toLocaleString('nl-BE')} → € ${(fr1.E||0).toLocaleString('nl-BE')}), rendement ${fr1.rendement!=null?fr1.rendement+'%':'?'}, TVT ${fr1.tvt!=null?fr1.tvt+'j':'?'}, optimaal ${opt.aantal_batterijen||'?'}×.`);
      } catch (e) { job.fout = e.message; job.status = 'fout'; _jlog(job, 'fout', 'Aansluiting-studie gefaald: ' + e.message); console.error('[kamino/aansluiting] async fout:', e.message); }
    })();
  } catch (e) { console.error('[kamino/aansluiting] fout:', e.message); return res.status(500).json({ error: e.message }); }
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
app.post('/api/nominatie-sim', async (req, res) => {
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
  // v15.70 (#1/label): opgeladen afname-profiel ook op de interactieve nominatie-sim (tegel 1 factuuranalyse) toepassen.
  input._mgr_ok = await _isManagerReq(req);
  const _piNom = await _pasOpgeladenAfnameToe(input);
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
    result._meta = { elapsed_ms:elapsed, server_version: SERVER_VERSIE };
    result._serverLog = stderr;
    if (_piNom) result.profiel_bron = { bron:'opgeladen_afname', ean:_piNom.ean, mwh:_piNom.mwh, maanden:_piNom.maanden, maandpiek_kw:_piNom.maandpiek_kw };   // v15.70 label
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
// v15.59.0: globale semafoor rond elke spawn. Bij SIM_MAX_PARALLEL=1 is dit een no-op (1 slot,
// FIFO) → exact sequentieel. Bij >1 begrenst het het totaal aantal gelijktijdige python-processen
// over ALLE call-sites heen (mix-lus + opstellingen + sweeps kunnen samen lopen zonder de box te
// oversubscriben). De await/finally garandeert dat een slot altijd vrijkomt, ook bij een fout.
async function _runSimulatorOnce(simInput) {
  await _simSlot();
  try { return await _runSimulatorRaw(simInput); }
  finally { _simSlotVrij(); }
}
function _runSimulatorRaw(simInput) {
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
  // v15.43: bestaande PV telt ook als PV voor de sturing (curtailment/injectie op de TOTALE PV).
  const _bpV = v.bestaande_pv || v.bestaandePv;
  const _bpAanw = !!(_bpV && _bpV.aanwezig!==false && (Number(_bpV.kwp)>0 || Number(_bpV.kva)>0 || Number(_bpV.piek_kw)>0));
  const heeftPv = (Number(v.pv_kwp || v.pvKwp || 0) > 0) || _bpAanw;
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
const _MAX_BATT_UNITS_SRV = 40;   // veiligheidsplafond op de gebouw-batterij-sweep (v15.35.0)
// v15.54 (Johan 01-08): batterij-MODULE schaalt mee met de site. Drie modules; de client kiest er één op basis van de
// trigger (tegel 3 = toegangsvermogen; tegel 4 = toegangsvermogen + 50% laadpleinvermogen) en stuurt ze mee als
// input.batt_module = {kw,kwh}. De sizing-functies lezen ze via _buKw/_buKwh met FALLBACK op 120/260 (oud gedrag →
// geen regressie als de module ontbreekt). Keuze/veelvouden gebeuren client-side (max 6 stappen); de server sizet
// gewoon op de meegegeven module. GEEN combinaties van modules in één simulatie.
const BATT_MODULES = [ { kw: 5, kwh: 10 }, { kw: 30, kwh: 60 }, { kw: 120, kwh: 260 } ];
// v15.55 (Johan 01-08): kabeltracé voor de batterij schaalt mee met de module (was vast €15.000). Gekeyed op module-kW.
const KABEL_BATT_TRACE = { 5: 1500, 30: 4000, 120: 15000 };
function _kabelBattTrace(mod){ const k = (mod && +mod.kw) || BATT_UNIT_KW; return (KABEL_BATT_TRACE[k] != null) ? KABEL_BATT_TRACE[k] : 15000; }
function _buKw(x){ return (x && x.batt_module && +x.batt_module.kw > 0) ? +x.batt_module.kw : BATT_UNIT_KW; }
function _buKwh(x){ return (x && x.batt_module && +x.batt_module.kwh > 0) ? +x.batt_module.kwh : BATT_UNIT_KWH; }
// grootste module met kW < basisvermogen; onder 5 kW → de kleinste (5/10).
function _kiesBattModule(baseKw){ let m = BATT_MODULES[0]; for (const mod of BATT_MODULES){ if (mod.kw < baseKw) m = mod; } return { kw: m.kw, kwh: m.kwh }; }
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
  const _bkwh = _buKwh(cfg), _bkw = _buKw(cfg);   // v15.54: module-maat (fallback 120/260)
  const eenheden = Math.max(1, Math.ceil(maat / _bkwh));
  const kwh = eenheden * _bkwh;
  const kw  = eenheden * _bkw;
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
// v15.25.0 — MIX = BATTERIJ-COUNT-SWEEP (Johan 17/07). Voor k = 1..N (N = batterijen van
// opstelling 2) zetten we k batterijen vast en zoeken we de MINIMALE verzwaring die de
// volledige laadvraag levert op STURING 2 (geen onbalans in de zoektocht). Per k berekenen we
// KPI2 = (E_base + CREG − factuur) / capex_excl_cabine en kiezen de hoogste. Dat vindt exact
// de "zo weinig mogelijk verzwaring + net genoeg batterij"-combinatie.
// De onbalans-opbrengst (windfall) zit al in de onbalans-variant en wordt apart getoond —
// niet in deze keuze.
const _subJF = r => (r && r.jaarfactuur) ? (r.jaarfactuur.subtotaal_excl_btw || 0) : 0;
// Netkosten-regel = distributie + transport (groep B + C + D), zoals _frComp in de frontend.
function _distributieJF(r) {
  const gr = (r && r.jaarfactuur && r.jaarfactuur.groepen) || {};
  const sub = g => (g && g._subtotaal != null) ? (Number(g._subtotaal) || 0) : 0;
  return sub(gr.B_netgebruik_afname || gr.B) + sub(gr.C_netgebruik_injectie || gr.C) + sub(gr.D_transport || gr.D);
}
// v15.33.0: factuur-componenten (energie / distributie+transport / capaciteit / heffingen / subtotaal)
// — spiegelt _frComp() in de frontend, zodat de groeipad-detailfactuur per stap kan renderen.
function _frCompJF(r) {
  const jf = (r && (r.jaarfactuur || r.factuur)) || {}, gr = jf.groepen || {};
  const g = (o, k) => { o = o || {}; return (o[k] != null) ? (Number(o[k]) || 0) : 0; };
  const A = g(gr.A_energiekost || gr.A, '_subtotaal'), B = g(gr.B_netgebruik_afname || gr.B, '_subtotaal'),
        C = g(gr.C_netgebruik_injectie || gr.C, '_subtotaal'), D = g(gr.D_transport || gr.D, '_subtotaal'),
        E = g(gr.E_heffingen || gr.E, '_subtotaal');
  const Bd = gr.B_netgebruik_afname || {}, Dd = gr.D_transport || {};
  const cap = g(Bd, 'toegangsvermogen') + g(Bd, 'maandpiek') + g(Bd, 'overschrijding_toegangsvermogen')
            + g(Dd, 'beschikbaar_vermogen') + g(Dd, 'maandpiek_transport') + g(Dd, 'jaarpiek_transport');
  return { energie: Math.round(A), distributie: Math.round(B + C + D), capaciteit: Math.round(cap),
           heffingen: Math.round(E), subtotaal: Math.round(Number(jf.subtotaal_excl_btw) || 0) };
}
function _mixZetAansluiting(cfg, kva, huidig) {
  cfg.aansluiting_kva = kva; cfg.aansluitingKva = kva; cfg.toegangsvermogen_kw = kva;
  cfg._mix_kva = kva; cfg._mix_huidig_kva = huidig;
  if (kva > LS_MAX_KVA && cfg.spanning !== 'MS') {
    cfg._spanning_origineel = cfg.spanning || 'LS';
    cfg.spanning = 'MS'; cfg._spanning_omgezet = true; cfg._cabine_nodig = true;
  }
}
// Zoek voor k vaste batterijen de kleinste aansluiting die de laadvraag levert (sturing 2).
// De sim vertelt zelf hoeveel ze de aansluiting moest optrekken (toegangsvermogen_verhoogd_kw)
// als de batterij te klein is — dus we springen daar meteen heen i.p.v. blind te groeien.
async function _mixZoekVerzwaring(input, cap, k, job) {
  const _bkw = _buKw(input), _bkwh = _buKwh(input);   // v15.54: module-maat (fallback 120/260)
  const cfg = _opstellingUi(input, 'batterij', cap);
  cfg.batterijCustom = { naam: 'Mix-batterij', kw: k * _bkw, kwh: k * _bkwh,
                         aantal_batterijen: k, dod_pct: 90, rte_pct: 92, capex_eur: 0, max_cycli: 8000 };
  const huidig = Number(input.aansluiting_kva || input.aansluitingKva || 0) || 0;
  let kva = Math.max(5, Math.ceil(huidig / 5) * 5);
  _mixZetAansluiting(cfg, kva, huidig);
  let runs = 0;
  let r = await _runSimulatorOnce(buildSimInput(_variantUi(cfg, 'sturing'))); runs++;
  if (job) job.runs = (job.runs || 0) + 1;
  let h = _opstellingHaalbaar(r, 'batterij');
  let iter = 0;
  while (!h.ok && iter < DIM_MAX_ITER + 1) {
    const geforceerd = Number((r.laadplein || {}).toegangsvermogen_verhoogd_kw) || 0;
    const stap = Math.max(geforceerd, Math.ceil(kva * (DIM_GROEI - 1)));   // sprong: wat de sim vroeg, of +30%
    kva = Math.ceil((kva + Math.max(stap, 5)) / 5) * 5;
    _mixZetAansluiting(cfg, kva, huidig);
    r = await _runSimulatorOnce(buildSimInput(_variantUi(cfg, 'sturing'))); runs++;
    if (job) job.runs = (job.runs || 0) + 1;
    h = _opstellingHaalbaar(r, 'batterij');
    iter++;
  }
  return { k, kva, kwh: k * _bkwh, kw: k * _bkw, resultaat: r, factuur: _subJF(r),
           haalbaar: h.ok, cfg, runs, huidig };
}
async function _dimensioneerMix(input, cap, job) {
  const _bkw = _buKw(input), _bkwh = _buKwh(input);   // v15.54: module-maat (fallback 120/260)
  const N = Math.max(1, Math.ceil((cap.advies_batterij_kwh || 0) / _bkwh));   // = batterijen van opstelling 2
  const K = Number(input._kpi_base_plus_creg);        // E_base + CREG (jaarbasis) — uit de frontend
  const capexVast = Number(input._kpi_capex_vast);    // laadpalen + PV + kabeltracé (excl cabine/batterij/verzwaring)
  const annf = Number(input._kpi_annfactor) || 1;     // factuurperiode → jaar (K is jaarbasis, factuur periode)
  const eurKva = Number((input._investering || {}).eur_per_kva) || 0;
  const eurKwh = Number((input._investering || {}).eur_per_kwh) || 0;
  const cabineEur = Number((input._investering || {}).cabine_eur) || 0;
  const horizon = Number((input._investering || {}).horizon_jaar) || 15;
  const tp = input._tco || null;                      // v15.28.0: TCO-escalatieparameters (rapport-afgestemd)
  const ohBatKw = Number((tp && tp.onderhoud && tp.onderhoud.batterij_kw)) || 0;   // €/kW/jaar batterij-onderhoud
  const ohVast = Number((tp && tp.onderhoud_vast)) || 0;                           // PV + laadpalen onderhoud (jaar 1)
  const baseNet = Number((tp && tp.base_net));                                     // v15.31.0: netkosten opstelling 0 (jaarbasis) → NPV
  const huidig = Number(input.aansluiting_kva || input.aansluitingKva || 0) || 0;
  // v15.59.0: de k-lus is onafhankelijk per batterij-aantal (elke _mixZoekVerzwaring start vers uit
  // input/cap/k) → parallelliseerbaar. _pmap behoudt de k-volgorde; filter(Boolean) laat de niet-
  // haalbare k's weg net als de oude `continue`.
  const _mixKs = Array.from({ length: N }, (_, i) => i + 1);
  const _mixResultaten = await _pmap(_mixKs, async (k) => {
    _jlog(job, 'opstelling', `Mix: ${k} batterij${k>1?'en':''} — welke verzwaring is dan nodig?`,
          { opstelling: 'mix', maat: k, eenheid: 'batt' });
    const z = await _mixZoekVerzwaring(input, cap, k, job);
    if (!z.haalbaar) { _jlog(job, 'faal', `Mix ${k} batt: geen werkende verzwaring gevonden — overgeslagen`); return null; }
    const verzwaring = Math.max(0, z.kva - huidig);
    // v15.28.0: keuze op de LAAGSTE meerjarige TCO incl. cabine, onderhoud, verzekering én het
    // realistisch netkosten-schema (identiek aan het financieel rapport, zie _tcoMeerjaar).
    // Cabine telt mee in de capex (LS→MS boven de grens). Onbalans blijft buiten (kers op de taart).
    const cabine = !!(z.cfg && z.cfg._spanning_omgezet) || (z.kva > LS_MAX_KVA);
    const capexExcl = (Number.isFinite(capexVast) ? capexVast : 0) + k * _bkwh * eurKwh + verzwaring * eurKva;
    const capex = capexExcl + (cabine ? cabineEur : 0);                     // capex INCL cabine
    const jaarkost = z.factuur * annf;                                      // factuur naar jaarbasis
    const netdeel = _distributieJF(z.resultaat) * annf;                     // netkosten (B+C+D), jaarbasis
    const oh1 = ohVast + (k * _bkw) * ohBatKw;                       // onderhoud jaar 1 (incl batterij)
    const opex1 = oh1 + capex * (Number((tp && tp.verzekering_promille)) || 0) / 1000;   // opex jaar 1
    const rendement = (Number.isFinite(K) && capex > 0) ? ((K - jaarkost - opex1) / capex * 100) : null;  // netto
    const tco = tp ? _tcoMeerjaar(tp, capex, jaarkost, netdeel, oh1) : (capex + jaarkost * horizon);
    // v15.31.0: KEUZEMAATSTAF = hoogste NPV @ cost of capital. besparingBruto = base0 − jaarkost;
    // besparingNet = base_net − netdeel (netkosten-component, kan negatief).
    const besparingBruto = (Number.isFinite(K)) ? (K - jaarkost) : null;
    const besparingNet = (Number.isFinite(baseNet) && besparingBruto != null) ? (baseNet - netdeel) : null;
    const npv = (tp && besparingBruto != null) ? _npvMeerjaar(tp, capex, besparingBruto, besparingNet, oh1) : null;
    const _punt = { k, kva: z.kva, kwh: z.kwh, factuur: z.factuur, capex, rendement, tco, npv, cabine, z };
    _jlog(job, 'resultaat',
          `Mix ${k} batt + ${z.kva} kVA${cabine?' (MS+cabine)':''}: factuur € ${Math.round(z.factuur).toLocaleString('nl-BE')}/jaar` +
          (npv != null ? `, NPV € ${Math.round(npv).toLocaleString('nl-BE')}` : `, TCO € ${Math.round(tco).toLocaleString('nl-BE')}`) +
          (rendement != null ? `, rendement ${rendement.toFixed(1)}%` : ''),
          { opstelling: 'mix', kva: z.kva, kwh: z.kwh });
    return _punt;
  });
  const punten = _mixResultaten.filter(Boolean);
  if (!punten.length) return null;
  let beste;
  if (punten.every(p => p.npv != null)) {
    beste = punten.reduce((a, b) => (b.npv > a.npv ? b : a));               // HOOGSTE NPV @ cost of capital
    _jlog(job, 'ok', `Beste mix: ${beste.k} batterij${beste.k>1?'en':''} + ${beste.kva} kVA ` +
          `(hoogste NPV € ${Math.round(beste.npv).toLocaleString('nl-BE')} van ${punten.length} onderzochte)`, { opstelling: 'mix' });
  } else if (punten.every(p => p.tco != null)) {
    beste = punten.reduce((a, b) => (b.tco < a.tco ? b : a));               // terugval: laagste TCO
    _jlog(job, 'waarschuwing', 'Geen NPV-basis (base_net/disconto) — mix gekozen op laagste TCO.', { opstelling: 'mix' });
  } else {
    beste = punten.reduce((a, b) => (b.factuur < a.factuur ? b : a));       // terugval: laagste factuur
    _jlog(job, 'waarschuwing', 'Geen TCO/NPV-basis meegegeven — mix gekozen op laagste factuur.', { opstelling: 'mix' });
  }
  // Teruggeefvorm compatibel met de assemblage in _draaiSim3 (m.cfg, m.dim.resultaat, m.kva, …).
  beste.cfg = beste.z.cfg;
  beste.dim = { resultaat: beste.z.resultaat, iteraties: beste.z.runs, stappen: [], start_maat: null };
  beste.fractie = null;
  // v15.26.0: per kandidaat ook afname (voor loadfactor/KPI3) en de netkosten-regel
  // (distributie+transport, voor de netkosten×2-blootstelling) meegeven, zodat de frontend
  // het groeipad als KPI1/2/3-evolutie kan tonen zonder extra sim-runs.
  beste.alternatieven = punten.map(p => ({ aantal_batterijen: p.k, kva: p.kva, kwh: p.kwh,
                                           jaarkost: Math.round(p.factuur), capex: Math.round(p.capex),
                                           rendement: p.rendement != null ? Math.round(p.rendement * 10) / 10 : null,
                                           tco: Math.round(p.tco), npv: (p.npv != null ? Math.round(p.npv) : null), cabine: !!p.cabine,
                                           afname_mwh: Math.round((((p.z.resultaat||{}).kpi||{}).totaal_afname_mwh||0) * 10) / 10,
                                           distributie_eur: Math.round(_distributieJF(p.z.resultaat)),
                                           gekozen: p === beste }));
  return beste;
}

function _opstellingUi(ui, opstelling, cap) {
  const v = JSON.parse(JSON.stringify(ui || {}));
  if (opstelling === 'verhogen') {
    // v15.24.0 — FIX: opstelling 1 is PUUR verzwaren, ZONDER batterij. De input draagt de
    // batterij uit stap 9 mee; die werd hier niet gewist, waardoor opstelling 1 stiekem
    // "verzwaren + batterij" werd en zo een onterecht goed rendement toonde. Nu expliciet weg.
    v.batterijId = '';
    v.batterijCustom = null;
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
    const _startEenheden = Math.max(1, Math.ceil((cap.advies_batterij_kwh || 0) / _buKwh(ui)));
    v.batterijCustom = {
      naam: 'Advies-batterij',
      kw: _startEenheden * _buKw(ui), kwh: _startEenheden * _buKwh(ui),
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
// v15.28.0 — meerjarige TCO, IDENTIEK aan het financieel rapport (rapport_generator AAN-blok):
//   capex + Σ_{y=0..H-1}(factuur_y + opex_y). De factuur splitst in niet-net (energie + heffingen,
//   groeit met inflatie) en netkosten (groeit met inflatie+net_extra, met een sprong vanaf
//   net_sprong_jaar). Opex = onderhoud×inflatie + verzekering (‰ op capex); omvormer-vervanging
//   in jaar 10. tp = input._tco (dezelfde parameters die de frontend gebruikt).
function _tcoMeerjaar(tp, capex, jaarkost, netdeel, oh1) {
  if (!tp) return capex + jaarkost * 15;                              // terugval: vlak, 15 j
  const inf = Number(tp.inflatie) || 0, nx = Number(tp.net_extra) || 0;
  const sj = Number(tp.startjaar) || 2026, H = Number(tp.horizon) || 15;
  const verz = capex * (Number(tp.verzekering_promille) || 0) / 1000;
  const nietnet = jaarkost - netdeel;
  const gN = (y) => { let g = Math.pow(1 + inf + nx, y);
    if (tp.net_sprong_jaar && (sj + y) >= Number(tp.net_sprong_jaar)) g *= (1 + (Number(tp.net_sprong) || 0)); return g; };
  let som = 0;
  for (let y = 0; y < H; y++) {
    som += nietnet * Math.pow(1 + inf, y) + netdeel * gN(y) + oh1 * Math.pow(1 + inf, y) + verz;
    if (y === 9) som += (Number(tp.pv_kwp) || 0) * (Number(tp.omvormer_vervang_kwp) || 0);
  }
  return capex + som;
}
// v15.31.0 — NPV @ cost of capital: contante waarde van de netto besparingen over de horizon
// (mét escalatie) MIN de investering. besparingBruto = base0 − jaarkost; besparingNet = base_net −
// netdeel (kan negatief zijn). De keuze van de mix gebeurt nu op de HOOGSTE NPV i.p.v. laagste TCO.
function _npvMeerjaar(tp, capex, besparingBruto, besparingNet, oh1) {
  if (!tp || !(capex > 0) || besparingBruto == null) return null;
  const inf = Number(tp.inflatie) || 0, nx = Number(tp.net_extra) || 0;
  const sj = Number(tp.startjaar) || 2026, H = Number(tp.horizon) || 15;
  const disc = Number(tp.disconto); const r = (isFinite(disc) && disc >= 0) ? disc : 0.05;
  const ed = Number(tp.besparing_energie_deel); const edeel = (isFinite(ed)) ? ed : 0.72;
  const verz = capex * (Number(tp.verzekering_promille) || 0) / 1000;
  const bNet = (besparingNet != null) ? besparingNet : besparingBruto * (1 - edeel);
  const bE = besparingBruto - bNet;
  const gN = (y) => { let g = Math.pow(1 + inf + nx, y);
    if (tp.net_sprong_jaar && (sj + y) >= Number(tp.net_sprong_jaar)) g *= (1 + (Number(tp.net_sprong) || 0)); return g; };
  let pv = 0;
  for (let y = 0; y < H; y++) {
    let netto = (bE * Math.pow(1 + inf, y) + bNet * gN(y)) - (oh1 * Math.pow(1 + inf, y) + verz);
    if (y === 9) netto -= (Number(tp.pv_kwp) || 0) * (Number(tp.omvormer_vervang_kwp) || 0);
    pv += netto / Math.pow(1 + r, y);
  }
  return pv - capex;
}

// ── v15.35.0 — BATTERIJ-ONLY KAMINO (gebouw zonder laadplein) ──────────────────
// Voor een gebouw ZONDER laadplein sweepen we op het BESTAANDE verbruik of één of
// meer batterijen rendement geven (piekshaving + arbitrage + PV-zelfconsumptie +
// onbalans). De aansluiting blijft VAST — er is geen verzwaring en geen cabine.
// Range: van 1 batterij tot Nmax = ceil((toegangsvermogen + 120)/120) eenheden,
// d.w.z. tot we 120 kW batterijvermogen boven het bestaande toegangsvermogen zitten.
// Keuzemaatstaf = HOOGSTE NPV @ cost of capital (= "Optimaal"). Stap 1 = de
// aanbevolen, capex-arme instap (altijd het meest rendabel per geïnvesteerde euro).
async function _batterijSweepGebouw(input, cap, probe, job, startTime) {
  const _sub = r => _subJF(r);
  const _bkw = _buKw(input), _bkwh = _buKwh(input);   // v15.54: module-maat (fallback 120/260)
  const P = Number(input.aansluiting_kva || input.aansluitingKva || input.toegangsvermogen_kw || 0) || 0;
  // v15.54 (Johan): veelvouden van de gekozen module tot de cumulatieve kW ≈ het basisvermogen (P), max 6 stappen.
  const Nmax = Math.min(6, Math.max(1, Math.floor(P / _bkw)));
  const annf = Number(input._kpi_annfactor) || 1;
  const eurKwh = Number((input._investering || {}).eur_per_kwh) || 0;
  const capexVast = Number(input._kpi_capex_vast) || 0;            // ~0 zonder laadplein (geen laadpalen/kabeltracé)
  const horizon = Number((input._investering || {}).horizon_jaar) || 15;
  const tp = input._tco || null;
  const ohBatKw = Number((tp && tp.onderhoud && tp.onderhoud.batterij_kw)) || 0;
  const ohVast = Number((tp && tp.onderhoud_vast)) || 0;
  // Opstelling 0 (Vandaag) = de 'geen'-run; base0 = jaarfactuur op jaarbasis, base0Net = netdeel.
  const base0 = _sub(probe) * annf;
  const base0Net = _distributieJF(probe) * annf;

  _jlog(job, 'start', `Gebouw zonder laadplein — batterij-sweep op bestaand verbruik ` +
                      `(1…${Nmax} batterij${Nmax>1?'en':''}, aansluiting ${P||'?'} kVA vast).`,
        { nmax: Nmax });
  if (job) job.runs_verwacht = Nmax + 2;

  // v15.59.0: elke batterij-stap k is een onafhankelijke sturing-run → parallelliseerbaar (_pmap
  // behoudt de k-volgorde, identiek aan de oude push-lus).
  const _sweepKs = Array.from({ length: Nmax }, (_, i) => i + 1);
  const punten = await _pmap(_sweepKs, async (k) => {
    const cfg = JSON.parse(JSON.stringify(input));
    cfg.batterijId = 'CUSTOM';
    cfg.batterijCustom = { naam: 'Batterij-sweep', kw: k * _bkw, kwh: k * _bkwh,
                           aantal_batterijen: k, dod_pct: 90, rte_pct: 92, capex_eur: 0, max_cycli: 8000 };
    _jlog(job, 'opstelling', `${k} batterij${k>1?'en':''} (${k*_bkw} kW / ${k*_bkwh} kWh) doorrekenen…`,
          { maat: k, eenheid: 'batt' });
    const rS = await _runSimulatorOnce(buildSimInput(_variantUi(cfg, 'sturing')));
    if (job) job.runs = (job.runs || 0) + 1;
    const jaarkost = _sub(rS) * annf;
    const netdeel = _distributieJF(rS) * annf;
    const capex = (Number.isFinite(capexVast) ? capexVast : 0) + k * _bkwh * eurKwh;
    const oh1 = ohVast + (k * _bkw) * ohBatKw;
    const opex1 = oh1 + capex * (Number((tp && tp.verzekering_promille)) || 0) / 1000;
    const besparingBruto = base0 - jaarkost;                        // > 0 = goedkoper dan Vandaag
    const besparingNet = base0Net - netdeel;
    const npv = (tp && capex > 0) ? _npvMeerjaar(tp, capex, besparingBruto, besparingNet, oh1) : null;
    const tco = tp ? _tcoMeerjaar(tp, capex, jaarkost, netdeel, oh1) : (capex + jaarkost * horizon);
    const rendement = (capex > 0) ? ((besparingBruto - opex1) / capex * 100) : null;   // netto jaar-1 rendement
    _jlog(job, 'resultaat',
          `${k} batterij${k>1?'en':''}: factuur € ${Math.round(jaarkost).toLocaleString('nl-BE')}/jaar, ` +
          `besparing € ${Math.round(besparingBruto).toLocaleString('nl-BE')}/jaar` +
          (npv != null ? `, NPV € ${Math.round(npv).toLocaleString('nl-BE')}` : '') +
          (rendement != null ? `, rendement ${rendement.toFixed(1)}%` : ''),
          { maat: k });
    return { k, kw: k * _bkw, kwh: k * _bkwh, cfg, resultaat: rS,
             jaarkost, netdeel, capex, npv, tco, rendement, besparingBruto };
  });
  if (!punten.length) return null;

  // Optimaal = hoogste NPV; terugval op laagste TCO, dan hoogste besparing.
  let beste;
  if (punten.every(p => p.npv != null)) {
    beste = punten.reduce((a, b) => (b.npv > a.npv ? b : a));
    _jlog(job, 'ok', `Optimaal: ${beste.k} batterij${beste.k>1?'en':''} ` +
          `(hoogste NPV € ${Math.round(beste.npv).toLocaleString('nl-BE')} van ${punten.length}). ` +
          `Aanbevolen instap = 1 batterij.`, {});
  } else if (punten.every(p => p.tco != null)) {
    beste = punten.reduce((a, b) => (b.tco < a.tco ? b : a));
    _jlog(job, 'waarschuwing', 'Geen NPV-basis — Optimaal gekozen op laagste TCO.', {});
  } else {
    beste = punten.reduce((a, b) => (b.besparingBruto > a.besparingBruto ? b : a));
    _jlog(job, 'waarschuwing', 'Geen TCO/NPV-basis — Optimaal gekozen op hoogste besparing.', {});
  }

  // Onbalans-variant voor de gekozen (Optimaal) batterij — kers op de taart (2e besparingsregel).
  const vOnb = await _runSimulatorOnce(buildSimInput(_variantUi(beste.cfg, 'onbalans')));
  if (job) job.runs = (job.runs || 0) + 1;
  const varianten = { geen: probe, sturing: beste.resultaat, onbalans: vOnb };
  const _kpiG = (() => {
    const kg = _sub(probe), ks = _sub(beste.resultaat), ko = _sub(vOnb);
    return { kost_geen_excl_btw: kg, kost_sturing_excl_btw: ks, kost_onbalans_excl_btw: ko,
             meerwaarde_sturing_excl_btw: kg - ks, meerwaarde_onbalans_excl_btw: ks - ko,
             onbalans_niet_van_toepassing: false };
  })();

  const alternatieven = punten.map(p => ({
    aantal_batterijen: p.k, kw: p.kw, kwh: p.kwh,
    jaarkost: Math.round(p.jaarkost), besparing_jaar: Math.round(p.besparingBruto),
    capex: Math.round(p.capex), rendement: p.rendement != null ? Math.round(p.rendement * 10) / 10 : null,
    tco: Math.round(p.tco), npv: (p.npv != null ? Math.round(p.npv) : null),
    distributie_eur: Math.round(_distributieJF(p.resultaat) * annf),
    gekozen: p === beste, aanbevolen: (p.k === 1) }));

  _jlog(job, 'klaar', `Batterij-analyse gebouw klaar — Optimaal ${beste.k}×, instap 1× ` +
                      `(${Math.round((Date.now()-startTime)/1000)}s)`, {});
  return { ok: true, modus: 'batterij_gebouw', varianten, kpi_sturing: _kpiG, capaciteit: cap,
           groeipad_gebouw: {
             alternatieven, nmax: Nmax, optimaal_k: beste.k, aanbevolen_stap: 1,
             aansluiting_kva: P, base0_jaar: Math.round(base0), base0_net_jaar: Math.round(base0Net),
             optimaal: { aantal_batterijen: beste.k, kw: beste.kw, kwh: beste.kwh,
                         capex: Math.round(beste.capex), npv: (beste.npv != null ? Math.round(beste.npv) : null),
                         besparing_jaar: Math.round(beste.besparingBruto) } },
           _meta: { elapsed_ms: Date.now() - startTime, server_version: '15.35.0',
                    modus: 'batterij_gebouw', nmax: Nmax, optimaal_k: beste.k } };
}

// ─── BEZOEKERS-SCENARIO'S (v15.81, Johan 20-08) ─────────────────────────────
// Betalend-laadplein-analyse: 6 varianten naast elkaar op DEZELFDE aansluiting
// (nooit verzwaren). Body: { input: <ui>, tarief_eur_mwh, kwh_km, percentages[] }.
//  1) geen batterij, geen plein        → factuur_basis
//  2) batterij, geen plein
//  3..) batterij + bezoekersplein @ pct% sessies
// Per variant: energieprijs (factuur/afname €/MWh), km gevraagd/geleverd (uit de
// bezoekers-pleinen ÷ kwh_km), opbrengst (geleverd × tarief), winst = opbrengst −
// factuur(variant) + factuur_basis. Draait elke variant als één dispatch (geen sweep).
app.post('/api/bezoekers-scenarios', async (req, res) => {
  try {
    const b = req.body || {};
    const baseInput = b.input || b.ui || null;
    if (!baseInput || typeof baseInput !== 'object') return res.status(400).json({ error: 'input (ui) is verplicht' });
    if (!MARKT) return res.status(503).json({ error: 'Marktdata nog niet geladen — probeer over 30 seconden opnieuw' });
    const tarief = Number(b.tarief_eur_mwh) || 500;
    const kwhKm = Number(b.kwh_km) || 0.16;
    const pcts = (Array.isArray(b.percentages) && b.percentages.length) ? b.percentages.map(Number) : [50, 100, 150, 200];
    const isBez = (p) => String((p && p.type_plein) || '').toLowerCase() === 'bezoekers';
    const clone = () => JSON.parse(JSON.stringify(baseInput));
    // v15.83: PIN elke variant op de sturing EXCL. onbalans (_variantUi 'sturing') — exact dezelfde
    // dispatch als de hoofdsim-tegel "factuur sturing zonder onbalans". Zonder deze normalisatie erfde
    // de scenario-run de RAUWE sturing-vlaggen uit STATE.lastSimInput (bsp.actief/pvInjStrategie), waardoor
    // de 100%-rij een ANDERE sturing (bv. incl. onbalans) kon draaien dan de tegel → cijfers liepen uiteen.
    const prep = (ui) => { const v = _variantUi(ui, 'sturing'); v.geen_aansluiting_verhoging = true; delete v._async; return v; };  // aansluiting vast + sturing excl. onbalans
    // v15.85: MIX betalend + gewoon. De basis (geen batt / batt) strippen ENKEL de BETALENDE (bezoekers) pleinen —
    //   de gewone (wagenpark) pleinen blijven vast in de installatie, zodat de sensitiviteit (50–200% sessies) puur
    //   over de betalende pleinen gaat. De CREG-besparing van het gewone plein zit dus al in de referentie en wordt
    //   NIET bij de marginale winst geteld (dubbeltelling); ze wordt apart als informatief cijfer teruggegeven.
    const stripBez = (ui) => { ui.laadpleinen = (ui.laadpleinen || []).filter(p => !isBez(p)); };
    // v15.89: schaal op 0,1 sessie i.p.v. hele sessies. De defaults kunnen fractioneel zijn (AC = 1,5 sessies/paal),
    //   dus Math.round(...) naar een integer rondde bv. 4,5 → 5, óók bij 100% (×1) → de 100%-rij vroeg/leverde méér
    //   dan de hoofdsim (restverschil ~€529 op de jaarfactuur). Op 0,1 afronden houdt 100% exact gelijk aan de hoofdsim.
    const scaleBez = (ui, f) => { (ui.laadpleinen || []).forEach(p => { if (isBez(p)) (p.vensters || []).forEach(v => { v.sessies = Math.round((Number(v.sessies) || 0) * f * 10) / 10; }); }); };
    const noBatt = (ui) => { ui.batterijId = ''; ui.batterijCustom = null; };
    const cregEurMwh = Number(b.creg_eur_mwh) || 0;   // v15.85: CREG-forfait per MWh (client: _cregTarief*1000) → wagenpark-besparing (informatief)

    // v15.88: als er GEEN betalende (bezoekers) pleinen zijn (of client vraagt alleen_basis), heeft het geen zin
    //   de sessie-schaling 50–200% te draaien (er valt niets te schalen). Dan enkel scenario 1 (geen batterij) +
    //   scenario 2 (batterij), telkens MET de gewone (wagenpark) pleinen. Scheelt 4 zware sims.
    const heeftBetalend = (baseInput.laadpleinen || []).some(isBez);
    const alleenBasis = !!b.alleen_basis || !heeftBetalend;

    const variants = [];
    { const ui = prep(clone()); noBatt(ui); stripBez(ui); variants.push({ key: 'geen_batt', label: 'Geen batterij', ui, heeftPlein: false, pct: null }); }
    { const ui = prep(clone()); stripBez(ui); variants.push({ key: 'batt', label: 'Batterij', ui, heeftPlein: false, pct: null }); }
    if (!alleenBasis) {
      for (const pct of pcts) { const ui = prep(clone()); scaleBez(ui, pct / 100); variants.push({ key: 'plein_' + pct, label: 'Batterij + laadplein · ' + pct + '% sessies', ui, heeftPlein: true, pct }); }
    }

    const runs = await _pmap(variants, async (v) => {
      const r = await _runSimulatorOnce(buildSimInput(v.ui));
      const jf = (r && (r.jaarfactuur || r.factuur)) || {};
      const factuur = Number(jf.subtotaal_excl_btw) || 0;
      const afnameMwh = Number((r && r.kpi || {}).totaal_afname_mwh) || 0;
      const pl = (r && r.laadplein && Array.isArray(r.laadplein.pleinen)) ? r.laadplein.pleinen : [];
      // v15.86: splits bezoekers en wagenpark. Opbrengst = Σ GELEVERD per bezoekers-plein × de VERKOOPPRIJS van
      //   DAT plein (niet één globaal tarief). Wagenpark → besparing via CREG (verderop).
      const bezTar = (v.ui.laadpleinen || []).filter(isBez).map(p => Number(p.opbrengst_eur_mwh) || tarief);
      let gevrBez = 0, gelBez = 0, gevrWag = 0, gelWag = 0, opbrengstBez = 0, bezIdx = 0;
      pl.forEach(p => { const bez = String(p.modus || '').toLowerCase() === 'bezoekers';
        const gv = Number(p.gevraagd_mwh) || 0, gl = Number(p.geladen_mwh) || 0;
        if (bez) { gevrBez += gv; gelBez += gl; opbrengstBez += gl * ((bezTar[bezIdx] != null) ? bezTar[bezIdx] : tarief); bezIdx++; }
        else { gevrWag += gv; gelWag += gl; } });
      return Object.assign({}, v, { factuur, energieprijs: afnameMwh > 0 ? factuur / afnameMwh : 0,
        gevrMwh: gevrBez, gelMwh: gelBez, gevrWag, gelWag, opbrengstBez });
    });

    const basis = runs.find(x => x.key === 'geen_batt');
    const factuurBasis = basis ? basis.factuur : 0;
    // v15.84: referentie voor de MARGINALE laadprijs = de batterij-rij (batterij, GEEN plein).
    //   marg. laadprijs = (factuur_scenario − factuur_batterij) / geladen_mwh = wat ENKEL de extra
    //   laadenergie kost, los van gebouw/vaste kosten. Tarief − marg. laadprijs = marge per MWh.
    const batt = runs.find(x => x.key === 'batt');
    const factuurBatt = batt ? batt.factuur : factuurBasis;
    // v15.87: GEWOON (wagenpark) plein → NETTO CREG-besparing, nu ÉÉN CONSTANTE over alle rijen.
    //   Besparing = geleverd_wag × (CREG-forfait − je EIGEN laadkost). Als eigen laadkost nemen we de
    //   all-in afnameprijs van de BASIS (geen_batt) — één vaste referentie, want de CREG-saving van het
    //   wagenpark hangt niet af van de bezoekers-schaal. Voorheen varieerde dit per rij (marg. vs all-in)
    //   → verwarrend (€1036–1416 voor hetzelfde plein). Nu constant en gelijk aan het scherm/rapport.
    //   NIET bruto (geleverd × CREG) — dat zou doen alsof laden gratis is (dubbeltelling met de factuur).
    const basisEnergieprijs = basis ? basis.energieprijs : 0;
    const basisGelWag = basis ? (basis.gelWag || 0) : 0;
    const besparingWagConst = Math.max(0, basisGelWag * (cregEurMwh - basisEnergieprijs));
    const rows = runs.map(v => {
      const opbrengst = v.heeftPlein ? (v.opbrengstBez || 0) : 0;           // v15.86: Σ GELEVERD per bezoekers-plein × verkoopprijs van DAT plein
      const margLaadprijs = (v.heeftPlein && v.gelMwh > 1e-9) ? (v.factuur - factuurBatt) / v.gelMwh : null;
      const besparingWag = besparingWagConst;   // constant (zie boven)
      // Marginale winst (batterij + betalend plein) + de vaste gewoon-plein besparing.
      const winst = opbrengst - v.factuur + factuurBasis + besparingWag;
      return {
        key: v.key, label: v.label, pct: v.pct, heeftPlein: v.heeftPlein,
        is_basis: v.key === 'geen_batt',
        energieprijs_eur_mwh: Math.round(v.energieprijs),   // all-in afnameprijs = factuur / totale afname
        km_gevraagd: v.heeftPlein && kwhKm > 0 ? Math.round(v.gevrMwh * 1000 / kwhKm) : null,
        km_geleverd: v.heeftPlein && kwhKm > 0 ? Math.round(v.gelMwh * 1000 / kwhKm) : null,   // v15.86: bezoekers, GELEVERD
        pct_geleverd: v.heeftPlein && v.gevrMwh > 1e-9 ? Math.round(v.gelMwh / v.gevrMwh * 1000) / 10 : null,
        opbrengst_eur: v.heeftPlein ? Math.round(opbrengst) : null,
        // v15.86: levering van de NIET-betalende (wagenpark) pleinen komt in ELKE rij mee, met hun CREG-besparing.
        geleverd_wag_mwh: (v.gelWag > 1e-9) ? Math.round(v.gelWag * 10) / 10 : null,
        km_wag: (v.gelWag > 1e-9 && kwhKm > 0) ? Math.round(v.gelWag * 1000 / kwhKm) : null,
        besparing_wag_eur: (besparingWag > 1e-9) ? Math.round(besparingWag) : null,   // v15.85: wagenpark-besparing (mix)
        jaarfactuur_eur: Math.round(v.factuur),
        winst_eur: Math.round(winst),
        laadprijs_marg_eur_mwh: (margLaadprijs == null) ? null : Math.round(margLaadprijs),
      };
    });
    return res.json({ ok: true, tarief_eur_mwh: tarief, kwh_km: kwhKm, creg_eur_mwh: cregEurMwh, factuur_basis: Math.round(factuurBasis),
      besparing_wag_const_eur: Math.round(besparingWagConst), basis_energieprijs_eur_mwh: Math.round(basisEnergieprijs),
      heeft_betalend: heeftBetalend, alleen_basis: alleenBasis,
      rows, _meta: { server_version: '15.89.0', runs: runs.length } });
  } catch (e) {
    console.error('[bezoekers-scenarios] fout:', e.message);
    return res.status(500).json({ error: 'bezoekers-scenarios gefaald: ' + e.message });
  }
});

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
  input._mgr_ok = await _isManagerReq(req);   // v15.68: opgeladen profiel enkel voor managers

  // v15.20: async-modus. De UI zet _async:true, krijgt meteen een job_id en pollt
  // /api/sim-voortgang/:id. Zonder _async blijft alles exact zoals vroeger — oude
  // clients en de retry-ladder merken niets.
  if (input._async) {
    const job = _jobNieuw();
    res.json({ ok: true, async: true, job_id: job.id });
    _draaiSim3(input, job)
      .then(r => { _verrijkIjk(r, input); if (input._profiel_info && r && r._meta) r._meta.profiel = input._profiel_info; job.resultaat = r; job.status = 'klaar';
                   _jlog(job, 'klaar', 'Simulatie afgerond.'); })
      .catch(e => { job.fout = e.message; job.status = 'fout';
                    _jlog(job, 'fout', 'Simulatie gefaald: ' + e.message);
                    console.error('[sim-3] async fout:', e.message); });
    return;
  }
  try {
    const r = await _draaiSim3(input, null);
    _verrijkIjk(r, input);
    if (input._profiel_info && r && r._meta) r._meta.profiel = input._profiel_info;   // v15.67: profiel-label
    return res.json(r);
  } catch (e) {
    console.error('[sim-3] fout:', e.message);
    return res.status(500).json({ error: 'Simulatie-3 gefaald: ' + e.message });
  }
});

// ─── v15.23.0 — GROEIPAD (blok 10) ──────────────────────────────────────────
// Voor de optimale opstelling houden we de aansluiting VAST en zetten we vanaf 1 batterij
// telkens één standaardeenheid (120 kW / 260 kWh) bij, tot het volgroeide aantal. Per stap:
// hoeveel % van de gevraagde laadenergie geraakt geleverd? Dat geeft de klant een instap-pad
// beginnend bij 1 batterij (overdracht §4 / spec Johan 17/07).
//
// ⚠ Elke stap is een echte dispatch-run. De sim kan de aansluiting nog zelf optrekken als de
// batterij te klein is (laadplein.toegangsvermogen_verhoogd_kw>0); we geven dat mee terug zodat
// de frontend ziet of een stap écht op de vaste aansluiting past. VEREIST een live smoke-test.
app.post('/api/groeipad', async (req, res) => {
  const input = req.body;
  if (!input || typeof input !== 'object') return res.status(400).json({ error: 'body is verplicht' });
  if (!MARKT) return res.status(503).json({ error: 'Marktdata nog niet geladen — probeer over 30 seconden opnieuw' });
  // v15.32.0: het groeipad rekent nu op MEERDERE vaste aansluitingen (die van opstelling 2 én
  // opstelling 3). Per batterijstap draaien we elke aansluiting → de frontend kiest per stap de
  // aansluiting met de hoogste NPV (partiële levering). Back-compat: enkel aansluiting_kva → 1 lijst.
  const aansluitingKvaEnkel = Number(input.aansluiting_kva || input.aansluitingKva || 0);
  let aansluitingen = Array.isArray(input.aansluitingen_kva)
    ? input.aansluitingen_kva.map(Number).filter((v) => v > 0)
    : [];
  if (!aansluitingen.length && aansluitingKvaEnkel > 0) aansluitingen = [aansluitingKvaEnkel];
  aansluitingen = [...new Set(aansluitingen.map((v) => Math.round(v)))].sort((a, b) => a - b);   // dedupe + oplopend
  const maxBatt = Math.max(1, Math.min(20, Math.round(Number(input.max_batterijen || 0)) || 1));
  if (!aansluitingen.length) return res.status(400).json({ error: 'aansluiting_kva of aansluitingen_kva (vast) is verplicht' });
  // v15.56 (Johan 01-08): het groeipad stapt nu met de GEBRUIKTE batterijmodule (input.batt_module) i.p.v. de vaste
  // 120/260. Zo groeit een kleine site in 5/10- of 30/60-stappen — consistent met de sizing/pv-sweep/opstelling.
  const _gpKw = _buKw(input), _gpKwh = _buKwh(input);
  input._mgr_ok = await _isManagerReq(req);   // v15.68: manager-only
  await _pasOpgeladenAfnameToe(input);   // v15.67: opgeladen afname-profiel (fallback = standaard). cfg-clones erven het.
  try {
    // Gevraagde laadenergie uit de input (Σ per plein), zodat we het % kunnen berekenen.
    const pleinen = Array.isArray(input.laadpleinen) ? input.laadpleinen : [];
    const gevraagdMwhTot = pleinen.reduce((s, p) =>
      s + (Number(p.aantal) || 0) * (Number(p.km_per_jaar) || 0) * (Number(p.kwh_per_km) || 0) / 1000, 0);
    // v15.59.0: elke (aansluiting c × batterij k)-stap is een onafhankelijke sturing-run →
    // parallelliseerbaar. Combos in dezelfde nestvolgorde (c buiten, k binnen) zodat de
    // stappen-volgorde identiek blijft aan de oude dubbele lus.
    const _combos = [];
    for (const c of aansluitingen) for (let k = 1; k <= maxBatt; k++) _combos.push({ c, k });
    const stappen = await _pmap(_combos, async ({ c, k }) => {
        const cfg = JSON.parse(JSON.stringify(input));
        // Aansluiting VAST op de kandidaat-aansluiting; batterij = k eenheden.
        cfg.aansluiting_kva = c; cfg.aansluitingKva = c;
        cfg.toegangsvermogen_kw = c;
        cfg.geen_aansluiting_verhoging = true;   // v15.29.0: aansluiting mag NIET verhoogd worden → clip + tekort
        cfg.batterijId = 'CUSTOM';
        cfg.batterijCustom = Object.assign({}, cfg.batterijCustom || {}, {
          naam: 'Groeipad-batterij', kw: k * _gpKw, kwh: k * _gpKwh, aantal_batterijen: k,   // v15.56: module-maat (fallback 120/260)
          dod_pct: 90, rte_pct: 92, capex_eur: 0, max_cycli: 8000,
        });
        const r = await _runSimulatorOnce(buildSimInput(_variantUi(cfg, 'sturing')));
        const lp = (r && r.laadplein) || {};
        const geladenMwh = Array.isArray(lp.pleinen)
          ? lp.pleinen.reduce((s, p) => s + (Number(p.geladen_mwh) || 0), 0)
          : (Number(lp.ev_last_mwh) || 0);
        const pct = gevraagdMwhTot > 0 ? Math.min(100, Math.round(geladenMwh / gevraagdMwhTot * 1000) / 10) : null;
        return {
          aansluiting_kva: c,   // v15.32.0: welke vaste aansluiting deze stap draaide
          aantal_batterijen: k, kw: k * _gpKw, kwh: k * _gpKwh,   // v15.56: module-maat (fallback 120/260)
          gevraagd_mwh: Math.round(gevraagdMwhTot * 10) / 10,
          geladen_mwh: Math.round(geladenMwh * 10) / 10,
          geleverd_pct: pct,
          aansluiting_verhoogd_kw: Number(lp.toegangsvermogen_verhoogd_kw) || 0,   // >0 → paste niet op de vaste aansluiting
          factuur_sturing_excl_btw: Math.round(Number((r.jaarfactuur || r.factuur || {}).subtotaal_excl_btw) || 0),
          distributie_eur: Math.round(_distributieJF(r)),   // v15.30.0: netkosten (B+C+D) → cumulatieve besparing frontend
          afname_mwh: Math.round((Number((r.kpi || {}).totaal_afname_mwh) || 0) * 10) / 10,   // v15.50.0: grid-afname → loadfactor per stap (KPI3) client-side = afname/(8760×aansluiting)
          factuur_detail: _frCompJF(r),   // v15.33.0: componenten voor de groeipad-detailfactuur per stap
        };
    });
    return res.json({ ok: true, aansluiting_kva: aansluitingen[aansluitingen.length - 1],
      aansluitingen_kva: aansluitingen, max_batterijen: maxBatt,
      gevraagd_mwh: Math.round(gevraagdMwhTot * 10) / 10, stappen,
      batt_module: { kw: _gpKw, kwh: _gpKwh },   // v15.56: welke module dit groeipad stapte
      _meta: { server_version: '15.56.0' } });
  } catch (e) {
    console.error('[groeipad] fout:', e.message);
    return res.status(500).json({ error: 'groeipad gefaald: ' + e.message });
  }
});

// ── THUISLADEN: POST /api/thuisladen ─────────────────────────────────────────
// Cafetariaplan-laadpaal (particulier). Bouwt één residentiële base-input via
// buildSimInput en laat simulator.py (_modus:'thuisladen') het ankernet van
// 30 dispatches (6 batterij-kWh × 5 PV-panelen, ALLE zonder onbalans) draaien.
// De frontend interpoleert daar tussen (spline/bilineair) en kan losse cellen
// bijsimuleren. Onbalans blijft — per Johan — buiten de zoektocht (extra winst
// apart, client-side indicatief). Geen regressie op andere tegels: de mode is
// volledig geïsoleerd (eigen _modus-tak in main() + run_thuisladen()).
// Gedeelde input-opbouw voor /api/thuisladen (volledig grid) én /api/thuisladen-cel (één cel).
// Bouwt de residentiële base-input + het thuisladen-parameterblok + de anker-assen.
function _thuisladenInput(inv, par) {
  const maxKva = Number(inv.fasen) === 2 ? Number(par.MAX_KVA_2F || 5) : Number(par.MAX_KVA_3F || 25);
  const piekKw = Math.max(0.5, Number(inv.piek_kw || 5));
  const wagens = Array.isArray(inv.wagens) ? inv.wagens : [];
  const battSeries = (Array.isArray(par.BATT) ? par.BATT : [])
    .map(x => ({ kva: Number(x.kva), kwh: Number(x.kwh), eur: Number(x.eur) }))
    .filter(x => x.kva > 0 && x.kwh > 0 && x.kva <= maxKva);
  // Bij 2 fasen (5 kVA) kappen we de PV-as af op 20 panelen (kleine aansluiting). Client spiegelt dit.
  const pvStap = Number(par.PV_STAP || 2);
  const pvMax = (Number(inv.fasen) === 2) ? Math.min(20, Number(par.PV_MAX || 40)) : Number(par.PV_MAX || 40);
  const pctPan = pct => Math.round(pvMax * pct / 100 / pvStap) * pvStap;
  const battAs = [0, 5, 10, 15, 20, 25];
  const pvAs   = [0, pctPan(25), pctPan(50), pctPan(75), pctPan(100)];
  const paneelWp = Number(par.PANEEL_WP || 450);
  const maxExtraKwp = pvMax * paneelWp / 1000;        // grootste anker (pvMax panelen)
  const bestaandeKwp = Number(inv.bestaand_pv_kwp) > 0 ? Number(inv.bestaand_pv_kwp) : 0;
  const ui = {
    grd: inv.grd || 'Fluvius West',
    spanning: 'LS',
    jaarverbruik_mwh: Number(inv.jaarverbruik_mwh || 3.5),
    profielNaam: inv.profielNaam || inv.profiel || 'residentieel',
    aansluiting_kva: maxKva,
    toegangsvermogen_kw: piekKw,
    // FIX: base met pv_kwp=0 bouwt GEEN zonvorm → extra PV per anker produceert niets. We bouwen
    // de base daarom met de MAX extra PV (zodat vorm + omvormer gebouwd worden) en resetten hieronder
    // pv.kwp naar enkel de bestaande PV; run_thuisladen telt per anker de panelen bovenop mét zonvorm.
    pv_kwp: maxExtraKwp,
    bestaande_pv: (bestaandeKwp > 0)
      ? { aanwezig: true, kwp: bestaandeKwp, inj_mwh_jaar: Number(inv.bestaand_inj_mwh || 0), maand: 6 }
      : { aanwezig: false },
    pvInjStrategie: 'passthrough',
    geen_arbitrage: false,
    laadpleinen: [],
  };
  const base = buildSimInput(ui);   // kan gooien → aanroeper vangt
  // Reset PV-kwp naar ENKEL de bestaande PV; de zonvorm + omvormer (voor max PV) blijven staan.
  if (base.pv) base.pv.kwp = bestaandeKwp;
  const thuisladen = {
    wagens: wagens.map(w => ({
      km: Number(w.km || 0), kwhkm: Number(w.kwhkm || 0.16),
      wd_start: Number(w.wd_start != null ? w.wd_start : 19), wd_eind: Number(w.wd_eind != null ? w.wd_eind : 7),
      we_start: Number(w.we_start != null ? w.we_start : 0), we_eind: Number(w.we_eind != null ? w.we_eind : 24),
      creg: !!w.creg,
    })),
    referentiekost: Number(inv.referentiekost || 0),
    creg_eur_mwh: Number(par.CREG || 322),
    diesel_100km: Number(par.DIESEL_100KM || 9),
    pv_kost: Number(par.PV_KOST || 500),
    paneel_wp: Number(par.PANEEL_WP || 450),
    laadvermogen_basis: Number(par.LAADVERMOGEN_BASIS || 4),
    max_kva: maxKva,
    batt_dod: Number(par.BATT_DOD || 90),
    batt_rte: Math.round((Number(par.RTE || 0.90)) * 100),
    batt_crate: Number(par.BATT_CRATE || 2),   // omvormer-sizing: kW = kWh / C-rate
    batt_series: battSeries,
    batt_kwh_as: battAs,
    pv_pan_as: pvAs,
  };
  return { base, thuisladen, battAs, pvAs };
}

app.post('/api/thuisladen', async (req, res) => {
  const b = req.body || {};
  if (typeof b !== 'object') return res.status(400).json({ error: 'body is verplicht' });
  if (!MARKT) return res.status(503).json({ error: 'Marktdata nog niet geladen — probeer over 30 seconden opnieuw' });
  const inv = b.in || {};                 // klant-invoer (IN uit de frontend)
  const par = b.params || {};             // model-/procesparameters (P uit de frontend)
  if (!(Array.isArray(inv.wagens) && inv.wagens.length)) return res.status(400).json({ error: 'minstens één wagen is verplicht' });

  let base, thuisladen, battAs, pvAs;
  try { ({ base, thuisladen, battAs, pvAs } = _thuisladenInput(inv, par)); }
  catch (e) { console.error('[thuisladen] input-opbouw fout:', e.message); return res.status(500).json({ error: 'base-input bouwen faalde: ' + e.message }); }
  const payload = { thuisladen };

  try {
    // PARALLELLE ORKESTRATIE (v15.75.0): i.p.v. één trage Python-lus (30 dispatches sequentieel →
    // client-time-out) spawnen we per cel een eigen dispatch via _runSimulatorOnce, gecapt op
    // SIM_MAX_PARALLEL (zelfde patroon als /api/groeipad). Python rekent per cel (_tl_cell) enkel
    // die ene dispatch + detail; de besparing (referentie − scenario) rekenen we hier na afloop.
    const t0 = Date.now();
    const cellList = [];
    for (const b of battAs) for (const pv of pvAs) cellList.push({ b, pv });
    let incomeBlk = null, evMwh = null, homeKw = null;
    const rawCells = await _pmap(cellList, async ({ b, pv }) => {
      const r = await _runSimulatorOnce({ _modus: 'thuisladen', base, thuisladen: payload.thuisladen, _tl_cell: { bkwh: b, pan: pv } });
      if (!r) return null;
      if (!incomeBlk && r.income) incomeBlk = r.income;
      if (evMwh == null && r.ev_mwh != null) evMwh = r.ev_mwh;
      if (homeKw == null && r.home_kw != null) homeKw = r.home_kw;
      return r.cell || null;
    });
    const cells = rawCells.filter(Boolean);
    // Referentie = 0/0-cel; besparing = referentie − scenariokost; rendement = besparing ÷ capex.
    const c00 = cells.find(c => c.battKwh === 0 && c.pvPan === 0);
    const refKost = (c00 && c00.kost != null) ? c00.kost : null;
    if (refKost != null) {
      for (const c of cells) {
        if (c.kost == null) continue;
        const besp = refKost - c.kost;
        c.ref_kost = Math.round(refKost * 10) / 10;
        c.besparing = Math.round(besp * 10) / 10;
        c.rendement = c.capex > 0 ? Math.round((besp / c.capex * 100) * 100) / 100 : (besp > 0 ? 999 : 0);
      }
    }
    console.log(`[thuisladen] ${cells.length} ankers parallel in ${Date.now() - t0}ms (SIM_MAX_PARALLEL=${SIM_MAX_PARALLEL})`);
    return res.json({
      ok: true,
      anchors: cells,
      grid: { batt_kwh_as: battAs, pv_pan_as: pvAs },
      referentie_kost: refKost,
      income: incomeBlk,
      ev_mwh: evMwh,
      home_kw: homeKw,
      runs: cells.length,
      // v15.79.0: welke tariefkaart draait er? Zonder postcode valt _thuisladenInput stil
      // terug op 'Fluvius West'|LS (Vlaanderen). De app toont dit expliciet zodat de klant
      // weet welke netbeheerder/regio verondersteld is (transport_* = 0 hoort bij VL/BR).
      netbeheer: {
        grd: (base.netbeheer && base.netbeheer.grd) || null,
        spanning: (base.netbeheer && base.netbeheer.spanning) || null,
        regio: (base.netbeheer && base.netbeheer.tarieven && base.netbeheer.tarieven._regio) || null,
        default: !inv.grd,   // true = geen postcode/grd meegegeven → West-fallback
      },
      _meta: { server_version: '15.79.0', modus: 'thuisladen-parallel', elapsedMs: Date.now() - t0 },
    });
  } catch (e) {
    console.error('[thuisladen] fout:', e.message);
    return res.status(500).json({ error: 'thuisladen gefaald: ' + e.message });
  }
});

// ── THUISLADEN single cel: POST /api/thuisladen-cel ──────────────────────────
// Eén ECHTE dispatch voor exact de aangeklikte (batterij-kWh, PV-panelen)-cel, zodat
// "klik om exact te simuleren" in server-modus de echte cijfers geeft i.p.v. het lokale
// benaderingsmodel (dat inconsistente uitschieters in de heatmap gaf). De besparing rekent
// de client zelf uit de al gekende 0/0-referentie. Snel (één dispatch).
app.post('/api/thuisladen-cel', async (req, res) => {
  const b = req.body || {};
  if (typeof b !== 'object') return res.status(400).json({ error: 'body is verplicht' });
  if (!MARKT) return res.status(503).json({ error: 'Marktdata nog niet geladen — probeer over 30 seconden opnieuw' });
  const inv = b.in || {}, par = b.params || {}, cel = b.cel || {};
  if (!(Array.isArray(inv.wagens) && inv.wagens.length)) return res.status(400).json({ error: 'minstens één wagen is verplicht' });
  let base, thuisladen;
  try { ({ base, thuisladen } = _thuisladenInput(inv, par)); }
  catch (e) { return res.status(500).json({ error: 'base-input bouwen faalde: ' + e.message }); }
  try {
    const r = await _runSimulatorOnce({ _modus: 'thuisladen', base, thuisladen,
      _tl_cell: { bkwh: Number(cel.b || 0), pan: Number(cel.pv || 0) } });
    return res.json({ ok: true, cell: (r && r.cell) || null });
  } catch (e) {
    console.error('[thuisladen-cel] fout:', e.message);
    return res.status(500).json({ error: 'thuisladen-cel gefaald: ' + e.message });
  }
});

// ── v15.36.0 — PV-SWEEP: POST /api/pv-sweep ──────────────────────────────────
// Sweept een reeks PV-vermogens (kWp) op een VASTE aansluiting + VAST batterij-aantal
// (de instap/Optimaal-config). Per stap draait een echte dispatch ('sturing') en geven we
// de factuur + netkosten terug, zodat de frontend de MARGINALE PV-waarde toont (besparing
// t.o.v. de 0-PV-stap) en de klant één PV-systeem kiest. Zelfde patroon als /api/groeipad.
app.post('/api/pv-sweep', async (req, res) => {
  const input = req.body;
  if (!input || typeof input !== 'object') return res.status(400).json({ error: 'body is verplicht' });
  if (!MARKT) return res.status(503).json({ error: 'Marktdata nog niet geladen — probeer over 30 seconden opnieuw' });
  const c = Number(input.aansluiting_kva || input.aansluitingKva || 0);
  const k = Math.max(0, Math.round(Number(input.aantal_batterijen || 0)));
  let pvLijst = Array.isArray(input.pv_kwp_lijst) ? input.pv_kwp_lijst.map(Number).filter((v) => v >= 0) : [];
  pvLijst = [...new Set(pvLijst.map((v) => Math.round(v)))].sort((a, b) => a - b);   // dedupe + oplopend
  if (!(c > 0)) return res.status(400).json({ error: 'aansluiting_kva (vast) is verplicht' });
  if (!pvLijst.length) return res.status(400).json({ error: 'pv_kwp_lijst is verplicht' });
  input._mgr_ok = await _isManagerReq(req);   // v15.68: manager-only
  await _pasOpgeladenAfnameToe(input);   // v15.67: opgeladen afname-profiel (fallback = standaard)
  try {
    const stappen = [];
    // v15.51: MARGINALE zelfconsumptie. Bij bestaande PV telde het geblende % (bestaand + nieuw) de zelfconsumptie
    // van de bestaande panelen mee, waardoor de nieuwe PV te gunstig oogde. We nemen nu het NULPUNT (pv=0 = enkel
    // bestaande PV) als baseline en beoordelen de nieuwe PV op zijn EIGEN merites:
    //   marginaal% = (zelfverbruik(pv) − zelfverbruik(bestaand-only)) / (productie(pv) − productie(bestaand-only)).
    // Zonder bestaande PV is de baseline 0 → marginaal == geblend (geen regressie).
    // v15.59.0: elke pv-stap is een onafhankelijke sturing-run → parallel draaien en dan de
    // marginale-baseline (pv=0) in een tweede pass toepassen. De deltas hangen af van het pv=0-punt,
    // dat we dus ná het verzamelen bepalen (niet meer tijdens de lus). Volgorde = pvLijst (oplopend).
    let _prod0 = null, _zelf0 = null;
    const _ruw = await _pmap(pvLijst, async (pv) => {
      const cfg = JSON.parse(JSON.stringify(input));
      cfg.aansluiting_kva = c; cfg.aansluitingKva = c; cfg.toegangsvermogen_kw = c;
      cfg.geen_aansluiting_verhoging = true;         // aansluiting blijft vast (net als het groeipad)
      cfg.pv_kwp = pv; cfg.pvKwp = pv;
      if (k > 0) {
        cfg.batterijId = 'CUSTOM';
        cfg.batterijCustom = Object.assign({}, cfg.batterijCustom || {}, {
          naam: 'PV-sweep-batterij', kw: k * _buKw(input), kwh: k * _buKwh(input), aantal_batterijen: k,   // v15.54: module-maat (fallback 120/260)
          dod_pct: 90, rte_pct: 92, capex_eur: 0, max_cycli: 8000,
        });
      } else { cfg.batterijId = ''; cfg.batterijCustom = null; }
      const r = await _runSimulatorOnce(buildSimInput(_variantUi(cfg, 'sturing')));
      const jf = r.jaarfactuur || r.factuur || {};
      const grC = (jf.groepen && (jf.groepen.C_netgebruik_injectie || jf.groepen.C)) || {};
      const kpi = (r && r.kpi) || {};
      // v15.38.0: PV-flows uit de KPI (voor de zelfconsumptie-% in de tabel). Zelfconsumptie =
      // (direct + via batterij) / bruto productie. Bruto = potentiële productie (na curtailment-verlies).
      const _prodBruto = Number(kpi.pv_potentiele_productie_mwh) || (pv * 0.95);
      const _zelf = (Number(kpi.pv_direct_zelfverbruik_mwh) || 0) + (Number(kpi.pv_naar_batterij_mwh) || 0);
      return { pv, r, jf, grC, kpi, _prodBruto, _zelf };
    });
    // v15.51: baseline op pv=0 (enkel bestaande PV). pvLijst is oplopend gesorteerd, dus 0 komt eerst.
    for (const x of _ruw) { if (x.pv === 0) { _prod0 = x._prodBruto; _zelf0 = x._zelf; } }
    for (const { pv, r, jf, grC, kpi, _prodBruto, _zelf } of _ruw) {
      const _dProd = (_prod0 != null ? (_prodBruto - _prod0) : _prodBruto);   // productie van de NIEUWE PV alleen
      const _dZelf = (_zelf0 != null ? (_zelf - _zelf0) : _zelf);             // extra zelfverbruik door de NIEUWE PV
      stappen.push({
        pv_kwp: pv,
        productie_mwh: Math.round((_prodBruto) * 10) / 10,             // bruto productie (KPI, of ~950 kWh/kWp)
        factuur_sturing_excl_btw: Math.round(Number(jf.subtotaal_excl_btw) || 0),
        distributie_eur: Math.round(_distributieJF(r)),               // netkosten (B+C+D) → besparingNet frontend
        injectie_eur: Math.round(Number(grC._subtotaal) || 0),        // groep C (injectie) — negatief = opbrengst
        factuur_detail: _frCompJF(r),
        // v15.38.0: zelfconsumptie-splitsing (MWh) + % t.o.v. productie
        pv_direct_mwh: Math.round((Number(kpi.pv_direct_zelfverbruik_mwh) || 0) * 10) / 10,
        pv_via_batterij_mwh: Math.round((Number(kpi.pv_naar_batterij_mwh) || 0) * 10) / 10,
        pv_injectie_mwh: Math.round((Number(kpi.pv_injectie_mwh) || 0) * 10) / 10,
        zelfconsumptie_pct: (_prodBruto > 0 && pv > 0) ? Math.round(_zelf / _prodBruto * 1000) / 10 : null,          // geblend (bestaand + nieuw) — voor de tabel/referentie
        marginale_zelfconsumptie_pct: (_dProd > 0 && pv > 0) ? Math.round(_dZelf / _dProd * 1000) / 10 : null,       // v15.51: nieuwe PV op eigen merites → de keuze-drempel rekent hierop
      });
    }
    return res.json({ ok: true, aansluiting_kva: c, aantal_batterijen: k, pv_kwp_lijst: pvLijst, stappen,
      _meta: { server_version: '15.52.0' } });
  } catch (e) {
    console.error('[pv-sweep] fout:', e.message);
    return res.status(500).json({ error: 'pv-sweep gefaald: ' + e.message });
  }
});

// ── v15.37.0 — POST /api/opstelling: één volledige opstelling (varianten + KPI) op een VASTE config ──
// Voor "Herbereken voor groeistap 1": draait de drie sturingen (geen/sturing/onbalans) op een vaste
// aansluiting + vast batterij-aantal en geeft één opstellings-object terug in dezelfde vorm als de
// opstellingen in _draaiSim3 (varianten, kpi_sturing, config, dimensionering). Zo kan de frontend het
// als extra opstelling "Groeistap 1" naast Optimaal tonen, met een echte detailfactuur.
app.post('/api/opstelling', async (req, res) => {
  const input = req.body;
  if (!input || typeof input !== 'object') return res.status(400).json({ error: 'body is verplicht' });
  if (!MARKT) return res.status(503).json({ error: 'Marktdata nog niet geladen — probeer over 30 seconden opnieuw' });
  const c = Number(input.aansluiting_kva || input.aansluitingKva || 0);
  const k = Math.max(0, Math.round(Number(input.aantal_batterijen || 0)));
  if (!(c > 0)) return res.status(400).json({ error: 'aansluiting_kva is verplicht' });
  input._mgr_ok = await _isManagerReq(req);   // v15.68: manager-only
  await _pasOpgeladenAfnameToe(input);   // v15.67: opgeladen afname-profiel (fallback = standaard)
  try {
    const cfg = JSON.parse(JSON.stringify(input));
    cfg.aansluiting_kva = c; cfg.aansluitingKva = c; cfg.toegangsvermogen_kw = c;
    cfg.geen_aansluiting_verhoging = true;              // vaste aansluiting (geen verzwaring)
    if (k > 0) {
      cfg.batterijId = 'CUSTOM';
      cfg.batterijCustom = Object.assign({}, cfg.batterijCustom || {}, {
        naam: 'Groeistap-batterij', kw: k * _buKw(input), kwh: k * _buKwh(input), aantal_batterijen: k,   // v15.54: module-maat (fallback 120/260)
        dod_pct: 90, rte_pct: 92, capex_eur: 0, max_cycli: 8000,
      });
    } else { cfg.batterijId = ''; cfg.batterijCustom = null; }
    const _sub = r => _subJF(r);
    const _pv = Number(cfg.pv_kwp || cfg.pvKwp || 0) > 0;
    const heeftFlex = _pv || k > 0;
    const geen    = await _runSimulatorOnce(buildSimInput(_variantUi(cfg, 'geen')));
    const sturing = await _runSimulatorOnce(buildSimInput(_variantUi(cfg, 'sturing')));
    const onbalans = heeftFlex ? await _runSimulatorOnce(buildSimInput(_variantUi(cfg, 'onbalans'))) : sturing;
    const varianten = { geen, sturing, onbalans };
    const kg = _sub(geen), ks = _sub(sturing), ko = _sub(onbalans);
    const kpi_sturing = { kost_geen_excl_btw: kg, kost_sturing_excl_btw: ks, kost_onbalans_excl_btw: ko,
      meerwaarde_sturing_excl_btw: kg - ks, meerwaarde_onbalans_excl_btw: ks - ko,
      onbalans_niet_van_toepassing: !heeftFlex };
    return res.json({ ok: true, opstelling: {
      varianten, kpi_sturing,
      config: {
        spanning: input.spanning || 'LS', spanning_omgezet: false, spanning_origineel: null,
        aansluiting_kva: c, toegangsvermogen_kw: c, batterij: cfg.batterijCustom || null,
        cabine_nodig: false, aantal_batterijen: k,
      },
      dimensionering: {
        haalbaar: true, iteraties: 0, beoordeeld_op: 'sturing', stappen: [], start_maat: null,
        gekozen_maat: k > 0 ? k * _buKwh(input) : null, eenheid: k > 0 ? 'kWh' : null,
        verloren_dagen: 0, totaal_dagen: null,
      },
    },
    _ijk: _bouwIjk('opstelling-batterij','kost',
      { aansluiting_kva:c, aantal_batterijen:k, batterij_kwh:k>0?k*_buKwh(input):null,
        pv_kwp:+(cfg.pv_kwp||cfg.pvKwp||0)||null, profiel:cfg.profielNaam||cfg.profiel_naam||null,
        jaarverbruik_mwh:+(cfg.jaarverbruik||0)||null, laadpleinen:(cfg.laadpleinen||[]).length },
      { paper_capture_rate:0.018, forecast_modus:(cfg.bsp&&cfg.bsp.forecast_modus)||'realistic', kalibratie:1.0 },
      { basis:kg, sturing:ks, onbalans:ko, plafond:null }),
    _meta: { server_version: SERVER_VERSIE } });
  } catch (e) {
    console.error('[opstelling] fout:', e.message);
    return res.status(500).json({ error: 'opstelling gefaald: ' + e.message });
  }
});

// v15.39: injectie-optimalisatie voor BESTAANDE PV (SolarActive). Geen dispatch/LP nodig — zuivere
// waardering van het injectieprofiel op spot + onbalans (imbalance settlement, passieve respons).
app.post('/api/injectie-optimalisatie', async (req, res) => {
  const input = req.body;
  if (!input || typeof input !== 'object') return res.status(400).json({ error: 'body is verplicht' });
  if (!MARKT) return res.status(503).json({ error: 'Marktdata nog niet geladen — probeer over 30 seconden opnieuw' });
  try {
    const profielNaam = input.profielNaam || input.profiel_naam || null;
    let profiel_kwartier = profielNaam ? _laadProfielKwartier(profielNaam) : null;
    // v15.69 (fase 4): opgeladen afname- én injectie-profiel toepassen (manager-only, project_id; fallback = standaard/zonvorm).
    const _mgrOk = await _isManagerReq(req);
    let _pbron = null;
    try { const opA = _mgrOk ? await _opgeladenProfiel(input.project_id, 'afname') : null; if (opA && Array.isArray(opA.kwartier) && opA.kwartier.length === 35040) { profiel_kwartier = opA.kwartier; _pbron = { bron:'opgeladen_afname', ean:opA.ean, mwh:opA.mwh, maanden:opA.maanden }; } } catch (e) {}
    const _iparams = {
      pv_kwp: Number(input.pv_kwp || input.pvKwp || 0),
      inverter_kva: Number(input.inverter_kva || input.kva || 0),
      piek_kw: Number(input.piek_kw || input.piekKw || 0),
      afname_mwh_jaar: Number(input.afname_mwh_jaar || input.jaarverbruik || 0),
      injectie_mwh_jaar: Number(input.injectie_mwh_jaar || 0),
      injectie_mwh_maand: Number(input.injectie_mwh_maand || 0),
      injectie_maand: Number(input.injectie_maand || 0),
      forecast_modus: input.forecast_modus || input.bspForecastModus || 'realistic',
      profiel_kwartier: profiel_kwartier,
      project_id: input.project_id, _mgr_ok: _mgrOk,
    };
    const _ibron = await _pasOpgeladenInjectieToe(_iparams);
    if (_ibron && !(_iparams.injectie_mwh_jaar > 0) && _iparams.injectie_profiel_mwh > 0) _iparams.injectie_mwh_jaar = _iparams.injectie_profiel_mwh;
    const r = _analyseerInjectieOptimalisatie(MARKT, _iparams);
    const _o = r.opbrengst_jaar || {}, _st = r.sturing || {};
    const _plafond = (r.opbrengst_jaar ? (+_o.met_curtail_eur||0) : 0) + (+_st.onbalans_potentie_eur||0);
    const _ijk = _bouwIjk('injectie-solaractive','opbrengst',
      { pv_kwp:(r.invoer&&r.invoer.pv_kwp)||null, inverter_kva:(r.invoer&&r.invoer.inverter_kva)||null,
        injectie_mwh:(r.energie_jaar&&r.energie_jaar.injectie_mwh)||null,
        afname_mwh_jaar:(r.invoer&&r.invoer.afname_mwh_jaar)||null,
        profiel:profielNaam||null },
      { forecast_modus:_st.forecast_modus, capture:_st.capture, kalibratie:_st.kalibratie,
        sigma_da_eur_mwh:_st.sigma_da_eur_mwh, sigma_imb_eur_mwh:_st.sigma_imb_eur_mwh, thr_factor:_st.thr_factor },
      { basis:+_o.vandaag_spot_eur||0, sturing:+_o.met_curtail_eur||0, onbalans:+_o.met_curtail_onbalans_eur||0,
        plafond:_plafond });
    return res.json({ ok: true, analyse: r, _ijk: _ijk, profiel_bron: _pbron, injectie_bron: _ibron, _meta: { server_version: SERVER_VERSIE } });
  } catch (e) {
    console.error('[injectie-opt] fout:', e.message);
    return res.status(500).json({ error: 'injectie-optimalisatie gefaald: ' + e.message });
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
  // v15.67: opgeladen afname-profiel toepassen (fallback = standaard). Eén keer resolven; buildSimInput
  // (via _variantUi-clone) neemt het over voor álle runs van deze studie.
  const _profielInfo = await _pasOpgeladenAfnameToe(input);
  if (_profielInfo) { input._profiel_info = _profielInfo; _jlog(job, 'start', `Gerekend met opgeladen afname-profiel (EAN ${_profielInfo.ean || '?'}, ${_profielInfo.mwh} MWh, ${_profielInfo.maanden} maanden).`, { profiel_bron: 'opgeladen_afname' }); }
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

    // ── v15.72.0 (Johan 15-08): "SIMULEER"-knop — forceer EXACT de opgegeven installatie in 3 sturingen.
    // Geguard op input._simuleer_enkel: enkel de nieuwe Simuleer-knop zet die vlag → alle bestaande flows
    // (Ontwerp/nominatie-sim-3, autopilot) blijven ONGEWIJZIGD. Hergebruikt de bestaande 'enkel'-tak: geen
    // opstellingen-vergelijking, geen batterij-/PV-sweep, geen groeipad. Zo simuleert de adviseur een
    // bestaande of gekozen installatie rechtstreeks (één config → gesimuleerde factuur + meerwaarde-sturing).
    if (input._simuleer_enkel) {
      const [_stF, _onbF] = await Promise.all([
        _runSimulatorOnce(buildSimInput(_variantUi(input, 'sturing'))),
        heeftFlex ? _runSimulatorOnce(buildSimInput(_variantUi(input, 'onbalans'))) : Promise.resolve(null),
      ]);
      const varianten = { geen: probe, sturing: _stF, onbalans: heeftFlex ? _onbF : _stF };
      const kpi_sturing = _kpi(varianten, !heeftFlex);
      _jlog(job, 'klaar', `Simulatie van de opgegeven installatie (${Math.round((Date.now()-startTime)/1000)}s)`);
      return { ok: true, modus: 'enkel', varianten, kpi_sturing, capaciteit: cap, simuleer: true,
        _meta: { elapsed_ms: Date.now() - startTime, server_version: '15.72.0', heeftFlex, simuleer: true } };
    }

    // ── v15.35.0: Geen laadplein → BATTERIJ-ONLY KAMINO op het bestaande verbruik.
    // We sweepen 1…Nmax batterijen (tot 120 kW boven het toegangsvermogen), kiezen de
    // hoogste NPV als Optimaal en tekenen het groeipad met stap 1 als aanbevolen instap.
    if (!heeftLaadplein) {
      const sweep = await _batterijSweepGebouw(input, cap, probe, job, startTime);
      if (sweep) return sweep;
      // Terugval (geen sweep-punten, bv. leeg verbruik): oude 'enkel'-weergave.
    }

    // ── Laadplein zonder batterij en aansluiting volstaat → normale 3-sturingen (enkel) ──
    if (!heeftLaadplein || (cap.voldoende && !_heeftBatt)) {
      // v15.59.0: sturing + onbalans zijn onafhankelijke runs op dezelfde config → parallel.
      const [_stEnk, _onbEnk] = await Promise.all([
        _runSimulatorOnce(buildSimInput(_variantUi(input, 'sturing'))),
        heeftFlex ? _runSimulatorOnce(buildSimInput(_variantUi(input, 'onbalans'))) : Promise.resolve(null),
      ]);
      const varianten = { geen: probe, sturing: _stEnk, onbalans: heeftFlex ? _onbEnk : _stEnk };
      const kpi_sturing = _kpi(varianten, !heeftFlex);
      _jlog(job, 'klaar', `Aansluiting volstaat — geen opstellingen nodig (${Math.round((Date.now()-startTime)/1000)}s)`);
      return { ok: true, modus: 'enkel', varianten, kpi_sturing, capaciteit: cap,
        _meta: { elapsed_ms: Date.now() - startTime, server_version: '15.20.0', heeftFlex } };
    }

    // ── v15.34.0 (optie a): laadplein + aansluiting VOLDOENDE → tóch de Kamino-analyse, maar met
    // ENKEL de batterij-opstelling. Geen verzwaring nodig ⇒ "Verzwaren" vervalt en Batterij = Optimaal.
    // De batterij is dan puur voor arbitrage + goedkope eigen km. Vandaag (opstelling 0) bouwt de
    // frontend zelf uit de basisfactuur + CREG-km. Zo verschijnt de volle analyse óók als het al past.
    if (cap.voldoende) {
      // v15.59.0: sturing + onbalans onafhankelijk → parallel.
      const [_stTwee, _onbTwee] = await Promise.all([
        _runSimulatorOnce(buildSimInput(_variantUi(input, 'sturing'))),
        heeftFlex ? _runSimulatorOnce(buildSimInput(_variantUi(input, 'onbalans'))) : Promise.resolve(null),
      ]);
      const v = { geen: probe, sturing: _stTwee, onbalans: heeftFlex ? _onbTwee : _stTwee };
      const _battKwh = (input.batterijCustom && Number(input.batterijCustom.kwh)) || null;
      const opstellingen = { batterij: {
        varianten: v, kpi_sturing: _kpi(v, !heeftFlex),
        config: {
          spanning: input.spanning || 'LS', spanning_omgezet: false, spanning_origineel: null,
          aansluiting_kva: input.aansluiting_kva || input.aansluitingKva || null,
          toegangsvermogen_kw: input.toegangsvermogen_kw || input.aansluiting_kva || null,
          batterij: input.batterijCustom || null, cabine_nodig: false,
        },
        dimensionering: {
          haalbaar: true, iteraties: 0, beoordeeld_op: 'sturing', stappen: [], start_maat: null,
          gekozen_maat: _battKwh, eenheid: _battKwh != null ? 'kWh' : null,
          verloren_dagen: 0, totaal_dagen: null,
        },
      }};
      _jlog(job, 'klaar', `Aansluiting volstaat — batterij-analyse (Vandaag vs Batterij) ` +
                          `(${Math.round((Date.now()-startTime)/1000)}s)`);
      return { ok: true, modus: 'twee_opstellingen', capaciteit: cap, opstellingen, vergelijking: {},
        _meta: { elapsed_ms: Date.now() - startTime, server_version: '15.34.0',
                 opstellingen: ['batterij'], cap_voldoende: true, heeftFlex } };
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
    // v15.59.0: de drie opstellingen (verhogen / batterij / mix) zijn onafhankelijke zoektochten
    // → parallelliseerbaar. Elke tak bouwt haar eigen cfg/dimensionering vers uit `input`+`cap` en
    // raakt geen gedeelde state (enkel _jlog/job.runs, cosmetisch). Binnen elke tak lopen de drie
    // sturingen (geen/sturing/onbalans) óók parallel. _pmap behoudt de OPSTELLINGEN-volgorde; de
    // globale semafoor begrenst het totaal aantal gelijktijdige python-processen. Bij
    // SIM_MAX_PARALLEL=1 valt dit terug op exact de oude sequentiële volgorde.
    const _opstResultaten = await _pmap(OPSTELLINGEN, async (opst) => {
      _jlog(job, 'opstelling', OPSTELLING_LABEL[opst], { opstelling: opst });
      // ── mix: eigen zoeklus over de mengverhoudingen ──
      if (opst === 'mix') {
        const m = await _dimensioneerMix(input, cap, job);
        if (!m) { _jlog(job, 'waarschuwing', 'Geen werkende mix gevonden — opstelling 3 overgeslagen.'); return null; }
        const cfgM = m.cfg;
        _jlog(job, 'run', `${OPSTELLING_LABEL.mix}: de drie sturingen doorrekenen…`, { opstelling: 'mix' });
        // geen + onbalans onafhankelijk; sturing komt uit de dimensioneringsrun.
        const [_vmg, _vmo] = await Promise.all([
          _runSimulatorOnce(buildSimInput(_variantUi(cfgM, 'geen'))),
          _runSimulatorOnce(buildSimInput(_variantUi(cfgM, 'onbalans'))),
        ]);
        const vm = { geen: _vmg, sturing: m.dim.resultaat, onbalans: _vmo };
        if (job) job.runs = (job.runs || 0) + 2;
        const _mixObj = {
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
        return ['mix', _mixObj];
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
      _jlog(job, 'run', `${OPSTELLING_LABEL[opst]}: de drie sturingen doorrekenen…`, { opstelling: opst });
      // v15.59.0: de nog-niet-gedraaide sturingen parallel; de reeds gedimensioneerde variant hergebruikt.
      const [_vg, _vs, _vo] = await Promise.all([
        dim.variant === 'geen'    ? Promise.resolve(dim.resultaat) : _runSimulatorOnce(buildSimInput(_variantUi(cfg, 'geen'))),
        dim.variant === 'sturing' ? Promise.resolve(dim.resultaat) : _runSimulatorOnce(buildSimInput(_variantUi(cfg, 'sturing'))),
        _runSimulatorOnce(buildSimInput(_variantUi(cfg, 'onbalans'))),
      ]);
      const v = { geen: _vg, sturing: _vs, onbalans: _vo };
      if (job) job.runs = (job.runs || 0) + 2;
      // v15.18: geef de gebruikte configuratie mee terug — vooral de spanning, want
      // opstelling 1 kan naar MS zijn omgezet (>100 kVA). Zonder dit ziet de verkoper
      // niet dat hij twee verschillende tariefkaarten vergelijkt.
      const _opstObj = {
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
      return [opst, _opstObj];
    });
    for (const _r of _opstResultaten) { if (_r) opstellingen[_r[0]] = _r[1]; }
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
// ─── v15.67.0 — OPGELADEN FLUVIUS-PROFIEL (FASE 2): CSV → genormaliseerde 35040-vorm ─────────────
// Zet het door de converter opgeslagen kwartier-CSV (Date;kWh, ~13 maanden) om naar de vorm die
// simulator.py verwacht: een 35040-array die op de 2025-kalender is geïndexeerd (dag-van-jaar×96 +
// kwartier-van-dag) en genormaliseerd is tot som=1.0. Dubbele kwartieren (13e maand-overlap) worden
// gemiddeld. Berekent meteen de echte maandpiek over de laatste 12 maanden (voor de toegangsvermogen-
// check, fase 3). Bij eender welke fout → null → de analyse valt terug op het standaardprofiel.
const _OPGELADEN_CACHE = new Map();   // pid|type|bestand → { val, exp }
const _CUMDAY_2025 = [0,31,59,90,120,151,181,212,243,273,304,334];   // dag-van-jaar-start per maand (2025, geen schrikkeljaar)
function _parseProfielCsv(csv) {
  if (!csv || typeof csv !== 'string') return null;
  const lines = csv.split('\n');
  const acc = new Array(35040).fill(0), cnt = new Array(35040).fill(0);
  const maandMax = {};
  let tot = 0, n = 0, van = null, totS = null;
  for (let i = 1; i < lines.length; i++) {
    const ln = lines[i].trim(); if (!ln) continue;
    const p = ln.split(';'); if (p.length < 2) continue;
    const ds = p[0].trim();
    const v = parseFloat(String(p[1] || '').replace(',', '.'));
    if (isNaN(v)) continue;
    const m = ds.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})\s+(\d{1,2}):(\d{2})/); if (!m) continue;
    let d = +m[1], mo = +m[2]; const y = +m[3], hh = +m[4], mi = +m[5];
    if (van === null) van = ds; totS = ds; tot += v; n++;
    const mk = y + '-' + String(mo).padStart(2, '0'); const kw = v * 4;
    if (!(mk in maandMax) || kw > maandMax[mk]) maandMax[mk] = kw;
    if (mo === 2 && d === 29) d = 28;   // 2025 kent geen 29 feb
    if (mo < 1 || mo > 12) continue;
    const doy = _CUMDAY_2025[mo - 1] + (d - 1);
    if (doy < 0 || doy > 364) continue;
    const q = hh * 4 + Math.floor(mi / 15);
    if (q < 0 || q > 95) continue;
    const idx = doy * 96 + q;
    acc[idx] += v; cnt[idx]++;
  }
  if (n === 0) return null;
  let som = 0;
  for (let i = 0; i < 35040; i++) { if (cnt[i] > 0) acc[i] = acc[i] / cnt[i]; som += acc[i]; }
  if (!(som > 0)) return null;
  for (let i = 0; i < 35040; i++) acc[i] = acc[i] / som;
  const mks = Object.keys(maandMax).sort();
  const laatste12 = mks.slice(-12);
  let maandpiek = 0; laatste12.forEach(k => { if (maandMax[k] > maandpiek) maandpiek = maandMax[k]; });
  return { kwartier: acc, maandpiek_kw: Math.round(maandpiek), mwh: Math.round(tot / 1000), van, tot: totS, aantal: n, maanden: mks.length };
}
async function _opgeladenProfiel(projectId, type) {
  if (!SUPABASE_OK || !projectId) return null;
  const pid = String(projectId).trim().toUpperCase();
  if (!/^FLX-[A-Z0-9]{3}-[A-Z0-9]{3,4}$/.test(pid)) return null;
  const veiligPid = pid.replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 40);
  let rec = null;
  try { rec = JSON.parse(await _factuurDownload(`kamino/${veiligPid}.json`)); } catch (e) { return null; }
  const meta = rec && rec.profielen && rec.profielen[type];
  if (!meta || !meta.bestand) return null;
  const ck = pid + '|' + type + '|' + meta.bestand;
  const hit = _OPGELADEN_CACHE.get(ck);
  if (hit && hit.exp > Date.now()) return hit.val;
  let csv = null;
  try { csv = await _factuurDownload(meta.bestand); } catch (e) { return null; }
  const val = _parseProfielCsv(csv);
  if (val) { val.bestand = meta.bestand; val.ean = meta.ean || null; val.type = type; }
  _OPGELADEN_CACHE.set(ck, { val, exp: Date.now() + 10 * 60 * 1000 });
  return val;
}
// v15.68: profiel-analyses zijn MANAGER-ONLY (Johan). Sellers/adviseurs blijven op het standaardprofiel
// → veel eenvoudiger + geen verwarring. Non-blocking role-check (fout → false → standaardprofiel).
async function _isManagerReq(req) {
  try { const u = await resolveUser(req); return !!(u && _isManager(u)); } catch (e) { return false; }
}
// Attach het opgeladen afname-profiel aan een sim-input (mutatie), fallback-safe + MANAGER-ONLY.
// De caller zet input._mgr_ok = true (via _isManagerReq) wanneer de aanvrager een manager is.
async function _pasOpgeladenAfnameToe(input) {
  try {
    // v15.97 (Johan 27-08): profiel-ladder OPEN voor alle gebruikers (niet langer manager-only).
    // Zodra er een gemeten profiel bij het project staat, gebruikt élke sim het i.p.v. het standaard-SLP.
    // Vereist enkel een project_id + een opgeladen profiel; `_mgr_ok` wordt niet meer gecheckt.
    if (!input || !input.project_id) return null;
    const op = await _opgeladenProfiel(input.project_id, 'afname');
    if (op && Array.isArray(op.kwartier) && op.kwartier.length === 35040) {
      input._opgeladen_profiel_kwartier = op.kwartier;
      input._profiel_bron = 'opgeladen_afname';
      return { bron: 'opgeladen_afname', ean: op.ean, mwh: op.mwh, van: op.van, tot: op.tot, maandpiek_kw: op.maandpiek_kw, maanden: op.maanden, bestand: op.bestand };
    }
  } catch (e) { console.warn('[opgeladen-profiel] toepassen faalde (val terug op standaard):', e.message); }
  return null;
}

// v15.69 (fase 4): opgeladen INJECTIE-profiel aan de injectie-analyse-params hangen (mutatie), fallback-safe +
// MANAGER-ONLY. De caller zet p._mgr_ok (via _isManagerReq) + p.project_id. Zet p.injectie_kwartier (35040, som=1)
// + p.injectie_profiel_mwh (gemeten volume). Bij geen manager / geen profiel / fout → null → zonvorm-fallback.
async function _pasOpgeladenInjectieToe(p) {
  try {
    // v15.97 (Johan 27-08): OPEN voor alle gebruikers (niet langer manager-only). Enkel project_id + profiel nodig.
    if (!p || !p.project_id) return null;
    const op = await _opgeladenProfiel(p.project_id, 'injectie');
    if (op && Array.isArray(op.kwartier) && op.kwartier.length === 35040) {
      p.injectie_kwartier = op.kwartier;
      p.injectie_profiel_mwh = +op.mwh || 0;
      return { bron: 'opgeladen_injectie', ean: op.ean, mwh: op.mwh, van: op.van, tot: op.tot, maandpiek_kw: op.maandpiek_kw, maanden: op.maanden, bestand: op.bestand };
    }
  } catch (e) { console.warn('[opgeladen-injectie] toepassen faalde (val terug op zonvorm):', e.message); }
  return null;
}

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

  const pvKwpNieuw = ui.pv_kwp || ui.pvKwp || 0;
  // v15.43: BESTAANDE PV (SolarActive) fysiek meenemen in de LP-run. pv.kwp (energie) = nieuw + bestaand;
  // de netto-afname gross-up (= bestaande zelfconsumptie) gaat via aanvullingen['pv_zelfverbruik'] op de
  // zonvorm. capex telt ENKEL de nieuwe PV (bestaande is sunk); de injectie-cap krijgt de bestaande omvormer
  // er wél bij. De SolarActive-injectiestudie blijft een aparte deliverable (los endpoint).
  const _bp = ui.bestaande_pv || ui.bestaandePv || null;
  let bpKwp = 0, bpInvKw = 0, bpSelfMwh = 0;
  const _cumDay2025 = [0,31,59,90,120,151,181,212,243,273,304,334,365];
  function _maandVanIdx2025(i){ const day=Math.floor(i/96); for(let m=0;m<12;m++){ if(day<_cumDay2025[m+1]) return m+1; } return 12; }
  if (_bp && (_bp.aanwezig!==false) && (Number(_bp.kwp)>0 || Number(_bp.kva)>0 || Number(_bp.piek_kw)>0)) {
    bpKwp = Number(_bp.kwp)>0 ? Number(_bp.kwp) : (Number(_bp.kva)>0 ? 1.3*Number(_bp.kva) : 1.3*Number(_bp.piek_kw));
    const bpKva = Number(_bp.kva)>0 ? Number(_bp.kva) : (Number(_bp.piek_kw)>0 ? Number(_bp.piek_kw) : bpKwp/1.3);
    bpInvKw = bpKva;
    const bpProdMwh = 900*bpKwp/1000;
    let bpInjMwh = Number(_bp.inj_mwh_jaar)>0 ? Number(_bp.inj_mwh_jaar) : 0;
    if (bpInjMwh<=0 && Number(_bp.inj_mwh_maand)>0 && MARKT && MARKT.solar_norm && MARKT.solar_norm.length===35040) {
      const m = Math.min(12, Math.max(1, Number(_bp.maand)||6)); const sn=MARKT.solar_norm;
      let sMaand=0, sJaar=0; for (let i=0;i<35040;i++){ sJaar+=sn[i]; if(_maandVanIdx2025(i)===m) sMaand+=sn[i]; }
      const frac = sJaar>0 ? sMaand/sJaar : 0;
      bpInjMwh = frac>0 ? Number(_bp.inj_mwh_maand)/frac : Number(_bp.inj_mwh_maand)*12;
    }
    bpSelfMwh = Math.max(0, bpProdMwh - bpInjMwh);   // netto-afname gross-up (= zelfconsumptie)
    console.log(`[sim] bestaande PV: ${bpKwp.toFixed(0)} kWp / ${bpKva.toFixed(0)} kVA → prod ${bpProdMwh.toFixed(1)} MWh, inj ${bpInjMwh.toFixed(1)} MWh, zelfverbruik-grossup ${bpSelfMwh.toFixed(1)} MWh`);
  }
  const pvKwp    = pvKwpNieuw + bpKwp;                // TOTALE PV in de dispatch (energie + productievorm)
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
  // v15.43: omvormer nieuw PV via de tabel (op de NIEUWE kWp), + de bestaande omvormer erbij voor de totale
  // injectie-cap (clipping op nieuw + bestaand). UI-override telt voor de nieuwe installatie.
  const pvInverterKwNieuw = Number(ui.pv_inverter_kw || ui.pvInverterKw || 0) ||
                       (pvKwpNieuw > 0 ? (_invTabel[pvKwpNieuw] || Math.round(pvKwpNieuw * 0.77)) : 0);
  const pvInverterKw = pvInverterKwNieuw + bpInvKw;
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
    // v15.67: OPGELADEN Fluvius-profiel heeft voorrang (fallback = standaard/named/MARKT → geen regressie).
    if (Array.isArray(ui._opgeladen_profiel_kwartier) && ui._opgeladen_profiel_kwartier.length === 35040) {
      return ui._opgeladen_profiel_kwartier;
    }
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

  // v15.43: bestaande-PV zelfconsumptie als aanvulling op de zonvorm (som=1 genormaliseerd), zodat de
  // bruto gebouw-demand hersteld wordt zonder de facturatie te wijzigen (PV-productie dekt ze op de middag).
  let _aanvullingen = {};
  if (bpSelfMwh > 0 && MARKT && MARKT.solar_norm && MARKT.solar_norm.length === 35040) {
    const sn = MARKT.solar_norm; let s = 0; for (let i=0;i<35040;i++) s += sn[i];
    if (s > 0) {
      const solarProfNorm = new Array(35040);
      for (let i=0;i<35040;i++) solarProfNorm[i] = sn[i]/s;
      _aanvullingen = { pv_zelfverbruik: { profiel_kwartier: solarProfNorm, jaarvolume_mwh: bpSelfMwh } };
    }
  }
  return {
    profiel_kwartier: profielKwartier,
    jaarverbruik_mwh: jaarverbruik,
    aanvullingen: _aanvullingen,
    pv: {
      kwp: pvKwp,
      specifiek_rendement_kwh_per_kwp: 900,
      vorm_kwartier: pvVorm,
      capex_eur: pvKwpNieuw > 0 ? (pvKwpNieuw <= 125 ? 71875 : pvKwpNieuw <= 150 ? 86250 : 115000) : 0,
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
    // v15.29.0: groeipad houdt de aansluiting VAST → simulator.py mag ze niet verhogen maar clipt.
    geen_aansluiting_verhoging: !!ui.geen_aansluiting_verhoging,
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

// ─── v15.110 (Fase 4 — EnergieKompas LEAD-CAPTURE / onderhandelingsnota per mail) ────────────
// PUBLIEK. De schil bouwt de onderhandelingsnota-data (D) client-side en POST'et ze met de mail +
// tel van de klant naar /api/lead. Wij (1) bewaren D onder een token → de nota-pagina haalt ze op
// via /api/lead/:token (?lead=<token>), (2) mailen de klant een persoonlijke link naar zijn nota
// (Brevo), (3) notificeren de lead-mailbox (Fluctus/call-partner). Zo voelt "waarom mijn mail?"
// logisch: hij krijgt de nota in zijn mailbox, en een warme lead is bereikbaar. /api/lead-interesse
// verrijkt dezelfde lead nadien met de diepte-analyse-interesses ("wil u weten hoe?").
const LEAD_MAIL_TO = process.env.LEAD_MAIL_TO || AUDIT_MAIL_TO;         // Fluctus/call-partner-mailbox
const WEB_BASE     = (process.env.WEB_BASE || 'https://fluctus.net').replace(/\/+$/, '');
const _LEADS = new Map();                                              // token → { data, mail, tel, naam, ts, interesses }
const LEAD_TTL_MS = 1000 * 60 * 60 * 24 * 90;                         // 90 dagen
const _leadDir = path.join(__dirname, 'data', 'leads');
function _leadToken() { return (Date.now().toString(36) + Math.random().toString(36).slice(2, 10)).toLowerCase(); }
function _leadOpslaan(token, rec) {
  _LEADS.set(token, rec);
  try { fs.mkdirSync(_leadDir, { recursive: true }); fs.writeFileSync(path.join(_leadDir, token + '.json'), JSON.stringify(rec)); } catch (e) { /* best-effort */ }
  // v15.119: leads persisteren ook naar Supabase-bucket (zelfde bucket als Kamino), zodat ze
  // Railway-deploys overleven. Fire-and-forget: het lokale bestand + geheugen blijven de snelle
  // paden; de bucket is de duurzame kopie waaruit we bij opstart hydrateren (_leadsHydrate).
  if (SUPABASE_OK && /^[a-z0-9]{1,40}$/.test(String(token || ''))) {
    try {
      const b64 = Buffer.from(JSON.stringify(rec), 'utf8').toString('base64');
      _factuurUpload(b64, 'application/json', `leads/${token}.json`)
        .catch(e => console.warn(`[lead] bucket-upload ${token} faalde (niet blokkerend): ${e.message}`));
    } catch (e) { /* best-effort */ }
  }
}
function _leadLezen(token) {
  if (!/^[a-z0-9]{1,40}$/.test(String(token || ''))) return null;
  let rec = _LEADS.get(token);
  if (!rec) { try { rec = JSON.parse(fs.readFileSync(path.join(_leadDir, token + '.json'), 'utf8')); _LEADS.set(token, rec); } catch (e) { rec = null; } }
  if (rec && (Date.now() - (rec.ts || 0)) > LEAD_TTL_MS) return null;
  return rec || null;
}
// v15.119: bij opstart het geheugen (+ lokale cache) hydrateren uit de Supabase-bucket, zodat een
// Railway-redeploy geen warme leads verliest en gemailde nota-links (die via _leadLezen op het
// geheugen landen) blijven werken. Niet-blokkerend en volledig gated op SUPABASE_OK; gepagineerd
// zodat >100 leads ook meekomen. Nooit een verse in-memory record overschrijven met een oudere
// bucket-kopie (bijgewerkt/ts-vergelijking).
async function _leadsHydrate() {
  if (!SUPABASE_OK) return { ok: false, reden: 'geen supabase' };
  let geladen = 0, offset = 0; const PAGINA = 100;
  try {
    for (;;) {
      const url = `${SUPABASE_URL}/storage/v1/object/list/${FACTUREN_BUCKET}`;
      const r = await fetch(url, { method: 'POST',
        headers: { 'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ prefix: 'leads/', limit: PAGINA, offset, sortBy: { column: 'name', order: 'asc' } }) });
      if (!r.ok) throw new Error(`list HTTP ${r.status}`);
      const rij = await r.json();
      if (!Array.isArray(rij) || rij.length === 0) break;
      for (const o of rij) {
        const naam = (o && o.name) || '';
        const m = naam.match(/^([a-z0-9]{1,40})\.json$/);
        if (!m) continue;
        const token = m[1];
        try {
          const txt = await _factuurDownload(`leads/${token}.json`);
          const rec = JSON.parse(txt);
          const bestaand = _LEADS.get(token);
          const nieuwerTs = (rec.bijgewerkt || rec.ts || 0);
          const oudTs = bestaand ? (bestaand.bijgewerkt || bestaand.ts || 0) : -1;
          if (!bestaand || nieuwerTs >= oudTs) {
            if ((Date.now() - (rec.ts || 0)) <= LEAD_TTL_MS) {
              _LEADS.set(token, rec);
              try { fs.mkdirSync(_leadDir, { recursive: true }); fs.writeFileSync(path.join(_leadDir, token + '.json'), JSON.stringify(rec)); } catch (e) {}
              geladen++;
            }
          }
        } catch (e) { console.warn(`[lead-hydrate] ${token} faalde: ${e.message}`); }
      }
      if (rij.length < PAGINA) break;
      offset += PAGINA;
    }
    console.log(`[lead-hydrate] ${geladen} lead(s) uit bucket in geheugen geladen`);
    return { ok: true, geladen };
  } catch (e) { console.warn(`[lead-hydrate] mislukt (niet blokkerend): ${e.message}`); return { ok: false, reden: e.message }; }
}
function _validMail(m) { return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(m || '')); }
async function _brevoMail(to, subject, textContent, htmlContent, senderNaam) {
  if (!BREVO_API_KEY) return { sent: false, reden: 'BREVO_API_KEY niet gezet' };
  try {
    const body = { sender: { name: senderNaam || 'Fluctus EnergieKompas', email: BREVO_SENDER },
      to: (Array.isArray(to) ? to : [to]).map(e => (typeof e === 'string' ? { email: e } : e)), subject, textContent };
    if (htmlContent) body.htmlContent = htmlContent;
    const r = await fetch('https://api.brevo.com/v3/smtp/email', {
      method: 'POST', headers: { 'api-key': BREVO_API_KEY, 'Content-Type': 'application/json', 'accept': 'application/json' },
      body: JSON.stringify(body) });
    if (!r.ok) { const t = await r.text(); return { sent: false, reden: `Brevo HTTP ${r.status} ${t.slice(0, 160)}` }; }
    return { sent: true };
  } catch (e) { return { sent: false, reden: e.message }; }
}
function _eurTxt(n) { const v = Math.round(+n || 0); try { return '€ ' + v.toLocaleString('nl-BE'); } catch (e) { return '€ ' + v; } }
function esc2(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])); }
// v15.112 (Fase 4): schil-lead → studieklaar Kamino-project. Seedt (service-role, best-effort) een projectrecord
// onder kamino/<FLX-id>.json met de factuur-baseCase + profiel + PV + klant + partner, zodat een lead met factuur
// meteen als voorgevuld project in Kamino verschijnt en de volledige studie (nominatie-sim → rapport) erop kan draaien.
async function _seedKaminoProject(token, lead, km) {
  if (!SUPABASE_OK || !km || !km.baseCase) return null;
  try {
    const id = _projectId('ek-lead-' + token);
    const veilig = id.replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 40);
    const nu = new Date().toISOString();
    const rec = {
      id, naam: lead.naam || 'EnergieKompas-lead',
      klant: { naam: lead.naam || '', email: lead.mail, tel: lead.tel || '' },
      adviseur: null, partner: lead.partner || null, bron: 'energiekompas-schil',
      baseCase: km.baseCase || null, input: km.input || null,
      profiel: km.profielNaam || null, pv: km.pv || null,
      scenarios: { standaard: { scenario: 'standaard', baseCase: km.baseCase || null, input: km.input || null, bijgewerkt: nu, door: 'EnergieKompas' } },
      studies: {}, lead_token: token, samenvatting: lead.samenvatting || null,
      aangemaakt: nu, bijgewerkt: nu, door: 'EnergieKompas',
    };
    await _factuurUpload(Buffer.from(JSON.stringify(rec), 'utf8').toString('base64'), 'application/json', `kamino/${veilig}.json`);
    try { _kaminoLijstCache.ts = 0; } catch (e) {}   // dropdown-cache verversen zodat het project meteen verschijnt
    return id;
  } catch (e) { console.warn('[lead] kamino-seed faalde (niet-blokkerend):', e.message); return null; }
}
app.post('/api/lead', async (req, res) => {
  try {
    const b = req.body || {};
    const mail = String(b.mail || '').trim(), tel = String(b.tel || '').trim(), naam = String(b.naam || '').trim().slice(0, 120);
    if (!_validMail(mail)) return res.status(400).json({ ok: false, error: 'Ongeldig e-mailadres.' });
    if (!b.data || typeof b.data !== 'object') return res.status(400).json({ ok: false, error: 'Geen nota-data.' });
    const token = _leadToken();
    const s = b.samenvatting || {};
    const factuurType = (['voorschot', 'afrekening'].indexOf(String(b.factuur_type)) >= 0) ? String(b.factuur_type) : null;
    const rec = { data: b.data, mail, tel, naam, partner: String(b.partner || '').slice(0, 40), herkomst: String(b.herkomst || '').slice(0, 40),
      factuur_type: factuurType,
      samenvatting: { afname_marge_jaar: +s.afname_marge_jaar || 0, injectie_marge_jaar: +s.injectie_marge_jaar || 0,
        energiekost_nu_mwh: +s.energiekost_nu_mwh || 0, energiekost_dyn_mwh: +s.energiekost_dyn_mwh || 0 },
      interesses: [], ts: Date.now() };
    _leadOpslaan(token, rec);
    // Schil → studieklaar Kamino-project (best-effort, enkel bij een factuur-baseCase).
    let kaminoProjectId = null;
    try { kaminoProjectId = await _seedKaminoProject(token, rec, b.kamino); if (kaminoProjectId) { rec.kamino_project_id = kaminoProjectId; _leadOpslaan(token, rec); } } catch (e) {}
    const thema = rec.partner ? ('&thema=' + encodeURIComponent(rec.partner)) : '';
    const notaUrl = `${WEB_BASE}/apps/energiekompas-nota.html?lead=${token}${thema}`;
    // 1) Klant-mail met de persoonlijke nota-link
    const kFout = [];
    if (rec.samenvatting.afname_marge_jaar > 0) kFout.push(`• Onderhandelingsmarge op uw afname: ± ${_eurTxt(rec.samenvatting.afname_marge_jaar)} per jaar`);
    if (rec.samenvatting.injectie_marge_jaar > 0) kFout.push(`• Meerwaarde op uw injectie: ± ${_eurTxt(rec.samenvatting.injectie_marge_jaar)} per jaar`);
    const kTxt = `Beste${naam ? ' ' + naam : ''},\n\nDank voor uw aanvraag. Uw persoonlijke onderhandelingsnota staat klaar:\n${notaUrl}\n\n${kFout.join('\n')}${kFout.length ? '\n\n' : ''}Deze nota vergelijkt uw huidige factuur met een dynamisch (spot-gebaseerd) contract — voor uw afname en, indien van toepassing, uw injectie — zonder extra investering.\n\nWilt u weten hoeveel besparing en rendement uw aansluiting nog in petto heeft? Antwoord gerust op deze mail.\n\nMet vriendelijke groeten,\nEnergieKompas`;
    const kHtml = `<div style="font:15px/1.55 Helvetica,Arial,sans-serif;color:#1F3864;max-width:560px">
      <p>Beste${naam ? ' ' + esc2(naam) : ''},</p>
      <p>Dank voor uw aanvraag. Uw persoonlijke <b>onderhandelingsnota</b> staat klaar:</p>
      <p><a href="${notaUrl}" style="display:inline-block;background:#05B050;color:#fff;text-decoration:none;font-weight:700;padding:11px 18px;border-radius:8px">Bekijk uw onderhandelingsnota →</a></p>
      ${kFout.length ? ('<ul style="color:#33404f">' + kFout.map(l => '<li>' + esc2(l.replace(/^•\s*/, '')) + '</li>').join('') + '</ul>') : ''}
      <p style="color:#5A6577;font-size:13px">Deze nota vergelijkt uw huidige factuur met een dynamisch (spot-gebaseerd) contract — voor uw afname en, indien van toepassing, uw injectie — <b>zonder extra investering</b>.</p>
      <p style="color:#5A6577;font-size:13px">Wilt u weten hoeveel besparing en rendement uw aansluiting nog in petto heeft? Antwoord gerust op deze mail.</p>
      <p>Met vriendelijke groeten,<br>EnergieKompas</p></div>`;
    const kSent = await _brevoMail(mail, 'Uw persoonlijke onderhandelingsnota', kTxt, kHtml);
    // 2) Lead-notificatie naar Fluctus/call-partner
    const lTxt = `NIEUWE LEAD via EnergieKompas${rec.partner ? ' (' + rec.partner + ')' : ''}\n\nNaam : ${naam || '—'}\nMail : ${mail}\nTel  : ${tel || '—'}\nHerkomst: ${rec.herkomst || 'direct'}\n\nAfname-marge : ${_eurTxt(rec.samenvatting.afname_marge_jaar)}/j  (nu ${Math.round(rec.samenvatting.energiekost_nu_mwh)} → dyn ${Math.round(rec.samenvatting.energiekost_dyn_mwh)} €/MWh)\nInjectie-marge: ${_eurTxt(rec.samenvatting.injectie_marge_jaar)}/j\n\nNota: ${notaUrl}${kaminoProjectId ? ('\n\nStudieklaar Kamino-project: ' + kaminoProjectId + ' (open in Kamino → volledige studie + rapport)') : ''}`;
    await _brevoMail(LEAD_MAIL_TO, `Lead EnergieKompas — ${naam || mail}`, lTxt, null, 'Fluctus EnergieKompas');
    res.json({ ok: true, token, nota_url: notaUrl, project_id: kaminoProjectId, mail_klant: kSent.sent, mail_reden: kSent.sent ? undefined : kSent.reden });
  } catch (e) { console.error('[lead]', e.message); res.status(500).json({ ok: false, error: e.message }); }
});
app.post('/api/lead-interesse', async (req, res) => {
  try {
    const b = req.body || {};
    const rec = _leadLezen(b.token);
    if (!rec) return res.status(404).json({ ok: false, error: 'Lead niet gevonden of verlopen.' });
    const int = Array.isArray(b.interesses) ? b.interesses.map(x => String(x).slice(0, 60)).slice(0, 10) : [];
    rec.interesses = int; rec.wil_contact = !!b.wil_contact; rec.ts_interesse = Date.now();
    _leadOpslaan(b.token, rec);
    if (b.wil_contact) {
      const lTxt = `WARME LEAD — wil weten hoe (EnergieKompas${rec.partner ? ' ' + rec.partner : ''})\n\nNaam : ${rec.naam || '—'}\nMail : ${rec.mail}\nTel  : ${rec.tel || '—'}\n\nInteresses:\n${int.map(i => '  • ' + i).join('\n') || '  (geen)'}\n\nAfname-marge ${_eurTxt(rec.samenvatting.afname_marge_jaar)}/j · injectie ${_eurTxt(rec.samenvatting.injectie_marge_jaar)}/j`;
      await _brevoMail(LEAD_MAIL_TO, `WARME LEAD EnergieKompas — ${rec.naam || rec.mail}`, lTxt, null, 'Fluctus EnergieKompas');
    }
    res.json({ ok: true });
  } catch (e) { console.error('[lead-interesse]', e.message); res.status(500).json({ ok: false, error: e.message }); }
});
app.get('/api/lead/:token', (req, res) => {
  const rec = _leadLezen(req.params.token);
  if (!rec) return res.status(404).json({ ok: false, error: 'Lead niet gevonden of verlopen.' });
  _leadEvent(rec, 'nota_opened', { once: true }, req.params.token);   // time-to-open = nota_opened − ts (lead-scoring)
  // tier gate: 'view' (nooit betalen) = enkel bekijken, geen download; later/direct/geen tier = download toegestaan.
  const mag_download = rec.tier ? rec.tier !== 'view' : true;
  res.json({ ok: true, nota: rec.data, tier: rec.tier || null, verified: !!rec.verified,
    mag_download, mandaat: rec.mandaat ? { status: rec.mandaat.status || 'aangevraagd', ts: rec.mandaat.ts } : null,
    herberekend: !!rec.herberekend });
});
// v15.114 (Fase 4 — MANDAAT-PARALLEL): de klant geeft, na "HOE" en zijn tier-keuze, akkoord om het Fluvius-
// mandaat (IMBY / read-mandaat op zijn meetgegevens) te laten organiseren. Wij loggen het akkoord, verwittigen
// de back-office (accountmanager start de effectieve mandaatprocedure via itsme) en bumpen de warmte-score.
// De effectieve mandaataanvraag zelf gebeurt buiten deze schil (fluvius-mandaat-flow); dit is de intentie-hook.
app.post('/api/lead-mandaat', async (req, res) => {
  try {
    const b = req.body || {}; const rec = _leadLezen(b.token);
    if (!rec) return res.status(404).json({ ok: false, error: 'Lead niet gevonden of verlopen.' });
    if (!rec.verified) return res.status(403).json({ ok: false, error: 'E-mail eerst bevestigen.' });
    rec.mandaat = { akkoord: true, status: 'aangevraagd', ts: Date.now(),
      ean: String(b.ean || '').replace(/[^0-9]/g, '').slice(0, 18) || null };
    _leadEvent(rec, 'mandaat_akkoord', {}, b.token);
    rec.score = _leadScore(rec); _leadOpslaan(b.token, rec);
    // Back-office: accountmanager start de effectieve Fluvius-mandaatprocedure (itsme) op dit EAN.
    const lTxt = `MANDAAT-AKKOORD — klant vraagt de exacte studie op echte meetgegevens (EnergieKompas${rec.partner ? ' ' + rec.partner : ''})\n\nNaam : ${rec.naam || '—'}\nMail : ${rec.mail}\nTel  : ${rec.tel || '—'}\nEAN  : ${rec.mandaat.ean || '(nog niet opgegeven)'}\nTier : ${rec.tier || '—'}\nScore: ${rec.score}\n\n→ Start de Fluvius-mandaatprocedure (IMBY/itsme). Zodra de kwartierdata binnen is: /api/lead-herbereken draaien → klant krijgt automatisch zijn exacte studie gemaild.\n\nNota: ${WEB_BASE}/apps/energiekompas-nota.html?lead=${b.token}`;
    await _brevoMail(LEAD_MAIL_TO, `MANDAAT-AKKOORD EnergieKompas — ${rec.naam || rec.mail}`, lTxt, null, 'Fluctus EnergieKompas');
    // Klant: bevestiging dat het mandaat wordt georganiseerd (parallel), hij hoeft niets af te wachten.
    const kTxt = `Beste${rec.naam ? ' ' + rec.naam : ''},\n\nDank — wij organiseren nu het mandaat op uw meetgegevens (via Fluvius). Zodra uw echte kwartierdata binnen is, herberekenen wij uw studie op úw profiel en mailen wij u het exacte resultaat.\n\nU hoeft niets af te wachten: uw indicatieve analyse blijft intussen beschikbaar.\n\nMet vriendelijke groeten,\nEnergieKompas`;
    await _brevoMail(rec.mail, 'Wij organiseren uw mandaat — exacte studie volgt', kTxt, null);
    res.json({ ok: true, status: rec.mandaat.status, score: rec.score });
  } catch (e) { console.error('[lead-mandaat]', e.message); res.status(500).json({ ok: false, error: e.message }); }
});
// v15.114 — HERBEREKEN + MAIL. Wordt (optie A) nu al gedraaid op het sector-profiel, en later opnieuw
// zodra de echte Fluvius-kwartierdata binnen is. Neemt een KPI-snapshot (exact_studie) over, markeert de
// lead als herberekend en mailt de klant dat zijn (exacte) studie klaarstaat. Manager-only (back-office /
// mandaat-flow triggert dit); niet publiek, zodat een klant zichzelf geen mail kan sturen.
app.post('/api/lead-herbereken', async (req, res) => {
  try {
    if (!(await _isManagerReq(req))) return res.status(401).json({ ok: false, error: 'Manager-login vereist' });
    const b = req.body || {}; const rec = _leadLezen(b.token);
    if (!rec) return res.status(404).json({ ok: false, error: 'Lead niet gevonden of verlopen.' });
    if (b.exact_studie && typeof b.exact_studie === 'object') rec.exact_studie = b.exact_studie;   // KPI-snapshot
    if (b.data && typeof b.data === 'object') rec.data = b.data;                                   // verse nota-data
    const echt = !!b.echte_data;                                                                   // true = op Fluvius-kwartierdata
    rec.herberekend = true; rec.herberekend_ts = Date.now(); rec.herberekend_echt = echt;
    if (rec.mandaat && echt) rec.mandaat.status = 'data_binnen';
    _leadEvent(rec, echt ? 'herberekend_echt' : 'herberekend_sector', {}, b.token);
    rec.score = _leadScore(rec); _leadOpslaan(b.token, rec);
    const notaUrl = `${WEB_BASE}/apps/energiekompas-nota.html?lead=${b.token}${rec.partner ? ('&thema=' + encodeURIComponent(rec.partner)) : ''}`;
    const kop = echt ? 'Uw exacte studie op uw echte meetgegevens staat klaar' : 'Uw studie op uw profiel staat klaar';
    const kTxt = `Beste${rec.naam ? ' ' + rec.naam : ''},\n\n${echt ? 'Uw Fluvius-kwartierdata is verwerkt. ' : ''}Uw ${echt ? 'exacte ' : ''}studie staat klaar:\n${notaUrl}\n\nMet vriendelijke groeten,\nEnergieKompas`;
    const kSent = await _brevoMail(rec.mail, kop, kTxt, null);
    res.json({ ok: true, herberekend: true, echt, mail_klant: kSent.sent, score: rec.score });
  } catch (e) { console.error('[lead-herbereken]', e.message); res.status(500).json({ ok: false, error: e.message }); }
});

// ─── v15.115 (Fase 4 — SELF-SERVICE MANDAAT-INTAKE) ───────────────────────────────────────────────
// De klant zet, na factuur-upload en een BEVESTIGDE identiteit (magic-link/OTP), zelf een mandaat op:
// hij geeft de EAN + wij kennen zijn factuuradres. Wij kunnen Fluvius niet live bevragen vanuit de schil,
// dus zetten we de intentie in de LOSSE wachtrij (mandaat_los/los.json, status 'wachtrij') met de AANVRAGER
// (naam/mail/tel uit de geverifieerde lead) + factuuradres. De bestaande back-office-flow (fluvius-mandaat)
// doet de adres-crosscheck + kwartierdata-check + aanvraag, en schrijft titularis-mail/status terug (sync).
// De klant volgt zijn status via /api/mandaat/self-status en bevestigt een adres-mismatch via self-bevestig-adres.
async function _losLaden() {
  try { const j = JSON.parse(await _factuurDownload('mandaat_los/los.json')); j.eans = j.eans || []; return j; }
  catch (e) { return { eans: [] }; }
}
async function _losBewaren(los) {
  los.bijgewerkt = new Date().toISOString();
  await _factuurUpload(Buffer.from(JSON.stringify(los), 'utf8').toString('base64'), 'application/json', 'mandaat_los/los.json');
  try { _kaminoLijstCache.ts = 0; } catch (e) {}
}
app.post('/api/mandaat/self-aanvraag', async (req, res) => {
  try {
    if (!SUPABASE_OK) return res.status(503).json({ ok: false, error: 'Opslag niet geconfigureerd' });
    const b = req.body || {}; const rec = _leadLezen(b.token);
    if (!rec) return res.status(404).json({ ok: false, error: 'Lead niet gevonden of verlopen.' });
    if (!rec.verified) return res.status(403).json({ ok: false, error: 'Bevestig eerst uw e-mail (magic link).' });
    const ean = String(b.ean || '').replace(/\D/g, '');
    if (!/^\d{18}$/.test(ean)) return res.status(400).json({ ok: false, error: 'Geef een geldig EAN (18 cijfers).' });
    const factuurAdres = b.factuur_adres != null ? String(b.factuur_adres).slice(0, 300) : (rec.data && rec.data.leveringsadres) || null;
    const los = await _losLaden();
    let en = los.eans.find(x => x.ean === ean);
    const bezig = ['actief', 'aangevraagd', 'geleverd'];
    if (en && bezig.indexOf(en.status) >= 0) {
      return res.json({ ok: true, already: true, status: en.status, titularis_mail_masked: en.titularis_mail_masked || null,
        fluvius_adres: en.fluvius_adres || null, kwartierdata_aanwezig: !!en.kwartierdata_aanwezig });
    }
    const nu = new Date().toISOString();
    if (!en) { en = { ean, in_wachtrij_sinds: nu, adres_bevestigd: false, bevestigd_door: null, bevestigd_op: null, opmerking: null, aangevraagd_op: null, actief_op: null, geleverd_op: null }; los.eans.push(en); }
    en.status = 'wachtrij';
    const rol = (['klant', 'adviseur'].indexOf(String(b.rol || rec.rol)) >= 0) ? String(b.rol || rec.rol) : 'klant';
    en.aanvrager = { naam: rec.naam || '', mail: rec.mail, tel: rec.tel || '', rol };
    if (b.partner || rec.partner) en.partner = String(b.partner || rec.partner).slice(0, 40);
    en.factuur_adres = factuurAdres;
    en.aangevraagd_via = 'self-service';
    en.lead_token = b.token;
    await _losBewaren(los);
    // Stempel op de lead zodat de schil de status kan tonen.
    rec.mandaat_self = { ean, status: 'wachtrij', factuur_adres: factuurAdres, ts: Date.now() };
    _leadEvent(rec, 'mandaat_self_aangevraagd', {}, b.token);
    rec.score = _leadScore(rec); _leadOpslaan(b.token, rec);
    // Back-office verwittigen (verwerkt de wachtrij via de fluvius-mandaat-flow).
    const lTxt = `SELF-SERVICE MANDAAT-AANVRAAG (EnergieKompas)\n\nAanvrager (geverifieerd — ${rol.toUpperCase()}):\n  Naam : ${rec.naam || '—'}\n  Mail : ${rec.mail}\n  Tel  : ${rec.tel || '—'}${en.partner ? ('\n  Partner: ' + en.partner) : ''}\n\nEAN  : ${ean}\nFactuuradres: ${factuurAdres || '—'}\n\n→ Staat in de wachtrij (losse lijst). Verwerk via de fluvius-mandaat-flow: adres-crosscheck (factuur ↔ Fluvius) + kwartierdata-check, dan aanvraag; schrijf titularis-mail/status terug via /api/mandaat/sync. Bij mismatch: klant bevestigt via self-bevestig-adres.${rol === 'adviseur' ? '\n\nLET OP: aanvrager is ADVISEUR — supermanager kan partner/rol nog corrigeren in de Mandaten-app.' : ''}`;
    await _brevoMail(LEAD_MAIL_TO, `Self-service mandaat — ${rec.naam || rec.mail} (EAN ${ean})`, lTxt, null, 'Fluctus EnergieKompas');
    res.json({ ok: true, already: false, status: 'wachtrij', ean });
  } catch (e) { console.error('[mandaat/self-aanvraag]', e.message); res.status(500).json({ ok: false, error: e.message }); }
});
app.get('/api/mandaat/self-status', async (req, res) => {
  try {
    const rec = _leadLezen(req.query.token);
    if (!rec) return res.status(404).json({ ok: false, error: 'Lead niet gevonden of verlopen.' });
    const ean = String(req.query.ean || (rec.mandaat_self && rec.mandaat_self.ean) || '').replace(/\D/g, '');
    if (!/^\d{18}$/.test(ean)) return res.status(400).json({ ok: false, error: 'Geen EAN.' });
    const los = await _losLaden();
    const en = los.eans.find(x => x.ean === ean);
    if (!en) return res.json({ ok: true, gevonden: false, status: null });
    // Enkel voor de eigen aanvrager (lead-token match) zichtbaar.
    if (en.lead_token && en.lead_token !== req.query.token) return res.status(403).json({ ok: false, error: 'Geen toegang tot dit mandaat.' });
    res.json({ ok: true, gevonden: true, ean, status: en.status,
      titularis_mail_masked: en.titularis_mail_masked || null, fluvius_adres: en.fluvius_adres || null,
      adres_match: (en.adres_match == null ? null : !!en.adres_match), adres_bevestigd: !!en.adres_bevestigd,
      kwartierdata_aanwezig: !!en.kwartierdata_aanwezig, factuur_adres: en.factuur_adres || null });
  } catch (e) { console.error('[mandaat/self-status]', e.message); res.status(500).json({ ok: false, error: e.message }); }
});
app.post('/api/mandaat/self-bevestig-adres', async (req, res) => {
  try {
    if (!SUPABASE_OK) return res.status(503).json({ ok: false, error: 'Opslag niet geconfigureerd' });
    const b = req.body || {}; const rec = _leadLezen(b.token);
    if (!rec) return res.status(404).json({ ok: false, error: 'Lead niet gevonden of verlopen.' });
    if (!rec.verified) return res.status(403).json({ ok: false, error: 'Bevestig eerst uw e-mail.' });
    if (typeof b.akkoord !== 'boolean') return res.status(400).json({ ok: false, error: 'akkoord (true/false) verplicht.' });
    const ean = String(b.ean || (rec.mandaat_self && rec.mandaat_self.ean) || '').replace(/\D/g, '');
    if (!/^\d{18}$/.test(ean)) return res.status(400).json({ ok: false, error: 'Geen EAN.' });
    const los = await _losLaden();
    const en = los.eans.find(x => x.ean === ean);
    if (!en) return res.status(404).json({ ok: false, error: 'EAN niet in de wachtrij.' });
    if (en.lead_token && en.lead_token !== b.token) return res.status(403).json({ ok: false, error: 'Geen toegang tot dit mandaat.' });
    if (en.status !== 'adres_mismatch') return res.status(409).json({ ok: false, error: "Dit mandaat staat niet op 'adres_mismatch'." });
    const nu = new Date().toISOString();
    en.bevestigd_door = { naam: rec.naam || '', mail: rec.mail, via: 'self-service' };
    en.bevestigd_op = nu;
    if (b.opmerking) en.opmerking = String(b.opmerking).slice(0, 300);
    if (b.akkoord) { en.adres_bevestigd = true; en.status = 'wachtrij'; } else { en.adres_bevestigd = false; en.status = 'geannuleerd'; }
    await _losBewaren(los);
    if (rec.mandaat_self && rec.mandaat_self.ean === ean) { rec.mandaat_self.status = en.status; _leadOpslaan(b.token, rec); }
    res.json({ ok: true, ean, akkoord: !!b.akkoord, status: en.status });
  } catch (e) { console.error('[mandaat/self-bevestig-adres]', e.message); res.status(500).json({ ok: false, error: e.message }); }
});
// MANAGER-ONLY: supermanager corrigeert een losse-lijst-entry (bv. adviseur op de juiste partner zetten, rol
// aanpassen, een opmerking of status). Zo blijft de attributie kloppen voor commissie/routing.
app.post('/api/mandaat/los-patch', async (req, res) => {
  const u = await _managerGuard(req, res); if (!u) return;
  try {
    const b = req.body || {};
    const ean = String(b.ean || '').replace(/\D/g, '');
    if (!/^\d{18}$/.test(ean)) return res.status(400).json({ ok: false, error: 'geldig EAN verplicht' });
    const patch = b.patch || {};
    const los = await _losLaden();
    const en = los.eans.find(x => x.ean === ean);
    if (!en) return res.status(404).json({ ok: false, error: 'EAN niet in de losse lijst.' });
    if (patch.partner !== undefined) en.partner = (patch.partner === null || patch.partner === '') ? null : String(patch.partner).slice(0, 40);
    if (patch.opmerking !== undefined) en.opmerking = (patch.opmerking == null) ? null : String(patch.opmerking).slice(0, 300);
    if (patch.rol !== undefined && ['klant', 'adviseur'].indexOf(String(patch.rol)) >= 0) {
      en.aanvrager = en.aanvrager || {}; en.aanvrager.rol = String(patch.rol);
    }
    if (patch.status !== undefined) {
      if (_MANDAAT_STATUSSEN.indexOf(String(patch.status)) < 0) return res.status(400).json({ ok: false, error: 'ongeldige status' });
      en.status = String(patch.status);
    }
    en.gecorrigeerd_door = { naam: u.naam || u.email || u.id, ts: new Date().toISOString() };
    await _losBewaren(los);
    console.log(`[mandaat/los-patch] ${ean} bijgewerkt door ${u.naam || u.id}`);
    res.json({ ok: true, ean, entry: en });
  } catch (e) { console.error('[mandaat/los-patch]', e.message); res.status(500).json({ ok: false, error: e.message }); }
});
// Lichtgewicht identiteit voor de self-service mandaat-intake: mint een lead-token (naam/mail/tel), ZONDER
// nota-mail of Kamino-seed. De klant bevestigt vervolgens via de bestaande OTP (/api/lead-verify-send + -check),
// zodat we zeker weten wie het mandaat aanvraagt (early registrar).
app.post('/api/mandaat/self-identiteit', (req, res) => {
  try {
    const b = req.body || {};
    const mail = String(b.mail || '').trim(), tel = String(b.tel || '').trim(), naam = String(b.naam || '').trim().slice(0, 120);
    if (!_validMail(mail)) return res.status(400).json({ ok: false, error: 'Ongeldig e-mailadres.' });
    const rol = (['klant', 'adviseur'].indexOf(String(b.rol)) >= 0) ? String(b.rol) : 'klant';
    const token = _leadToken();
    const rec = { mail, tel, naam, bron: 'mandaat-intake', rol, partner: String(b.partner || '').slice(0, 40),
      data: { leveringsadres: b.leveringsadres != null ? String(b.leveringsadres).slice(0, 300) : null },
      samenvatting: { afname_marge_jaar: 0, injectie_marge_jaar: 0, energiekost_nu_mwh: 0, energiekost_dyn_mwh: 0 },
      interesses: [], ts: Date.now() };
    _leadOpslaan(token, rec);
    res.json({ ok: true, token });
  } catch (e) { console.error('[mandaat/self-identiteit]', e.message); res.status(500).json({ ok: false, error: e.message }); }
});

// ─── v15.113 (Fase 4 — self-service: e-mailverificatie (OTP) + lead-verrijking + engagement) ──────
// De klant passeert na de 3 vragen een e-mailverificatie (code per mail) → we weten zeker welk adres
// werkt; daarna kiest hij een rapport-tier en definieert hij zijn laadpleinen. Elke stap logt een event
// (timestamps) → lead-kwaliteit meten (time-to-open, hoe volledig, welke tier). Geen betaalprovider hier.
function _leadEvent(rec, ev, opt, token) {
  if (!rec) return;
  opt = opt || {};
  rec.events = Array.isArray(rec.events) ? rec.events : [];
  if (opt.once && rec.events.some(e => e.ev === ev)) return;           // eenmalig event (bv. eerste nota-open)
  rec.events.push({ ev: String(ev).slice(0, 40), ts: Date.now(), ...(opt.extra ? { extra: opt.extra } : {}) });
  if (rec.events.length > 40) rec.events = rec.events.slice(-40);
  if (token) { try { _leadOpslaan(token, rec); } catch (e) {} }
}
app.post('/api/lead-verify-send', async (req, res) => {
  try {
    const b = req.body || {}; const rec = _leadLezen(b.token);
    if (!rec) return res.status(404).json({ ok: false, error: 'Lead niet gevonden of verlopen.' });
    const code = String(Math.floor(100000 + Math.random() * 900000));   // 6 cijfers
    rec.verify_code = code; rec.verify_exp = Date.now() + 15 * 60 * 1000; rec.verify_tries = 0;
    _leadOpslaan(b.token, rec);
    const txt = `Uw verificatiecode voor EnergieKompas is: ${code}\n\nGeef deze code in op de pagina om uw analyse te bekijken. De code is 15 minuten geldig.`;
    const html = `<div style="font:15px/1.55 Helvetica,Arial,sans-serif;color:#1F3864"><p>Uw verificatiecode voor <b>EnergieKompas</b>:</p><p style="font-size:30px;font-weight:800;letter-spacing:4px;color:#05B050">${code}</p><p style="color:#5A6577;font-size:13px">Geef deze in op de pagina om uw analyse te bekijken. 15 minuten geldig.</p></div>`;
    const sent = await _brevoMail(rec.mail, 'Uw EnergieKompas-verificatiecode', txt, html);
    _leadEvent(rec, 'verify_sent', {}, b.token);
    res.json({ ok: true, sent: sent.sent, reden: sent.sent ? undefined : sent.reden });
  } catch (e) { console.error('[lead-verify-send]', e.message); res.status(500).json({ ok: false, error: e.message }); }
});
app.post('/api/lead-verify-check', (req, res) => {
  try {
    const b = req.body || {}; const rec = _leadLezen(b.token);
    if (!rec) return res.status(404).json({ ok: false, error: 'Lead niet gevonden of verlopen.' });
    if (rec.verified) return res.json({ ok: true, verified: true });
    if (!rec.verify_code || !rec.verify_exp || Date.now() > rec.verify_exp) return res.status(400).json({ ok: false, error: 'Code verlopen — vraag een nieuwe.' });
    rec.verify_tries = (rec.verify_tries || 0) + 1;
    if (rec.verify_tries > 6) { rec.verify_code = null; _leadOpslaan(b.token, rec); return res.status(429).json({ ok: false, error: 'Te veel pogingen — vraag een nieuwe code.' }); }
    if (String(b.code || '').trim() !== rec.verify_code) { _leadOpslaan(b.token, rec); return res.status(400).json({ ok: false, error: 'Onjuiste code.' }); }
    rec.verified = true; rec.verified_ts = Date.now(); rec.verify_code = null;
    _leadEvent(rec, 'verified', {}, b.token);
    res.json({ ok: true, verified: true });
  } catch (e) { console.error('[lead-verify-check]', e.message); res.status(500).json({ ok: false, error: e.message }); }
});
// Verrijk de lead: rapport-tier (view|later|direct), laadplein-definitie, of een engagement-event.
app.post('/api/lead-update', (req, res) => {
  try {
    const b = req.body || {}; const rec = _leadLezen(b.token);
    if (!rec) return res.status(404).json({ ok: false, error: 'Lead niet gevonden of verlopen.' });
    if (b.tier && ['view', 'later', 'direct'].includes(b.tier)) { rec.tier = b.tier; _leadEvent(rec, 'tier_' + b.tier); }
    if (b.laadpleinen && typeof b.laadpleinen === 'object') { rec.laadpleinen = b.laadpleinen; _leadEvent(rec, 'laadpleinen_ingevuld'); }
    if (b.event) _leadEvent(rec, b.event);
    if (b.exact_studie && typeof b.exact_studie === 'object') rec.exact_studie = b.exact_studie;   // KPI-snapshot exacte heatmap
    rec.bijgewerkt = Date.now();
    rec.score = _leadScore(rec);
    _leadOpslaan(b.token, rec);
    res.json({ ok: true, tier: rec.tier || null, verified: !!rec.verified, score: rec.score });
  } catch (e) { console.error('[lead-update]', e.message); res.status(500).json({ ok: false, error: e.message }); }
});
// Warmte-score (0–100) uit engagement + tier + intentie + marges. Voedt de accountmanager-routing.
function _leadScore(rec) {
  let s = 0; const ev = (rec.events || []).map(e => e.ev);
  if (rec.verified) s += 25;                                   // e-mail bevestigd = echt
  s += ({ direct: 30, later: 18, view: 8 })[rec.tier] || 0;   // koopintentie
  if (ev.includes('laadpleinen_ingevuld')) s += 15;           // zelf ingevuld = warm
  if (ev.includes('groeistap_aanvaard')) s += 28;            // eerste groeistap aanvaard = warmste categorie (§14.6)
  if (rec.mandaat && rec.mandaat.akkoord) s += 20;            // mandaat-akkoord = zeer warm (echte data volgt)
  if (ev.some(e => e.indexOf('scan_gecorrigeerd') === 0) || ev.includes('scan_bevestigd')) s += 8;  // locatiescan-engagement
  if (ev.includes('exact_gestart')) s += 12;                  // exacte studie gevraagd
  if (ev.includes('nota_opened')) s += 6;                     // nota bekeken
  let marge = (rec.samenvatting && (+rec.samenvatting.afname_marge_jaar || 0) + (+rec.samenvatting.injectie_marge_jaar || 0)) || 0;
  // Voorschot/raming: de marge is een RAMING, geen factuurbedrag → warmte niet kunstmatig hoog.
  if (rec.factuur_type === 'voorschot') marge = Math.min(marge * 0.4, 4000);
  s += Math.min(14, Math.round(marge / 1000));                // hoge marge = hogere waarde (cap 14)
  return Math.min(100, s);
}
// MANAGER-ONLY: overzicht van de EnergieKompas-leads voor de supermanager/accountmanagers.
app.get('/api/leads', async (req, res) => {
  try {
    if (!(await _isManagerReq(req))) return res.status(401).json({ ok: false, error: 'Manager-login vereist' });
    // v15.119: bron = het gehydrateerde geheugen (_LEADS, gevuld uit de bucket bij opstart) samengevoegd
    // met de lokale cache. Zo overleven leads een Railway-redeploy. Geheugen wint bij dubbele token.
    const recs = new Map();
    let bestanden = [];
    try { bestanden = fs.readdirSync(_leadDir).filter(f => f.endsWith('.json')); } catch (e) {}
    for (const f of bestanden) {
      const token = f.replace(/\.json$/, '');
      try { recs.set(token, JSON.parse(fs.readFileSync(path.join(_leadDir, f), 'utf8'))); } catch (e) {}
    }
    for (const [token, r] of _LEADS) { recs.set(token, r); }
    const leads = [];
    for (const [token, r] of recs) {
      if (!r) continue;
      if ((Date.now() - (r.ts || 0)) > LEAD_TTL_MS) continue;
      const opened = (r.events || []).find(e => e.ev === 'nota_opened');
      leads.push({ token, naam: r.naam || '', mail: r.mail || '', tel: r.tel || '', partner: r.partner || '',
        tier: r.tier || null, verified: !!r.verified, score: r.score != null ? r.score : _leadScore(r),
        factuur_type: r.factuur_type || null,
        mandaat: r.mandaat ? (r.mandaat.status || 'aangevraagd') : null, herberekend: !!r.herberekend,
        afname_marge_jaar: (r.samenvatting && r.samenvatting.afname_marge_jaar) || 0,
        injectie_marge_jaar: (r.samenvatting && r.samenvatting.injectie_marge_jaar) || 0,
        kamino_project_id: r.kamino_project_id || null,
        time_to_open_ms: (opened && r.ts) ? (opened.ts - r.ts) : null,
        events: (r.events || []).map(e => e.ev), ts: r.ts, bijgewerkt: r.bijgewerkt || r.ts });
    }
    leads.sort((a, b) => (b.score - a.score) || (b.ts - a.ts));
    res.json({ ok: true, aantal: leads.length, leads });
  } catch (e) { console.error('[leads]', e.message); res.status(500).json({ ok: false, error: e.message }); }
});

// ═══ v15.117 (Fase 5 — LOCATIESCAN) ══════════════════════════════════════════════════════════════
// Uit het adres op de factuur een luchtfoto-gebaseerde locatiescan die dak, sectorprofiel en
// laadpotentieel inschat en stap 8 (scherm 7bis) vooringevuld aanlevert i.p.v. leeg. Draait ASYNC
// en NIET-BLOKKEREND: elke externe bron is een pluggable functie die op een env-key/datafile draait en
// anders `null` teruggeeft (confidence 'laag'/afwezig). De job voltooit altijd; is de scan niet klaar bij
// stap 8, valt de UI terug op het lege formulier van vandaag. Geen enkele regressie.
// Bronnen & vereisten (zie Locatiescan_bouwspec §6.3/§12.6):
//   - MAPBOX_TOKEN        → geocoding + satellite-tile (luchtfoto)
//   - GOOGLE_PLACES_KEY   → openingsuren/venster + categorie
//   - KBO_API of data/kbo → NACE-sector + multi-tenant (ondernemingen op adres)
//   - OpenChargeMap       → publiek, geen key (laadpunten in de buurt)
//   - Geopunt GRB (WFS)   → publiek, geen key (dakoppervlak)
//   - data/cabines.json   → Fluvius-capaciteitskaart (lokaal), LS/MS-advies via Haversine
const _SCANS = new Map();                              // scan_id → { status, scan, confidence, ts }
const SCAN_TTL_MS = 1000 * 60 * 60 * 24 * 30;         // 30 dagen
const _scanDir = path.join(__dirname, 'data', 'scans');
const SCAN_BRON_TIMEOUT_MS = 90000;                    // per-bron timeout (bouwspec §8)
function _scanToken() { return 'scan_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 8); }
function _scanBewaar(id, rec) {
  _SCANS.set(id, rec);
  try { fs.mkdirSync(_scanDir, { recursive: true }); fs.writeFileSync(path.join(_scanDir, id + '.json'), JSON.stringify(rec)); } catch (e) {}
}
function _scanLees(id) {
  if (!/^scan_[a-z0-9]{1,40}$/.test(String(id || ''))) return null;
  let rec = _SCANS.get(id);
  if (!rec) { try { rec = JSON.parse(fs.readFileSync(path.join(_scanDir, id + '.json'), 'utf8')); _SCANS.set(id, rec); } catch (e) { rec = null; } }
  if (rec && (Date.now() - (rec.ts || 0)) > SCAN_TTL_MS) return null;
  return rec || null;
}
// Wikkel een bron in een timeout + try/catch → faalt zacht naar null (nooit blokkerend).
function _bronZacht(naam, fn) {
  return Promise.race([
    Promise.resolve().then(fn).catch(e => { console.warn(`[locatiescan] bron '${naam}' faalde: ${e.message}`); return null; }),
    new Promise(res => setTimeout(() => res(null), SCAN_BRON_TIMEOUT_MS)),
  ]);
}
// ── Bron-functies (B2) — elk env/data-gated, elk degradeert naar null ────────────────────────────
async function _scanGeocode(adres) {
  const tok = process.env.MAPBOX_TOKEN; if (!tok || !adres) return null;
  const url = `https://api.mapbox.com/geocoding/v5/mapbox.places/${encodeURIComponent(adres)}.json?country=be&limit=1&access_token=${tok}`;
  const r = await fetch(url); if (!r.ok) return null;
  const j = await r.json(); const f = j && j.features && j.features[0];
  if (!f || !Array.isArray(f.center)) return null;
  return { lat: f.center[1], lon: f.center[0], plaats_naam: f.place_name || null };
}
function _scanLuchtfotoUrl(lat, lon) {
  const tok = process.env.MAPBOX_TOKEN; if (!tok || lat == null || lon == null) return null;
  // satellite-v9, zoom 18, 640×420 (bouwspec §12.6e). URL bevat de token — enkel server-side samenstellen.
  return `https://api.mapbox.com/styles/v1/mapbox/satellite-v9/static/${lon},${lat},18,0/640x420@2x?access_token=${tok}`;
}
async function _scanGrbDak(lat, lon) {
  if (lat == null || lon == null) return null;
  // Geopunt GRB gebouwvlak (publiek WFS). Best-effort; egress-afhankelijk → null bij fout.
  const url = `https://geo.api.vlaanderen.be/GRB/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=GRB:GBG&outputFormat=application/json&count=1&srsName=EPSG:4326&cql_filter=${encodeURIComponent(`CONTAINS(SHAPE,POINT(${lon} ${lat}))`)}`;
  const r = await fetch(url); if (!r.ok) return null;
  const j = await r.json(); const f = j && j.features && j.features[0];
  if (!f || !f.properties) return null;
  const opp = +f.properties.OPPERVL || +f.properties.SHAPE_AREA || null;
  return opp ? { opp_m2: Math.round(opp), bron_opp: 'GRB' } : null;
}
// v15.120: provider-tolerante CBE/KBO-REST-adapter. Werkt zowel met cbeapi.be als met de
// Crossroads/kbodata.app-API (en elke soortgelijke): je zet KBO_API = de basis-URL tot vlak vóór
// het ondernemingsnummer, en KBO_API_KEY = de bearer-token. De parser leest NACE, ondernemingen-op-
// adres en (indien de provider ze levert) de bestuurders uit meerdere mogelijke veldvormen. Zonder
// bron of key → null; de scan valt netjes terug op de sector-heuristiek. Geen regressie.
//   cbeapi.be           → KBO_API=https://cbeapi.be/api/v1/enterprise   (NACE/adres, GEEN bestuurders)
//   crossroadsbankent.  → KBO_API=https://api.kbodata.app/v2/enterprise (NACE/adres + bestuurders*)
//   (*bestuurders: EnterpriseRoles, doorgaans op een groter plan)
function _kboTekst(v) { // multilingual {nl,fr,en} of string → string
  if (!v) return null;
  if (typeof v === 'string') return v;
  return v.nl || v.NL || v.nederlands || v.fr || v.en || v.value || null;
}
async function _scanKbo(btw) {
  // NACE + multi-tenant (+ bestuurders indien beschikbaar). Vereist KBO-databron (API of open data). Zonder → null.
  if (!btw || (!process.env.KBO_API && !fs.existsSync(path.join(__dirname, 'data', 'kbo')))) return null;
  try {
    if (process.env.KBO_API) {
      const nr = String(btw).replace(/\D/g, '');
      if (!nr) return null;
      const headers = { 'accept': 'application/json' };
      const key = process.env.KBO_API_KEY || process.env.CBEAPI_KEY;
      if (key) headers['Authorization'] = /\s/.test(key) ? key : `Bearer ${key}`;  // 'Bearer xxx' of kale token
      const r = await fetch(`${process.env.KBO_API.replace(/\/+$/, '')}/${nr}`, { headers });
      if (!r.ok) return null;
      const j = await r.json();
      const ent = j.enterprise || j.data || j;   // sommige providers wikkelen in {enterprise:...}
      // NACE — top-level of uit een activiteitenlijst
      const acts = ent.Activities || ent.activities || ent.nace_activities || [];
      let nace = ent.nace || ent.naceCode || null;
      if (!nace && Array.isArray(acts) && acts.length) {
        const a = acts[0];
        nace = (a.Nace && (a.Nace.naceCode || a.Nace.code)) || a.nace || a.naceCode || a.code || null;
      }
      // Ondernemingen op adres (multi-tenant) — meerdere mogelijke veldnamen
      const opAdres = ent.ondernemingen_op_adres != null ? ent.ondernemingen_op_adres
                    : (ent.enterprises_at_address != null ? ent.enterprises_at_address
                    : (ent.establishments_count != null ? ent.establishments_count : null));
      // Bestuurders — EnterpriseRoles / roles / mandataries / directors, tolerant gemapt
      const rollen = ent.EnterpriseRoles || ent.enterpriseRoles || ent.roles || ent.mandataries || ent.directors || null;
      let bestuurders = null;
      if (Array.isArray(rollen) && rollen.length) {
        bestuurders = rollen.map(x => {
          const voor = x.nameFirst || x.firstName || x.voornaam || '';
          const achter = x.nameLast || x.lastName || x.naam || x.name || '';
          const naam = (`${voor} ${achter}`).trim() || _kboTekst(x.denomination) || null;
          const functie = _kboTekst(x.Role && (x.Role.title || x.Role.name)) || _kboTekst(x.role) || _kboTekst(x.function) || _kboTekst(x.title) || null;
          return { naam, functie, sinds: x.dateInOffice || x.since || x.sinds || null };
        }).filter(b => b.naam);
        if (!bestuurders.length) bestuurders = null;
      }
      const naam = _kboTekst(ent.denomination) || _kboTekst(ent.name) || ent.enterpriseName || null;
      return { nace: nace || null, type: null, ondernemingen_op_adres: opAdres, bestuurders, naam, bron: 'KBO' };
    }
  } catch (e) {}
  return null;
}
// v15.121: WINSTCIJFERS uit de neergelegde jaarrekening via de GRATIS NBB-Balanscentrale-webservice
// "Authentic Data Query" (developer.cbso.nbb.be → gratis registratie → 'Authentic Data Query'
// abonneren → primary key). Env NBB_CBSO_KEY (verplicht), NBB_CBSO_BASE (default prod). Twee calls:
//   1) GET {base}/authentic/legalEntity/{ondernemingsnr}/references   → lijst neerleggingen
//   2) GET {base}/authentic/deposit/{referentie}/accountingData        → rubrieken (code→waarde)
// Headers: NBB-CBSO-Subscription-Key, X-Request-Id, Accept: application/json.
// Winst-codes: 9904 = winst/verlies boekjaar (netto), 9903/9901 = winst vóór belasting,
// 9900 = brutomarge (verkort/micro), 70 = omzet (volledig schema). Enkel vennootschappen die
// neerleggen; eenmanszaken → null. Zonder key → null (nota valt terug op sector/geen winst-noemer).
function _reqId() { return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => { const r = Math.random() * 16 | 0; return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16); }); }
async function _nbbHaal(url) {
  const key = process.env.NBB_CBSO_KEY; if (!key) return null;
  const r = await fetch(url, { headers: { 'NBB-CBSO-Subscription-Key': key, 'X-Request-Id': _reqId(), 'Accept': 'application/json' } });
  if (!r.ok) return null;
  return r.json();
}
async function _bedrijfsWinst(btw) {
  const key = process.env.NBB_CBSO_KEY;
  if (!btw || !key) return null;
  const nr = String(btw).replace(/\D/g, '');
  if (nr.length < 9) return null;
  const base = (process.env.NBB_CBSO_BASE || 'https://ws.cbso.nbb.be').replace(/\/+$/, '');
  try {
    const refs = await _nbbHaal(`${base}/authentic/legalEntity/${nr}/references`);
    const lijst = Array.isArray(refs) ? refs : ((refs && (refs.references || refs.Data || refs.data)) || []);
    if (!Array.isArray(lijst) || !lijst.length) return null;
    // Kies de meest recente neerlegging (op einddatum boekjaar, anders neerleggingsdatum).
    const gekozen = lijst.map(x => ({
      ref: x.ReferenceNumber || x.referenceNumber || x.reference || x.Reference || null,
      eind: (x.ExerciseDates && (x.ExerciseDates.endDate || x.ExerciseDates.EndDate)) || x.exerciseEndDate || x.EndDate || x.DepositDate || x.depositDate || null,
    })).filter(x => x.ref).sort((a, b) => String(b.eind || '').localeCompare(String(a.eind || '')))[0];
    if (!gekozen) return null;
    const acc = await _nbbHaal(`${base}/authentic/deposit/${encodeURIComponent(gekozen.ref)}/accountingData`);
    if (!acc) return null;
    // Recursief alle {code, waarde} verzamelen voor de HUIDIGE periode (N, niet N-1).
    const rub = {};
    (function walk(o) {
      if (!o || typeof o !== 'object') return;
      if (Array.isArray(o)) { o.forEach(walk); return; }
      const code = o.Code != null ? o.Code : (o.code != null ? o.code : (o.rubricCode != null ? o.rubricCode : o.RubricCode));
      const per = String(o.Period || o.period || o.PeriodType || 'N').toUpperCase();
      let val = o.Value != null ? o.Value : (o.value != null ? o.value : (o.amount != null ? o.amount : o.Amount));
      if (code != null && val != null && per.charAt(0) === 'N' && !per.includes('1')) {
        const c = String(code).trim();
        const n = Number(val);
        if (rub[c] === undefined && !isNaN(n)) rub[c] = n;
      }
      for (const k in o) walk(o[k]);
    })(acc);
    const g = c => (rub[c] != null && !isNaN(rub[c])) ? rub[c] : null;
    const nettowinst = g('9904');
    const winstVoorBelasting = g('9903') != null ? g('9903') : g('9901');
    const brutomarge = g('9900');
    const omzet = g('70');
    if (nettowinst == null && winstVoorBelasting == null && brutomarge == null && omzet == null) return null;
    return { boekjaar_einde: gekozen.eind || null, nettowinst, winst_voor_belasting: winstVoorBelasting, brutomarge, omzet, bron: 'NBB', bron_ref: gekozen.ref };
  } catch (e) { return null; }
}
async function _scanPlaces(adres, naam) {
  const key = process.env.GOOGLE_PLACES_KEY; if (!key || (!adres && !naam)) return null;
  try {
    const q = encodeURIComponent([naam, adres].filter(Boolean).join(' '));
    const r = await fetch(`https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input=${q}&inputtype=textquery&fields=opening_hours,types,name&key=${key}`);
    if (!r.ok) return null; const j = await r.json(); const c = j && j.candidates && j.candidates[0];
    if (!c) return null;
    return { categorie: (c.types && c.types[0]) || null, openingsuren: (c.opening_hours && c.opening_hours.weekday_text) || null, bron: 'Places' };
  } catch (e) { return null; }
}
async function _scanOcm(lat, lon) {
  if (lat == null || lon == null) return null;
  // OpenChargeMap — publiek, geen key (bouwspec §12.6d). DC = maxPowerKw>=50, niet LevelID.
  const url = `https://api.openchargemap.io/v3/poi/?latitude=${lat}&longitude=${lon}&distance=5&distanceunit=KM&countrycode=BE&maxresults=50&compact=true`;
  const r = await fetch(url, { headers: { 'User-Agent': 'FluctusEnergieKompas/1.0' } }); if (!r.ok) return null;
  const arr = await r.json(); if (!Array.isArray(arr)) return null;
  let dc = 0, ac = 0, dichtstbij = null;
  for (const p of arr) {
    const kw = Math.max(0, ...(((p.Connections || []).map(c => +c.PowerKW || 0))));
    if (kw >= 50) dc++; else ac++;
    const d = p.AddressInfo && p.AddressInfo.Distance;
    if (d != null && (dichtstbij == null || d < dichtstbij)) dichtstbij = d;
  }
  return { dc, ac, dichtstbij_m: dichtstbij != null ? Math.round(dichtstbij * 1000) : null };
}
function _haversine(aLat, aLon, bLat, bLon) {
  const R = 6371000, t = Math.PI / 180;
  const dLa = (bLat - aLat) * t, dLo = (bLon - aLon) * t;
  const h = Math.sin(dLa / 2) ** 2 + Math.cos(aLat * t) * Math.cos(bLat * t) * Math.sin(dLo / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(h));
}
let _cabinesCache = null;
function _scanCabines(lat, lon) {
  if (lat == null || lon == null) return null;
  if (_cabinesCache === null) {
    try { _cabinesCache = JSON.parse(fs.readFileSync(path.join(__dirname, 'data', 'cabines.json'), 'utf8')); }
    catch (e) { _cabinesCache = false; }   // false = bestand ontbreekt → bron uit
  }
  if (!_cabinesCache) return null;
  const lijst = Array.isArray(_cabinesCache) ? _cabinesCache : (_cabinesCache.cabines || []);
  const nabij = [];
  for (const c of lijst) {
    const cl = c.lat != null ? c.lat : c.latitude, co = c.lon != null ? c.lon : c.longitude;
    if (cl == null || co == null) continue;
    const d = Math.round(_haversine(lat, lon, cl, co));
    if (d <= 500) nabij.push({ lat: cl, lon: co, naam: c.naam || c.name || null, afstand_m: d, rest_kva_afname: c.rest_kva_afname != null ? c.rest_kva_afname : (c.restcapaciteit_afname_kva != null ? c.restcapaciteit_afname_kva : null) });
  }
  nabij.sort((a, b) => a.afstand_m - b.afstand_m);
  const best = nabij[0]; if (!best) return null;
  const m = best.afstand_m;
  const advies = m <= 80 ? 'MS_MOGELIJK' : (m <= 150 ? 'TWIJFEL' : 'LS');
  // `cabines` (≤500 m, gesorteerd) = door te geven in de kabelplanner-seed (§12.6a); `ms_*` = het klant-verborgen advies.
  return { ms_cabine_dichtstbij_m: m, ms_cabine_naam: best.naam, restcapaciteit_afname_kva: best.rest_kva_afname, advies, openbare_weg_tussen: null, cabines: nabij.slice(0, 8), bron: 'Fluvius capaciteitskaart (lokale cabines.json)' };
}
// Vision-pass (fase 2, bouwspec §7): tel panelen + parkeervakken op de luchtfoto met Claude-vision.
// Vereist MAPBOX_TOKEN (tile) + ANTHROPIC_API_KEY. Zonder → null (fase 1 blijft: vragen aan de klant).
// KRUISCONTROLE: wijkt het paneelvlak >35% af van de factuur-injectie → factuur wint, confidence 'laag' (§3.1/§8).
async function _scanVision(luchtfotoUrl, injectieMwh) {
  const key = process.env.ANTHROPIC_API_KEY; if (!key || !luchtfotoUrl) return null;
  try {
    const img = await fetch(luchtfotoUrl); if (!img.ok) return null;
    const buf = Buffer.from(await img.arrayBuffer());
    const media = (img.headers.get('content-type') || 'image/png').split(';')[0];
    const prompt = 'Dit is een luchtfoto (satelliet) van een bedrijfslocatie in Vlaanderen. Antwoord ENKEL met JSON, geen tekst eromheen: {"zonnepanelen_geteld": <geheel getal of null>, "paneelvlak_m2": <getal of null>, "parkeerplaatsen_geschat": <geheel getal of null>, "zekerheid": "hoog"|"midden"|"laag"}. Tel enkel wat je duidelijk ziet; bij twijfel null.';
    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST', headers: { 'x-api-key': key, 'anthropic-version': '2023-06-01', 'content-type': 'application/json' },
      body: JSON.stringify({ model: process.env.FACTUUR_MODEL || 'claude-sonnet-4-5', max_tokens: 400,
        messages: [{ role: 'user', content: [{ type: 'image', source: { type: 'base64', media_type: media, data: buf.toString('base64') } }, { type: 'text', text: prompt }] }] }) });
    if (!r.ok) return null;
    const j = await r.json(); const t = (j.content && j.content[0] && j.content[0].text) || '';
    const m = t.match(/\{[\s\S]*\}/); if (!m) return null;
    const v = JSON.parse(m[0]);
    // kWp uit paneelaantal (± 400 Wp/paneel) of paneelvlak (÷1,95 m²/paneel × 0,55 O-W × 0,4 kWp)
    let kwp = null;
    if (v.zonnepanelen_geteld > 0) kwp = Math.round(v.zonnepanelen_geteld * 0.4 * 10) / 10;
    else if (v.paneelvlak_m2 > 0) kwp = Math.round(v.paneelvlak_m2 / 1.95 * 0.55 * 0.4 * 10) / 10;
    let confidence = v.zekerheid || 'laag';
    // kruiscontrole tegen de factuur-injectie (kWp ≈ injectie_kWh / 950)
    if (kwp != null && injectieMwh > 0) { const kwpFactuur = injectieMwh * 1000 / 950; if (kwpFactuur > 0 && Math.abs(kwp - kwpFactuur) / kwpFactuur > 0.35) { kwp = Math.round(kwpFactuur * 10) / 10; confidence = 'laag'; } }
    return { pv_bestaand_kwp: kwp, pv_panelen_geteld: v.zonnepanelen_geteld || null, parkeerplaatsen: v.parkeerplaatsen_geschat || null, confidence };
  } catch (e) { console.warn('[locatiescan] vision faalde:', e.message); return null; }
}
// NACE → sectorprofiel-mapping (bouwspec §4). Pure, geen externe call.
function _scanSectorprofiel(nace) {
  if (!nace) return null;
  const g = String(nace).replace(/\D/g, '').slice(0, 2);
  const T = { '01': ['agri', 'seizoensgebonden'], '10': ['industrie_continu', '24/7'], '41': ['kmo_productie', '07:00-17:00'], '42': ['kmo_productie', '07:00-17:00'], '43': ['kmo_productie', '07:00-17:00'], '45': ['kmo_productie', '08:00-18:00'], '46': ['retail', 'uit Places'], '47': ['retail', 'uit Places'], '49': ['kmo_productie', '05:00-20:00'], '55': ['horeca', 'uit Places'], '56': ['horeca', 'uit Places'], '62': ['kantoor', '08:00-18:00'], '70': ['kantoor', '08:00-18:00'], '80': ['kantoor', '08:00-18:00'] };
  const hit = T[g] || (g >= '10' && g <= '33' ? ['industrie_continu', '24/7'] : (g >= '62' && g <= '70' ? ['kantoor', '08:00-18:00'] : null));
  return hit ? { type: hit[0], venster: hit[1] } : null;
}
// ── Orkestrator (B1) ─────────────────────────────────────────────────────────────────────────────
async function _runLocatiescan(id, inp) {
  const rec = _scanLees(id); if (!rec) return;
  try {
    const geo = await _bronZacht('geocode', () => _scanGeocode(inp.adres));
    const lat = geo && geo.lat, lon = geo && geo.lon;
    const [dak, kbo, places, ocm, winst] = await Promise.all([
      _bronZacht('grb', () => _scanGrbDak(lat, lon)),
      _bronZacht('kbo', () => _scanKbo(inp.btw)),
      _bronZacht('places', () => _scanPlaces(inp.adres, inp.bedrijfsnaam)),
      _bronZacht('ocm', () => _scanOcm(lat, lon)),
      _bronZacht('nbb-winst', () => _bedrijfsWinst(inp.btw)),
    ]);
    const cabines = _scanCabines(lat, lon);   // lokaal, synchroon
    const sector = _scanSectorprofiel(kbo && kbo.nace);
    const luchtfoto = _scanLuchtfotoUrl(lat, lon);
    const vision = luchtfoto ? await _bronZacht('vision', () => _scanVision(luchtfoto, +inp.injectie_mwh || 0)) : null;   // fase 2
    const conf = {};
    const scan = {
      versie: 1, adres: inp.adres || null,
      coord: (lat != null) ? { lat, lon } : null,
      beeld: luchtfoto ? { bron: 'mapbox-satellite', url: luchtfoto } : null,
      dak: (dak || vision) ? { opp_m2: dak && dak.opp_m2 || null, bron_opp: dak && dak.bron_opp || null, pv_bestaand_kwp: vision && vision.pv_bestaand_kwp != null ? vision.pv_bestaand_kwp : null, pv_panelen_geteld: vision && vision.pv_panelen_geteld || null, pv_vrij_kwp: null, confidence: vision && vision.confidence || (dak ? 'midden' : 'laag') } : null,
      parking: (vision && vision.parkeerplaatsen != null) ? { plaatsen_totaal: vision.parkeerplaatsen, belijning: null, methode: 'vision', confidence: vision.confidence || 'laag' } : null,   // fase 2 (vision); anders leeg → klant vult in
      profiel: (sector || kbo) ? { nace: kbo && kbo.nace || null, type: sector && sector.type || null, venster: (places && places.openingsuren) ? 'uit Places' : (sector && sector.venster || null), onderneming: kbo && kbo.naam || null, bestuurders: kbo && kbo.bestuurders || null, bron: kbo ? 'KBO' + (places ? '+Places' : '') : 'heuristiek', confidence: kbo ? 'hoog' : 'laag' } : null,
      omgeving: ocm ? { laadpunten_5km: { dc: ocm.dc, ac: ocm.ac, dichtstbij_m: ocm.dichtstbij_m } } : null,
      netaansluiting: cabines || null,
      eigendom: (kbo && kbo.ondernemingen_op_adres != null) ? { multi_tenant: kbo.ondernemingen_op_adres > 1, ondernemingen_op_adres: kbo.ondernemingen_op_adres } : null,
      financieel: winst ? { boekjaar_einde: winst.boekjaar_einde, nettowinst: winst.nettowinst, winst_voor_belasting: winst.winst_voor_belasting, brutomarge: winst.brutomarge, omzet: winst.omzet, bron: 'NBB-jaarrekening', confidence: 'hoog' } : null,   // v15.121 — winst-noemer voor "besparing = X% van uw winst"
    };
    // status: 'klaar' als er iets bruikbaars is, anders 'leeg' (UI valt terug op leeg formulier).
    const iets = !!(scan.beeld || scan.dak || scan.profiel || scan.omgeving || scan.netaansluiting);
    rec.status = iets ? 'klaar' : 'leeg';
    rec.scan = scan; rec.confidence = conf; rec.klaar_ts = Date.now();
    _scanBewaar(id, rec);
    // Best-effort merge in het projectrecord (bouwspec §6.2), niet-blokkerend.
    if (inp.projectId && SUPABASE_OK) {
      try {
        const veilig = String(inp.projectId).replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 40);
        let pr = null; try { pr = JSON.parse(await _factuurDownload(`kamino/${veilig}.json`)); } catch (e) {}
        if (pr) { pr.locatiescan = scan; pr.bijgewerkt = new Date().toISOString();
          await _factuurUpload(Buffer.from(JSON.stringify(pr), 'utf8').toString('base64'), 'application/json', `kamino/${veilig}.json`); }
      } catch (e) { /* niet-blokkerend */ }
    }
  } catch (e) {
    console.error('[locatiescan] job faalde:', e.message);
    rec.status = 'leeg'; rec.fout = e.message; _scanBewaar(id, rec);
  }
}
app.post('/api/locatiescan', (req, res) => {
  try {
    const b = req.body || {};
    const adres = String(b.adres || '').trim().slice(0, 300);
    if (!adres) return res.status(400).json({ ok: false, error: 'adres verplicht' });
    const id = _scanToken();
    const rec = { status: 'bezig', scan: null, confidence: {}, ts: Date.now(),
      input: { adres, btw: String(b.btw || '').replace(/[^0-9A-Za-z.]/g, '').slice(0, 20) || null, bedrijfsnaam: String(b.bedrijfsnaam || '').slice(0, 160) || null, ean: String(b.ean || '').replace(/\D/g, '').slice(0, 18) || null, injectie_mwh: +b.injectie_mwh || 0, projectId: String(b.projectId || '').slice(0, 40) || null } };
    _scanBewaar(id, rec);
    setTimeout(() => { _runLocatiescan(id, rec.input); }, 0);   // async, niet-blokkerend
    res.json({ ok: true, scan_id: id, status: 'bezig' });
  } catch (e) { console.error('[locatiescan POST]', e.message); res.status(500).json({ ok: false, error: e.message }); }
});
app.get('/api/locatiescan/:scan_id', (req, res) => {
  const rec = _scanLees(req.params.scan_id);
  if (!rec) return res.status(404).json({ ok: false, error: 'Scan niet gevonden of verlopen.' });
  res.json({ ok: true, status: rec.status, scan: rec.scan || null, confidence: rec.confidence || {} });
});
// v15.121: winst-koppeling los testbaar — GET /api/bedrijfswinst?btw=0757494180 → NBB-jaarrekeningcijfers.
// Zo kan de NBB-link (key + endpoints) geverifieerd worden zonder een volledige scan. Winst is publieke
// data; geen login vereist. Zonder NBB_CBSO_KEY → 503 met duidelijke reden.
app.get('/api/bedrijfswinst', async (req, res) => {
  try {
    if (!process.env.NBB_CBSO_KEY) return res.status(503).json({ ok: false, error: 'NBB_CBSO_KEY niet gezet — zet de sleutel van developer.cbso.nbb.be (Authentic Data Query).' });
    const btw = String(req.query.btw || '').replace(/\D/g, '');
    if (btw.length < 9) return res.status(400).json({ ok: false, error: 'Geef een geldig ondernemingsnummer via ?btw=' });
    const w = await _bedrijfsWinst(btw);
    if (!w) return res.json({ ok: true, gevonden: false, reden: 'Geen neergelegde jaarrekening gevonden (of eenmanszaak).', btw });
    res.json({ ok: true, gevonden: true, btw, winst: w, _meta: { server_version: SERVER_VERSIE } });
  } catch (e) { console.error('[bedrijfswinst]', e.message); res.status(500).json({ ok: false, error: e.message }); }
});

// ═══ v15.118 (Fase 5 — HARDWARE-BRUG) ════════════════════════════════════════════════════════════
// Vertaalt een gekozen combo (PV + batterij + laadpalen) naar een CONCRETE shoppinglist met prijs en
// terugverdientijd — "welke PV, welke batterij, welke palen" = de stap het dichtst bij een bestelling.
// Prijzen: Jacops-lijst als default (Locatiescan_bouwspec §13.1/§13.8) + degressieve KMO-batterij- en
// PV-staffel (§14.8; exponent 0,187, batterij-anker €212,21/kWh @261kWh). Verkoop AC €0,35/DC €0,55,
// afschrijving 8 j. LET OP: de kostprijs/kWh komt ALTIJD uit de sim (§12.5) — nooit een forfait hier.
// Deze getallen horen op termijn uit jacops_prijstabel.json te komen; nu als geijkte constanten.
const HW = {
  PRIJS_AC_KWH: 0.35, PRIJS_DC_KWH: 0.55, AFSCHRIJF_JAAR: 8,
  // AC dubbel 22 kW — Jacops (§13.1): PLUON004+JAC012+JAC014+JAC117+JAC124
  AC_DUBBEL_CAPEX: 4203, AC_DUBBEL_RECURRING_J: 540,      // Eplus005 2×€22,50/m
  // DC 2×80 kW (160 kVA) — Jacops ondergrens (§13.1): PLUON055+JAC013+JAC118+JAC124
  DC_2X80_CAPEX: 46463, DC_2X80_RECURRING_J: 2400,        // Eplus006 2×€100/m
  LB_CAPEX: 2500, LB_RECURRING_J: 480,                    // Eplus016 + Eplus017
  BETAALTERMINAL: 1925,                                   // PLUON072 (enkel betalend plein)
  BAT_ANKER_KWH: 261, BAT_ANKER_EUR_KWH: 212.21, BAT_EXP: 0.187,
  BAT_418_EUR_KWH: 209.43, BAT_RECURRING_KWH_J: 6.23,     // SV-02000 3,23 + ond-bat 3,00
  PV_5_EUR_KWP: 1227, PV_EXP: 0.187,                      // degressief, gelijk aan de batterij-staffel
};
function _hwBatterij(kwh) {
  kwh = +kwh || 0; if (kwh <= 0) return null;
  let capex, eurKwh, bron = 'jacops-degressief';
  if (kwh <= HW.BAT_ANKER_KWH) { eurKwh = HW.BAT_ANKER_EUR_KWH * Math.pow(HW.BAT_ANKER_KWH / kwh, HW.BAT_EXP); capex = eurKwh * kwh; }
  else if (kwh <= 418) { const c261 = 55386, c418 = 87542; capex = c261 + (c418 - c261) * (kwh - 261) / (418 - 261); eurKwh = capex / kwh; bron = 'jacops-anker'; }
  else { eurKwh = HW.BAT_418_EUR_KWH; capex = eurKwh * kwh; bron = 'jacops-418-geschat'; }
  return { kwh: Math.round(kwh), eur_per_kwh: Math.round(eurKwh * 100) / 100, capex: Math.round(capex), recurring_j: Math.round(kwh * HW.BAT_RECURRING_KWH_J), bron };
}
function _hwPv(kwp) {
  kwp = +kwp || 0; if (kwp <= 0) return null;
  let eurKwp = HW.PV_5_EUR_KWP * Math.pow(5 / kwp, HW.PV_EXP);
  eurKwp = Math.max(455, Math.min(1227, eurKwp));
  return { kwp: Math.round(kwp * 10) / 10, eur_per_kwp: Math.round(eurKwp), capex: Math.round(eurKwp * kwp), bron: 'epc-template-degressief' };
}
// cfg: { pv_kwp?, batt_kwh?, ac_dubbel?, dc_2x80?, load_balancer?, betalend?, besparing_j?, kostprijs_kwh? }
function _hwVoorstel(cfg) {
  cfg = cfg || {}; const items = []; let capex = 0, recurring = 0;
  const pv = _hwPv(cfg.pv_kwp); if (pv) { items.push({ sleutel: 'pv', naam: `Zonnepanelen ${pv.kwp} kWp`, detail: `± € ${pv.eur_per_kwp}/kWp (degressief)`, capex: pv.capex, recurring_j: 0 }); capex += pv.capex; }
  const bat = _hwBatterij(cfg.batt_kwh); if (bat) { items.push({ sleutel: 'batterij', naam: `Batterij ${bat.kwh} kWh`, detail: `± € ${bat.eur_per_kwh}/kWh · onderhoud/sturing € ${bat.recurring_j}/j`, capex: bat.capex, recurring_j: bat.recurring_j, geschat: bat.bron !== 'jacops-anker' }); capex += bat.capex; recurring += bat.recurring_j; }
  const nAc = Math.max(0, Math.round(+cfg.ac_dubbel || 0));
  if (nAc > 0) { items.push({ sleutel: 'ac', naam: `${nAc}× dubbele AC-laadpaal (22 kW)`, detail: `E+Drive-abonnement inbegrepen`, capex: nAc * HW.AC_DUBBEL_CAPEX, recurring_j: nAc * HW.AC_DUBBEL_RECURRING_J }); capex += nAc * HW.AC_DUBBEL_CAPEX; recurring += nAc * HW.AC_DUBBEL_RECURRING_J; }
  const nDc = Math.max(0, Math.round(+cfg.dc_2x80 || 0));
  if (nDc > 0) { items.push({ sleutel: 'dc', naam: `${nDc}× DC-snellader (2×80 kW)`, detail: `plaatsing via regie — capex is ondergrens`, capex: nDc * HW.DC_2X80_CAPEX, recurring_j: nDc * HW.DC_2X80_RECURRING_J, geschat: true }); capex += nDc * HW.DC_2X80_CAPEX; recurring += nDc * HW.DC_2X80_RECURRING_J; }
  if (cfg.load_balancer || nAc + nDc >= 2) { items.push({ sleutel: 'lb', naam: 'Dynamische load balancing', detail: 'houdt de palen binnen uw aansluiting', capex: HW.LB_CAPEX, recurring_j: HW.LB_RECURRING_J }); capex += HW.LB_CAPEX; recurring += HW.LB_RECURRING_J; }
  if (cfg.betalend && (nAc + nDc) > 0) { items.push({ sleutel: 'terminal', naam: 'Betaalterminal', detail: 'enkel voor een betalend plein', capex: HW.BETAALTERMINAL, recurring_j: 0 }); capex += HW.BETAALTERMINAL; }
  const jaarlast = Math.round(capex / HW.AFSCHRIJF_JAAR + recurring);
  const besparing = +cfg.besparing_j || 0;
  const netto = besparing > 0 ? Math.round(besparing - recurring) : null;
  const payback = (besparing - recurring) > 0 ? Math.round(capex / (besparing - recurring) * 10) / 10 : null;
  return { items, capex_totaal: Math.round(capex), recurring_totaal_j: Math.round(recurring), jaarlast_j: jaarlast, netto_j: netto, payback_j: payback, afschrijf_jaar: HW.AFSCHRIJF_JAAR, kostprijs_kwh: (cfg.kostprijs_kwh != null ? +cfg.kostprijs_kwh : null) };
}
app.post('/api/hardware-voorstel', (req, res) => {
  try { const v = _hwVoorstel(req.body || {}); res.json({ ok: true, voorstel: v, _meta: { server_version: SERVER_VERSIE } }); }
  catch (e) { console.error('[hardware-voorstel]', e.message); res.status(500).json({ ok: false, error: e.message }); }
});

// ═══ v15.118 (Fase 5 — DESTINATION-LUIK SPOOR 2) ═════════════════════════════════════════════════
// Parametriseert een BETALEND laadplein — ENKEL na de klant-keuze (Ontwerp deelbeslissing 5; §13.0).
// Capture rate/dwell per sub-segment (§12.3), volumeformule → sessies/dag met sensitiviteit 0/50/100/150%,
// drempel = FUNCTIE van de kostprijs uit de sim (§13.1, geen constante), AC/DC-mix uit de verblijfsduur.
// Onder de AC-drempel: GEEN opbrengstcijfer, wel de reden (§13.3 waarborg 1).
const DEST = {
  UNIQUE_FACTOR: 0.375, AVG_SESSIE_KWH: 30, SESSIE_KWH_AC: 18, SESSIE_KWH_DC: 28,
  // sub-segment → { capture:[lo,hi], dwell_min:[lo,hi] } (§12.3)
  SUB: {
    horeca_lunch: { capture: [0.15, 0.25], dwell: [60, 75] }, horeca_diner_mid: { capture: [0.25, 0.35], dwell: [100, 120] },
    horeca_diner_premium: { capture: [0.35, 0.45], dwell: [150, 180] }, horeca_hotel: { capture: [0.35, 0.50], dwell: [300, 600] },
    sport_padel: { capture: [0.25, 0.40], dwell: [75, 90] }, sport_tennis: { capture: [0.30, 0.45], dwell: [120, 150] },
    sport_voetbal: { capture: [0.20, 0.50], dwell: [120, 180] }, sport_hockey_golf: { capture: [0.40, 0.55], dwell: [180, 240] },
    bedrijf_kantoor: { capture: [0.70, 0.85], dwell: [420, 540] }, bedrijf_garage: { capture: [0.60, 0.75], dwell: [240, 540] },
    bedrijf_logistiek: { capture: [0.75, 0.90], dwell: [600, 720] }, bedrijf_industrieel: { capture: [0.50, 0.70], dwell: [480, 720] },
    retail: { capture: [0.10, 0.25], dwell: [30, 60] },
  },
  // inverse benchmark-drempel voor de VOLLEDIGE hub (§12.3), enkel als kruiscontrole
  HUB: { restaurant: { eenheid: 'zitplaatsen', per: 90, drempel: 330 }, sportclub: { eenheid: 'leden', per: 38, drempel: 800 }, bedrijfsparking: { eenheid: 'BEV-wagens', per: 2000, drempel: 15 } },
};
function _destVerblijfsTechniek(dwellMin) {
  if (dwellMin >= 180) return { techniek: 'AC 11 kW', dc: false };
  if (dwellMin >= 60) return { techniek: 'AC 22 kW', dc: false };
  if (dwellMin >= 20) return { techniek: 'DC 60-80 kW', dc: true };
  return { techniek: 'geen destination charging', dc: false };
}
// cfg: { verticaal, sub_segment, bezoekers_per_dag, ev_aandeel?, open_dagen?, kostprijs_kwh, concurrentie_500m? }
function _destinationRaming(cfg) {
  cfg = cfg || {};
  const sub = DEST.SUB[cfg.sub_segment] || DEST.SUB.retail;
  const capLo = sub.capture[0], capHi = sub.capture[1];
  const dwell = (sub.dwell[0] + sub.dwell[1]) / 2;
  const evA = (cfg.ev_aandeel != null ? +cfg.ev_aandeel : (String(cfg.verticaal).indexOf('bedrijf') === 0 ? 0.30 : 0.10));
  const openF = (cfg.open_dagen != null ? +cfg.open_dagen : 300) / 365;
  const bezoekers = +cfg.bezoekers_per_dag || 0;
  // concurrentie <500 m drukt de capture rate ~20% relatief per punt (§5.2)
  const conc = Math.max(0, +cfg.concurrentie_500m || 0);
  const concFactor = Math.pow(0.8, conc);
  function volume(captureFrac) {
    return Math.round(bezoekers * 365 * openF * DEST.UNIQUE_FACTOR * evA * captureFrac * concFactor * DEST.AVG_SESSIE_KWH);
  }
  const midCap = (capLo + capHi) / 2;
  const jaar100 = volume(midCap);
  const sens = { 0: 0, 50: Math.round(jaar100 * 0.5), 100: jaar100, 150: Math.round(jaar100 * 1.5) };
  // drempel = functie van de kostprijs uit de sim (§13.1)
  const kost = (cfg.kostprijs_kwh != null ? +cfg.kostprijs_kwh : null);
  let drempels = null, boven_ac = null;
  if (kost != null) {
    const margeAc = HW.PRIJS_AC_KWH - kost, margeDc = HW.PRIJS_DC_KWH - kost;
    const beAc = margeAc > 0 ? Math.round(HW.AC_DUBBEL_RECURRING_J + HW.AC_DUBBEL_CAPEX / HW.AFSCHRIJF_JAAR) / margeAc : null;
    const beDc = margeDc > 0 ? Math.round(HW.DC_2X80_RECURRING_J + HW.DC_2X80_CAPEX / HW.AFSCHRIJF_JAAR) / margeDc : null;
    drempels = { ac_kwh: beAc ? Math.round(beAc) : null, dc_kwh: beDc ? Math.round(beDc) : null };
    boven_ac = (beAc != null) ? (jaar100 >= beAc) : null;
  }
  const tech = _destVerblijfsTechniek(dwell);
  // opbrengst enkel tonen als boven de AC-drempel (§13.3 waarborg 1)
  let opbrengst = null;
  if (boven_ac && kost != null) {
    const prijs = tech.dc ? HW.PRIJS_DC_KWH : HW.PRIJS_AC_KWH;
    opbrengst = { eur_jaar_100: Math.round(jaar100 * (prijs - kost)), prijs_eur_kwh: prijs };
  }
  const config = { ac_dubbel: tech.dc ? 0 : 1, dc_2x80: tech.dc ? 1 : 0, techniek: tech.techniek };
  const reden = tech.techniek === 'geen destination charging'
    ? 'te korte verblijfsduur — geen destination charging'
    : (boven_ac === false ? 'onder de rendabele drempel bij uw huidige kostprijs — slimmere sturing verlaagt die drempel' : null);
  return {
    spoor: 2, heuristieken_gedraaid: true, verticaal: cfg.verticaal || null, sub_segment: cfg.sub_segment || null,
    capture_rate: [capLo, capHi], dwell_min: sub.dwell, jaarvolume_kwh_raming: jaar100,
    sessies_per_dag: Math.round(jaar100 / 365 / (tech.dc ? DEST.SESSIE_KWH_DC : DEST.SESSIE_KWH_AC) * 10) / 10,
    sensitiviteit_kwh: sens, drempels, boven_drempel: boven_ac, config_advies: config, opbrengst, reden,
    prijsbasis: { ac_eur_kwh: HW.PRIJS_AC_KWH, dc_eur_kwh: HW.PRIJS_DC_KWH, kost_eur_kwh: kost }, exploitatie_route: 'eplusdrive',
  };
}
app.post('/api/destination-raming', (req, res) => {
  try { res.json({ ok: true, destination: _destinationRaming(req.body || {}), _meta: { server_version: SERVER_VERSIE } }); }
  catch (e) { console.error('[destination-raming]', e.message); res.status(500).json({ ok: false, error: e.message }); }
});

// ─── START ────────────────────────────────────────────────────────────────────
laadMarktdata();  // laad marktdata synchroon bij startup
_leadsHydrate();  // v15.119: leads uit de Supabase-bucket in het geheugen laden (niet-blokkerend)

app.listen(PORT, () => {
  console.log(`Fluctus proxy v${SERVER_VERSIE} luistert op poort ${PORT}`);
  console.log(`simulator.py: ${fs.existsSync(path.join(__dirname,'simulator.py')) ? 'aanwezig':'ONTBREEKT'}`);
  console.log(`Markt status: ${MARKT_STATUS}${MARKT ? ' ('+MARKT.n_kwartieren+' kwartieren)' : ''}`);
});
