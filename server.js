'use strict';
// ============================================================================
// FLUCTUS PROXY SERVER
// Versie:        v15.14.1 (hotfix — robuuste marktdata-loading + retry-ladder)
// Geproduceerd:  2026-05-14
// Doelomgeving:  Railway (lucid-amazement-production.up.railway.app)
// Repo:          JohanMMK/fluctus-proxy (auto-deploy bij merge naar main)
// Vereist:       Simulator.txt v1.19+ / simulator.py v1.7+
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
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
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
addRange([[9000,9700]],                         'Fluvius Enet',     'Fluvius Enet');
addRange([[2000,3000]],                         'Fluvius Antwerpen','Fluvius Antwerpen');
addRange([[1500,2000],[3000,3500]],             'Fluvius Brabant',  'Fluvius Brabant');
addRange([[3500,3900],[3900,4000]],             'Fluvius Limburg',  'Fluvius Limburg');
addRange([[1000,1300]],                         'Sibelga',          'Sibelga');
addRange([[1300,1500]],                         'IECBW',            'IECBW');
addRange([[4000,5000]],                         'RESA',             'RESA');
addRange([[5000,6000],[6000,7000],[7000,8000]], 'ORES',             'ORES');
for (const pc of [2800,2801,2811,2812,2820,2830])
  POSTCODE_GRD[String(pc)] = { grd: 'Fluvius Mechelen', dnb: 'Fluvius Mechelen' };

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

async function _scenariosGithubWrite(filepath, data, sha) {
  const url = `https://api.github.com/repos/${SCENARIOS_REPO_OWNER}/${SCENARIOS_REPO_NAME}/contents/${filepath}`;
  const content = Buffer.from(JSON.stringify(data, null, 2)).toString('base64');
  const headers = { 'User-Agent': 'fluctus-proxy', 'Content-Type': 'application/json' };
  if (GITHUB_TOKEN) headers['Authorization'] = `token ${GITHUB_TOKEN}`;
  const body = {
    message: `auto: scenario ${filepath.replace(SCENARIOS_PATH_PREFIX + '/', '').replace('.json', '')} (${new Date().toISOString().slice(0,10)})`,
    content,
  };
  if (sha) body.sha = sha;
  const r = await fetch(url, { method: 'PUT', headers, body: JSON.stringify(body) });
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

// ─── ROUTES ───────────────────────────────────────────────────────────────────
app.get('/',       (req, res) => res.json({
  status:'ok', version:'15.14.1', ts:new Date().toISOString(), markt_geladen: !!MARKT,
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

app.get('/api/regio-tarieven', (req, res) => {
  const grdKey = req.query.grd || 'Fluvius West';
  const t = TARIEVEN_MAP[grdKey] || TARIEVEN_LS;
  res.json({ grd:req.query.grd, spanning:req.query.spanning, tarieven:{
    distributie: t.proportioneel_eur_mwh,
    capaciteit:  t.maandpiek_eur_kw_jaar,
    transmissie: t.transport_systeembeheer_eur_mwh + t.transport_reserves_eur_mwh + t.transport_marktintegratie_eur_mwh,
    federale_heffing: t.accijnzen_staffel[0][1],
  }, raw:t });
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
      const match = files.find(f => f.toLowerCase() === naam.toLowerCase() + '.json');
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

app.get('/api/projecten', (req, res) => res.json({ projecten:[...PROJECTEN_DB] }));

// v15.11 sessie 4 sub-track 4: GET /api/scenarios — read-through cache.
// Bij cache-miss (eerste call voor project, of na Railway-restart):
// listen we projecten/{project}/ in de fluctus-scenarios repo.
app.get('/api/scenarios', async (req, res) => {
  const project = req.query.project;
  if (!project) return res.status(400).json({ error: 'project query-param verplicht' });
  // Cache hit: returnt direct
  if (SCENARIOS_DB[project]) {
    return res.json({ scenarios: Object.keys(SCENARIOS_DB[project]), source: 'cache' });
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
    return res.json({ scenarios: names, source: 'github' });
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
  // Cache hit (en data niet null = niet alleen lazy-marker)
  const cached = (SCENARIOS_DB[project] || {})[scenario];
  if (cached) {
    return res.json({ data: cached, source: 'cache' });
  }
  // Cache miss of lazy-marker: lees van GitHub
  try {
    const { data } = await _scenariosGithubRead(_scenarioPad(project, scenario));
    if (!SCENARIOS_DB[project]) SCENARIOS_DB[project] = {};
    SCENARIOS_DB[project][scenario] = data;
    PROJECTEN_DB.add(project);
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
    res.json(result);
  } catch (e) {
    console.error('[factuur-extract] FOUT:', e.message);
    if (/HTTP 4|niet-ondersteund/i.test(e.message)) {
      res.status(422).json({ error: e.message });
    } else if (/timeout|abort/i.test(e.message)) {
      res.status(504).json({ error: 'Anthropic API timeout (>30s) — probeer opnieuw' });
    } else {
      res.status(500).json({ error: e.message });
    }
  }
});


// ─── FACTUUR-STAFFEL ──────────────────────────────────────────────────────────
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
    const match = files.find(f => f.toLowerCase() === profielNaam.toLowerCase() + '.json');
    if (match) {
      const data = JSON.parse(fs.readFileSync(path.join(profielDir, match), 'utf8'));
      return Array.isArray(data) ? data : (data.profiel_kwartier || null);
    }
  } catch (e) {}
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
      max_afname_kw_zacht:  zachtAfnameKw,   max_afname_kw_hard:  aanslKw,
      max_injectie_kw_zacht: maxInjectieKw,  max_injectie_kw_hard: maxInjectieKw,
      tarief_overschrijding_afname_eur_per_kw_jaar: TARIEVEN_LS.overschrijding_toegangsvermogen_eur_kw_jaar,
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
    netbeheer: { grd, spanning, tarieven: TARIEVEN_LS },
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
  console.log(`Fluctus proxy v15.14.1 luistert op poort ${PORT}`);
  console.log(`simulator.py: ${fs.existsSync(path.join(__dirname,'simulator.py')) ? 'aanwezig':'ONTBREEKT'}`);
  console.log(`Markt status: ${MARKT_STATUS}${MARKT ? ' ('+MARKT.n_kwartieren+' kwartieren)' : ''}`);
});
