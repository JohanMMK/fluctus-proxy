'use strict';

const express     = require('express');
const compression = require('compression');
const { spawn, execFileSync } = require('child_process');
const path        = require('path');
const fs          = require('fs');

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

function laadMarktdata() {
  const prebuildScript = path.join(__dirname, 'prebuild_data.py');
  if (!fs.existsSync(prebuildScript)) {
    console.warn('[markt] prebuild_data.py niet gevonden — simulator zal lege marktdata gebruiken');
    return;
  }
  try {
    console.log('[markt] Marktdata pre-bouwen...');
    execFileSync('python3', [prebuildScript], { timeout: 60000 });
    const marktPath = '/tmp/fluctus_markt.json';
    if (fs.existsSync(marktPath)) {
      MARKT = JSON.parse(fs.readFileSync(marktPath, 'utf8'));
      console.log(`[markt] OK — ${MARKT.n_kwartieren} kwartieren, periode ${MARKT.van} → ${MARKT.tot}`);
    }
  } catch (e) {
    console.error('[markt] Pre-build gefaald:', e.message);
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

const SCENARIOS_DB = {};
const PROJECTEN_DB = new Set();

// ─── ROUTES ───────────────────────────────────────────────────────────────────
app.get('/',       (req, res) => res.json({ status:'ok', version:'15.6', ts:new Date().toISOString(), markt_geladen: !!MARKT }));
app.get('/health', (req, res) => res.json({ status:'ok' }));

app.get('/api/postcode-grd', (req, res) => {
  const pc = String(req.query.postcode||'').trim();
  if (!/^\d{4}$/.test(pc)) return res.status(400).json({ error:'postcode moet 4 cijfers zijn' });
  const hit = POSTCODE_GRD[pc] || POSTCODE_GRD[String(Math.floor(parseInt(pc)/10)*10)];
  if (!hit) return res.status(404).json({ error:`Postcode ${pc} niet gevonden` });
  const gemeenten = (PC_GEMEENTE_INDEX[pc] || []);
  res.json({ postcode:pc, grd:hit.grd, dnb_volledig:hit.dnb, gemeenten });
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
app.get('/api/scenarios', (req, res) => res.json({ scenarios:Object.keys((SCENARIOS_DB[req.query.project])||{}) }));
app.get('/api/scenario', (req, res) => {
  const d = (SCENARIOS_DB[req.query.project]||{})[req.query.scenario];
  if (!d) return res.status(404).json({ error:'Scenario niet gevonden' });
  res.json({ data:d });
});
app.post('/api/scenario-bewaren', (req, res) => {
  const { project, scenario, data } = req.body||{};
  if (!project||!scenario) return res.status(400).json({ error:'project en scenario zijn verplicht' });
  if (!SCENARIOS_DB[project]) SCENARIOS_DB[project] = {};
  SCENARIOS_DB[project][scenario] = data;
  PROJECTEN_DB.add(project);
  res.json({ ok:true });
});

// ─── SIMULATIE ────────────────────────────────────────────────────────────────
app.post('/api/nominatie-sim', (req, res) => {
  const input = req.body;
  if (!input || typeof input !== 'object')
    return res.status(400).json({ error:'body is verplicht' });
  if (!MARKT)
    return res.status(503).json({ error:'Marktdata nog niet geladen — probeer over 30 seconden opnieuw' });

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
    result._meta = { elapsed_ms:elapsed, server_version:'15.6' };
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
  let batt = { kw:0, kwh:0, dod_pct:90, rte_pct:85, capex_eur:0, max_cycli:8000 };
  if (ui.batterijId) {
    const b = BATTERIJEN.find(x => x.id===ui.batterijId || x.naam===ui.batterijId);
    if (b) batt = { kw:b.kw, kwh:b.kwh, dod_pct:Math.round((b.dod||0.90)*100),
                    rte_pct:Math.round((b.eta||0.85)*100), capex_eur:b.capex||0, max_cycli:b.max_cycli||8000 };
  }

  const pvKwp    = ui.pv_kwp || ui.pvKwp || 0;
  const aanslKw  = ui.aansluiting_kva || ui.aansluitingKva || 80;
  const stacked  = batt.kwh > 0;
  const bspActief    = !!(ui.bsp && ui.bsp.actief);
  const curtailActief = !!(ui.pv_curtailment && ui.pv_curtailment.actief);

  // Gebruik de dynamisch bepaalde marktperiode als rolling12 gevraagd wordt
  let simPeriode = ui.simulatieperiode || {};
  if (!simPeriode.van || ui.jaar === 'rolling12') {
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

  return {
    profiel_kwartier: (() => {
      // Laad het gevraagde profiel uit data/profielen/
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
    })(),
    jaarverbruik_mwh: jaarverbruik,
    aanvullingen: {},
    pv: {
      kwp: pvKwp,
      specifiek_rendement_kwh_per_kwp: 900,
      vorm_kwartier: pvVorm,
      capex_eur: pvKwp > 0 ? (pvKwp <= 125 ? 71875 : pvKwp <= 150 ? 86250 : 115000) : 0,
    },
    pv_curtailment: {
      actief: curtailActief,
      trigger_eur_mwh: (ui.pv_curtailment && ui.pv_curtailment.trigger_eur_mwh) || 0,
      strategie: 'cap_op_verbruik',
    },
    batterij: batt,
    aansluiting: {
      max_afname_kw_zacht:  aanslKw, max_afname_kw_hard:  aanslKw,
      max_injectie_kw_zacht: aanslKw, max_injectie_kw_hard: aanslKw,
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
      spot_kwartier: (MARKT && MARKT.spot_q) || [],
      imb_kwartier:  (MARKT && MARKT.imb_q)  || [],
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

// ─── START ────────────────────────────────────────────────────────────────────
laadMarktdata();  // laad marktdata synchroon bij startup

app.listen(PORT, () => {
  console.log(`Fluctus proxy v15.6 luistert op poort ${PORT}`);
  console.log(`simulator.py: ${fs.existsSync(path.join(__dirname,'simulator.py')) ? 'aanwezig':'ONTBREEKT'}`);
  console.log(`Markt geladen: ${MARKT ? 'ja ('+MARKT.n_kwartieren+' kwartieren)' : 'nee'}`);
});
