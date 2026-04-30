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

let BATTERIJEN = [
  { id:'bess-50',  naam:'BESS 50 kWh / 25 kW',  kwh:50,  kw:25, eta:0.85, dod:0.90, capex:20000, max_cycli:8000 },
  { id:'bess-100', naam:'BESS 100 kWh / 49 kW', kwh:100, kw:49, eta:0.85, dod:0.90, capex:35000, max_cycli:8000 },
  { id:'bess-200', naam:'BESS 200 kWh / 79 kW', kwh:200, kw:79, eta:0.85, dod:0.90, capex:62000, max_cycli:8000 },
];

const PROFIELEN = [
  { naam:'slager',     beschrijving:'Slager / voedingszaak — dagprofiel met ochtend- en middagpiek' },
  { naam:'bakker',     beschrijving:'Bakkerij — vroege ochtendpiek (3-7u)' },
  { naam:'kantoor',    beschrijving:'Kantoor — weekdag 8-18u, weekend laag' },
  { naam:'supermarkt', beschrijving:'Supermarkt — dag 7-22u, 7 dagen/week' },
  { naam:'industrie',  beschrijving:'Industrie — 2-ploegensysteem' },
  { naam:'horeca',     beschrijving:'Horeca — middaglunch + avondspits' },
];

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
  res.json({ postcode:pc, grd:hit.grd, dnb_volledig:hit.dnb, gemeenten:[] });
});

app.get('/api/gemeenten-lijst', (req, res) => {
  const gemeenten = Object.entries(POSTCODE_GRD).slice(0,500).map(([pc,d]) => ({
    postcode:pc, gemeente:`${d.grd} ${pc}`, dnb:d.dnb
  }));
  res.json({ gemeenten });
});

app.get('/api/regio-tarieven', (req, res) => {
  const t = TARIEVEN_LS;
  res.json({ grd:req.query.grd, spanning:req.query.spanning, tarieven:{
    distributie: t.proportioneel_eur_mwh,
    capaciteit:  t.maandpiek_eur_kw_jaar,
    transmissie: t.transport_systeembeheer_eur_mwh + t.transport_reserves_eur_mwh + t.transport_marktintegratie_eur_mwh,
    federale_heffing: t.accijnzen_staffel[0][1],
  }, raw:t });
});

app.get('/api/leveringscontract-staffel', (req, res) => {
  res.json({ leverancier:'Enwyse', schijven:CONTRACT_STAFFEL, staffel:CONTRACT_STAFFEL,
             vergroening_eur_per_mwh:2.50, vast_eur_per_maand:10.00,
             gsc_eur_mwh:11.0, wkk_eur_mwh:4.20 });
});

app.get('/api/profielen-lijst', (req, res) => res.json({ profielen:PROFIELEN }));

app.get('/api/profiel', (req, res) => {
  const naam = req.query.naam || 'slager';
  // Geef het profiel terug vanuit MARKT (al geladen bij startup)
  if (MARKT && MARKT.profiel && MARKT.profiel.length === 35040) {
    return res.json({ naam, kwartier: MARKT.profiel });
  }
  // Fallback: lees slager.json direct
  for (const p of ['slager.json', 'data/slager.json']) {
    const fp = path.join(__dirname, p);
    if (fs.existsSync(fp)) {
      const data = JSON.parse(fs.readFileSync(fp, 'utf8'));
      return res.json({ naam, kwartier: Array.isArray(data) ? data : data.profiel_kwartier || [] });
    }
  }
  res.status(404).json({ error:`Profiel '${naam}' niet gevonden` });
});

app.get('/api/batterijen', (req, res) => res.json({ batterijen:BATTERIJEN }));

app.post('/api/batterij-toevoegen', (req, res) => {
  const { naam, kwh, kw, eta, capex } = req.body || {};
  if (!naam||!kwh||!kw) return res.status(400).json({ error:'naam, kwh en kw zijn verplicht' });
  const id = naam.toLowerCase().replace(/\s+/g,'-');
  BATTERIJEN.push({ id, naam, kwh:Number(kwh), kw:Number(kw), eta:Number(eta)||0.85, capex:Number(capex)||0, max_cycli:8000 });
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
  const simInput  = buildSimInput(input);
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

  // PV solar vorm — gebruik pre-built solar_norm als UI geen vorm stuurt
  const pvVorm = (pvKwp > 0 && MARKT && MARKT.solar_norm) ? MARKT.solar_norm : [];

  const simPeriode = ui.simulatieperiode || { van:'2025-04-01', tot:'2026-04-01' };

  return {
    profiel_kwartier: (MARKT && MARKT.profiel) || [],
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
      modus: (ui.contract && ui.contract.modus) || 'passthrough',
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
