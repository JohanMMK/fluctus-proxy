// voertuigen.js — v15.420.0 (Johan 14-11): VOERTUIGTABEL voor de intake (merk → model per voertuigtype).
// Bronnen: OpenEV Data (personenwagens, enkele bestelwagens; CDLA-Permissive-2.0) + onze aanvullende tabel
// (data/voertuigen_aanvullend.json: bestel- en vrachtwagens, bron per rij). Bij een conflict wint onze tabel.
// Maandelijkse update op de 1ste–5de werkdag (ma–vr, zonder Belgische feestdagen): downloaden → valideren →
// samenvoegen → opslaan als nieuwe versie → activeren via een atomische pointer-switch. Mislukt een stap, dan blijft
// de vorige versie actief en gaat er een mail naar deps.alertTo (ENV VOERTUIG_ALERT_TO). Er is ALTIJD een werkende
// versie: zonder opgeslagen versie draait de tabel op de aanvullende tabel ('basis').
// Opslag via deps.store (Supabase-bucket in productie, geheugen in de test):
//   voertuigen/v<versie>.json · voertuigen/actief.json (pointer) · voertuigen/historiek.json · voertuigen/status.json
'use strict';
const fs = require('fs');

const TYPES = ['personenwagen', 'bestelwagen', 'lichte_vracht', 'vrachtwagen', 'bus'];
const OPENEV_API = 'https://api.github.com/repos/open-ev-data/open-ev-data-dataset/releases/latest';
const OPENEV_FALLBACK = 'https://github.com/open-ev-data/open-ev-data-dataset/releases/latest/download/open-ev-data.json';
const PLAUS = { batt: [5, 1200], kwhkm: [0.08, 2.5], range: [30, 1500], ac: [1, 50], dc: [0, 1500] };

const n = (x) => { const v = Number(x); return (x != null && x !== '' && Number.isFinite(v)) ? v : null; };
const txt = (x) => (x == null ? '' : (typeof x === 'object' ? String(x.name || x.slug || '') : String(x))).trim();
const slug = (s) => String(s || '').toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, '').replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
const sleutel = (r) => [r.type, slug(r.merk), slug(r.model), slug(r.variant)].join('|');
const r3 = (x) => Math.round(x * 1000) / 1000;

function typeVanOpenEV(t) {
  t = String(t || '').toLowerCase();
  if (t === 'passenger_car' || t === 'suv' || t === 'pickup') return 'personenwagen';
  if (t === 'van' || t === 'commercial') return 'bestelwagen';
  if (t === 'truck') return 'vrachtwagen';
  if (t === 'bus') return 'bus';
  return null;   // motorcycle, scooter, other → niet relevant voor de intake
}

// Eén OpenEV-record → ons formaat (of null als het type niet relevant is).
function normOpenEV(v) {
  const type = typeVanOpenEV(v.vehicle_type);
  if (!type) return null;
  const b = v.battery || {};
  const batt = n(b.pack_capacity_kwh_net) || n(b.pack_capacity_kwh_gross) || n(b.capacity_kwh);
  const battSoort = n(b.pack_capacity_kwh_net) ? 'netto' : 'nominaal';
  const rated = Array.isArray(v.range && v.range.rated) ? v.range.rated : [];
  const wltp = rated.find(x => String(x.cycle).toLowerCase() === 'wltp') || rated[0] || null;
  const rangeKm = n(wltp && wltp.range_km) || n(v.range && v.range.wltp && v.range.wltp.km);
  const real = Array.isArray(v.range && v.range.real_world) ? v.range.real_world.find(x => x.profile === 'mixed') : null;
  let kwhkm = null, bronKm = null;
  const whkm = n(v.efficiency && v.efficiency.energy_consumption_wh_per_km);
  if (whkm) { kwhkm = whkm / 1000; bronKm = 'constructeur'; }
  else if (batt && real && n(real.range_km)) { kwhkm = batt / n(real.range_km); bronKm = 'reeel_berekend'; }
  else if (batt && rangeKm) { kwhkm = batt / rangeKm; bronKm = 'wltp_berekend'; }
  const ch = v.charging || {};
  const variant = [txt(v.trim), txt(v.variant), v.year ? String(v.year) : ''].filter(Boolean).join(' ');
  const src = Array.isArray(v.sources) && v.sources[0] ? (v.sources[0].url || '') : '';
  return {
    type, merk: txt(v.make) || txt(v.brand), model: txt(v.model), variant,
    batt_kwh: batt, batt_soort: battSoort, range_km: rangeKm, kwh_km: kwhkm != null ? r3(kwhkm) : null, kwh_km_bron: bronKm,
    ac_kw: n(ch.ac && ch.ac.max_power_kw), dc_kw: n(ch.dc && ch.dc.max_power_kw),
    bron: 'OpenEV Data', bron_url: src || 'https://github.com/open-ev-data/open-ev-data-dataset', controle: 'bron',
  };
}

// Plausibiliteit + kernvelden. Geeft null (ok) of een reden.
function plausibel(r) {
  if (!r || !TYPES.includes(r.type)) return 'type';
  if (!r.merk || !r.model) return 'merk/model leeg';
  const inR = (x, [a, b]) => x != null && x >= a && x <= b;
  if (!inR(r.batt_kwh, PLAUS.batt)) return 'batterij';
  if (!inR(r.kwh_km, PLAUS.kwhkm)) return 'kWh/km';
  if (r.range_km != null && !inR(r.range_km, PLAUS.range)) return 'actieradius';
  if (r.ac_kw != null && !inR(r.ac_kw, PLAUS.ac)) return 'AC-vermogen';
  if (r.dc_kw != null && !inR(r.dc_kw, PLAUS.dc)) return 'DC-vermogen';
  return null;
}

// Valideer een volledige OpenEV-release. Gooit een Error met een duidelijke boodschap als ze wordt afgekeurd.
function valideerRelease(json, opt) {
  const min = (opt && opt.min) || 150;
  const lijst = Array.isArray(json) ? json : (json && Array.isArray(json.vehicles) ? json.vehicles : null);
  if (!lijst) throw new Error('schema: geen lijst «vehicles» in de release');
  if (lijst.length < min) throw new Error(`te weinig records in de release: ${lijst.length} < ${min}`);
  const ok = [], verworpen = {};
  let relevant = 0;
  for (const v of lijst) {
    if (!v || typeof v !== 'object') { verworpen.schema = (verworpen.schema || 0) + 1; continue; }
    const r = normOpenEV(v);
    if (!r) continue;   // niet-relevant type (motor, scooter …)
    relevant++;
    const reden = plausibel(r);
    if (reden) { verworpen[reden] = (verworpen[reden] || 0) + 1; continue; }
    ok.push(r);
  }
  const nv = relevant - ok.length;
  if (relevant === 0) throw new Error('schema: geen enkel record met een bruikbaar voertuigtype');
  if (ok.length < min * 0.6) throw new Error(`te weinig geldige records na validatie: ${ok.length} (verworpen: ${JSON.stringify(verworpen)})`);
  if (nv / relevant > 0.35) throw new Error(`te veel records met lege of onplausibele kernvelden: ${nv} van ${relevant} (${JSON.stringify(verworpen)})`);
  return { records: ok, verworpen, totaal: lijst.length, relevant };
}

function laadAanvullend(pad) {
  const j = JSON.parse(fs.readFileSync(pad, 'utf8'));
  const lijst = (j.voertuigen || []).map(v => Object.assign({}, v, { kwh_km: v.kwh_km != null ? r3(+v.kwh_km) : null }));
  const fouten = lijst.map(r => ({ r, reden: plausibel(r) })).filter(x => x.reden);
  if (fouten.length) throw new Error('aanvullende tabel ongeldig: ' + fouten.map(f => `${f.r.merk} ${f.r.model}: ${f.reden}`).join('; '));
  return { bijgewerkt: j.bijgewerkt || null, records: lijst };
}

// Samenvoegen: onze tabel wint. Staat een model (type + merk + model) in onze tabel, dan vervangen onze varianten
// ALLE OpenEV-varianten van dat model (OpenEV zet o.a. het modeljaar in de variant, dus exacte variant-sleutels
// botsen zelden). Andere OpenEV-modellen blijven.
function merge(openev, eigen) {
  const m = new Map();
  const modelKey = (r) => [r.type, slug(r.merk), slug(r.model)].join('|');
  const eigenModellen = new Set(eigen.map(modelKey));
  for (const r of openev) { if (eigenModellen.has(modelKey(r))) continue; m.set(sleutel(r), Object.assign({}, r, { herkomst: 'openev' })); }
  for (const r of eigen) m.set(sleutel(r), Object.assign({}, r, { herkomst: 'eigen' }));
  return [...m.values()].map(r => Object.assign(r, { id: slug(sleutel(r)).slice(0, 120) }))
    .sort((a, b) => a.type.localeCompare(b.type) || a.merk.localeCompare(b.merk) || a.model.localeCompare(b.model) || String(a.variant).localeCompare(String(b.variant)));
}

// ── Kalender: werkdag-nummer in de maand (Europe/Brussels), 0 als de dag geen werkdag is ──
function brussel(ts) {
  const iso = new Date(ts).toLocaleString('sv-SE', { timeZone: 'Europe/Brussels' });   // 'YYYY-MM-DD HH:MM:SS'
  return { datum: iso.slice(0, 10), uur: +iso.slice(11, 13), maand: iso.slice(0, 7) };
}
function werkdagNr(datum, feestdagen) {
  const [j, m, d] = datum.split('-').map(Number);
  const isWd = (dd) => { const dt = new Date(Date.UTC(j, m - 1, dd)); const wd = dt.getUTCDay(); return wd !== 0 && wd !== 6 && !feestdagen(j).has(dt.toISOString().slice(0, 10)); };
  if (!isWd(d)) return 0;
  let nr = 0; for (let k = 1; k <= d; k++) if (isWd(k)) nr++;
  return nr;
}

function createVoertuigen(deps) {
  const store = deps.store, log = deps.log || console;
  const now = deps.now || (() => Date.now());
  const MIN = deps.minRecords || 150;
  const KEEP = Math.max(3, deps.keep || 6);
  let actief = null;          // { versie, datum, records, openev_tag }
  let bezig = false;

  const basis = () => { const a = laadAanvullend(deps.aanvPad); return { versie: 'basis-' + (a.bijgewerkt || 'onbekend'), datum: a.bijgewerkt, records: merge([], a.records), openev_tag: null }; };
  const leesJson = async (pad) => { const s = await store.get(pad); return s == null ? null : JSON.parse(s); };

  async function init() {
    try {
      const p = await leesJson('voertuigen/actief.json');
      if (p && p.versie) {
        const v = await leesJson(`voertuigen/v${p.versie}.json`);
        if (v && Array.isArray(v.records) && v.records.length) { actief = Object.assign({}, v, { versie: p.versie }); log.log(`[voertuigen] actieve versie ${p.versie} (${v.records.length} modellen)`); return actief; }
      }
    } catch (e) { log.warn('[voertuigen] actieve versie niet leesbaar, basis gebruikt:', e.message); }
    actief = basis(); log.log(`[voertuigen] basisversie ${actief.versie} (${actief.records.length} modellen)`);
    return actief;
  }
  const huidig = () => actief || basis();

  async function status() { try { return (await leesJson('voertuigen/status.json')) || {}; } catch (e) { return {}; } }
  async function bewaarStatus(s) { try { await store.put('voertuigen/status.json', JSON.stringify(s)); } catch (e) { log.warn('[voertuigen] status niet bewaard:', e.message); } }

  async function meld(stap, fout, extra) {
    const v = huidig().versie;
    const b = brussel(now());
    const eind = !!(extra && extra.eind);
    const onderwerp = eind ? 'Voertuigtabel: maandelijkse update niet gelukt – vorige versie blijft actief' : `Voertuigtabel: update mislukt (${stap})`;
    const tekst = (eind ? 'Maandelijkse update niet gelukt – vorige versie blijft actief.\n\n' : '')
      + `Datum: ${b.datum} ${String(b.uur).padStart(2, '0')} u (Brussel)\nStap: ${stap}\nFout: ${fout}\nActieve versie blijft: ${v}\n\n`
      + 'Manueel opnieuw proberen: POST /api/vehicles/update (manager). Status: GET /api/vehicles/status.';
    if (!deps.alertTo) { log.warn('[voertuigen] geen VOERTUIG_ALERT_TO ingesteld — melding enkel in de log:', onderwerp, fout); return; }
    try { await deps.mail(deps.alertTo, onderwerp, tekst); } catch (e) { log.warn('[voertuigen] alertmail faalde:', e.message); }
  }

  async function download() {
    let url = OPENEV_FALLBACK, tag = null;
    try {
      const r = await deps.fetch(OPENEV_API, { headers: { 'User-Agent': 'fluctus-proxy', Accept: 'application/vnd.github+json' } });
      if (r.ok) {
        const j = await r.json(); tag = j.tag_name || null;
        const a = (j.assets || []).find(x => /\.json$/i.test(x.name || ''));
        if (a && a.browser_download_url) url = a.browser_download_url;
      }
    } catch (e) { /* val terug op de vaste URL */ }
    const r = await deps.fetch(url, { headers: { 'User-Agent': 'fluctus-proxy' } });
    if (!r.ok) throw new Error(`HTTP ${r.status} op ${url}`);
    const s = await r.text();
    try { return { json: JSON.parse(s), tag, url }; } catch (e) { throw new Error('geen geldige JSON: ' + e.message); }
  }

  // Volledige update. Geeft { ok, versie } of { ok:false, stap, fout, actief }.
  async function update(opt) {
    if (bezig) return { ok: false, stap: 'start', fout: 'er loopt al een update', actief: huidig().versie };
    bezig = true;
    const b = brussel(now());
    const st = await status();
    if (st.maand !== b.maand) { st.maand = b.maand; st.geslaagd = false; st.pogingen = []; st.eindmelding = false; }
    const poging = { datum: b.datum, uur: b.uur, manueel: !!(opt && opt.manueel) };
    let stap = 'download';
    try {
      const d = await download();
      stap = 'validatie';
      const val = valideerRelease(d.json, { min: MIN });
      stap = 'merge';
      const eigen = laadAanvullend(deps.aanvPad).records;
      const records = merge(val.records, eigen);
      if (records.length < MIN * 0.6) throw new Error(`te weinig modellen na samenvoegen: ${records.length}`);
      stap = 'activatie';
      const hist = (await leesJson('voertuigen/historiek.json')) || [];
      const pre = b.maand.replace('-', '.');
      const nr = hist.filter(h => String(h.versie).startsWith(pre)).length + 1;
      const versie = `${pre}-${nr}`;
      const doc = { versie, datum: b.datum, openev_tag: d.tag, openev_url: d.url, aantal: records.length, verworpen: val.verworpen, records };
      await store.put(`voertuigen/v${versie}.json`, JSON.stringify(doc));
      const terug = await leesJson(`voertuigen/v${versie}.json`);          // read-back: pas activeren als de versie leesbaar is
      if (!terug || !Array.isArray(terug.records) || terug.records.length !== records.length) throw new Error('nieuwe versie niet correct teruggelezen');
      await store.put('voertuigen/actief.json', JSON.stringify({ versie, datum: b.datum, aantal: records.length }));   // atomische switch
      actief = doc;
      hist.push({ versie, datum: b.datum, aantal: records.length, openev_tag: d.tag });
      try { await store.put('voertuigen/historiek.json', JSON.stringify(hist.slice(-KEEP))); } catch (e) { log.warn('[voertuigen] historiek niet bijgewerkt:', e.message); }
      poging.ok = true; poging.versie = versie;
      st.geslaagd = true; st.pogingen = (st.pogingen || []).concat(poging); await bewaarStatus(st);
      log.log(`[voertuigen] versie ${versie} actief (${records.length} modellen, OpenEV ${d.tag || '?'})`);
      return { ok: true, versie, aantal: records.length };
    } catch (e) {
      poging.ok = false; poging.stap = stap; poging.fout = String((e && e.message) || e);
      st.pogingen = (st.pogingen || []).concat(poging); await bewaarStatus(st);
      log.warn(`[voertuigen] update mislukt bij ${stap}: ${poging.fout} — actief blijft ${huidig().versie}`);
      await meld(stap, poging.fout);
      return { ok: false, stap, fout: poging.fout, actief: huidig().versie };
    } finally { bezig = false; }
  }

  // Elk uur: op de 1ste–5de werkdag van de maand, vanaf 6 u, één automatische poging per werkdag tot er één
  // geslaagd is. Mislukt de poging op de 5de werkdag (of is er na de 5de werkdag nog geen geslaagde), dan volgt
  // één eindmelding.
  async function tick() {
    const b = brussel(now());
    const st = await status();
    const dezeMaand = st.maand === b.maand;
    if (dezeMaand && st.geslaagd) return { actie: 'niets', reden: 'al geslaagd deze maand' };
    const wd = werkdagNr(b.datum, deps.feestdagen);
    const pogingVandaag = dezeMaand && (st.pogingen || []).some(p => p.datum === b.datum && !p.manueel);
    if (wd >= 1 && wd <= 5 && b.uur >= 6 && !pogingVandaag) {
      const r = await update({ manueel: false });
      if (!r.ok && wd === 5) await eindmelding();
      return { actie: 'update', wd, r };
    }
    if (wd > 5 || (wd === 0 && dezeMaand && (st.pogingen || []).length >= 5)) {
      const st2 = await status();
      if (st2.maand === b.maand && !st2.geslaagd && !st2.eindmelding) { await eindmelding(); return { actie: 'eindmelding', wd }; }
      if (st2.maand !== b.maand && wd > 5) {   // server lag stil op de 1ste–5de werkdag: geen enkele poging deze maand
        const st3 = { maand: b.maand, geslaagd: false, pogingen: [], eindmelding: false }; await bewaarStatus(st3);
        await eindmelding(); return { actie: 'eindmelding', wd };
      }
    }
    return { actie: 'niets', wd };
  }
  async function eindmelding() {
    const st = await status(); if (st.eindmelding) return;
    st.eindmelding = true; await bewaarStatus(st);
    await meld('maandelijkse update', 'geen geslaagde update op de 1ste tot 5de werkdag', { eind: true });
  }

  async function rollback(versie) {
    const v = await leesJson(`voertuigen/v${versie}.json`);
    if (!v || !Array.isArray(v.records) || !v.records.length) throw new Error('versie niet gevonden of leeg: ' + versie);
    await store.put('voertuigen/actief.json', JSON.stringify({ versie, datum: brussel(now()).datum, aantal: v.records.length, rollback: true }));
    actief = Object.assign({}, v, { versie });
    return { ok: true, versie };
  }

  // Lijst voor de intake: merken met hun modellen, gefilterd op type.
  function lijst(type) {
    const a = huidig();
    const recs = a.records.filter(r => !type || r.type === type);
    const per = new Map();
    for (const r of recs) {
      if (!per.has(r.merk)) per.set(r.merk, []);
      per.get(r.merk).push({ id: r.id, model: r.model, variant: r.variant || '', label: [r.model, r.variant].filter(Boolean).join(' · '),
        batt_kwh: r.batt_kwh, batt_soort: r.batt_soort || null, range_km: r.range_km, kwh_km: r.kwh_km, kwh_km_bron: r.kwh_km_bron || null,
        ac_kw: r.ac_kw, dc_kw: r.dc_kw, bron: r.bron, bron_url: r.bron_url || null, controle: r.controle || null, opmerking: r.opmerking || null });
    }
    const merken = [...per.entries()].sort((x, y) => x[0].localeCompare(y[0])).map(([merk, modellen]) => ({ merk, modellen }));
    return { versie: a.versie, datum: a.datum || null, type: type || null, aantal: recs.length, merken };
  }

  return { init, update, tick, rollback, lijst, status,
    huidig: () => { const h = huidig(); return { versie: h.versie, datum: h.datum || null, aantal: h.records.length, openev_tag: h.openev_tag || null }; } };
}

module.exports = { createVoertuigen, valideerRelease, normOpenEV, merge, plausibel, werkdagNr, laadAanvullend, TYPES };
