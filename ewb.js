// ═══════════════════════════════════════════════════════════════════════════════════════════════
//  ewb.js v1.0.0 (16-11-2026) — ENERGIEWEERBERICHT: dagelijkse video voor bedrijven (Energie-Compas)
//
//  Elke werkdag (Europe/Brussels):
//    06:15  dagrapport: Elia (mix, grens, wind/zon-prognose), day-ahead BE, weer, gas
//           → script volgens vast format (weer · prijzen · net · actie · week · CTA), check van gisteren
//           → video 1080×1350 (stills per zin via Chromium + ffmpeg), optioneel AI-voice-over (vermeld)
//           → opslag Supabase Storage (bucket «ewb», publiek)
//    direct daarna: controlemail (vetomail) met «Tegenhouden»-knop naar EWB_REVIEW_TO (of AUDIT_MAIL_TO)
//    07:30  publicatie, tenzij tegengehouden: website-pagina (GET /api/ewb/vandaag), Brevo-campagne naar de
//           lijst «Energieweerbericht», LinkedIn (persoonlijk profiel, als gekoppeld)
//
//  ENV
//    EWB_MODE            uit | test (standaard: enkel genereren + vetomail, niets publiek) | aan
//    EWB_REVIEW_TO       ontvanger(s) vetomail, komma-gescheiden (default AUDIT_MAIL_TO)
//    EWB_SECRET          sleutel voor ondertekende links (default afgeleid van SUPABASE_SERVICE_ROLE_KEY)
//    EWB_PUBLIC_BASE     publieke pagina (default https://www.energie-compas.eu/energieweerbericht)
//    TTS_PROVIDER        openai | elevenlabs | (leeg = geen stem, enkel ondertitels)
//    OPENAI_API_KEY / OPENAI_TTS_VOICE (default «cedar»)  ·  ELEVENLABS_API_KEY / ELEVENLABS_VOICE_ID
//    OPEN_METEO_KEY      commerciële Open-Meteo-sleutel (gratis API = niet-commercieel gebruik!)
//    LINKEDIN_CLIENT_ID / LINKEDIN_CLIENT_SECRET / LINKEDIN_VERSION (default 202510)
//    BREVO_API_KEY / BREVO_SENDER (bestaand)  ·  SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY (bestaand)
//
//  Bronnen in beeld vermeld: Elia Open Data · ENTSO-E/Energy-Charts (day-ahead) · Open-Meteo · GIE AGSI+ ·
//  TTF via Yahoo Finance (onofficieel). Geen muziek.
// ═══════════════════════════════════════════════════════════════════════════════════════════════
'use strict';
const crypto = require('crypto');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { execFile } = require('child_process');

const EWB_VERSIE = '1.0.5'; // 1.0.5: gasprijs sinds 1/1/2024 (IMF/FRED maandgemiddelde + eigen TTF-log) in beeld en tekst // 1.0.4: verhaal van de dag → thematisch sfeerbeeld (Unsplash) achter het blok Net en markt // 1.0.3: tempo 1.15, check van gisteren enkel bij een foute verwachting // 1.0.2: stem per blok + warmere instructies (stem marin), eindbeeld met adres (niet uitgesproken), weerbeeld als achtergrond // 1.0.1: spreektekst «2 uur», atempo 1.15 (EWB_TEMPO), kortere zinnen — doel 60–90 s
const TZ = 'Europe/Brussels';
const W = 1080, H = 1350, FPS = 30;
const CTA_MAIL = 'dossier@energie-compas.eu';
const LOGO = 'https://app.energie-compas.eu/apps/ec-logo-dark.png';

module.exports = function mountEwb(app, opt = {}) {
  const SB = (process.env.SUPABASE_URL || '').replace(/\/$/, '');
  const SBK = process.env.SUPABASE_SERVICE_ROLE_KEY || '';
  const MODE = () => (process.env.EWB_MODE || 'test').toLowerCase();
  const SECRET = process.env.EWB_SECRET || crypto.createHash('sha256').update('ewb|' + SBK).digest('hex');
  const PORT = process.env.PORT || 3000;
  const SELF = `http://127.0.0.1:${PORT}`;
  const API_BASE = process.env.EWB_API_BASE || 'https://lucid-amazement-production.up.railway.app';
  const PUBLIC_BASE = process.env.EWB_PUBLIC_BASE || 'https://www.energie-compas.eu/energieweerbericht';
  const log = (...a) => console.log('[ewb]', ...a);

  // ── tijd ───────────────────────────────────────────────────────────────────────────────────
  function brussel(d = new Date()) {
    const p = Object.fromEntries(new Intl.DateTimeFormat('en-GB', { timeZone: TZ, year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', weekday: 'short', hour12: false })
      .formatToParts(d).map(x => [x.type, x.value]));
    return { datum: `${p.year}-${p.month}-${p.day}`, uur: Number(p.hour) % 24, min: Number(p.minute), wd: p.weekday };
  }
  const WD = ['zondag', 'maandag', 'dinsdag', 'woensdag', 'donderdag', 'vrijdag', 'zaterdag'];
  const MND = ['januari', 'februari', 'maart', 'april', 'mei', 'juni', 'juli', 'augustus', 'september', 'oktober', 'november', 'december'];
  function dagNaam(datum) { const d = new Date(datum + 'T12:00:00Z'); return WD[d.getUTCDay()]; }
  function datumLang(datum) { const d = new Date(datum + 'T12:00:00Z'); return `${WD[d.getUTCDay()]} ${d.getUTCDate()} ${MND[d.getUTCMonth()]}`; }
  function plusDag(datum, n) { const d = new Date(datum + 'T12:00:00Z'); d.setUTCDate(d.getUTCDate() + n); return d.toISOString().slice(0, 10); }
  function lokaalUur(isoUtc) { // → {datum, uur} in Brussel
    const b = brussel(new Date(isoUtc)); return { datum: b.datum, uur: b.uur };
  }
  const nl1 = x => (Math.round(x * 10) / 10).toFixed(1).replace('.', ',');
  const nl0 = x => String(Math.round(x));
  const esc = s => String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  const sign = s => crypto.createHmac('sha256', SECRET).update(String(s)).digest('hex').slice(0, 32);

  // ── Supabase Storage ───────────────────────────────────────────────────────────────────────
  const sbH = (extra = {}) => ({ apikey: SBK, Authorization: `Bearer ${SBK}`, ...extra });
  let _buckets = false;
  async function zorgBuckets() {
    if (_buckets) return;
    for (const [id, pub] of [['ewb', true], ['ewb-priv', false]]) {
      const r = await fetch(`${SB}/storage/v1/bucket`, { method: 'POST', headers: sbH({ 'content-type': 'application/json' }), body: JSON.stringify({ id, name: id, public: pub }) });
      if (!r.ok && r.status !== 409 && r.status !== 400) log('bucket', id, r.status, await r.text().catch(() => ''));
    }
    _buckets = true;
  }
  async function sbPut(bucket, p, body, type) {
    await zorgBuckets();
    const r = await fetch(`${SB}/storage/v1/object/${bucket}/${p}`, { method: 'POST', headers: sbH({ 'content-type': type, 'x-upsert': 'true', 'cache-control': 'max-age=300' }), body });
    if (!r.ok) throw new Error(`storage put ${p}: ${r.status} ${await r.text().catch(() => '')}`);
  }
  async function sbGetJson(bucket, p) {
    const r = await fetch(`${SB}/storage/v1/object/${bucket === 'ewb' ? 'public/' : ''}${bucket}/${p}`, { headers: sbH() });
    if (!r.ok) return null;
    return r.json().catch(() => null);
  }
  const publiekeUrl = p => `${SB}/storage/v1/object/public/ewb/${p}`;

  let _state = null;
  async function state() {
    if (_state) return _state;
    _state = (await sbGetJson('ewb-priv', 'state.json')) || {};
    _state.dagen = _state.dagen || {};
    return _state;
  }
  async function bewaarState() { await sbPut('ewb-priv', 'state.json', JSON.stringify(_state), 'application/json'); }

  // ── data ───────────────────────────────────────────────────────────────────────────────────
  async function jget(url, opt) { const r = await fetch(url, opt); if (!r.ok) throw new Error(`${url.split('?')[0]} HTTP ${r.status}`); return r.json(); }
  const ELIA = 'https://opendata.elia.be/api/explore/v2.1/catalog/datasets/';
  async function eliaExport(ds, where, select) {
    return jget(`${ELIA}${ds}/exports/json?where=${encodeURIComponent(where)}&select=${encodeURIComponent(select)}&timezone=UTC`);
  }

  // v1.0.5: gasprijs sinds 1/1/2024 — maandgemiddelde EU-gasprijs (IMF via FRED, gebaseerd op TTF, USD/MMBtu)
  //   omgerekend naar €/MWh met de maandkoers USD/EUR (FRED DEXUSEU). FRED loopt ± 2 maanden achter; de maanden
  //   daarna komen uit de eigen dagelijkse TTF-log (state.ttf). Cache 24 u.
  let _gasHist = null;
  async function gasHistoriek() {
    if (_gasHist && Date.now() - _gasHist.t < 864e5) return _gasHist.d;
    const csv = async id => (await (await fetch(`https://fred.stlouisfed.org/graph/fredgraph.csv?id=${id}&cosd=2024-01-01&fq=Monthly&fam=avg`)).text()).trim().split('\n').slice(1).map(x => x.split(','));
    const gas = await csv('PNGASEUUSDM'), fxr = await csv('DEXUSEU');
    const fx = Object.fromEntries(fxr.filter(r => r[1] && r[1] !== '.').map(r => [r[0].slice(0, 7), +r[1]]));
    const fxLaatst = Object.values(fx).slice(-1)[0];
    const d = gas.filter(r => r[1] && r[1] !== '.').map(r => { const m = r[0].slice(0, 7), k = fx[m] || fxLaatst; return { m, v: Math.round(+r[1] / 0.293071 / k * 10) / 10, bron: 'imf' }; });
    if (d.length) _gasHist = { t: Date.now(), d };
    return d;
  }

  async function haalData(datum) {
    const fouten = [];
    const d = { datum, gisteren: plusDag(datum, -1), morgen: plusDag(datum, 1) };
    // 1) productiemix + grens (eigen endpoint, uurdata UTC)
    try { d.mix = await jget(`${SELF}/energiemix?from=${plusDag(datum, -3)}&to=${datum}`); } catch (e) { fouten.push('mix: ' + e.message); }
    // 2) gas
    try { d.gas = await jget(`${SELF}/gas-context`); } catch (e) { fouten.push('gas: ' + e.message); }
    // 3) day-ahead BE (Energy-Charts, ENTSO-E-data, CC BY 4.0) — gisteren t.e.m. morgen, lokale uren
    try {
      const j = await jget(`https://api.energy-charts.info/price?bzn=BE&start=${plusDag(datum, -2)}T00:00%2B01:00&end=${plusDag(datum, 1)}T23:59%2B01:00`);
      const per = {};
      j.unix_seconds.forEach((u, i) => { const v = j.price[i]; if (v == null) return; const l = lokaalUur(new Date(u * 1000).toISOString()); const k = l.datum + 'T' + String(l.uur).padStart(2, '0'); (per[k] = per[k] || []).push(v); });
      d.prijs = {}; for (const k in per) d.prijs[k] = per[k].reduce((a, b) => a + b, 0) / per[k].length;
    } catch (e) { fouten.push('prijs: ' + e.message); }
    // 4) wind- en zonprognose (Elia near real-time), per lokale dag + per uur
    try {
      const van = new Date(Date.parse(datum + 'T00:00:00Z') - 2 * 3600e3).toISOString().slice(0, 16);
      const tot = plusDag(datum, 9);
      const w = await eliaExport('ods086', `datetime>="${van}" and datetime<"${tot}"`, 'datetime,mostrecentforecast,weekaheadconfidence10,weekaheadconfidence90');
      let s = await eliaExport('ods210', `datetime>="${van}" and datetime<"${tot}" and region="Belgium" and gridconnectiontype="Total"`, 'datetime,mostrecentforecast');
      if (!s.length) s = await eliaExport('ods210', `datetime>="${van}" and datetime<"${tot}" and region="Belgium"`, 'datetime,mostrecentforecast');
      const somKw = rows => { const q = {}; for (const r of rows) { const t = r.datetime.slice(0, 16); q[t] = (q[t] || 0) + (r.mostrecentforecast || 0); } return q; };
      const perUur = q => { const h = {}; for (const t in q) { const l = lokaalUur(t + ':00Z'); const k = l.datum + 'T' + String(l.uur).padStart(2, '0'); (h[k] = h[k] || []).push(q[t]); } const o = {}; for (const k in h) o[k] = h[k].reduce((a, b) => a + b, 0) / h[k].length; return o; };
      d.windU = perUur(somKw(w)); d.zonU = perUur(somKw(s));
    } catch (e) { fouten.push('prognose: ' + e.message); }
    try { d.gasHist = await gasHistoriek(); } catch (e) { fouten.push('gashistoriek: ' + e.message); }
    // 5) weer (Open-Meteo; commercieel gebruik → OPEN_METEO_KEY)
    try {
      const key = process.env.OPEN_METEO_KEY;
      const host = key ? 'https://customer-api.open-meteo.com' : 'https://api.open-meteo.com';
      const j = await jget(`${host}/v1/forecast?latitude=50.85&longitude=4.35&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,sunshine_duration,wind_speed_10m_max,weather_code&past_days=1&forecast_days=8&timezone=Europe%2FBrussels${key ? '&apikey=' + encodeURIComponent(key) : ''}`);
      d.weer = j.daily.time.map((t, i) => ({ datum: t, tmin: j.daily.temperature_2m_min[i], tmax: j.daily.temperature_2m_max[i], regen: j.daily.precipitation_sum[i], zonuren: j.daily.sunshine_duration[i] / 3600, windmax: j.daily.wind_speed_10m_max[i], code: j.daily.weather_code[i] }));
    } catch (e) { fouten.push('weer: ' + e.message); }
    d.fouten = fouten;
    return d;
  }

  // ── analyse ────────────────────────────────────────────────────────────────────────────────
  function dagPrijzen(d, datum) { const a = []; for (let h = 0; h < 24; h++) { const v = d.prijs && d.prijs[datum + 'T' + String(h).padStart(2, '0')]; a.push(v == null ? null : v); } return a; }
  function gem(a) { const v = a.filter(x => x != null); return v.length ? v.reduce((x, y) => x + y, 0) / v.length : null; }
  function blok(a, len, goedkoop, van = 0, tot = 24) {
    let best = null;
    for (let h = van; h + len <= tot; h++) { const s = a.slice(h, h + len); if (s.some(x => x == null)) continue; const g = gem(s); if (best == null || (goedkoop ? g < best.g : g > best.g)) best = { van: h, tot: h + len, g }; }
    return best;
  }
  function dagGem(map, datum) { const v = []; for (let h = 0; h < 24; h++) { const x = map && map[datum + 'T' + String(h).padStart(2, '0')]; if (x != null) v.push(x); } return v.length ? { gem: v.reduce((a, b) => a + b, 0) / v.length, som: v.reduce((a, b) => a + b, 0), n: v.length } : null; }
  const uu = h => `${h}u`;

  function analyse(d, vorige) {
    const A = { datum: d.datum };
    // prijzen
    A.pVandaag = dagPrijzen(d, d.datum); A.pGisteren = dagPrijzen(d, d.gisteren); A.pMorgen = dagPrijzen(d, d.morgen);
    A.morgenBekend = A.pMorgen.filter(x => x != null).length >= 20;
    A.gemV = gem(A.pVandaag); A.gemG = gem(A.pGisteren);
    A.goedkoop = blok(A.pVandaag, 3, true, 7, 22); A.duur = blok(A.pVandaag, 3, false, 6, 23);
    A.nacht = blok(A.pVandaag, 3, true, 0, 7);
    A.negatief = A.pVandaag.map((v, h) => v != null && v < 0 ? h : null).filter(x => x != null);
    A.minV = Math.min(...A.pVandaag.filter(x => x != null)); A.maxV = Math.max(...A.pVandaag.filter(x => x != null));
    // wind/zon
    A.windV = dagGem(d.windU, d.datum); A.windM = dagGem(d.windU, d.morgen); A.zonV = dagGem(d.zonU, d.datum); A.zonM = dagGem(d.zonU, d.morgen);
    // weer
    const wd = k => (d.weer || []).find(x => x.datum === k);
    A.weerV = wd(d.datum); A.weerM = wd(d.morgen);
    // week (vanaf morgen, 5 werkdagen-achtige dagen = 5 kalenderdagen)
    A.week = [];
    for (let i = 1; i <= 5; i++) {
      const k = plusDag(d.datum, i); const w = wd(k) || {};
      const wi = dagGem(d.windU, k), zo = dagGem(d.zonU, k);
      A.week.push({ datum: k, dag: dagNaam(k).slice(0, 2), tmin: w.tmin, tmax: w.tmax, regen: w.regen, zonuren: w.zonuren, windGw: wi ? wi.gem / 1000 : null, zonGwh: zo ? zo.som / 1000 : null });
    }
    // prijsrichting per dag: hernieuwbaar (wind gem + zon/24) en temperatuur t.o.v. vandaag
    const ren = x => (x.windGw || 0) + (x.zonGwh || 0) / 24;
    const renV = (A.windV ? A.windV.gem / 1000 : 0) + (A.zonV ? A.zonV.som / 1000 / 24 : 0);
    const tV = A.weerV ? (A.weerV.tmin + A.weerV.tmax) / 2 : 10;
    for (const x of A.week) {
      const r = ren(x); const t = x.tmin != null ? (x.tmin + x.tmax) / 2 : tV;
      const score = (r - renV) * 1.0 + (t - tV) * 0.08; // + = goedkoper
      x.richting = score > 0.6 ? 'goedkoper' : score < -0.6 ? 'duurder' : 'gelijk';
    }
    A.richtingMorgen = A.week[0] ? A.week[0].richting : 'gelijk';
    // mix: laatste 24 u beschikbaar
    if (d.mix && d.mix.uren && d.mix.uren.length) {
      const n = d.mix.uren.length, i0 = Math.max(0, n - 48);
      const g = t => { const a = (d.mix.mix[t] || []).slice(i0).filter(x => x != null); return a.length ? a.reduce((x, y) => x + y, 0) / a.length : 0; };
      A.mix = { nucleair: g('Nuclear'), gas: g('Natural Gas'), wind: g('Wind Onshore') + g('Wind Offshore'), zon: g('Solar'), water: g('Water'), opslag: g('Energy Storage'), overig: g('Biofuels') + g('Other') + g('Other Fossil Fuels') };
      A.mix.tot = Object.values(A.mix).reduce((a, b) => a + b, 0);
      const gk = Object.keys(d.mix.grens || {});
      const imp = []; for (let i = i0; i < n; i++) { let s = 0, ok = false; for (const k of gk) { const v = d.mix.grens[k][i]; if (v != null) { s += v; ok = true; } } if (ok) imp.push(-s); }
      A.netImport = imp.length ? imp.reduce((a, b) => a + b, 0) / imp.length : null;
      A.grens = {}; for (const k of gk) { const a = d.mix.grens[k].slice(i0).filter(x => x != null); if (a.length) A.grens[k] = a.reduce((x, y) => x + y, 0) / a.length; }
      A.mixTot = d.mix.laatste;
    }
    A.gas = d.gas || null;
    A.gasHist = d.gasHist || null; A.gasBasis = A.gasHist && A.gasHist.length ? A.gasHist[0].v : null;
    // v1.0.4: «verhaal van de dag» → keuze van het thematische sfeerbeeld (Unsplash, enkel met sleutel)
    A.verhaal = (() => {
      const m = A.mix, tot = m && m.tot || 0, g = A.gas || {}, t = g.ttf, o = g.opslag && g.opslag.eu;
      if (A.negatief.length) return 'zon';
      if (m && tot && (m.nucleair < 1500 || m.nucleair / tot < 0.2)) return 'kern';
      if ((A.windV && A.windV.gem >= 3500) || (A.weerV && A.weerV.windmax >= 55)) return 'wind';
      const ttfSprong = t && t.vorige_slot ? Math.abs(t.prijs / t.vorige_slot - 1) >= 0.05 : false;
      const opslagLaag = o && o.vol_pct != null && o.vorig_jaar_pct != null && o.vol_pct - o.vorig_jaar_pct <= -10;
      if ((m && tot && m.gas / tot >= 0.3) || ttfSprong || opslagLaag) return 'gas';
      if (A.netImport != null && A.netImport >= 2500) return 'import';
      if (A.weerV && (A.weerV.tmin + A.weerV.tmax) / 2 <= 2) return 'koude';
      return 'weer';
    })();
    // check gisteren
    if (vorige && vorige.richtingMorgen && A.gemV != null && vorige.gemV != null) {
      const werd = A.gemV < vorige.gemV - 5 ? 'goedkoper' : A.gemV > vorige.gemV + 5 ? 'duurder' : 'gelijk';
      A.check = { voorspeld: vorige.richtingMorgen, werd, juist: werd === vorige.richtingMorgen, gemGisteren: vorige.gemV, gemVandaag: A.gemV };
    }
    return A;
  }

  // ── script (vast format) ───────────────────────────────────────────────────────────────────
  const GRENS = { France: 'Frankrijk', Netherlands: 'Nederland', Germany: 'Duitsland', UnitedKingdom: 'het VK', Luxembourg: 'Luxemburg' };
  function maakScript(A) {
    const sc = [];
    sc.push({ id: 'intro', zinnen: [`Goedemorgen. Dit is het energieweerbericht van ${datumLang(A.datum)}.`] });
    // 1. weer in energietermen
    const z = [];
    if (A.weerV) z.push(`Vandaag ${nl0(A.weerV.tmin)} tot ${nl0(A.weerV.tmax)} graden${A.weerV.regen >= 1 ? `, ${nl0(A.weerV.regen)} millimeter regen` : ''}${A.weerV.zonuren >= 1 ? ` en ${nl0(A.weerV.zonuren)} uur zon` : ''}.`);
    if (A.zonV && A.windV) z.push(`Zon levert ongeveer ${nl0(A.zonV.som / 1000)} gigawattuur, wind gemiddeld ${nl1(A.windV.gem / 1000)} gigawatt.`);
    if (A.weerV && (A.weerV.tmin + A.weerV.tmax) / 2 < 3) z.push(`Het is koud: reken op meer verbruik voor verwarming.`);
    sc.push({ id: 'weer', titel: 'Weer in energietermen', zinnen: z.length ? z : ['Het weerbericht is vandaag niet beschikbaar.'] });
    // 2. prijzen
    const p = [];
    if (A.gemV != null) {
      p.push(`Stroom kost vandaag gemiddeld ${nl0(A.gemV)} euro per megawattuur${A.gemG != null ? (Math.abs(A.gemV - A.gemG) < 5 ? ', zoals gisteren' : `, ${A.gemV < A.gemG ? 'minder' : 'meer'} dan gisteren`) : ''}.`);
      if (A.nacht && A.goedkoop && A.nacht.g < A.goedkoop.g - 15) p.push(`Goedkoopst 's nachts tussen ${uu(A.nacht.van)} en ${uu(A.nacht.tot)}, overdag tussen ${uu(A.goedkoop.van)} en ${uu(A.goedkoop.tot)}.`);
      else if (A.goedkoop) p.push(`Overdag is het goedkoopst tussen ${uu(A.goedkoop.van)} en ${uu(A.goedkoop.tot)}, rond ${nl0(A.goedkoop.g)} euro.`);
      if (A.duur) p.push(`Het duurst tussen ${uu(A.duur.van)} en ${uu(A.duur.tot)}, rond ${nl0(A.duur.g)} euro.`);
      if (A.negatief.length) p.push(`Let op: ${A.negatief.length} ${A.negatief.length === 1 ? 'uur' : 'uren'} met een negatieve prijs, vanaf ${uu(A.negatief[0])}.`);
    } else p.push('De day-aheadprijzen van vandaag zijn nog niet beschikbaar.');
    sc.push({ id: 'prijs', titel: 'Stroomprijs vandaag', zinnen: p });
    // 3. net en systeem
    const n = [];
    if (A.mix && A.mix.tot) {
      n.push(`Het voorbije etmaal: ${nl0(A.mix.nucleair / A.mix.tot * 100)} procent kernenergie, ${nl0(A.mix.gas / A.mix.tot * 100)} procent gas en ${nl0((A.mix.wind + A.mix.zon) / A.mix.tot * 100)} procent wind en zon.`);
      if (A.netImport != null) {
        const grootste = Object.entries(A.grens || {}).sort((a, b) => a[1] - b[1])[0];
        n.push(A.netImport > 100 ? `België importeerde netto ${nl1(A.netImport / 1000)} gigawatt${grootste && grootste[1] < -100 ? `, vooral uit ${GRENS[grootste[0]] || grootste[0]}` : ''}.` : A.netImport < -100 ? `België exporteerde netto ${nl1(-A.netImport / 1000)} gigawatt.` : 'Import en export hielden elkaar in evenwicht.');
      }
    }
    if (A.gas && A.gas.ttf) {
      const o = A.gas.opslag && A.gas.opslag.eu;
      const f = A.gasBasis ? A.gas.ttf.prijs / A.gasBasis : null;
      const vgl = f == null ? '' : f >= 1.3 ? `, ${nl1(f)} keer zoveel als begin 2024` : f <= 0.77 ? `, ${nl0((1 - f) * 100)} procent goedkoper dan begin 2024` : ', ongeveer zoals begin 2024';
      n.push(`Gas kost ${nl0(A.gas.ttf.prijs)} euro per megawattuur${vgl}.`);
      if (o && o.vol_pct != null) n.push(`De Europese gasopslag is voor ${nl0(o.vol_pct)} procent vol${o.vorig_jaar_pct != null ? `, vorig jaar ${nl0(o.vorig_jaar_pct)}` : ''}.`);
    }
    if (A.check && !A.check.juist) n.push(`Gisteren verwachtten we een ${({ gelijk: 'gelijkaardige', goedkoper: 'goedkopere', duurder: 'duurdere' })[A.check.voorspeld] || 'andere'} dag; het werd ${A.check.werd === 'gelijk' ? 'ongeveer even duur' : A.check.werd}.`);
    sc.push({ id: 'net', titel: 'Net en markt', zinnen: n.length ? n : ['De netgegevens van Elia zijn vandaag niet beschikbaar.'] });
    // 4. actie van de dag
    const a = [];
    if (A.negatief.length) a.push(`Actie van de dag: verschuif verbruik en het laden van je vloot naar ${uu(A.negatief[0])} tot ${uu(A.negatief[A.negatief.length - 1] + 1)}, en zet zonnepanelen zonder vaste vergoeding af bij een negatieve prijs.`);
    else if (A.goedkoop && A.duur) {
      if (A.nacht && A.nacht.g < A.goedkoop.g - 15) a.push(`Actie van de dag: laad 's nachts tussen ${uu(A.nacht.van)} en ${uu(A.nacht.tot)}, overdag tussen ${uu(A.goedkoop.van)} en ${uu(A.goedkoop.tot)}.`);
      else a.push(`Actie van de dag: laad je elektrische vloot bij voorkeur tussen ${uu(A.goedkoop.van)} en ${uu(A.goedkoop.tot)}.`);
      a.push(`Batterij vol vóór ${uu(A.duur.van)}, ontladen tijdens het duurste blok.`);
    } else a.push('Actie van de dag: vandaag geen uitgesproken goedkope of dure uren; houd je normale planning aan.');
    sc.push({ id: 'actie', titel: 'Actie van de dag', zinnen: a });
    // 5. morgen + week
    const wk = [];
    const m = A.week[0];
    if (m) wk.push(`Morgen ${m.tmin != null ? nl0(m.tmin) + ' tot ' + nl0(m.tmax) + ' graden' : ''}${m.windGw != null ? ` en ${nl1(m.windGw)} gigawatt wind` : ''}: we verwachten ${m.richting === 'goedkoper' ? 'een goedkopere' : m.richting === 'duurder' ? 'een duurdere' : 'een gelijkaardige'} stroomdag.`);
    const duur = A.week.slice(1).filter(x => x.richting === 'duurder').map(x => dagNaam(x.datum));
    const goed = A.week.slice(1).filter(x => x.richting === 'goedkoper').map(x => dagNaam(x.datum));
    if (duur.length) wk.push(`Later deze week ${duur.length === 1 ? 'is vooral ' + duur[0] : 'zijn ' + duur.slice(0, -1).join(', ') + ' en ' + duur[duur.length - 1]} duurder: weinig wind of zon${A.week.some(x => x.tmin != null && x.tmin < 1) ? ' en vrieskou' : ''}.`);
    else if (goed.length) wk.push(`Later deze week wordt ${goed.join(' en ')} goedkoper dankzij meer wind of zon.`);
    sc.push({ id: 'week', titel: 'Morgen en de komende dagen', zinnen: wk.length ? wk : ['Het vooruitzicht voor de komende dagen volgt morgen.'] });
    // 6. CTA (vast)
    sc.push({ id: 'cta', geenCaption: true, naloop: 3.5, zinnen: ['Wil je weten wat dit voor jouw bedrijf betekent? Stuur ons je energiefactuur. Het adres staat in beeld.'] });
    return sc;
  }

  // ── TTS ────────────────────────────────────────────────────────────────────────────────────
  // v1.0.2: stem roteert per aflevering (vaste stem binnen één video). Lijst via OPENAI_TTS_VOICES (komma's);
  //   OPENAI_TTS_VOICE = één vaste stem (wint). Index = dagnummer sinds 1970, dus elke werkdag een andere.
  let _stemVanDag = null;
  function stemVoor(datum) {
    if (process.env.OPENAI_TTS_VOICE) return process.env.OPENAI_TTS_VOICE;
    const l = (process.env.OPENAI_TTS_VOICES || 'marin,cedar,coral,sage,verse').split(',').map(x => x.trim()).filter(Boolean);
    return l[Math.floor(Date.parse(datum + 'T12:00:00Z') / 864e5) % l.length];
  }
  async function tts(tekst, uitPad) {
    const prov = (process.env.TTS_PROVIDER || '').toLowerCase();
    if (prov === 'openai' && process.env.OPENAI_API_KEY) {
      const r = await fetch('https://api.openai.com/v1/audio/speech', { method: 'POST', headers: { Authorization: `Bearer ${process.env.OPENAI_API_KEY}`, 'content-type': 'application/json' },
        body: JSON.stringify({ model: 'gpt-4o-mini-tts', voice: _stemVanDag || 'marin', input: tekst, response_format: 'mp3', instructions: 'Je bent een Vlaamse weerpresentator op de radio. Spreek natuurlijk, warm en betrokken, met levendige intonatie en een glimlach in de stem, alsof je een collega bijpraat. Vlot tempo, korte natuurlijke pauzes tussen zinnen, nadruk op de uren en de actie. Spreek Nederlands met een Vlaamse tongval. Getallen voluit.' }) });
      if (!r.ok) throw new Error('openai tts ' + r.status + ' ' + (await r.text()).slice(0, 200));
      fs.writeFileSync(uitPad, Buffer.from(await r.arrayBuffer())); return true;
    }
    if (prov === 'elevenlabs' && process.env.ELEVENLABS_API_KEY && process.env.ELEVENLABS_VOICE_ID) {
      const r = await fetch(`https://api.elevenlabs.io/v1/text-to-speech/${process.env.ELEVENLABS_VOICE_ID}?output_format=mp3_44100_128`, { method: 'POST', headers: { 'xi-api-key': process.env.ELEVENLABS_API_KEY, 'content-type': 'application/json' },
        body: JSON.stringify({ text: tekst, model_id: 'eleven_multilingual_v2', language_code: 'nl', voice_settings: { stability: 0.5, similarity_boost: 0.75 } }) });
      if (!r.ok) throw new Error('elevenlabs tts ' + r.status);
      fs.writeFileSync(uitPad, Buffer.from(await r.arrayBuffer())); return true;
    }
    return false;
  }
  const run = (cmd, args) => new Promise((ok, ko) => execFile(cmd, args, { maxBuffer: 64 * 1024 * 1024 }, (e, so, se) => e ? ko(new Error(cmd + ': ' + (se || e.message).slice(-600))) : ok(so)));
  async function duur(p) { const o = await run('ffprobe', ['-v', 'error', '-show_entries', 'format=duration', '-of', 'csv=p=0', p]); return parseFloat(o) || 0; }

  // ── beelden (SVG/HTML) ─────────────────────────────────────────────────────────────────────
  const C = { bg: '#14203a', kaart: '#1c2b4d', tekst: '#ffffff', grijs: '#b9c4d6', groen: '#05B050', blauw: '#4ea1ff', oranje: '#f0a500', rood: '#f85149', paars: '#a371f7', windG: '#39d353', water: '#388bfd', cyan: '#56d4dd' };
  function prijsSvg(A, w, h) {
    const reeks = A.pVandaag.concat(A.morgenBekend ? A.pMorgen : []);
    const vals = reeks.filter(x => x != null); if (!vals.length) return '';
    const lo = Math.min(0, ...vals), hi = Math.max(...vals) * 1.08, n = reeks.length, bw = w / n;
    const y = v => h - 70 - (v - lo) / (hi - lo) * (h - 90); // onderaan ruimte voor uren + dag
    let s = `<svg width="${w}" height="${h}" style="overflow:visible" xmlns="http://www.w3.org/2000/svg">`;
    if (lo < 0) s += `<line x1="0" x2="${w}" y1="${y(0)}" y2="${y(0)}" stroke="${C.grijs}" stroke-width="2"/>`;
    reeks.forEach((v, i) => {
      if (v == null) return;
      const h0 = i % 24;
      const kl = i < 24 && A.nacht && A.goedkoop && A.nacht.g < A.goedkoop.g - 15 && h0 >= A.nacht.van && h0 < A.nacht.tot ? '#2f8f5b' : i < 24 && A.goedkoop && h0 >= A.goedkoop.van && h0 < A.goedkoop.tot ? C.groen : i < 24 && A.duur && h0 >= A.duur.van && h0 < A.duur.tot ? C.rood : v < 0 ? C.groen : (i >= 24 ? '#2c4a7a' : C.blauw);
      const y1 = y(Math.max(v, 0)), y2 = y(Math.min(v, 0));
      s += `<rect x="${i * bw + 2}" y="${Math.min(y1, y2)}" width="${bw - 4}" height="${Math.max(2, Math.abs(y2 - y1))}" rx="3" fill="${kl}"/>`;
    });
    // v1.0.2: uren om de 3 u onder de staven, daaronder de dag per blok van 24 u
    const dagen = [A.datum].concat(A.morgenBekend ? [plusDag(A.datum, 1)] : []);
    dagen.forEach((dg, di) => {
      const x0 = di * 24 * bw;
      for (let t = 0; t < 24; t += 3) s += `<text x="${(x0 + (t + 0.5) * bw).toFixed(1)}" y="${h - 40}" fill="${C.grijs}" font-size="${dagen.length > 1 ? 21 : 26}" text-anchor="middle">${t}u</text>`;
      const lbl = new Date(dg + 'T12:00:00Z'); const naam = `${WD[lbl.getUTCDay()]} ${lbl.getUTCDate()}/${lbl.getUTCMonth() + 1}`;
      s += `<line x1="${(x0 + 4).toFixed(1)}" x2="${(x0 + 24 * bw - 4).toFixed(1)}" y1="${h - 30}" y2="${h - 30}" stroke="#2c3e63" stroke-width="2"/>`;
      s += `<text x="${(x0 + 12 * bw).toFixed(1)}" y="${h - 4}" fill="#e8edf5" font-size="28" font-weight="600" text-anchor="middle">${di === 0 ? 'vandaag · ' : 'morgen · '}${naam}</text>`;
      if (di > 0) s += `<line x1="${x0.toFixed(1)}" x2="${x0.toFixed(1)}" y1="20" y2="${h - 30}" stroke="#2c3e63" stroke-width="2" stroke-dasharray="6 6"/>`;
    });
    s += `<text x="${w - 4}" y="${y(hi / 1.08) - 6}" fill="${C.grijs}" font-size="24" text-anchor="end">${nl0(Math.max(...vals))} €/MWh</text>`;
    return s + '</svg>';
  }
  function gasSvg(A, w, h) {
    const hst = (A.gasHist || []).slice(); if (hst.length < 3) return '';
    const nu = A.gas && A.gas.ttf && A.gas.ttf.prijs; const pts = hst.map(x => x.v).concat(nu ? [nu] : []);
    const hi = Math.max(...pts) * 1.12, lo = 0, N = pts.length;
    const x = i => 10 + i / (N - 1) * (w - 120), y = v => h - 34 - (v - lo) / (hi - lo) * (h - 50);
    let p = `M${x(0)},${y(pts[0])}`; pts.forEach((v, i) => { if (i) p += `L${x(i).toFixed(1)},${y(v).toFixed(1)}`; });
    let s = `<svg width="${w}" height="${h}" style="overflow:visible" xmlns="http://www.w3.org/2000/svg">`;
    s += `<path d="${p}L${x(N - 1)},${y(0)}L${x(0)},${y(0)}Z" fill="${C.rood}" opacity=".18"/><path d="${p}" stroke="${C.rood}" stroke-width="4" fill="none" stroke-linejoin="round"/>`;
    s += `<circle cx="${x(0)}" cy="${y(pts[0])}" r="6" fill="${C.rood}"/><text x="${x(0) + 10}" y="${y(pts[0]) - 12}" fill="${C.grijs}" font-size="24">${nl0(pts[0])}</text>`;
    if (nu) s += `<circle cx="${x(N - 1)}" cy="${y(nu)}" r="10" fill="#fff" stroke="${C.rood}" stroke-width="5"/><text x="${x(N - 1) + 18}" y="${y(nu) + 9}" fill="#fff" font-size="30" font-weight="800">${nl0(nu)}</text>`;
    hst.forEach((q, i) => { if (q.m.endsWith('-01')) s += `<text x="${x(i)}" y="${h - 4}" fill="${C.grijs}" font-size="22">${q.m.slice(0, 4)}</text><line x1="${x(i)}" x2="${x(i)}" y1="${y(0)}" y2="${y(0) + 8}" stroke="${C.grijs}"/>`; });
    if (nu) s += `<text x="${x(N - 1)}" y="${h - 4}" fill="#e8edf5" font-size="22" text-anchor="middle">nu</text>`;
    return s + '</svg>';
  }
  function mixSvg(d, w, h) {
    if (!d.mix || !d.mix.uren) return '';
    const n = d.mix.uren.length, i0 = Math.max(0, n - 48), N = n - i0;
    const T = [['Nuclear', C.paars], ['Natural Gas', C.rood], ['Wind Offshore', '#1f9e4a'], ['Wind Onshore', C.windG], ['Solar', C.oranje], ['Water', C.water], ['Energy Storage', C.cyan], ['Biofuels', '#8b6f47'], ['Other', '#566273'], ['Other Fossil Fuels', '#6e7681']];
    const tot = []; for (let i = 0; i < N; i++) { let s = 0; for (const [k] of T) s += (d.mix.mix[k] || [])[i0 + i] || 0; tot.push(s); }
    const imp = []; const gk = Object.keys(d.mix.grens || {}); for (let i = 0; i < N; i++) { let s = 0, ok = false; for (const k of gk) { const v = d.mix.grens[k][i0 + i]; if (v != null) { s += v; ok = true; } } imp.push(ok ? -s : null); }
    const hi = Math.max(...tot, ...imp.filter(x => x != null)) * 1.05 || 1, x = i => i / (N - 1) * w, y = v => h - 34 - v / hi * (h - 44);
    let s = `<svg width="${w}" height="${h}" xmlns="http://www.w3.org/2000/svg">`, onder = new Array(N).fill(0);
    for (const [k, kl] of T) {
      const a = (d.mix.mix[k] || []).slice(i0); if (!a.some(v => v > 5)) continue;
      const boven = onder.map((o, i) => o + (a[i] || 0));
      let p = `M${x(0)},${y(onder[0])}`; for (let i = 0; i < N; i++) p += `L${x(i)},${y(boven[i])}`; for (let i = N - 1; i >= 0; i--) p += `L${x(i)},${y(onder[i])}`;
      s += `<path d="${p}Z" fill="${kl}" opacity="0.92"/>`; onder = boven;
    }
    let pl = ''; imp.forEach((v, i) => { if (v != null) pl += (pl ? 'L' : 'M') + x(i) + ',' + y(Math.max(0, v)); });
    s += `<path d="${pl}" stroke="#ffffff" stroke-width="4" stroke-dasharray="10 8" fill="none"/>`;
    s += `<text x="0" y="${h - 4}" fill="${C.grijs}" font-size="22">−48 u</text><text x="${w}" y="${h - 4}" fill="${C.grijs}" font-size="22" text-anchor="end">laatste Elia-gegevens</text>`;
    s += `<text x="${w}" y="22" fill="${C.grijs}" font-size="22" text-anchor="end">${nl0(hi / 1000)} GW · stippellijn = netto import</text>`;
    return s + '</svg>';
  }
  const ICOON = { goedkoper: '▼', duurder: '▲', gelijk: '■' };
  const KLEUR = { goedkoper: C.groen, duurder: C.rood, gelijk: C.grijs };
  function scenePaneel(id, A, d) {
    if (id === 'intro') return `<div class="groot">Wat doet de stroom${A.gemV != null ? ' vandaag' : ''}?</div><div class="sub2">Elke werkdag om 7u30.<br>Stroomprijs, weer en markt in 90 seconden.<br>En wat je er vandaag mee doet.</div>`;
    if (id === 'weer') {
      const w = A.weerV || {}; const kv = [['Temperatuur', w.tmin != null ? `${nl0(w.tmin)}–${nl0(w.tmax)} °C` : '—'], ['Zon (PV België)', A.zonV ? `${nl0(A.zonV.som / 1000)} GWh` : '—'], ['Wind gemiddeld', A.windV ? `${nl1(A.windV.gem / 1000)} GW` : '—'], ['Neerslag', w.regen != null ? `${nl0(w.regen)} mm` : '—']];
      return `<div class="grid">${kv.map(([k, v]) => `<div class="tegel"><div class="tl">${k}</div><div class="tv">${v}</div></div>`).join('')}</div>`;
    }
    if (id === 'prijs') return `<div class="kpi"><span>gemiddeld <b>${A.gemV != null ? nl0(A.gemV) : '—'} €/MWh</b></span><span style="color:${C.groen}">goedkoop ${A.nacht && A.goedkoop && A.nacht.g < A.goedkoop.g - 15 ? uu(A.nacht.van) + '–' + uu(A.nacht.tot) + ' · ' : ''}${A.goedkoop ? uu(A.goedkoop.van) + '–' + uu(A.goedkoop.tot) : '—'}</span><span style="color:${C.rood}">duur ${A.duur ? uu(A.duur.van) + '–' + uu(A.duur.tot) : '—'}</span></div>${prijsSvg(A, 960, 480)}`;
    if (id === 'net') {
      const g = A.gas && A.gas.ttf, o = A.gas && A.gas.opslag && A.gas.opslag.eu;
      return `${mixSvg(d, 960, 300)}<div class="leg">${[['kern', C.paars], ['gas', C.rood], ['wind', C.windG], ['zon', C.oranje], ['water', C.water], ['opslag', C.cyan]].map(([k, c]) => `<span><i style="background:${c}"></i>${k}</span>`).join('')}</div>${A.gasHist && A.gasHist.length > 2 ? `<div class="sub2" style="font-size:26px;margin-top:6px">Europese gasprijs sinds 1/1/2024 (€/MWh, maandgemiddelde · nu = TTF)</div>${gasSvg(A, 960, 175)}` : ''}<div class="kpi">${g ? `<span>TTF gas <b>${nl0(g.prijs)} €/MWh</b></span>` : ''}${o && o.vol_pct != null ? `<span>EU-gasopslag <b>${nl0(o.vol_pct)}%</b>${o.vorig_jaar_pct != null ? ` (vorig jaar ${nl0(o.vorig_jaar_pct)}%)` : ''}</span>` : ''}</div>`;
    }
    if (id === 'actie') return `${!A.negatief.length && A.nacht && A.goedkoop && A.nacht.g < A.goedkoop.g - 15 ? `<div class="actie">🌙 ${uu(A.nacht.van)}–${uu(A.nacht.tot)}</div>` : ''}<div class="actie">${A.negatief.length ? `⚡ ${uu(A.negatief[0])}–${uu(A.negatief[A.negatief.length - 1] + 1)}` : A.goedkoop ? `🔌 ${uu(A.goedkoop.van)}–${uu(A.goedkoop.tot)}` : '—'}</div><div class="sub2">${A.duur && !A.negatief.length ? `🔋 vol vóór ${uu(A.duur.van)}` : ''}</div>`;
    if (id === 'week') return `<div class="week">${A.week.map(x => `<div class="wd"><div class="wn">${x.dag}</div><div class="wt">${x.tmin != null ? nl0(x.tmin) + '/' + nl0(x.tmax) + '°' : '—'}</div><div class="wv">💨 ${x.windGw != null ? nl1(x.windGw) : '—'}</div><div class="wv">☀️ ${x.zonGwh != null ? nl0(x.zonGwh) : '—'}</div><div class="wr" style="color:${KLEUR[x.richting]}">${ICOON[x.richting]}</div></div>`).join('')}</div><div class="sub2">💨 wind GW · ☀️ zon GWh · ▼ goedkoper ▲ duurder dan vandaag (verwachting)</div>`;
    if (id === 'cta') return `<div class="sub2" style="font-size:40px;color:#e8edf5">Wat betekent dit voor jouw bedrijf?</div><div class="cta">Je afrekening naar</div><div class="cta" style="color:#4be08a;font-size:54px;white-space:nowrap">${CTA_MAIL}</div><div class="sub2">Persoonlijke voorspelling terug.</div>`;
    return '';
  }
  // ── weerbeeld van de dag (v1.0.2) ─────────────────────────────────────────────────────────
  // Zonder sleutel: getekend sfeerbeeld (lucht volgens het weer van vandaag, Vlaams landschap met windmolens).
  // Met UNSPLASH_ACCESS_KEY: echte foto volgens het weertype (gratis licentie, vermelding fotograaf in beeld).
  function weerType(code) {
    if (code == null) return 'bewolkt';
    if (code >= 95) return 'onweer'; if (code >= 71 && code <= 77 || code === 85 || code === 86) return 'sneeuw';
    if (code >= 51 && code <= 67 || code >= 80 && code <= 82) return 'regen'; if (code === 45 || code === 48) return 'mist';
    if (code === 3) return 'bewolkt'; if (code === 2) return 'halfbewolkt'; return 'helder';
  }
  function luchtSvg(type) {
    const L = { helder: ['#3f8fd8', '#bfe0f6'], halfbewolkt: ['#5b98cf', '#d3e4f0'], bewolkt: ['#7d8a9b', '#c8cfd8'], regen: ['#4e5b6c', '#9aa6b4'], mist: ['#aab4bd', '#e2e6e9'], sneeuw: ['#93a7bd', '#e6edf3'], onweer: ['#2c3442', '#6b7584'] }[type] || ['#7d8a9b', '#c8cfd8'];
    const wolk = (x, y, s, c) => `<g fill="${c}" opacity=".92"><ellipse cx="${x}" cy="${y}" rx="${110 * s}" ry="${42 * s}"/><ellipse cx="${x - 60 * s}" cy="${y + 8 * s}" rx="${70 * s}" ry="${34 * s}"/><ellipse cx="${x + 55 * s}" cy="${y - 18 * s}" rx="${75 * s}" ry="${45 * s}"/></g>`;
    let g = `<defs><linearGradient id="l" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="${L[0]}"/><stop offset="1" stop-color="${L[1]}"/></linearGradient><radialGradient id="z"><stop offset="0" stop-color="#fff7d6"/><stop offset=".35" stop-color="#ffe08a" stop-opacity=".9"/><stop offset="1" stop-color="#ffe08a" stop-opacity="0"/></radialGradient></defs><rect width="1080" height="760" fill="url(#l)"/>`;
    if (type === 'helder' || type === 'halfbewolkt') g += `<circle cx="820" cy="170" r="230" fill="url(#z)"/><circle cx="820" cy="170" r="62" fill="#fff6c9"/>`;
    if (type === 'halfbewolkt') g += wolk(300, 190, 1.2, '#ffffff') + wolk(640, 120, 0.8, '#f4f7fa');
    if (['bewolkt', 'regen', 'onweer', 'sneeuw'].includes(type)) { const c = type === 'bewolkt' ? '#e3e7ec' : type === 'sneeuw' ? '#f2f5f8' : '#8b95a3'; g += wolk(220, 150, 1.5, c) + wolk(620, 110, 1.7, c) + wolk(960, 190, 1.3, c) + wolk(420, 260, 1.2, c); }
    if (type === 'regen' || type === 'onweer') { let r = ''; for (let k = 0; k < 140; k++) { const x = (k * 97) % 1100, y = 230 + (k * 53) % 420; r += `<line x1="${x}" y1="${y}" x2="${x - 14}" y2="${y + 38}"/>`; } g += `<g stroke="#dde4ec" stroke-width="3" opacity=".55">${r}</g>`; }
    if (type === 'onweer') g += `<path d="M640 200 L600 320 L640 320 L590 450 L690 290 L648 290 L690 200Z" fill="#fff3b0"/>`;
    if (type === 'sneeuw') { let r = ''; for (let k = 0; k < 120; k++) r += `<circle cx="${(k * 89) % 1080}" cy="${200 + (k * 61) % 460}" r="${3 + k % 4}"/>`; g += `<g fill="#fff" opacity=".85">${r}</g>`; }
    if (type === 'mist') for (let k = 0; k < 5; k++) g += `<rect x="-50" y="${330 + k * 70}" width="1200" height="34" rx="17" fill="#fff" opacity=".35"/>`;
    // landschap: polders, populieren, windmolens
    const veld = type === 'sneeuw' ? ['#dfe6ec', '#c9d3dc'] : ['#5f7d4f', '#46623b'];
    g += `<path d="M0 600 Q270 560 540 590 T1080 575 V760 H0Z" fill="${veld[0]}"/><path d="M0 655 Q300 620 620 650 T1080 640 V760 H0Z" fill="${veld[1]}"/>`;
    for (const x of [80, 110, 140, 975, 1005]) g += `<ellipse cx="${x}" cy="565" rx="11" ry="46" fill="#3c5435"/>`;
    for (const [x, y, s] of [[330, 590, 1], [470, 585, 0.8], [760, 580, 1.15]]) {
      const h = 210 * s; g += `<g stroke="#f1f3f5" stroke-linecap="round"><line x1="${x}" y1="${y}" x2="${x}" y2="${y - h}" stroke-width="${7 * s}"/>`;
      for (const a of [0, 120, 240]) { const r = (a - 90) * Math.PI / 180, l = 95 * s; g += `<line x1="${x}" y1="${y - h}" x2="${(x + Math.cos(r) * l).toFixed(1)}" y2="${(y - h + Math.sin(r) * l).toFixed(1)}" stroke-width="${6 * s}"/>`; }
      g += `<circle cx="${x}" cy="${y - h}" r="${8 * s}" fill="#f1f3f5"/></g>`;
    }
    if (type === 'mist') g += `<rect width="1080" height="760" fill="#e9edf0" opacity=".35"/>`;
    return 'data:image/svg+xml;base64,' + Buffer.from(`<svg xmlns="http://www.w3.org/2000/svg" width="1080" height="760" viewBox="0 0 1080 760">${g}</svg>`).toString('base64');
  }
  const THEMA = { kern: 'nuclear power plant cooling tower', gas: 'gas pipeline industry', wind: 'offshore wind farm', zon: 'solar panels sunlight', import: 'high voltage power lines', koude: 'frost winter morning landscape' };
  async function weerFoto(type, datum, thema) {
    const key = process.env.UNSPLASH_ACCESS_KEY; if (!key) return null;
    const q = thema && THEMA[thema] ? THEMA[thema] : { helder: 'sunny countryside Belgium', halfbewolkt: 'clouds fields Flanders', bewolkt: 'overcast sky landscape Belgium', regen: 'rain city Belgium', mist: 'foggy morning Flanders', sneeuw: 'snow landscape Belgium', onweer: 'storm clouds landscape' }[type];
    try {
      const j = await jget(`https://api.unsplash.com/search/photos?query=${encodeURIComponent(q)}&orientation=landscape&per_page=20&content_filter=high&client_id=${encodeURIComponent(key)}`);
      const lijst = (j.results || []).filter(x => x.urls); if (!lijst.length) return null;
      const f = lijst[(Date.parse(datum) / 864e5 | 0) % lijst.length];
      fetch(`${f.links.download_location}${f.links.download_location.includes('?') ? '&' : '?'}client_id=${encodeURIComponent(key)}`).catch(() => {}); // Unsplash-richtlijn: download melden
      return { url: f.urls.regular, credit: `${thema ? 'Sfeerbeeld · ' : ''}Foto: ${f.user && f.user.name || 'Unsplash'} / Unsplash`, thema: thema || null };
    } catch (e) { log('unsplash:', e.message); return null; }
  }

  // Look-and-feel = LinkedIn-campagne factuurcheck (navy #14203a, Poppins, groene eyebrow met streep,
  // witte koppen, groene band onderaan, witte chip met kompas-symbool + groene badge «ACTIE»).
  const FOTO = datum => { const d = new Date(datum + 'T12:00:00Z'); const n = Math.floor((d - new Date(Date.UTC(d.getUTCFullYear(), 0, 0))) / 864e5); return `https://app.energie-compas.eu/ec-img/factuurcheck/fc-${String((n % 24) + 1).padStart(2, '0')}.jpg`; };
  function sceneHtml(sc, A, d, caption, metStem) {
    const fotoScene = sc.id === 'intro' || sc.id === 'cta';
    const bg = (A.weerFoto && A.weerFoto.url) || luchtSvg(weerType(A.weerV && A.weerV.code));
    const chip = `<div class="chip"><div class="kompas"><img src="https://app.energie-compas.eu/apps/ec-logo.png" onerror="this.style.display='none'"></div>${sc.id === 'actie' || sc.id === 'cta' ? '<div class="badge">ACTIE</div>' : '<div class="badge stil">7u30</div>'}</div>`;
    return `<!DOCTYPE html><html><head><meta charset="utf-8">
<link href="https://fonts.googleapis.com/css2?family=Poppins:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}body{width:${W}px;height:${H}px;background:#14203a;color:#fff;font-family:Poppins,'DejaVu Sans',Arial,sans-serif;overflow:hidden;position:relative}
.foto{position:absolute;left:0;right:0;top:0;height:760px;background:url('${bg}') center/cover}.foto.zacht{opacity:.38}
.foto:after{content:'';position:absolute;inset:0;background:linear-gradient(180deg,rgba(20,32,58,.85) 0%,rgba(20,32,58,.1) 22%,rgba(20,32,58,.55) 60%,#14203a 100%)}
.brow{position:absolute;top:56px;left:72px;right:72px;display:flex;align-items:center;gap:16px;font-size:26px;font-weight:600;letter-spacing:4px;color:#05B050;text-transform:uppercase;z-index:2}
.brow:before{content:'';width:7px;height:34px;background:#05B050;border-radius:2px}
.titel{position:absolute;top:122px;left:72px;right:72px;font-size:66px;font-weight:800;line-height:1.08;z-index:2}
.paneel{position:absolute;top:260px;left:72px;right:72px;height:660px;display:flex;flex-direction:column;justify-content:center;gap:26px;z-index:2}.paneel>*{flex-shrink:0}.paneel.net{gap:12px;top:240px;height:700px}
.paneel.onder{top:auto;bottom:420px;height:auto;justify-content:flex-end}.paneel.eind{bottom:170px}
.groot{font-size:88px;font-weight:800;line-height:1.05}.sub{font-size:40px;font-weight:600;line-height:1.3;color:#e8edf5}.sub2{font-size:31px;color:#b9c4d6;line-height:1.35}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:26px}.tegel{background:#1c2b4d;border-radius:20px;padding:34px}.tl{font-size:28px;color:#b9c4d6}.tv{font-size:60px;font-weight:800;margin-top:6px}
.kpi{display:flex;gap:30px;flex-wrap:wrap;font-size:31px;color:#b9c4d6}.kpi b{color:#fff}
.leg{display:flex;gap:22px;font-size:25px;color:#b9c4d6}.leg i{display:inline-block;width:20px;height:20px;border-radius:4px;margin-right:8px;vertical-align:-2px}
.actie{font-size:128px;font-weight:800;color:#05B050;line-height:1}.cta{font-size:72px;font-weight:800;line-height:1.08}
.week{display:flex;gap:14px}.wd{flex:1;background:#1c2b4d;border-radius:18px;padding:22px 6px;text-align:center}.wn{font-size:38px;font-weight:800;text-transform:capitalize}.wt{font-size:32px;font-weight:600;margin:10px 0}.wv{font-size:27px;color:#b9c4d6;margin:6px 0}.wr{font-size:46px;margin-top:10px}
.cap{position:absolute;left:56px;right:56px;bottom:150px;min-height:210px;background:rgba(8,14,28,.72);border-left:7px solid #05B050;border-radius:18px;padding:24px 34px;font-size:38px;font-weight:500;line-height:1.32;display:flex;align-items:center;z-index:3}
.voet{position:absolute;left:72px;bottom:46px;right:420px;font-size:19px;color:#7f8ca3;line-height:1.35;z-index:3}
.chip{position:absolute;right:56px;bottom:40px;background:#fff;border-radius:16px;padding:12px 16px;display:flex;align-items:center;gap:14px;z-index:3}
.kompas{width:62px;height:62px;overflow:hidden}.kompas img{height:62px;display:block}
.badge{background:#05B050;color:#fff;font-weight:800;font-size:30px;letter-spacing:3px;padding:8px 20px;border-radius:10px}.badge.stil{background:#14203a}
.band{position:absolute;left:0;right:0;bottom:0;height:12px;background:#05B050;z-index:3}
</style></head><body>
${fotoScene ? '<div class="foto"></div>' : sc.id === 'weer' ? '<div class="foto zacht"></div>' : sc.id === 'net' && A.themaFoto ? `<div class="foto zacht" style="background-image:url('${A.themaFoto.url}')"></div>` : ''}
<div class="brow">Energieweerbericht · ${esc(datumLang(A.datum))}</div>
${sc.titel ? `<div class="titel">${esc(sc.titel)}</div>` : ''}
<div class="paneel${fotoScene ? ' onder' : ''}${sc.id === 'cta' ? ' eind' : ''}${sc.id === 'net' ? ' net' : ''}">${scenePaneel(sc.id, A, d)}</div>
${caption ? `<div class="cap">${esc(caption)}</div>` : ''}
<div class="voet">Bronnen: Elia Open Data · ENTSO-E/Energy-Charts · Open-Meteo · GIE AGSI+ · TTF: Yahoo Finance · gas historiek: IMF/FRED${sc.id === 'net' && A.themaFoto ? ' · ' + esc(A.themaFoto.credit) : A.weerFoto && A.weerFoto.credit ? ' · ' + esc(A.weerFoto.credit) : ''}${metStem ? ' · Stem gegenereerd met AI' : ''}</div>
${chip}<div class="band"></div>
</body></html>`;
  }

  // ── render ─────────────────────────────────────────────────────────────────────────────────
  async function render(d, A, script, dir) {
    const puppeteer = require('puppeteer-core');
    const browser = await puppeteer.launch({ executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || '/usr/bin/chromium', args: ['--no-sandbox', '--disable-dev-shm-usage', '--font-render-hinting=none'] });
    const segs = [], audios = []; let metStem = null, ttsFout = null;
    try {
      const page = await browser.newPage(); await page.setViewport({ width: W, height: H });
      let i = 0;
      const spreekVorm = t => t.replace(/\b(\d{1,2})u\b/g, '$1 uur').replace(/\b(\d{1,2}) uur en (\d{1,2}) uur\b/g, '$1 en $2 uur');
      for (const sc of script) {
        // v1.0.2: één stemopname per BLOK (natuurlijker intonatie dan per zin); de zinnen krijgen elk een deel van
        //   de blokduur volgens hun lengte. sc.naloop = stilstaand beeld na de stem (eindbeeld).
        const spreek = sc.zinnen.map(spreekVorm);
        const lens = spreek.map(t => Math.max(8, t.length)); const somLen = lens.reduce((a, b) => a + b, 0);
        let blokDur = spreek.reduce((a, t) => a + Math.max(2.4, t.split(/\s+/).length / 2.8 + 0.5), 0); // leestempo zonder stem
        let audio = null;
        if (metStem !== false) {
          try {
            const ap = path.join(dir, 'blok' + String(i).padStart(3, '0') + '.mp3');
            if (await tts(spreek.join(' '), ap)) {
              metStem = true;
              const tempo = Math.min(1.3, Math.max(1, +process.env.EWB_TEMPO || 1.15));
              if (tempo !== 1) { const ap2 = ap.replace('.mp3', '.t.mp3'); await run('ffmpeg', ['-y', '-i', ap, '-filter:a', `atempo=${tempo}`, ap2]); fs.renameSync(ap2, ap); }
              blokDur = (await duur(ap)) + 0.35; audio = ap;
            } else metStem = false;
          } catch (e) { ttsFout = e.message; metStem = false; log('tts fout → zonder stem:', e.message); }
        }
        if (metStem === false && audios.length) audios.length = 0; // consistent: geen half-stem
        const totaal = blokDur + (sc.naloop || 0);
        if (audio) audios.push({ ap: audio, dur: totaal });
        for (let z = 0; z < sc.zinnen.length; z++) {
          const toon = sc.geenCaption ? '' : ((sc.tekst && sc.tekst[z]) || sc.zinnen[z]);
          const nm = String(i).padStart(3, '0');
          let dur = blokDur * lens[z] / somLen; if (z === sc.zinnen.length - 1) dur += (sc.naloop || 0);
          await page.setContent(sceneHtml(sc, A, d, toon, metStem === true), { waitUntil: 'load', timeout: 10000 }).catch(() => {});
          const png = path.join(dir, nm + '.png');
          await page.screenshot({ path: png, type: 'png' });
          if (z === 0 && sc.id === 'intro') await page.screenshot({ path: path.join(dir, 'poster.jpg'), type: 'jpeg', quality: 88 });
          segs.push({ png, dur, nm, fadeIn: z === 0, blok: sc.id });
          i++;
        }
      }
    } finally { await browser.close().catch(() => {}); }
    // segmenten (zelfde encoder-instellingen), concat, audio
    const lijst = [];
    for (const s of segs) {
      const out = path.join(dir, s.nm + '.mp4'), tmp = path.join(dir, s.nm + '.tmp.mp4');
      const vf = `format=yuv420p,loop=-1:1,trim=duration=${s.dur.toFixed(3)}${s.fadeIn ? ',fade=t=in:st=0:d=0.35' : ''},setpts=PTS-STARTPTS`;
      await run('ffmpeg', ['-y', '-framerate', String(FPS), '-i', s.png, '-vf', vf, '-r', String(FPS), '-c:v', 'libx264', '-crf', '20', '-preset', 'veryfast', '-pix_fmt', 'yuv420p', tmp]);
      fs.renameSync(tmp, out); lijst.push(`file '${out}'`);
    }
    const lf = path.join(dir, 'lijst.txt'); fs.writeFileSync(lf, lijst.join('\n'));
    const stil = path.join(dir, 'video_stil.mp4');
    await run('ffmpeg', ['-y', '-f', 'concat', '-safe', '0', '-i', lf, '-c', 'copy', stil]);
    const eind = path.join(dir, 'video.mp4');
    if (metStem && audios.length === script.length) {
      // audio per blok, opgevuld tot de blokduur (incl. naloop)
      const al = [];
      for (const a of audios) { const o = a.ap.replace('.mp3', '.wav'); await run('ffmpeg', ['-y', '-i', a.ap, '-af', `apad,atrim=0:${a.dur.toFixed(3)}`, '-ar', '44100', '-ac', '2', o]); al.push(`file '${o}'`); }
      const af = path.join(dir, 'alijst.txt'); fs.writeFileSync(af, al.join('\n'));
      const wav = path.join(dir, 'stem.wav'); await run('ffmpeg', ['-y', '-f', 'concat', '-safe', '0', '-i', af, '-c', 'copy', wav]);
      await run('ffmpeg', ['-y', '-i', stil, '-i', wav, '-map', '0:v', '-map', '1:a', '-c:v', 'copy', '-c:a', 'aac', '-b:a', '128k', '-shortest', '-movflags', '+faststart', eind]);
    } else {
      await run('ffmpeg', ['-y', '-i', stil, '-c', 'copy', '-movflags', '+faststart', eind]);
    }
    return { video: eind, poster: path.join(dir, 'poster.jpg'), duur: segs.reduce((a, s) => a + s.dur, 0), metStem: !!(metStem && audios.length === script.length), ttsFout };
  }

  // ── mail (Brevo) ───────────────────────────────────────────────────────────────────────────
  async function brevo(pad, method, body) {
    const r = await fetch('https://api.brevo.com/v3' + pad, { method, headers: { 'api-key': process.env.BREVO_API_KEY || '', accept: 'application/json', 'content-type': 'application/json' }, body: body ? JSON.stringify(body) : undefined });
    const t = await r.text(); let j = null; try { j = t ? JSON.parse(t) : null; } catch (_) {}
    if (!r.ok) throw new Error(`brevo ${pad} ${r.status} ${t.slice(0, 300)}`);
    return j;
  }
  const afzender = () => ({ name: 'Energie-Compas', email: (process.env.BREVO_SENDER || '').replace(/^.*<|>.*$/g, '').trim() });
  function mailHtml(dag, A, metStop) {
    const zinnen = (dag.script || []).flatMap(s => (s.tekst || s.zinnen).map(z => `<p style="margin:0 0 10px;font-size:15px;line-height:1.5">${esc(z)}</p>`)).join('');
    return `<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;color:#1b2a3d">
<p style="font-size:13px;color:#05B050;letter-spacing:1px;text-transform:uppercase;margin:18px 0 4px">Energieweerbericht · ${esc(datumLang(dag.datum))}</p>
<a href="${esc(PUBLIC_BASE)}"><img src="${esc(dag.poster_url)}" alt="Bekijk de video" style="width:100%;border-radius:10px;display:block"></a>
<p style="text-align:center;margin:14px 0 20px"><a href="${esc(PUBLIC_BASE)}" style="background:#05B050;color:#fff;padding:12px 22px;border-radius:6px;text-decoration:none;font-weight:bold">▶ Bekijk de video (${nl0(dag.duur || 0)} s)</a></p>
${zinnen}
${metStop || ''}
<p style="font-size:12px;color:#7b8794;border-top:1px solid #e5e9ef;padding-top:12px;margin-top:20px">Energie-Compas · een dienst van Fluctus.net CVSO · BE 0757 494 180${dag.metStem ? ' · De stem in de video is gegenereerd met AI.' : ''}<br>Bronnen: Elia Open Data, ENTSO-E/Energy-Charts, Open-Meteo, GIE AGSI+, TTF via Yahoo Finance.</p></div>`;
  }
  async function vetomail(dag) {
    const to = (opt.reviewTo || process.env.EWB_REVIEW_TO || process.env.AUDIT_MAIL_TO || '').split(',').map(s => s.trim()).filter(Boolean);
    if (!to.length || !process.env.BREVO_API_KEY) { log('vetomail: geen ontvanger of BREVO_API_KEY'); return false; }
    const stop = `${API_BASE}/api/ewb/stop?d=${dag.datum}&s=${sign('stop|' + dag.datum)}`;
    const mode = MODE();
    const knop = `<div style="background:#fff4e5;border:1px solid #f0a500;border-radius:8px;padding:14px 16px;margin:18px 0"><b>Controle vóór publicatie</b> — ${mode === 'aan' ? 'gaat om 7u30 automatisch uit naar de website, de mailinglijst en LinkedIn' : 'PROEFMODUS (EWB_MODE=' + esc(mode) + '): niets wordt publiek gezet'}.<br>${dag.fouten && dag.fouten.length ? `<span style="color:#b42318">Ontbrekende bronnen: ${esc(dag.fouten.join('; '))}</span><br>` : ''}<a href="${esc(dag.video_url)}">Video openen</a> · <a href="${esc(stop)}" style="color:#b42318;font-weight:bold">⛔ Tegenhouden</a></div>`;
    await brevo('/smtp/email', 'POST', { sender: afzender(), to: to.map(email => ({ email })), subject: `Controle energieweerbericht ${datumLang(dag.datum)}${mode === 'aan' ? ' — publicatie 7u30' : ' (proef)'}`, htmlContent: mailHtml(dag, null, knop) });
    return true;
  }
  async function lijstId() {
    const st = await state(); if (st.lijst_id) return st.lijst_id;
    const f = await brevo('/contacts/folders?limit=1&offset=0', 'GET');
    let folderId = f && f.folders && f.folders[0] && f.folders[0].id;
    if (!folderId) folderId = (await brevo('/contacts/folders', 'POST', { name: 'Energie-Compas' })).id;
    st.lijst_id = (await brevo('/contacts/lists', 'POST', { name: 'Energieweerbericht', folderId })).id; await bewaarState();
    return st.lijst_id;
  }
  async function syncAdviseurs() {
    const st = await state(); const lid = await lijstId(); st.adviseurs = st.adviseurs || [];
    const prof = await jget(`${SB}/rest/v1/profiles?select=auth_uid,role`, { headers: sbH() });
    const uids = new Set(prof.filter(p => ['seller', 'manager'].includes(p.role) && p.auth_uid).map(p => p.auth_uid));
    const users = await jget(`${SB}/auth/v1/admin/users?per_page=1000`, { headers: sbH() });
    let n = 0;
    for (const u of (users.users || [])) {
      if (!uids.has(u.id) || !u.email || st.adviseurs.includes(u.email.toLowerCase())) continue;
      try { await brevo('/contacts', 'POST', { email: u.email, listIds: [lid], updateEnabled: true, attributes: { BRON: 'adviseur' } }); st.adviseurs.push(u.email.toLowerCase()); n++; } catch (e) { log('adviseur', e.message); }
    }
    if (n) await bewaarState();
    return n;
  }
  async function campagne(dag) {
    const lid = await lijstId();
    const html = mailHtml(dag, null, `<p style="font-size:15px;margin:16px 0"><b>Wil je weten wat dit voor jouw bedrijf betekent?</b> Mail je energiefactuur naar <a href="mailto:${CTA_MAIL}">${CTA_MAIL}</a> en je krijgt een persoonlijke voorspelling.</p>`)
      + `<p style="font-size:11px;color:#9aa5b1;text-align:center">Je ontvangt dit omdat je je inschreef of adviseur bent bij Energie-Compas. <a href="{{ unsubscribe }}">Uitschrijven</a></p>`;
    const c = await brevo('/emailCampaigns', 'POST', { name: `Energieweerbericht ${dag.datum}`, subject: `⚡ Energieweerbericht ${datumLang(dag.datum)}`, sender: afzender(), htmlContent: html, recipients: { listIds: [lid] } });
    await brevo(`/emailCampaigns/${c.id}/sendNow`, 'POST');
    return c.id;
  }

  // ── LinkedIn ───────────────────────────────────────────────────────────────────────────────
  const LI_VER = () => process.env.LINKEDIN_VERSION || '202510';
  const liRedirect = () => `${API_BASE}/api/ewb/linkedin/callback`;
  async function liToken() { const t = await sbGetJson('ewb-priv', 'linkedin.json'); return t && t.access_token && Date.now() < t.verloopt ? t : null; }
  async function liPost(dag) {
    const t = await liToken(); if (!t) throw new Error('LinkedIn niet gekoppeld of token verlopen');
    const H = { Authorization: `Bearer ${t.access_token}`, 'LinkedIn-Version': LI_VER(), 'X-Restli-Protocol-Version': '2.0.0', 'content-type': 'application/json' };
    const buf = fs.readFileSync(dag._video_lokaal);
    const init = await jget('https://api.linkedin.com/rest/videos?action=initializeUpload', { method: 'POST', headers: H, body: JSON.stringify({ initializeUploadRequest: { owner: t.urn, fileSizeBytes: buf.length, uploadCaptions: false, uploadThumbnail: false } }) });
    const v = init.value; const etags = [];
    for (const ins of v.uploadInstructions) {
      const r = await fetch(ins.uploadUrl, { method: 'PUT', headers: { 'content-type': 'application/octet-stream' }, body: buf.subarray(ins.firstByte, ins.lastByte + 1) });
      if (!r.ok) throw new Error('LinkedIn upload ' + r.status); etags.push(r.headers.get('etag'));
    }
    const f = await fetch('https://api.linkedin.com/rest/videos?action=finalizeUpload', { method: 'POST', headers: H, body: JSON.stringify({ finalizeUploadRequest: { video: v.video, uploadToken: v.uploadToken || '', uploadedPartIds: etags } }) });
    if (!f.ok) throw new Error('LinkedIn finalize ' + f.status + ' ' + (await f.text()).slice(0, 200));
    for (let i = 0; i < 24; i++) { // wachten op verwerking (max ± 2 min)
      const s = await jget(`https://api.linkedin.com/rest/videos/${encodeURIComponent(v.video)}`, { headers: H }).catch(() => null);
      if (s && s.status === 'AVAILABLE') break; if (s && s.status === 'PROCESSING_FAILED') throw new Error('LinkedIn verwerking mislukt');
      await new Promise(r => setTimeout(r, 5000));
    }
    const m = dag.script.find(s => s.id === 'actie');
    const tekst = `⚡ Energieweerbericht ${datumLang(dag.datum)}\n\n${(m && m.zinnen[0]) || ''}\n\nWil je weten wat dit voor jouw bedrijf betekent? Mail je energiefactuur naar ${CTA_MAIL} en je krijgt een persoonlijke voorspelling.\n\nElke werkdag om 7u30: ${PUBLIC_BASE}\n\n#energie #stroomprijs #NetBeterGebruiken${dag.metStem ? '\n(Stem gegenereerd met AI.)' : ''}`;
    const p = await fetch('https://api.linkedin.com/rest/posts', { method: 'POST', headers: H, body: JSON.stringify({ author: t.urn, commentary: tekst.replace(/([\\|{}@\[\]()<>#*_~])/g, '\\$1'), visibility: 'PUBLIC', distribution: { feedDistribution: 'MAIN_FEED', targetEntities: [], thirdPartyDistributionChannels: [] }, content: { media: { title: `Energieweerbericht ${datumLang(dag.datum)}`, id: v.video } }, lifecycleState: 'PUBLISHED', isReshareDisabledByAuthor: false }) });
    if (!p.ok) throw new Error('LinkedIn post ' + p.status + ' ' + (await p.text()).slice(0, 300));
    return p.headers.get('x-restli-id') || 'ok';
  }

  // ── dagelijkse keten ───────────────────────────────────────────────────────────────────────
  let bezig = false;
  async function genereer(datum, { force = false } = {}) {
    const st = await state(); if (bezig) throw new Error('al bezig');
    if (st.dagen[datum] && st.dagen[datum].video_url && !force) return st.dagen[datum];
    bezig = true; const t0 = Date.now();
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ewb-'));
    try {
      const d = await haalData(datum);
      const vorigeDatum = Object.keys(st.dagen).filter(k => k < datum).sort().pop();
      if (d.gas && d.gas.ttf && d.gas.ttf.prijs) { st.ttf = st.ttf || {}; st.ttf[datum] = d.gas.ttf.prijs; }
      if (d.gasHist && d.gasHist.length && st.ttf) { // maanden na de laatste IMF-maand aanvullen met het gemiddelde van de eigen dagwaarden
        const laatste = d.gasHist[d.gasHist.length - 1].m, per = {};
        for (const [dg, v] of Object.entries(st.ttf)) { const m = dg.slice(0, 7); if (m > laatste) (per[m] = per[m] || []).push(v); }
        for (const m of Object.keys(per).sort()) d.gasHist.push({ m, v: Math.round(per[m].reduce((a, b) => a + b, 0) / per[m].length * 10) / 10, bron: 'ttf' });
      }
      const A = analyse(d, vorigeDatum ? st.dagen[vorigeDatum].kern : null);
      A.weerFoto = await weerFoto(weerType(A.weerV && A.weerV.code), datum);
      A.themaFoto = A.verhaal && A.verhaal !== 'weer' ? await weerFoto(null, datum, A.verhaal) : null;
      _stemVanDag = stemVoor(datum);
      const script = maakScript(A);
      const r = await render(d, A, script, dir);
      const v = `${datum}/video.mp4`, po = `${datum}/poster.jpg`;
      await sbPut('ewb', v, fs.readFileSync(r.video), 'video/mp4');
      await sbPut('ewb', po, fs.readFileSync(r.poster), 'image/jpeg');
      const dag = { datum, versie: EWB_VERSIE, gegenereerd: new Date().toISOString(), duur: r.duur, metStem: r.metStem, stem: r.metStem ? _stemVanDag : null, ttsFout: r.ttsFout || undefined,
        video_url: publiekeUrl(v) + '?v=' + Date.now(), poster_url: publiekeUrl(po) + '?v=' + Date.now(), script, fouten: d.fouten,
        kern: { gemV: A.gemV, richtingMorgen: A.richtingMorgen, goedkoop: A.goedkoop, duur: A.duur, negatief: A.negatief, week: A.week, mix: A.mix, netImport: A.netImport, gas: A.gas, check: A.check, verhaal: A.verhaal, pVandaag: A.pVandaag, weerV: A.weerV },
        status: (st.dagen[datum] && st.dagen[datum].status) || 'klaar' };
      if (force && dag.status === 'tegengehouden') dag.status = 'klaar';
      st.dagen[datum] = dag; await bewaarState();
      dag._video_lokaal = r.video;
      try { if (await vetomail(dag)) { dag.vetomail = new Date().toISOString(); } } catch (e) { log('vetomail fout:', e.message); dag.vetomail_fout = e.message; }
      _laatsteVideo = { datum, pad: r.video, dir };
      await bewaarState();
      log(`gegenereerd ${datum} in ${Math.round((Date.now() - t0) / 1000)} s, ${nl0(r.duur)} s video, stem=${r.metStem}, fouten=${d.fouten.length}`);
      return dag;
    } catch (e) { fs.rmSync(dir, { recursive: true, force: true }); throw e; }
    finally { bezig = false; }
  }
  let _laatsteVideo = null;
  async function publiceer(datum) {
    const st = await state(); const dag = st.dagen[datum];
    if (!dag || !dag.video_url) throw new Error('geen video voor ' + datum);
    if (dag.status === 'tegengehouden') return { overgeslagen: 'tegengehouden' };
    if (dag.gepubliceerd) return { al: dag.gepubliceerd };
    const res = { web: true };
    dag.status = 'gepubliceerd'; dag.gepubliceerd = new Date().toISOString(); st.vandaag = datum; await bewaarState();
    try { res.adviseurs_nieuw = await syncAdviseurs(); } catch (e) { res.adviseurs_fout = e.message; }
    try { res.campagne = await campagne(dag); } catch (e) { res.campagne_fout = e.message; }
    try {
      if (!_laatsteVideo || _laatsteVideo.datum !== datum || !fs.existsSync(_laatsteVideo.pad)) {
        const p = path.join(os.tmpdir(), `ewb-${datum}.mp4`); const r = await fetch(dag.video_url); fs.writeFileSync(p, Buffer.from(await r.arrayBuffer())); dag._video_lokaal = p;
      } else dag._video_lokaal = _laatsteVideo.pad;
      res.linkedin = await liPost(dag);
    } catch (e) { res.linkedin_fout = e.message; }
    delete dag._video_lokaal; dag.publicatie = res; await bewaarState();
    log('gepubliceerd', datum, JSON.stringify(res));
    return res;
  }
  async function liHerinnering() {
    const t = await sbGetJson('ewb-priv', 'linkedin.json'); if (!t || !t.verloopt) return;
    const dagen = (t.verloopt - Date.now()) / 864e5; const st = await state();
    if (dagen < 10 && st.li_herinnerd !== t.verloopt) {
      const to = (opt.reviewTo || process.env.EWB_REVIEW_TO || process.env.AUDIT_MAIL_TO || '').split(',').map(s => s.trim()).filter(Boolean);
      if (to.length) await brevo('/smtp/email', 'POST', { sender: afzender(), to: to.map(email => ({ email })), subject: `LinkedIn-koppeling energieweerbericht verloopt over ${Math.max(0, Math.round(dagen))} dagen`, htmlContent: `<p>De LinkedIn-koppeling verloopt over ${Math.round(dagen)} dagen. Eén klik om te verlengen:</p><p><a href="${API_BASE}/api/ewb/linkedin/start?s=${sign('li')}">LinkedIn opnieuw koppelen</a></p>` });
      st.li_herinnerd = t.verloopt; await bewaarState();
    }
  }
  // tik elke minuut
  let tikBezig = false;
  setInterval(async () => {
    if (tikBezig || MODE() === 'uit' || !SB || !SBK) return; tikBezig = true;
    try {
      const b = brussel(); if (['Sat', 'Sun'].includes(b.wd)) return;
      const m = b.uur * 60 + b.min; const st = await state(); const dag = st.dagen[b.datum];
      if (m >= 6 * 60 + 15 && m < 9 * 60 && !(dag && dag.video_url) && !bezig) {
        const f = (st.pogingen = st.pogingen || {}); if ((f[b.datum] || 0) < 3) { f[b.datum] = (f[b.datum] || 0) + 1; await bewaarState(); await genereer(b.datum).catch(e => log('genereer fout:', e.message)); }
      }
      const klaar = st.dagen[b.datum];
      if (MODE() === 'aan' && klaar && klaar.video_url && !klaar.gepubliceerd && klaar.status !== 'tegengehouden') {
        const vetoTot = Date.parse(klaar.gegenereerd) + 15 * 60e3; // minstens 15 min tussen vetomail en publicatie
        if (m >= 7 * 60 + 30 && Date.now() >= vetoTot) await publiceer(b.datum).catch(e => log('publiceer fout:', e.message));
      }
      if (b.uur === 9 && b.min === 0) await liHerinnering().catch(() => {});
    } catch (e) { log('tik:', e.message); } finally { tikBezig = false; }
  }, 60e3);

  // ── manager-check (Supabase-sessie) ────────────────────────────────────────────────────────
  async function isManager(req) {
    const tok = (req.headers.authorization || '').replace(/^Bearer\s+/i, ''); if (!tok) return false;
    try {
      const u = await jget(`${SB}/auth/v1/user`, { headers: { apikey: SBK, Authorization: `Bearer ${tok}` } });
      const p = await jget(`${SB}/rest/v1/profiles?select=role&auth_uid=eq.${u.id}`, { headers: sbH() });
      return p && p[0] && p[0].role === 'manager';
    } catch (_) { return false; }
  }
  const pagina = (titel, tekst) => `<!DOCTYPE html><html lang="nl"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>${esc(titel)}</title><body style="font-family:Arial,sans-serif;max-width:560px;margin:60px auto;padding:0 20px;color:#1b2a3d"><h2 style="color:#05B050">${esc(titel)}</h2><p>${tekst}</p><p><a href="${esc(PUBLIC_BASE)}">Naar het energieweerbericht</a></p></body></html>`;

  // ── routes ─────────────────────────────────────────────────────────────────────────────────
  app.get('/api/ewb/vandaag', async (req, res) => {
    try {
      const st = await state(); const pre = req.query.preview && req.query.s === sign('preview');
      let datum = req.query.d && /^\d{4}-\d{2}-\d{2}$/.test(req.query.d) ? req.query.d : null;
      const ok = k => st.dagen[k] && st.dagen[k].video_url && (pre || st.dagen[k].gepubliceerd);
      if (!datum) datum = Object.keys(st.dagen).filter(ok).sort().pop();
      if (!datum || !ok(datum)) return res.json({ ok: false, reden: 'nog geen aflevering' });
      const dg = st.dagen[datum];
      res.set('cache-control', 'public, max-age=120');
      res.json({ ok: true, versie: EWB_VERSIE, datum, titel: `Energieweerbericht ${datumLang(datum)}`, video: dg.video_url, poster: dg.poster_url, duur: dg.duur, metStem: dg.metStem, script: dg.script.map(s => ({ id: s.id, titel: s.titel, zinnen: s.tekst || s.zinnen })), kern: dg.kern, cta_mail: CTA_MAIL,
        archief: Object.keys(st.dagen).filter(ok).sort().reverse().slice(0, 30) });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });
  app.get('/api/ewb/stop', async (req, res) => {
    const d = String(req.query.d || ''); if (!/^\d{4}-\d{2}-\d{2}$/.test(d) || req.query.s !== sign('stop|' + d)) return res.status(403).send(pagina('Ongeldige link', 'Deze link is niet (meer) geldig.'));
    const st = await state(); st.dagen[d] = st.dagen[d] || { datum: d };
    if (st.dagen[d].gepubliceerd) return res.send(pagina('Al gepubliceerd', `De aflevering van ${esc(datumLang(d))} is al om ${esc(st.dagen[d].gepubliceerd.slice(11, 16))} UTC gepubliceerd.`));
    st.dagen[d].status = 'tegengehouden'; st.dagen[d].tegengehouden = new Date().toISOString(); await bewaarState();
    res.send(pagina('Tegengehouden', `De aflevering van ${esc(datumLang(d))} wordt niet gepubliceerd. <a href="${API_BASE}/api/ewb/vrijgeven?d=${d}&s=${sign('vrij|' + d)}">Toch vrijgeven</a>`));
  });
  app.get('/api/ewb/vrijgeven', async (req, res) => {
    const d = String(req.query.d || ''); if (req.query.s !== sign('vrij|' + d)) return res.status(403).send(pagina('Ongeldige link', ''));
    const st = await state(); if (!st.dagen[d]) return res.status(404).send(pagina('Niet gevonden', ''));
    st.dagen[d].status = 'klaar'; await bewaarState();
    const r = MODE() === 'aan' ? await publiceer(d).catch(e => ({ fout: e.message })) : { proef: true };
    res.send(pagina('Vrijgegeven', `Aflevering ${esc(datumLang(d))}: ${esc(JSON.stringify(r))}`));
  });
  app.post('/api/ewb/run', async (req, res) => {
    if (!(await isManager(req))) return res.status(401).json({ error: 'Manager-login vereist' });
    try {
      const datum = (req.body && req.body.datum) || brussel().datum;
      const dag = await genereer(datum, { force: true });
      let pub = null; if (req.body && req.body.publiceer && MODE() === 'aan') pub = await publiceer(datum);
      res.json({ ok: true, datum, video: dag.video_url, duur: dag.duur, metStem: dag.metStem, stem: dag.stem, ttsFout: dag.ttsFout, fouten: dag.fouten, vetomail: !!dag.vetomail, preview: `${PUBLIC_BASE}?preview=1&s=${sign('preview')}`, publicatie: pub });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });
  app.get('/api/ewb/status', async (req, res) => {
    if (!(await isManager(req))) return res.status(401).json({ error: 'Manager-login vereist' });
    const st = await state(); const li = await sbGetJson('ewb-priv', 'linkedin.json');
    res.json({ versie: EWB_VERSIE, mode: MODE(), tts: process.env.TTS_PROVIDER || null, lijst_id: st.lijst_id || null, adviseurs: (st.adviseurs || []).length, linkedin: li ? { naam: li.naam, verloopt: new Date(li.verloopt).toISOString() } : null, dagen: Object.fromEntries(Object.entries(st.dagen).slice(-7).map(([k, v]) => [k, { status: v.status, duur: v.duur, gepubliceerd: v.gepubliceerd, publicatie: v.publicatie, fouten: v.fouten }])),
      linkedin_koppelen: process.env.LINKEDIN_CLIENT_ID ? `${API_BASE}/api/ewb/linkedin/start?s=${sign('li')}` : 'LINKEDIN_CLIENT_ID ontbreekt' });
  });
  // inschrijven (dubbele bevestiging)
  app.post('/api/ewb/inschrijven', async (req, res) => {
    try {
      const email = String((req.body && req.body.email) || '').trim().toLowerCase();
      if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return res.status(400).json({ error: 'ongeldig e-mailadres' });
      const t = Buffer.from(JSON.stringify({ e: email, t: Date.now() })).toString('base64url');
      const link = `${API_BASE}/api/ewb/bevestig?t=${t}&s=${sign('ins|' + t)}`;
      await brevo('/smtp/email', 'POST', { sender: afzender(), to: [{ email }], subject: 'Bevestig je inschrijving op het energieweerbericht', htmlContent: `<div style="font-family:Arial,sans-serif;max-width:560px;color:#1b2a3d"><p>Bedankt voor je interesse in het dagelijkse energieweerbericht van Energie-Compas: elke werkdag om 7u30 stroomprijs, weer en markt in 90 seconden.</p><p><a href="${link}" style="background:#05B050;color:#fff;padding:12px 20px;border-radius:6px;text-decoration:none;font-weight:bold">Ja, schrijf mij in</a></p><p style="font-size:12px;color:#7b8794">Heb je dit niet aangevraagd? Dan hoef je niets te doen.</p></div>` });
      res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });
  app.get('/api/ewb/bevestig', async (req, res) => {
    const t = String(req.query.t || ''); if (req.query.s !== sign('ins|' + t)) return res.status(403).send(pagina('Ongeldige link', ''));
    try {
      const { e, t: ts } = JSON.parse(Buffer.from(t, 'base64url').toString());
      if (Date.now() - ts > 14 * 864e5) return res.send(pagina('Link verlopen', 'Schrijf je opnieuw in via de pagina.'));
      await brevo('/contacts', 'POST', { email: e, listIds: [await lijstId()], updateEnabled: true, attributes: { BRON: 'website' } });
      res.send(pagina('Ingeschreven', 'Je ontvangt het energieweerbericht voortaan elke werkdag om 7u30.'));
    } catch (er) { res.status(500).send(pagina('Er liep iets mis', esc(er.message))); }
  });
  // LinkedIn OAuth
  app.get('/api/ewb/linkedin/start', (req, res) => {
    if (req.query.s !== sign('li')) return res.status(403).send(pagina('Ongeldige link', ''));
    if (!process.env.LINKEDIN_CLIENT_ID) return res.status(503).send(pagina('Niet ingesteld', 'LINKEDIN_CLIENT_ID ontbreekt op de server.'));
    const state_ = sign('listate|' + Math.floor(Date.now() / 6e5));
    res.redirect(`https://www.linkedin.com/oauth/v2/authorization?response_type=code&client_id=${encodeURIComponent(process.env.LINKEDIN_CLIENT_ID)}&redirect_uri=${encodeURIComponent(liRedirect())}&state=${state_}&scope=${encodeURIComponent('openid profile w_member_social')}`);
  });
  app.get('/api/ewb/linkedin/callback', async (req, res) => {
    try {
      const ok = [0, 1].some(i => req.query.state === sign('listate|' + (Math.floor(Date.now() / 6e5) - i)));
      if (!ok || !req.query.code) return res.status(403).send(pagina('Koppeling mislukt', esc(req.query.error_description || 'ongeldige state')));
      const body = new URLSearchParams({ grant_type: 'authorization_code', code: req.query.code, redirect_uri: liRedirect(), client_id: process.env.LINKEDIN_CLIENT_ID, client_secret: process.env.LINKEDIN_CLIENT_SECRET || '' });
      const tok = await jget('https://www.linkedin.com/oauth/v2/accessToken', { method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded' }, body });
      const me = await jget('https://api.linkedin.com/v2/userinfo', { headers: { Authorization: `Bearer ${tok.access_token}` } });
      await sbPut('ewb-priv', 'linkedin.json', JSON.stringify({ access_token: tok.access_token, verloopt: Date.now() + (tok.expires_in || 5184000) * 1000, urn: `urn:li:person:${me.sub}`, naam: me.name || '', gekoppeld: new Date().toISOString() }), 'application/json');
      res.send(pagina('LinkedIn gekoppeld', `Het energieweerbericht post voortaan als ${esc(me.name || 'u')}. De koppeling blijft ${Math.round((tok.expires_in || 5184000) / 86400)} dagen geldig; tien dagen vooraf krijgt u een mail om te verlengen.`));
    } catch (e) { res.status(500).send(pagina('Koppeling mislukt', esc(e.message))); }
  });

  log(`gemonteerd v${EWB_VERSIE} (mode=${MODE()}, tts=${process.env.TTS_PROVIDER || 'geen'})`);
  return { genereer, publiceer, _intern: { analyse, maakScript, sceneHtml, render, haalData, brussel } };
};
