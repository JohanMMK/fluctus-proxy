// ============================================================================
// bl-ruimte.js — Betere Lader: KWARTIERANALYSE van de ruimte op de aansluiting (v1.0.1, 2026-11-18, Johan; 1.0.1: extra_dom_kw/verzwaren_dom terug)
// ----------------------------------------------------------------------------
// Niet «toegangsvermogen − piek» (dat is één kwartier per maand), maar: hoeveel ENERGIE kan er door de
// aansluiting tijdens de uren dat de wagens er staan, zonder het toegangsvermogen te overschrijden.
//
// 1. Synthetisch jaarprofiel (35.040 kwartieren, kalender 2025) uit de factuur:
//    vorm = sectorprofiel (bv. industrie met zomersluiting) · dag/nacht-verhouding = gemeten normale/stille uren
//    van de factuur · jaartotaal = jaarverbruik op de factuur, anders het factuurvolume geëxtrapoleerd MET het
//    profiel (een decembermaand met sluiting telt dan niet ×12).
// 2. Per laaddag (werkdag, of elke dag bij thuisladen) het laadvenster (aankomst + duur).
// 3. SLIM: vul de dalen onder de grens (toegangsvermogen − 10 % marge) — eerst onder de huidige piek (geen extra
//    capaciteitskost), dan pas hoger; binnen die ruimte de goedkoopste spotkwartieren.
//    DOM: elke wagen laadt meteen bij aankomst aan vol vermogen (aankomst gespreid over het eerste uur).
// 4. Vergelijking: extra kW boven de huidige piek × €/kW/jaar → €/100 km, en spotprijs dom vs slim → €/100 km.
// Enkel energy arbitrage op de spotmarkt (day-ahead) als prijssignaal.
// ============================================================================
'use strict';

const Q = 35040, QD = 96, DAG0_WEEKDAG = 3;   // 1 jan 2025 = woensdag (0 = zondag)
const MARGE = 0.10;                            // 10 % onder het toegangsvermogen: cos φ en korte pieken die een profiel niet toont

function _datum(dag) { const d = new Date(Date.UTC(2025, 0, 1 + dag)); return d; }
function _weekdag(dag) { return (DAG0_WEEKDAG + dag) % 7; }
function _normaal(q) { const dag = Math.floor(q / QD), u = Math.floor((q % QD) / 4), w = _weekdag(dag); return w >= 1 && w <= 5 && u >= 7 && u < 22; }
function _doy(iso) { const d = new Date(String(iso).slice(0, 10) + 'T00:00:00Z'); if (isNaN(d)) return null; return Math.floor((Date.UTC(2025, d.getUTCMonth(), d.getUTCDate()) - Date.UTC(2025, 0, 1)) / 864e5); }
function _pct(arr, p) { if (!arr.length) return 0; const a = arr.slice().sort((x, y) => x - y); return a[Math.min(a.length - 1, Math.max(0, Math.floor(a.length * p)))]; }

// Spotprijs per profielkwartier: zelfde kalenderdag in het marktvenster, verschoven tot dezelfde weekdag.
function _spotKaart(markt) {
  if (!markt || !Array.isArray(markt.spot_q) || markt.spot_q.length < QD * 28 || !markt.van) return null;
  const sp = markt.spot_q, nD = Math.floor(sp.length / QD), t0 = new Date(String(markt.van).slice(0, 10) + 'T00:00:00Z');
  if (isNaN(t0)) return null;
  const w0 = t0.getUTCDay(), kaart = new Int32Array(365);
  for (let dag = 0; dag < 365; dag++) {
    const d = _datum(dag); let best = 0;
    for (const y of [t0.getUTCFullYear(), t0.getUTCFullYear() + 1]) {
      const k = Math.floor((Date.UTC(y, d.getUTCMonth(), d.getUTCDate()) - t0) / 864e5);
      if (k >= 0 && k < nD) { best = k; break; }
    }
    let delta = ((_weekdag(dag) - (w0 + best) % 7) + 7) % 7; if (delta > 3) delta -= 7;
    kaart[dag] = Math.max(0, Math.min(nD - 1, best + delta));
  }
  return q => { const v = +sp[kaart[Math.floor(q / QD) % 365] * QD + (q % QD)]; return isFinite(v) ? v : 0; };
}

function bouwProfiel(vorm, o) {
  const s = Float64Array.from(vorm.slice(0, Q)); let som = 0; for (let i = 0; i < Q; i++) som += (s[i] = Math.max(0, +s[i] || 0));
  if (!(som > 0)) throw new Error('leeg profiel');
  for (let i = 0; i < Q; i++) s[i] /= som;
  // factuurperiode in profieldagen
  let d0 = o.periodeVan ? _doy(o.periodeVan) : null, d1 = o.periodeTot ? _doy(o.periodeTot) : null;
  const inPer = (q) => { if (d0 == null || d1 == null) return true; const dag = Math.floor(q / QD); return d0 <= d1 ? (dag >= d0 && dag <= d1) : (dag >= d0 || dag <= d1); };
  // dag/nacht: herweeg normale ↔ stille uren naar de gemeten verhouding
  let herweging = null;
  if (o.normaal_aandeel > 0.05 && o.normaal_aandeel < 0.98) {
    let n = 0, t = 0; for (let i = 0; i < Q; i++) if (inPer(i)) { t += s[i]; if (_normaal(i)) n += s[i]; }
    const np = t > 0 ? n / t : 0;
    if (np > 0.02 && np < 0.99) {
      const a = o.normaal_aandeel / np, b = (1 - o.normaal_aandeel) / (1 - np); let z = 0;
      for (let i = 0; i < Q; i++) { s[i] *= _normaal(i) ? a : b; z += s[i]; }
      for (let i = 0; i < Q; i++) s[i] /= z;
      herweging = { profiel: Math.round(np * 1000) / 10, factuur: Math.round(o.normaal_aandeel * 1000) / 10 };
    }
  }
  // jaarverbruik
  let jaarKwh = +o.jaar_kwh || 0, bron = 'jaarverbruik op de factuur';
  if (!(jaarKwh > 0) && +o.afname_kwh > 0 && d0 != null && d1 != null) {
    let aandeel = 0; for (let i = 0; i < Q; i++) if (inPer(i)) aandeel += s[i];
    if (aandeel > 0.02) { jaarKwh = o.afname_kwh / aandeel; bron = 'factuurperiode geëxtrapoleerd met het profiel'; }
  }
  if (!(jaarKwh > 0) && +o.afname_kwh > 0) { jaarKwh = +o.afname_kwh * 12; bron = 'factuur × 12 (ruw)'; }
  if (!(jaarKwh > 0)) throw new Error('geen verbruik');
  const L = new Float64Array(Q); for (let i = 0; i < Q; i++) L[i] = s[i] * jaarKwh * 4;   // kW per kwartier
  return { L, jaarKwh, bron, herweging };
}

// Laaddagen + venster (indexen, kan over middernacht lopen)
function vensters(L, o) {
  const dagE = new Float64Array(365); for (let i = 0; i < Q; i++) dagE[Math.floor(i / QD)] += L[i] / 4;
  const med = _pct(Array.from(dagE), 0.5);
  const start = o.doelgroep === 'kmo' ? 8 : 18, lengte = Math.max(4, Math.min(96, Math.round((+o.venster_u || 8) * 4)));
  const out = [];
  for (let dag = 0; dag < 365; dag++) {
    const w = _weekdag(dag);
    if (o.doelgroep !== 'thuisladen') { if (w === 0 || w === 6) continue; if (dagE[dag] < 0.4 * med) continue; }   // weekend, sluiting, feestdag: geen wagens
    const qs = []; for (let k = 0; k < lengte; k++) qs.push((dag * QD + start * 4 + k) % Q);
    out.push({ dag, maand: _datum(dag).getUTCMonth(), qs });
  }
  return out;
}

function ruimte(L, dagen, grens) {
  return dagen.map(d => d.qs.reduce((e, q) => e + Math.max(0, grens - L[q]) * 0.25, 0));
}

// minimaal niveau λ zodat D kWh past onder λ met maximaal C kW laadvermogen
function _niveau(L, qs, D, C, hoog) {
  const lever = (lam) => qs.reduce((e, q) => e + Math.min(C, Math.max(0, lam - L[q])) * 0.25, 0);
  if (lever(hoog) < D - 1e-6) return null;
  let lo = 0, hi = hoog; for (let it = 0; it < 40; it++) { const m = (lo + hi) / 2; if (lever(m) >= D) hi = m; else lo = m; }
  return hi;
}

function simHorizon(L, dagen, h, o, spot) {
  const D = +h.edag_kwh || 0, C = Math.max(0.1, (+h.lp || 0) * (+h.paal_kw || 11)), grens = o.grens, nivo0 = o.nivo0;
  const res = { ev: +h.ev || 0, dag_kwh: Math.round(D), laadvermogen_kw: Math.round(C) };
  if (!(D > 0)) return res;
  let maxSlim = 0, maxDom = 0, kortDagen = 0, eSlim = 0, eDom = 0, kSlim = 0, kDom = 0;
  const gemSlim = new Float64Array(QD), gemDom = new Float64Array(QD);
  for (const d of dagen) {
    // SLIM: niveau zo laag mogelijk, minstens tot de huidige piek (die kost niets extra)
    const lam = _niveau(L, d.qs, D, C, grens);
    const lvl = lam == null ? grens : Math.max(nivo0, lam);
    if (lam == null) kortDagen++;
    maxSlim = Math.max(maxSlim, lam == null ? grens : lam);
    const volg = spot ? d.qs.slice().sort((a, b) => spot(a) - spot(b)) : d.qs;
    let rest = D;
    for (const q of volg) { if (rest <= 0) break; const p = Math.min(C, Math.max(0, lvl - L[q]), rest * 4); if (p <= 0) continue; rest -= p * 0.25; eSlim += p * 0.25; if (spot) kSlim += p * 0.25 * spot(q); gemSlim[q % QD] += p; }
    // DOM: aankomst gespreid over het eerste uur (4 cohorten), elk laadt meteen aan vol vermogen
    const coh = [D / 4, D / 4, D / 4, D / 4];
    d.qs.forEach((q, k) => {
      let p = 0; for (let c = 0; c < 4; c++) if (k >= c && coh[c] > 0) { const pc = Math.min(C / 4, coh[c] * 4); coh[c] -= pc * 0.25; p += pc; }
      if (p > 0) { maxDom = Math.max(maxDom, L[q] + p); eDom += p * 0.25; if (spot) kDom += p * 0.25 * spot(q); gemDom[q % QD] += p; }
    });
  }
  const n = dagen.length || 1, km100 = (+h.km_jaar || 0) / 100, kwhkm = +h.kwh_km || 0.16, eur = o.eur_kw;
  const extraSlim = Math.max(0, maxSlim - nivo0), extraDom = Math.max(0, maxDom - nivo0);
  Object.assign(res, {
    past: kortDagen <= Math.floor(n * 0.05),          // past op ≥ 95 % van de laaddagen
    dagen_te_kort: kortDagen,
    niveau_slim_kw: Math.round(maxSlim), piek_slim_kw: Math.round(Math.max(nivo0, maxSlim)), extra_slim_kw: Math.round(extraSlim),
    piek_dom_kw: Math.round(Math.max(nivo0, maxDom)), extra_dom_kw: Math.round(extraDom), verzwaren_dom: maxDom > o.toeg,   // piek dom: je hoogste kwartier blijft sowieso staan
    net_eur_jaar_slim: Math.round(extraSlim * eur), net_eur_jaar_dom: Math.round(extraDom * eur),
    net_eur100_slim: km100 > 0 ? extraSlim * eur / km100 : null, net_eur100_dom: km100 > 0 ? extraDom * eur / km100 : null,
    gem_slim_kw: Array.from(gemSlim, v => Math.round(v / n)), gem_dom_kw: Array.from(gemDom, v => Math.round(v / n)),
  });
  if (spot && eSlim > 0 && eDom > 0) {
    const pS = kSlim / eSlim, pD = kDom / eDom;   // €/MWh
    Object.assign(res, { spot_slim_eur_mwh: Math.round(pS * 10) / 10, spot_dom_eur_mwh: Math.round(pD * 10) / 10,
      spot_eur100: (pD - pS) / 1000 * kwhkm * 100, spot_eur_jaar: Math.round((kDom / eDom - kSlim / eSlim) / 1000 * D * n) });
  }
  return res;
}

function simuleer(vorm, o, markt) {
  const P = bouwProfiel(vorm, o), L = P.L;
  const toeg = +o.toeg_kw || 0; if (!(toeg > 0)) throw new Error('toegangsvermogen ontbreekt');
  let maxL = 0; for (let i = 0; i < Q; i++) if (L[i] > maxL) maxL = L[i];
  const piek = Math.max(+o.piek_kw || 0, maxL), grens = toeg * (1 - MARGE);
  const dagen = vensters(L, o), spot = _spotKaart(markt);
  const eWagen = +o.wagen_kwh_dag > 0 ? +o.wagen_kwh_dag : 15000 / 230 * 0.16;
  const rGrens = ruimte(L, dagen, grens), rPiek = ruimte(L, dagen, Math.min(piek, grens));
  const lastVenster = []; dagen.forEach(d => d.qs.forEach(q => lastVenster.push(L[q])));
  const startMax = dagen.reduce((m, d) => Math.max(m, ...d.qs.slice(0, 4).map(q => L[q])), 0);
  const gemDag = new Float64Array(QD); dagen.forEach(d => { for (let k = 0; k < QD; k++) gemDag[k] += L[d.dag * QD + k]; });
  const opt = { grens, nivo0: piek, toeg, eur_kw: +o.eur_kw > 0 ? +o.eur_kw : 100 };
  const H = {};
  for (const k of ['nu', 'm12', 'm24', 'm36']) if (o.horizons && o.horizons[k]) H[k] = simHorizon(L, dagen, o.horizons[k], opt, spot);
  // toegangsvermogen dat volstaat: hoogste van (huidige piek, slim laadniveau van de laatste horizon) + marge
  const laatste = ['m36', 'm24', 'm12', 'nu'].map(k => H[k]).filter(x => x && x.ev > 0)[0];
  const nodig = Math.max(piek, laatste ? (laatste.niveau_slim_kw || 0) : 0) / (1 - MARGE);
  return {
    jaar_mwh: Math.round(P.jaarKwh / 100) / 10, jaar_bron: P.bron, dag_nacht: P.herweging, profiel: o.profiel,
    toeg_kw: toeg, grens_kw: Math.round(grens), piek_kw: Math.round(piek), profielpiek_kw: Math.round(maxL), gem_kw: Math.round(P.jaarKwh / 8760),
    laaddagen: dagen.length, venster: { start_u: o.doelgroep === 'kmo' ? 8 : 18, duur_u: +o.venster_u || 8 },
    last_venster_gem_kw: Math.round(lastVenster.reduce((a, b) => a + b, 0) / Math.max(1, lastVenster.length)),
    ruimte_kwh_dag_p5: Math.round(_pct(rGrens, 0.05)), ruimte_kwh_dag_med: Math.round(_pct(rGrens, 0.5)),
    ruimte_onder_piek_kwh_dag_p5: Math.round(_pct(rPiek, 0.05)),
    wagen_kwh_dag: Math.round(eWagen * 10) / 10,
    wagens_slim: Math.floor(_pct(rGrens, 0.05) / eWagen), wagens_onder_piek: Math.floor(_pct(rPiek, 0.05) / eWagen),
    wagens_dom: Math.max(0, Math.floor((grens - startMax) / Math.max(3.7, +o.paal_kw || 11))),
    toegang_volstaat_kw: Math.ceil(nodig / 5) * 5,
    dag_last_kw: Array.from(gemDag, v => Math.round(v / Math.max(1, dagen.length))),
    spot: !!spot, eur_kw: opt.eur_kw, horizons: H,
  };
}

module.exports = { simuleer, bouwProfiel, vensters, VERSIE: '1.0.1' };
