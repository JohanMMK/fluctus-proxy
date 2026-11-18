// ============================================================================
// bl-ruimte.js — Betere Lader: KWARTIERANALYSE van de ruimte op de aansluiting (v1.2.0, 2026-11-18, Johan)
// 1.2.1: gemiddeld verbruik in de laaduren zonder de korte pieken (last_venster_gem_kw), met pieken apart.
// 1.2.0: PIEKCORRECTIE — korte kwartierpieken tot de gemeten maandpiek (standaardprofiel is te glad), jaarverbruik gelijk.
// 1.1.0: (0) per dag eigen verbruik + laden, per maand de hoogste piek → netkost per maand; grafiek = de drukste laaddag
//        (a) netkost zonder sturing als VORK — beste geval (laden valt op je gemiddelde verbruik) en slechtste geval
//        (laden valt bovenop je hoogste kwartier: + het volle laadvermogen); (b) €/kW uit de tariefkaart van de DNB:
//        extra piek × maandpiektarief + wat boven het toegangsvermogen komt × toegangstarief (geen kaart → € 100/kW);
//        (c) kost per 100 km met een DYNAMISCH contract: spot (dom/slim) + opslag + vergroening + GSC + WKK +
//        netkost per MWh + accijns (marginale schijf), excl. btw.
// 1.0.1: extra_dom_kw/verzwaren_dom terug
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
  // 1.2.0 PIEKCORRECTIE: een standaardprofiel is glad (hoogste kwartier ± 2 × het gemiddelde). Ligt de gemeten maandpiek
  //   duidelijk hoger, dan leggen we per werkdag één korte kwartierpiek op een willekeurig (vast gezaaid) tijdstip, hoogte
  //   50–100 % tot de gemeten piek, en per maand minstens één die de piek haalt. Het jaarverbruik blijft gelijk.
  let pieken = null; const piekQ = new Set();
  const piek = +o.piek_kw || 0;
  if (piek > 0) {
    let maxPer = 0; for (let i = 0; i < Q; i++) if (inPer(i) && L[i] > maxPer) maxPer = L[i];
    if (piek > 1.3 * maxPer) {
      let seed = 20251; const rnd = () => { seed |= 0; seed = seed + 0x6D2B79F5 | 0; let t = Math.imul(seed ^ seed >>> 15, 1 | seed); t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t; return ((t ^ t >>> 14) >>> 0) / 4294967296; };
      const dagE = new Float64Array(365); for (let i = 0; i < Q; i++) dagE[Math.floor(i / QD)] += L[i];
      const med = _pct(Array.from(dagE), 0.5), doel = new Map(), maandBest = {};
      for (let dag = 0; dag < 365; dag++) {
        const w = _weekdag(dag), r1 = rnd(), r2 = rnd(); if (w === 0 || w === 6 || dagE[dag] < 0.4 * med) continue;
        const q = dag * QD + Math.floor(r1 * QD), f = 0.5 + 0.5 * r2, m = _datum(dag).getUTCMonth();
        doel.set(q, f * piek);
        if (!maandBest[m] || f > maandBest[m].f) maandBest[m] = { q, f };
      }
      for (const m in maandBest) doel.set(maandBest[m].q, piek);   // per maand haalt minstens één kwartier de gemeten piek
      let eExtra = 0; for (const [q, v] of doel) eExtra += Math.max(0, v - L[q]) / 4;
      const k = Math.max(0.5, (jaarKwh - eExtra) / jaarKwh);   // jaarverbruik behouden: de rest schaalt licht mee
      for (let i = 0; i < Q; i++) L[i] *= k;
      let n = 0; for (const [q, v] of doel) if (v > L[q]) { L[q] = v; n++; piekQ.add(q); }
      pieken = { aantal: n, piek_kw: Math.round(piek), profiel_max_kw: Math.round(maxPer), aanname: '1 kwartierpiek per werkdag, willekeurig tijdstip, 50–100 % van je gemeten piek' };
    }
  }
  return { L, jaarKwh, bron, herweging, pieken, piekQ };
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

// variabele kost per MWh bovenop de spot (dynamisch contract), voor een totaal jaarverbruik E (MWh)
function tariefVar(t, E) {
  if (!t) return null;
  const band = (lijst, f) => { for (const r of (lijst || [])) if (E <= f(r)) return r; return (lijst || [])[(lijst || []).length - 1]; };
  const sch = band(t.staffel, r => +r.max_mwh || 1e12) || {};
  const acc = band(t.accijns, r => +r[0]) || [0, 0];
  const d = { opslag: +sch.consumption_dam_markup || 0, vergroening: +t.vergroening || 0, gsc: +t.gsc || 0, wkk: +t.wkk || 0, net: +t.net_mwh || 0, accijns: +acc[1] || 0 };
  d.totaal = d.opslag + d.vergroening + d.gsc + d.wkk + d.net + d.accijns;
  for (const k in d) d[k] = Math.round(d[k] * 100) / 100;
  return d;
}
function simHorizon(L, dagen, h, o, spot) {
  const D = +h.edag_kwh || 0, C = Math.max(0.1, (+h.lp || 0) * (+h.paal_kw || 11)), grens = o.grens, nivo0 = o.nivo0;
  const res = { ev: +h.ev || 0, dag_kwh: Math.round(D), laadvermogen_kw: Math.round(C) };
  if (!(D > 0)) return res;
  // één laaddag: slim (dalen vullen, goedkoopste kwartieren) en dom (meteen bij aankomst) — kW per kwartier
  function dag(d) {
    const lam = _niveau(L, d.qs, D, C, grens), lvl = lam == null ? grens : Math.max(nivo0, lam);
    const slim = new Map(), dom = new Map();
    const volg = spot ? d.qs.slice().sort((a, b) => spot(a) - spot(b)) : d.qs;
    let rest = D;
    for (const q of volg) { if (rest <= 0) break; const p = Math.min(C, Math.max(0, lvl - L[q]), rest * 4); if (p <= 0) continue; rest -= p * 0.25; slim.set(q, p); }
    const coh = [D / 4, D / 4, D / 4, D / 4];   // aankomst gespreid over het eerste uur
    d.qs.forEach((q, k) => { let p = 0; for (let c = 0; c < 4; c++) if (k >= c && coh[c] > 0) { const pc = Math.min(C / 4, coh[c] * 4); coh[c] -= pc * 0.25; p += pc; } if (p > 0) dom.set(q, p); });
    return { lam, slim, dom };
  }
  let maxSlim = 0, maxDom = 0, maxEvDom = 0, kortDagen = 0, eSlim = 0, eDom = 0, kSlim = 0, kDom = 0, piekDag = null;
  const mDom = new Float64Array(12), mSlim = new Float64Array(12), mBasis = new Float64Array(12);
  for (const d of dagen) {
    const r = dag(d);
    if (r.lam == null) kortDagen++;
    maxSlim = Math.max(maxSlim, r.lam == null ? grens : r.lam);
    for (const q of d.qs) {   // site-last per kwartier = eigen verbruik + laden, per maand de hoogste
      const ps = r.slim.get(q) || 0, pd = r.dom.get(q) || 0;
      mSlim[d.maand] = Math.max(mSlim[d.maand], L[q] + ps); mBasis[d.maand] = Math.max(mBasis[d.maand], L[q]);
      if (L[q] + pd > mDom[d.maand]) mDom[d.maand] = L[q] + pd;
      if (L[q] + pd > maxDom) { maxDom = L[q] + pd; piekDag = d; }
      if (ps > 0) { eSlim += ps * 0.25; if (spot) kSlim += ps * 0.25 * spot(q); }
      if (pd > 0) { maxEvDom = Math.max(maxEvDom, pd); eDom += pd * 0.25; if (spot) kDom += pd * 0.25 * spot(q); }
    }
  }
  const n = dagen.length || 1, km100 = (+h.km_jaar || 0) / 100, kwhkm = +h.kwh_km || 0.16;
  const extraSlim = Math.max(0, maxSlim - nivo0), extraDom = Math.max(0, maxDom - nivo0);
  const piekDomMax = nivo0 + maxEvDom;                      // slechtste geval: laden valt bovenop je hoogste kwartier
  // netkost: maandpiek per MAAND (hoogste kwartier van die maand boven je huidige piek), toegangsvermogen op de jaarpiek
  const mp = o.tarief ? o.tarief.mp_eur_kw : o.eur_kw, tv = o.tarief ? o.tarief.tv_eur_kw : 0, plafond = Math.max(o.toeg, nivo0);
  const nk = (maanden, jaarPiek) => maanden.reduce((s, x) => s + Math.max(0, x - nivo0) * mp / 12, 0) + Math.max(0, jaarPiek - plafond) * tv;
  const actief = Array.from(mDom, (x, m) => x > 0 ? m : -1).filter(m => m >= 0);
  const nS = nk(actief.map(m => mSlim[m]), Math.max(nivo0, maxSlim)), nD = nk(actief.map(m => mDom[m]), maxDom), nDmax = nk(actief.map(() => piekDomMax), piekDomMax);
  Object.assign(res, {
    past: kortDagen <= Math.floor(n * 0.05),          // past op ≥ 95 % van de laaddagen
    dagen_te_kort: kortDagen,
    niveau_slim_kw: Math.round(maxSlim), piek_slim_kw: Math.round(Math.max(nivo0, maxSlim)), extra_slim_kw: Math.round(extraSlim),
    piek_dom_kw: Math.round(Math.max(nivo0, maxDom)), extra_dom_kw: Math.round(extraDom), verzwaren_dom: maxDom > o.toeg,
    piek_dom_max_kw: Math.round(piekDomMax), extra_dom_max_kw: Math.round(maxEvDom),
    maandpiek_dom_kw: Array.from(mDom, Math.round), maandpiek_slim_kw: Array.from(mSlim, Math.round),
    net_eur_jaar_slim: Math.round(nS), net_eur_jaar_dom: Math.round(nD), net_eur_jaar_dom_max: Math.round(nDmax),
    net_eur100_slim: km100 > 0 ? nS / km100 : null, net_eur100_dom: km100 > 0 ? nD / km100 : null, net_eur100_dom_max: km100 > 0 ? nDmax / km100 : null,
  });
  // de drukste laaddag (hoogste piek zonder sturing) — 24 u vanaf middernacht van de aankomstdag
  if (piekDag) {
    const r = dag(piekDag), q0 = piekDag.dag * QD, b = [], sl = [], dm = [];
    for (let k = 0; k < QD; k++) { const q = (q0 + k) % Q; b.push(Math.round(L[q])); sl.push(Math.round(r.slim.get(q) || 0)); dm.push(Math.round(r.dom.get(q) || 0)); }
    const dt = new Date(Date.UTC(2025, 0, 1 + piekDag.dag));
    res.piekdag = { datum: dt.toISOString().slice(0, 10), weekdag: _weekdag(piekDag.dag), last_kw: b, slim_kw: sl, dom_kw: dm };
  }
  if (spot && eSlim > 0 && eDom > 0) {
    const pS = kSlim / eSlim, pD = kDom / eDom;   // €/MWh
    Object.assign(res, { spot_slim_eur_mwh: Math.round(pS * 10) / 10, spot_dom_eur_mwh: Math.round(pD * 10) / 10,
      spot_eur100: (pD - pS) / 1000 * kwhkm * 100, spot_eur_jaar: Math.round((kDom / eDom - kSlim / eSlim) / 1000 * D * n) });
    const tvr = tariefVar(o.tarief, (o.jaarMwh || 0) + D * n / 1000);
    if (tvr) {
      const k100 = kwhkm * 100, eD = k100 * (pD + tvr.totaal) / 1000, eS = k100 * (pS + tvr.totaal) / 1000;
      Object.assign(res, { var_eur_mwh: tvr, stroom100_dom: eD, stroom100_slim: eS,
        kost100_slim: eS + (res.net_eur100_slim || 0), kost100_dom: eD + (res.net_eur100_dom || 0), kost100_dom_max: eD + (res.net_eur100_dom_max || 0) });
    }
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
  const lastVenster = [], lastMet = []; dagen.forEach(d => d.qs.forEach(q => { lastMet.push(L[q]); if (!P.piekQ.has(q)) lastVenster.push(L[q]); }));   // 1.2.1: gemiddelde zonder de korte pieken
  const startMax = _pct(dagen.map(d => Math.max(...d.qs.slice(0, 4).map(q => L[q]))), 0.95);   // 1.2.0: P95 van het eerste uur (één toevallige machinepiek bepaalt het niet alleen)
  const gemDag = new Float64Array(QD); dagen.forEach(d => { for (let k = 0; k < QD; k++) gemDag[k] += L[d.dag * QD + k]; });
  const opt = { grens, nivo0: piek, toeg, eur_kw: +o.eur_kw > 0 ? +o.eur_kw : 100, tarief: o.tarief || null, jaarMwh: P.jaarKwh / 1000 };
  const H = {};
  for (const k of ['nu', 'm12', 'm24', 'm36']) if (o.horizons && o.horizons[k]) H[k] = simHorizon(L, dagen, o.horizons[k], opt, spot);
  // toegangsvermogen dat volstaat: hoogste van (huidige piek, slim laadniveau van de laatste horizon) + marge
  const laatste = ['m36', 'm24', 'm12', 'nu'].map(k => H[k]).filter(x => x && x.ev > 0)[0];
  const nodig = Math.max(piek, laatste ? (laatste.niveau_slim_kw || 0) : 0) / (1 - MARGE);
  return {
    jaar_mwh: Math.round(P.jaarKwh / 100) / 10, jaar_bron: P.bron, dag_nacht: P.herweging, pieken: P.pieken, profiel: o.profiel,
    toeg_kw: toeg, grens_kw: Math.round(grens), piek_kw: Math.round(piek), profielpiek_kw: Math.round(maxL), gem_kw: Math.round(P.jaarKwh / 8760),
    laaddagen: dagen.length, venster: { start_u: o.doelgroep === 'kmo' ? 8 : 18, duur_u: +o.venster_u || 8 },
    last_venster_gem_kw: Math.round(lastVenster.reduce((a, b) => a + b, 0) / Math.max(1, lastVenster.length)),
    last_venster_gem_met_pieken_kw: Math.round(lastMet.reduce((a, b) => a + b, 0) / Math.max(1, lastMet.length)),
    ruimte_kwh_dag_p5: Math.round(_pct(rGrens, 0.05)), ruimte_kwh_dag_med: Math.round(_pct(rGrens, 0.5)),
    ruimte_onder_piek_kwh_dag_p5: Math.round(_pct(rPiek, 0.05)),
    wagen_kwh_dag: Math.round(eWagen * 10) / 10,
    wagens_slim: Math.floor(_pct(rGrens, 0.05) / eWagen), wagens_onder_piek: Math.floor(_pct(rPiek, 0.05) / eWagen),
    wagens_dom: Math.max(0, Math.floor((grens - startMax) / Math.max(3.7, +o.paal_kw || 11))),
    toegang_volstaat_kw: Math.ceil(nodig / 5) * 5,
    dag_last_kw: Array.from(gemDag, v => Math.round(v / Math.max(1, dagen.length))),
    spot: !!spot, eur_kw: opt.eur_kw, horizons: H,
    tarief: o.tarief ? { dnb: o.tarief.dnb, spanning: o.tarief.spanning, jaar: o.tarief.jaar, mp_eur_kw: Math.round(o.tarief.mp_eur_kw * 100) / 100, tv_eur_kw: Math.round(o.tarief.tv_eur_kw * 100) / 100, var_eur_mwh: tariefVar(o.tarief, P.jaarKwh / 1000) } : null,
  };
}

module.exports = { simuleer, bouwProfiel, vensters, tariefVar, VERSIE: '1.2.1' };
