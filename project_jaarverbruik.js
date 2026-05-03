// ============================================================================
// project_jaarverbruik.js — Fase 2 BaseCase Uitbreiding, sessie 2
// ----------------------------------------------------------------------------
// Pure JavaScript-port van project_jaarverbruik.py. 1-op-1 spiegel — logica-
// wijzigingen ALTIJD eerst in Python, dan hier.
//
// Doel: gegeven afnameKwh + factuurperiode + profiel-kwartierdata + Enwyse-
// staffel, projecteer naar geprojecteerd jaarverbruik en bepaal tier.
//
// Mapping: WEEKDAG-BEWUST (vraag 1B). Profielen zijn op kalenderjaar 2025
// gemodelleerd. Voor elke datum d in [van, tot] wordt een 2025-doy gezocht
// met dezelfde weekdag binnen ±3 dagen.
//
// Drempel: <14 dagen → status "ONBETROUWBAAR".
//
// Architecturele context:
// - Wordt aangeroepen door route POST /api/factuur-staffel-bepalen in server.js
// - Anti-regressie regel 3: puur additief, leest profiel-data en staffel
//   zonder bestaande state te wijzigen
// - STATE-mutatie gebeurt in de snippet (tab 3 modale): STATE.profiel,
//   STATE.jaarverbruikMWh, STATE.baseCase.geprojecteerdeTier
// ============================================================================

(function (root) {
  'use strict';

  // ─── CONSTANTEN ───────────────────────────────────────────────────────────

  var KWARTIEREN_PER_DAG = 96;
  var DAGEN_2025 = 365;
  var KWARTIEREN_2025 = DAGEN_2025 * KWARTIEREN_PER_DAG;  // 35040

  var MIN_DAGEN_BETROUWBAAR = 14;
  var WEEKDAG_ZOEK_RADIUS = 3;

  // Identiek aan server.js regels 82-96 (CONTRACT_STAFFEL fallback).
  var DEFAULT_CONTRACT_STAFFEL = [
    { min_mwh:0,    max_mwh:100,    label:'0-100 MWh',    code:'S1',
      consumption_dam_markup:20.0, consumption_imbalance_markup:5.0,
      injection_dam_markdown:0.0,  injection_imbalance_markdown:11.0 },
    { min_mwh:100,  max_mwh:200,    label:'100-200 MWh',  code:'S2',
      consumption_dam_markup:19.0, consumption_imbalance_markup:5.0,
      injection_dam_markdown:0.0,  injection_imbalance_markdown:11.0 },
    { min_mwh:200,  max_mwh:300,    label:'200-300 MWh',  code:'S3',
      consumption_dam_markup:18.0, consumption_imbalance_markup:5.0,
      injection_dam_markdown:0.0,  injection_imbalance_markdown:11.0 },
    { min_mwh:300,  max_mwh:400,    label:'300-400 MWh',  code:'S4',
      consumption_dam_markup:17.0, consumption_imbalance_markup:5.0,
      injection_dam_markdown:0.0,  injection_imbalance_markdown:11.0 },
    { min_mwh:400,  max_mwh:500,    label:'400-500 MWh',  code:'S5',
      consumption_dam_markup:16.0, consumption_imbalance_markup:5.0,
      injection_dam_markdown:0.0,  injection_imbalance_markdown:11.0 },
    { min_mwh:500,  max_mwh:600,    label:'500-600 MWh',  code:'S6',
      consumption_dam_markup:15.0, consumption_imbalance_markup:5.0,
      injection_dam_markdown:0.0,  injection_imbalance_markdown:11.0 },
    { min_mwh:600,  max_mwh:700,    label:'600-700 MWh',  code:'S7',
      consumption_dam_markup:14.0, consumption_imbalance_markup:5.0,
      injection_dam_markdown:0.0,  injection_imbalance_markdown:11.0 },
    { min_mwh:700,  max_mwh:800,    label:'700-800 MWh',  code:'S8',
      consumption_dam_markup:13.5, consumption_imbalance_markup:5.0,
      injection_dam_markdown:0.0,  injection_imbalance_markdown:11.0 },
    { min_mwh:800,  max_mwh:900,    label:'800-900 MWh',  code:'S9',
      consumption_dam_markup:13.0, consumption_imbalance_markup:5.0,
      injection_dam_markdown:0.0,  injection_imbalance_markdown:11.0 },
    { min_mwh:900,  max_mwh:1000,   label:'900-1000 MWh', code:'S10',
      consumption_dam_markup:12.5, consumption_imbalance_markup:5.0,
      injection_dam_markdown:0.0,  injection_imbalance_markdown:11.0 },
    { min_mwh:1000, max_mwh:2000,   label:'1-2 GWh',      code:'S11',
      consumption_dam_markup:8.0,  consumption_imbalance_markup:5.0,
      injection_dam_markdown:0.0,  injection_imbalance_markdown:11.0 },
    { min_mwh:2000, max_mwh:5000,   label:'2-5 GWh',      code:'S12',
      consumption_dam_markup:5.0,  consumption_imbalance_markup:5.0,
      injection_dam_markdown:0.0,  injection_imbalance_markdown:11.0 },
    { min_mwh:5000, max_mwh:999999, label:'>5 GWh',       code:'S13',
      consumption_dam_markup:3.5,  consumption_imbalance_markup:5.0,
      injection_dam_markdown:0.0,  injection_imbalance_markdown:11.0 },
  ];

  // ─── HELPERS — datum & profiel-index ──────────────────────────────────────

  function parseIsoDate(s) {
    if (!s || typeof s !== 'string') throw new Error('lege of niet-string datum');
    // Tolerant: accepteer ook full ISO datetime door eerste 10 chars te nemen.
    var d = s.slice(0, 10);
    var m = d.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!m) throw new Error('ongeldige datum-formaat: ' + s);
    var dt = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3]));
    if (isNaN(dt.getTime())) throw new Error('ongeldige datum: ' + s);
    return dt;
  }

  // Aantal dagen verschil (UTC, zodat geen DST-issues).
  function dayDiff(a, b) {
    return Math.round((b - a) / (24 * 3600 * 1000));
  }

  function addDays(d, n) {
    return new Date(d.getTime() + n * 24 * 3600 * 1000);
  }

  // 0=ma .. 6=zo (zelfde conventie als Python's date.weekday()).
  // JS getUTCDay() geeft 0=zo..6=za, dus omzetten.
  function weekdayMaZo(d) {
    var w = d.getUTCDay();      // 0=zo, 1=ma, ..., 6=za
    return (w + 6) % 7;          // 0=ma, ..., 6=zo
  }

  // Day-of-year voor (year, month, day) in 2025 — geen schrikkeljaar.
  // Voor 29 feb (alleen schrikkeljaren) clamp naar 28 feb.
  function dayOfYear2025(month, day) {
    if (month === 2 && day === 29) day = 28;
    var d = new Date(Date.UTC(2025, month - 1, day));
    var jan1 = Date.UTC(2025, 0, 1);
    return Math.round((d - jan1) / (24 * 3600 * 1000)) + 1;
  }

  // 2025-jan-1 als referentie voor doy-berekeningen.
  var REF_2025_JAN1 = new Date(Date.UTC(2025, 0, 1));

  // Returnt 96 indices in profiel-array (lengte 35040) voor gegeven datum d.
  // Weekdag-bewust: zoekt 2025-doy met dezelfde weekdag binnen ±3 dagen.
  function profielKwartierIndicesVoorDatum(d) {
    var month = d.getUTCMonth() + 1;
    var day = d.getUTCDate();
    var targetDoy = dayOfYear2025(month, day);
    var targetWd = weekdayMaZo(d);

    var bestDoy = null;
    var bestOffset = WEEKDAG_ZOEK_RADIUS + 1;
    for (var offset = -WEEKDAG_ZOEK_RADIUS; offset <= WEEKDAG_ZOEK_RADIUS; offset++) {
      var candDoy = targetDoy + offset;
      if (candDoy < 1 || candDoy > DAGEN_2025) continue;
      var candDate = addDays(REF_2025_JAN1, candDoy - 1);
      if (weekdayMaZo(candDate) === targetWd && Math.abs(offset) < bestOffset) {
        bestDoy = candDoy;
        bestOffset = Math.abs(offset);
        if (bestOffset === 0) break;
      }
    }

    var doy = bestDoy !== null ? bestDoy : targetDoy;
    var base = (doy - 1) * KWARTIEREN_PER_DAG;
    var indices = new Array(KWARTIEREN_PER_DAG);
    for (var k = 0; k < KWARTIEREN_PER_DAG; k++) indices[k] = base + k;
    return indices;
  }

  function aantalDagen(van, tot) {
    return dayDiff(van, tot) + 1;  // inclusief
  }

  // ─── CORE — projectie & tier-keuze ────────────────────────────────────────

  function somProfielInPeriode(profielKwartier, van, tot) {
    if (!Array.isArray(profielKwartier) || profielKwartier.length !== KWARTIEREN_2025) {
      throw new Error(
        'profiel_kwartier moet ' + KWARTIEREN_2025 + ' waarden hebben, kreeg er ' +
        (profielKwartier ? profielKwartier.length : 'undefined')
      );
    }
    var totaal = 0.0;
    var d = new Date(van.getTime());
    while (d <= tot) {
      var indices = profielKwartierIndicesVoorDatum(d);
      for (var i = 0; i < indices.length; i++) {
        totaal += profielKwartier[indices[i]];
      }
      d = addDays(d, 1);
    }
    return totaal;
  }

  // Strikte tier-keuze: min_mwh ≤ jaarverbruik < max_mwh.
  // Boven max van laatste tier: gebruik laatste tier.
  function kiesTier(jaarverbruikMwh, staffel) {
    if (jaarverbruikMwh < 0) return null;
    for (var i = 0; i < staffel.length; i++) {
      var t = staffel[i];
      if (t.min_mwh <= jaarverbruikMwh && jaarverbruikMwh < t.max_mwh) {
        // Shallow copy zodat caller niet per ongeluk de staffel muteert.
        return Object.assign({}, t);
      }
    }
    if (staffel.length && jaarverbruikMwh >= staffel[staffel.length - 1].min_mwh) {
      return Object.assign({}, staffel[staffel.length - 1]);
    }
    return null;
  }

  function round2(x) { return Math.round(x * 100) / 100; }
  function round4(x) { return Math.round(x * 10000) / 10000; }
  function round6(x) { return Math.round(x * 1000000) / 1000000; }

  function sumArr(a) {
    var s = 0; for (var i = 0; i < a.length; i++) s += a[i]; return s;
  }

  /**
   * Hoofdfunctie. Pure: geen I/O, geen state-mutatie.
   *
   * @param {object} input
   * @param {string} input.profielNaam      - naam (informatief, voor diagnose)
   * @param {number[]} input.profielKwartier - 35040 floats, 2025-kalender
   * @param {number} input.afnameKwh        - factuur-afname in kWh
   * @param {string} input.periodeVan       - ISO YYYY-MM-DD
   * @param {string} input.periodeTot       - ISO YYYY-MM-DD
   * @param {object[]} [input.staffel]      - default DEFAULT_CONTRACT_STAFFEL
   * @returns {object} response — zie docstring Python-versie
   */
  function projectJaarverbruik(input) {
    var staffel = input.staffel || DEFAULT_CONTRACT_STAFFEL;
    var diagnoseBase = {
      profielNaam: input.profielNaam,
      afnameKwh: input.afnameKwh,
      periodeVan: input.periodeVan,
      periodeTot: input.periodeTot
    };

    var van, tot;
    try {
      van = parseIsoDate(input.periodeVan);
      tot = parseIsoDate(input.periodeTot);
    } catch (e) {
      return { ok: false, status: 'FOUT', reden: 'Ongeldige datums: ' + e.message,
               _diagnose: diagnoseBase };
    }

    if (tot < van) {
      return { ok: false, status: 'FOUT',
               reden: 'periodeTot (' + input.periodeTot + ') ligt vóór periodeVan (' + input.periodeVan + ').',
               _diagnose: diagnoseBase };
    }

    if (input.afnameKwh == null || input.afnameKwh <= 0) {
      return { ok: false, status: 'FOUT', reden: 'afnameKwh moet positief zijn.',
               _diagnose: diagnoseBase };
    }

    var dagen = aantalDagen(van, tot);
    diagnoseBase.dagenInPeriode = dagen;

    var somPeriode;
    try {
      somPeriode = somProfielInPeriode(input.profielKwartier, van, tot);
    } catch (e) {
      return { ok: false, status: 'FOUT', reden: e.message, _diagnose: diagnoseBase };
    }

    var somJaar = sumArr(input.profielKwartier);
    diagnoseBase.som_profiel_periode = round4(somPeriode);
    diagnoseBase.som_profiel_jaar = round4(somJaar);

    if (somJaar <= 0) {
      return { ok: false, status: 'FOUT',
               reden: 'Profiel-jaarsom is 0 of negatief — profiel-bestand corrupt.',
               _diagnose: diagnoseBase };
    }

    var fractie = somPeriode / somJaar;
    diagnoseBase.profielFractieInPeriode = round6(fractie);
    diagnoseBase.weekdag_mapping = 'weekdag_bewust';

    if (dagen < MIN_DAGEN_BETROUWBAAR) {
      return {
        ok: false, status: 'ONBETROUWBAAR',
        reden: 'Factuurperiode (' + dagen + ' dagen) is korter dan minimum ' +
               MIN_DAGEN_BETROUWBAAR + ' dagen voor betrouwbare projectie.',
        _diagnose: diagnoseBase
      };
    }

    if (fractie <= 0) {
      return { ok: false, status: 'FOUT',
               reden: 'Profielfractie in periode = 0 — kan niet projecteren.',
               _diagnose: diagnoseBase };
    }

    var geprojKwh = input.afnameKwh / fractie;
    var geprojMwh = round2(geprojKwh / 1000);

    var tier = kiesTier(geprojMwh, staffel);
    if (!tier) {
      return { ok: false, status: 'FOUT',
               reden: 'Geen tier gevonden voor jaarverbruik ' + geprojMwh + ' MWh.',
               _diagnose: diagnoseBase };
    }

    return {
      ok: true,
      geprojecteerdJaarverbruikMWh: geprojMwh,
      tier: tier,
      _diagnose: diagnoseBase
    };
  }

  // ─── EXPORT ───────────────────────────────────────────────────────────────

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
      projectJaarverbruik: projectJaarverbruik,
      DEFAULT_CONTRACT_STAFFEL: DEFAULT_CONTRACT_STAFFEL,
      KWARTIEREN_2025: KWARTIEREN_2025,
      MIN_DAGEN_BETROUWBAAR: MIN_DAGEN_BETROUWBAAR,
      _internal: {
        parseIsoDate: parseIsoDate,
        weekdayMaZo: weekdayMaZo,
        dayOfYear2025: dayOfYear2025,
        profielKwartierIndicesVoorDatum: profielKwartierIndicesVoorDatum,
        aantalDagen: aantalDagen,
        somProfielInPeriode: somProfielInPeriode,
        kiesTier: kiesTier
      }
    };
  } else {
    root.projectJaarverbruik = projectJaarverbruik;
  }
})(typeof window !== 'undefined' ? window : this);
