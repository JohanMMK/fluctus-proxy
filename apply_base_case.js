// ============================================================================
// FLUCTUS — applyBaseCaseToWizard (canonical reference)
// Versie:        v1.18 (Per-dag-vermogen vangnet — YUSO/Luminus-presentatie)
// Geproduceerd:  2026-08-07
// Doelomgeving:  Referentie-bestand (canonical) in JohanMMK/fluctus-proxy.
//                Geïnlined in Simulator.txt voor productie in Odoo.
// Repo:          JohanMMK/fluctus-proxy
// ----------------------------------------------------------------------------
// Wijzigingen v1.18 vs v1.17:
//   - PER-DAG-VERMOGEN VANGNET. Sommige leveranciers (bevestigd: YUSO; wellicht
//     Luminus) tonen de VERMOGENS-posten (toegangsvermogen, maandpiek,
//     overschrijding) als "kW × aantal dagen", met eenheidsprijs = maandtarief/
//     dagen. Het BEDRAG klopt, maar de afgelezen kW is ~dagen× te hoog
//     (bv. 7.500 "kW" toegangsvermogen op een periode van 30 dagen = 250 kW echt;
//     6.641 "kW" maandpiek = 221 kW). Zonder correctie leest de simulator een
//     absurde aansluiting/piek → aansluiting-/batterij-sizing, piekshaving en
//     load factor 30× fout. Dit speelt ENKEL bij de kW-posten, niet bij kWh.
//   - Fix (defensief, zelf-consistent): detecteer een onmogelijk lage load factor
//     op aansluitVermogenKva over de factuurperiode; is die < 2% én na deling door
//     het aantal dagen weer plausibel (≤ 100%), dan is het een per-dag-presentatie
//     → deel door het aantal dagen. Nieuwe reason-code W7 (WAARSCHUWING).
//   - Helpers _dagenTussen() en _perDagVermogen() toegevoegd (+ in _internal voor tests).
//   - LET OP: dit is een VANGNET in de mapper. De ROBUUSTE fix (Methode A:
//     echte_kW = bedrag ÷ maandtarief-per-kW uit de tariefkaart; ratio ≈ dagen ⇒
//     per-dag) hoort in de EXTRACTIE-laag die `bc` bouwt (server-side), waar de
//     rauwe kW-postregels (volume/eenheidsprijs/bedrag) én de maandpiek zitten.
//     Deze mapper ziet enkel bc.aansluitVermogenKva.
// ----------------------------------------------------------------------------
// Eerdere wijzigingen v1.17 vs v1.16:
//   - HEADER-ONLY BUMP. Geen logica-wijzigingen. (Maak-Voorstel + BESS-Custom.)
//   - state.klantBtw, state.leveringsadres en state.scenarioActie='nieuw' als
//     fundering voor de "Maak voorstel"-flow (sessie 5b).
// Eerdere wijzigingen v1.16 vs v1.15:
//   - state.klantBtw / state.leveringsadres expliciet top-level; scenarioActie='nieuw'.
// Eerdere wijzigingen v1.15 vs v1.14:
//   - gotoStep standaard 7 (stap 8 PERIODE); Simulator.txt-compatibele veldnamen.
// ============================================================================

(function (root) {
  'use strict';

  function extractPostcode(adres) {
    if (!adres) return null;
    var m = String(adres).match(/\b\d{4}\b/);
    return m ? m[0] : null;
  }

  function roundUp5(n) {
    return Math.ceil(n / 5) * 5;
  }

  function pickGemeente(adres, gemeenten) {
    if (!gemeenten || !gemeenten.length) return null;
    if (!adres) return gemeenten[0];
    var adresUpper = String(adres).toUpperCase();
    // Probeer 1: direct match op gemeente-naam (zonder haakjes-suffix)
    for (var i = 0; i < gemeenten.length; i++) {
      var g = gemeenten[i];
      var gClean = g.replace(/\s*\([^)]+\)\s*/g, '').trim().toUpperCase();
      if (gClean && adresUpper.indexOf(gClean) !== -1) return g;
    }
    // Probeer 2: match op haakjes-suffix ("Beveren (Roeselare)" matcht "ROESELARE")
    for (var j = 0; j < gemeenten.length; j++) {
      var m = gemeenten[j].match(/\(([^)]+)\)/);
      if (m && adresUpper.indexOf(m[1].toUpperCase()) !== -1) return gemeenten[j];
    }
    return gemeenten[0];
  }

  // v1.18: aantal dagen van de factuurperiode (inclusief begin- en einddag).
  // Aanvaardt 'YYYY-MM-DD' of Date-parseable strings. Geeft null bij ontbrekend/onzinnig.
  function _dagenTussen(van, tot) {
    if (!van || !tot) return null;
    var d1 = new Date(van), d2 = new Date(tot);
    if (isNaN(d1.getTime()) || isNaN(d2.getTime())) return null;
    var d = Math.round((d2.getTime() - d1.getTime()) / 86400000) + 1; // +1 = inclusief einddag
    return (d > 0 && d < 400) ? d : null;
  }

  // v1.18: detecteer een "kW × dagen"-presentatie van het vermogen (YUSO-stijl).
  // Retourneert { kva, corr, dagen, lfRaw, lfCorr }. Enkel corrigeren als de rauwe
  // load factor fysiek onmogelijk laag is (<2%) én na deling door de dagen weer
  // plausibel (≤100%). Zo blijft een correct-geparste aansluiting ongemoeid.
  function _perDagVermogen(kva, dagen, afnameKwh) {
    if (!(kva > 0) || !(dagen > 1) || !(afnameKwh > 0)) return { kva: kva, corr: false };
    var uren = dagen * 24;
    var lfRaw = afnameKwh / (kva * uren);            // load factor met de rauwe kW
    var lfCorr = lfRaw * dagen;                        // load factor na ÷ dagen
    if (lfRaw < 0.02 && lfCorr <= 1.0) {
      return { kva: kva / dagen, corr: true, dagen: dagen, lfRaw: lfRaw, lfCorr: lfCorr };
    }
    return { kva: kva, corr: false, dagen: dagen, lfRaw: lfRaw };
  }

  function applyBaseCaseToWizard(baseCase, options) {
    options = options || {};
    var strictTariefjaar = options.strictTariefjaar === true;
    var bc = baseCase;
    var reasons = [];

    // ===== BLOKKEER checks =====

    if (bc.factuurType === 'injectie') {
      reasons.push({
        code: 'B1', severity: 'BLOKKEER',
        message: 'Injectiefactuur is niet bruikbaar als base case. Upload een afnamefactuur.'
      });
    }

    var afname = bc.afnameKwh;
    if (afname == null || afname === 0) {
      reasons.push({
        code: 'B2', severity: 'BLOKKEER',
        message: 'Geen verbruik op factuur. Upload een afnamefactuur.'
      });
    }

    var dnbLookup = bc._dnbLookup || {};
    if (!bc.dnb || !dnbLookup.tariefKey_gebruikt) {
      reasons.push({
        code: 'B4', severity: 'BLOKKEER',
        message: 'Distributienetbeheerder of tariefset niet gevonden.'
      });
    }

    if (strictTariefjaar) {
      var tjm = bc._tariefjaar_match || {};
      if (tjm.match === 'ander_tariefjaar' || tjm.match === 'overspant_jaargrens') {
        var jaren = Object.keys(tjm.jaren || {}).join(', ');
        reasons.push({
          code: 'B5', severity: 'BLOKKEER',
          message: 'Factuurperiode (' + jaren + ') ligt buiten huidig tariefjaar 2026. Vraag een factuur volledig in 2026.'
        });
      }
    }

    if (reasons.some(function (r) { return r.severity === 'BLOKKEER'; })) {
      return { status: 'BLOKKEER', reasons: reasons, state: null, gotoStep: null };
    }

    // ===== v1.18: PER-DAG-VERMOGEN VANGNET =====
    // Vóór de aansluiting-checks: reken een per-dag-gepresenteerd toegangsvermogen
    // terug naar de echte kW, zodat aansluitOntbreekt/kVA en alles downstream de
    // juiste waarde gebruiken. Corrigeert bc.aansluitVermogenKva in-place.
    var _dagen = _dagenTussen(bc.periodeVan, bc.periodeTot);
    var _pd = _perDagVermogen(bc.aansluitVermogenKva, _dagen, afname);
    if (_pd.corr) {
      bc.aansluitVermogenKva = _pd.kva;
      reasons.push({
        code: 'W7', severity: 'WAARSCHUWING',
        message: 'Toegangsvermogen leek per dag gepresenteerd (kW × ' + _dagen +
          ' dagen — leverancier-stijl, bv. YUSO). Automatisch teruggerekend naar ' +
          Math.round(_pd.kva) + ' kW. Controleer tegen de factuur.',
        _detail: { rauwe_kva: Math.round(_pd.kva * _dagen), dagen: _dagen,
                   load_factor_rauw: Math.round(_pd.lfRaw * 10000) / 100 + '%',
                   load_factor_gecorrigeerd: Math.round(_pd.lfCorr * 10000) / 100 + '%' }
      });
    }

    // ===== WAARSCHUWING / INFO checks =====

    var aansluitOntbreekt = bc.aansluitVermogenKva == null;
    if (aansluitOntbreekt) {
      reasons.push({
        code: 'W3', severity: 'WAARSCHUWING',
        message: 'Aansluitvermogen kon niet uit factuur worden afgeleid — vul handmatig in op stap 7.'
      });
    }
    if (bc.spanningsniveau == null) {
      reasons.push({
        code: 'W5', severity: 'INFO',
        message: 'Spanningsniveau niet bepaald — wordt afgeleid uit kVA op stap 7 (≥100 kW = MS).'
      });
    }

    var cons = bc._consistentie || {};
    if (cons.status === 'AFWIJKING') {
      reasons.push({
        code: 'W3prime', severity: 'WAARSCHUWING',
        message: 'Som componenten wijkt af van factuurtotaal — controleer factuur.'
      });
    }
    if (cons.status === 'CAPACITEIT_DUBBEL') {
      reasons.push({
        code: 'W6', severity: 'INFO',
        message: 'Capaciteit dubbel gedetecteerd, automatisch gecorrigeerd in extractie.'
      });
    }

    var leeftijd = bc._factuur_leeftijd || {};
    if (leeftijd.oud) {
      reasons.push({
        code: 'W4', severity: 'INFO',
        message: 'Factuur is ' + leeftijd.leeftijdMaanden + ' maanden oud — verbruikspatroon kan veranderd zijn.'
      });
    }

    var match = (bc._tariefjaar_match || {}).match;
    if (match === 'overspant_jaargrens') {
      reasons.push({
        code: 'W1', severity: 'WAARSCHUWING',
        message: 'Factuur overspant jaargrens — simulatie gebruikt tarieven 2026 voor de hele periode.'
      });
    } else if (match === 'aangrenzend_jaar') {
      reasons.push({
        code: 'W2', severity: 'INFO',
        message: 'Factuur uit aangrenzend jaar — tarieven 2026 gebruikt, kleine afwijking mogelijk.'
      });
    } else if (match === 'ander_tariefjaar') {
      reasons.push({
        code: 'W_OUD', severity: 'WAARSCHUWING',
        message: 'Factuur uit ander jaar dan 2026 — vergelijking is indicatief, vraag voor productie een 2026-factuur.'
      });
    }

    // ===== STATE invullen =====

    var project = bc.klantNaam || 'Onbekend';
    var scenario = ('base case ' + (bc.factuurNummer || '')).trim();

    var postcode = extractPostcode(bc.leveringsadres);
    var gemeenten = (dnbLookup.gemeenten) || [];
    var gemeente = pickGemeente(bc.leveringsadres, gemeenten);

    // CORRECTIE 2: jaarverbruikMWh = afname zelf in MWh, geen extrapolatie.
    // Periode wordt SPECIFIEK uit factuur. Simulator rekent exact die periode.
    var jaarverbruikMWh = Math.round((afname / 1000) * 1000) / 1000;

    var periode = {
      type: 'specifiek',
      van: bc.periodeVan,
      tot: bc.periodeTot
    };

    var pvKwp = bc.pvKwpAanwezig || 0;
    var pvInjStrategie = 'geen';

    // CORRECTIE 3: contract (Enwyse-staffel) NIET invullen. Verkoper kiest in stap 6.
    // baseCase.leverancier en .leverancierTariefformule blijven beschikbaar voor KPI-tegel.

    var kVA = aansluitOntbreekt ? null : roundUp5(bc.aansluitVermogenKva);

    var profiel = null; // door tab 3 in modale gezet

    // v1.15 sessie 4: gotoStep=7 (= stap 8 PERIODE, 0-indexed) zodat verkoper
    // de "📎 Periode komt uit factuur" badge ziet vóór de simulatie. Bij
    // ontbrekend kVA blijft hij op stap 7 (= 0-indexed 6) hangen.
    var gotoStep = aansluitOntbreekt ? 6 : 7;

    var state = {
      // Oude veldnamen (backwards-compat met sessie-3 tests)
      project: project,
      scenario: scenario,
      postcode: postcode,
      gemeente: gemeente,
      profiel: profiel,
      jaarverbruikMWh: jaarverbruikMWh,
      pvKwp: pvKwp,
      pvInjStrategie: pvInjStrategie,
      contract: null,
      kVA: kVA,
      periode: periode,
      baseCase: bc,
      // v1.15 sessie 4: Simulator.txt-compatibele veldnamen (direct
      // toepasbaar met Object.assign(STATE, state) in de UI-mapper).
      profielNaam: profiel,           // alias voor profiel
      jaarverbruik: jaarverbruikMWh,  // alias voor jaarverbruikMWh
      aansluitingKva: kVA,            // alias voor kVA
      jaar: 'specifiek',              // STATE.jaar markeer als base-case-modus
      periodeVan: bc.periodeVan,      // expliciete periode-velden
      periodeTot: bc.periodeTot,
      baseCaseLoskoppeld: false,      // reset bij elke nieuwe factuur-apply
      // v1.16 sessie 5a: expliciete top-level velden voor "Maak voorstel"-flow.
      klantBtw: bc.klantBtw || null,
      leveringsadres: bc.leveringsadres || null,
      // v1.16 sessie 5a: markeer dat dit een nieuw project is.
      scenarioActie: 'nieuw'
    };

    var hasWarning = reasons.some(function (r) { return r.severity === 'WAARSCHUWING'; });
    var hasInfo = reasons.some(function (r) { return r.severity === 'INFO'; });
    var status = hasWarning ? 'OK_MET_WAARSCHUWING' : (hasInfo ? 'OK_MET_INFO' : 'OK');

    return { status: status, reasons: reasons, state: state, gotoStep: gotoStep };
  }

  // Export: zowel als CommonJS module (voor Node test) als als globaal in browser.
  if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
      applyBaseCaseToWizard: applyBaseCaseToWizard,
      _internal: {
        extractPostcode: extractPostcode, roundUp5: roundUp5, pickGemeente: pickGemeente,
        dagenTussen: _dagenTussen, perDagVermogen: _perDagVermogen
      }
    };
  } else {
    root.applyBaseCaseToWizard = applyBaseCaseToWizard;
  }
})(typeof window !== 'undefined' ? window : this);
