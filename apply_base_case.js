// ============================================================================
// FLUCTUS — applyBaseCaseToWizard (canonical reference)
// Versie:        v1.16 (BaseCase Uitbreiding Fase 3 — sessie 5a)
// Geproduceerd:  2026-05-13 15:30 UTC
// Doelomgeving:  Referentie-bestand (canonical) in JohanMMK/fluctus-proxy.
//                Geïnlined in Simulator.txt v1.16 voor productie in Odoo. De
//                inline-versie in Simulator.txt v1.16 sessie 5a doet bovendien:
//                  - project-naam clean (strip rechtsvormen, "T.a.v." prefix)
//                  - scenario-naam "1_Maandfactuur_MM-YY" uit periodeVan
//                  - async collision-check via /api/scenarios?project=
//                  - auto-save scenario 1 via /api/scenario-bewaren
//                Deze canonical-reference houdt de PURE state-mapping (zoals in
//                validate_v2.py). De Simulator.txt-versie wraps deze mapping
//                met auto-naamgeving + auto-save UI-flow.
// Repo:          JohanMMK/fluctus-proxy
// ----------------------------------------------------------------------------
// applyBaseCaseToWizard — Fase 3 van BaseCase Uitbreiding (v1.16 sessie 5a)
// ============================================================================
// Pure functie. Neemt het baseCase object dat /api/factuur-extract teruggaf,
// en produceert:
//   - status: 'OK' | 'OK_MET_INFO' | 'OK_MET_WAARSCHUWING' | 'BLOKKEER'
//   - reasons: array van {code, severity, message}
//   - state: object met velden voor STATE-mutatie (null bij BLOKKEER)
//   - gotoStep: 0-indexed stap (6=stap 7 kVA, 7=stap 8 periode, 8=stap 9 resultaat)
//
// Spiegelt validate_v2.py 1-op-1. Wijzigingen aan logica gebeuren ALTIJD
// eerst in de Python validator, dan hier.
//
// Architecturele context:
// - Stap 6 (contract) wordt NIET ingevuld -> verkoper kiest Enwyse-staffel
//   handmatig, of (later) via POST /api/factuur-staffel-bepalen.
// - Periode is ALTIJD specifiek uit factuur (geen rolling, geen kalenderjaar).
//   Simulator.py v1.5+ accepteert {type:"specifiek", van, tot} — sessie 4.
// - Postcode-cascade: snippet roept /api/postcode-grd; bij 404 fallback naar
//   /api/postcode-fallback. Hier alleen de pure logica.
//
// Wijzigingen v1.16 sessie 5a:
// - state.klantBtw en state.leveringsadres expliciet als top-level velden
//   gemarkeerd (zaten al impliciet in state.baseCase). Wordt door sessie 5b
//   "Maak voorstel"-flow gebruikt om in scenario 4 (commerciële basis)
//   meteen het BTW-nummer + leveringsadres beschikbaar te hebben.
// - state.scenarioActie='nieuw' geset zodat de Simulator.txt PDF-CTA-logica
//   weet dat dit een nieuw project is (vs. 'staand' bij scenario-load).
// - state.project en state.scenario blijven RUW (klantnaam + "base case <nr>").
//   De Simulator.txt v1.16 _fmodApplyToState overschrijft deze met de
//   clean+scenario1-naam variant. Voor backwards-compat met sessie-3 tests
//   van applyBaseCaseToWizard zelf blijven de oude waardes hier staan.
//
// Wijzigingen v1.15 sessie 4:
// - gotoStep nu standaard 7 (= stap 8 PERIODE) zodat verkoper de groene
//   "📎 Periode komt uit factuur" badge visueel kan verifiëren vóór hij
//   doorklikt naar resultaat. Bij ontbrekend kVA blijft 6 (= stap 7 aansluiting).
// - state object uitgebreid met Simulator.txt-compatibele veldnamen:
//   jaar='specifiek', periodeVan, periodeTot, baseCaseLoskoppeld=false.
//   Oude velden (periode, kVA, jaarverbruikMWh, profiel) blijven beschikbaar
//   voor backwards-compat met sessie 3 tests.
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
      // baseCase bevat ze ook (state.baseCase.klantBtw / leveringsadres) maar
      // top-level versie is handiger voor directe scenario-bewaring.
      klantBtw: bc.klantBtw || null,
      leveringsadres: bc.leveringsadres || null,
      // v1.16 sessie 5a: markeer dat dit een nieuw project is. Simulator.txt
      // gebruikt dit om PDF-CTA-zichtbaarheid op stap 1 te bepalen.
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
      _internal: { extractPostcode: extractPostcode, roundUp5: roundUp5, pickGemeente: pickGemeente }
    };
  } else {
    root.applyBaseCaseToWizard = applyBaseCaseToWizard;
  }
})(typeof window !== 'undefined' ? window : this);
