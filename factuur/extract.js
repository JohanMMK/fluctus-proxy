'use strict';

/**
 * Fluctus Simulator — BaseCase factuur-extractie
 * ===============================================
 * Module: factuur/extract.js
 * Versie: 1.3 (juli 2026)
 *
 * Wijzigingen v1.2 → v1.3 (bug: netto te betalen i.p.v. totale kost):
 *   1. Prompt: totaalExclBtw/totaalInclBtw = TOTALE kost van de periode (bruto,
 *      vóór aftrek voorschotten). Expliciet NIET het netto te betalen saldo.
 *   2. Prompt + schema: nieuwe velden voorschottenInclBtw en
 *      totaalTeBetalenInclBtw (informatief) voor afrekeningen/slotfacturen.
 *   3. Server-side reconciliatie: bij AFWIJKING + voorschot-indicatie wordt
 *      totaalExclBtw gecorrigeerd naar de som van de kost-componenten (bruto),
 *      totaalInclBtw herberekend, sumcheck bijgewerkt, _voorschot_correctie
 *      + flag 'totaal_gecorrigeerd_voor_voorschotten' toegevoegd.
 *
 * Wijzigingen v1.0 → v1.1 (na test-batch Eneco/Elindus/Luminus):
 *   1. Prompt: leverancier = commerciële merk (Eneco niet "Eneco Belgium",
 *      Luminus niet "EDF Luminus", Elindus niet "Elindus NV").
 *   2. Prompt: pvKwpAanwezig is STRIKT number|null. Geen string-flags.
 *   3. Schema: nieuw veld factuurType: "afname"|"injectie"|"gemengd"|"regularisatie"
 *   4. Prompt: klantnaam dedup ("NV NV"), adres-cleanup (interne codes verwijderen)
 *   5. Server-side sumCheck: Energie+Distributie+Heffingen+Capaciteit ≈ totaalExclBtw
 *   6. Server-side leeftijdscheck: factuurDatum > 18 maanden oud → flag.
 *   7. Detector volledig herontworpen — beslisboom met indicatoren:
 *        a. Expliciet niveau in factuur (MS/MV/HS/hoogspanning of LS/laagspanning)
 *        b. toegangsvermogen ≥ 100 kW → MS
 *        c. toegangsvermogen ≤ 56 kW + proportioneel afnametarief → LS
 *        d. LS reverse-engineering binnen 5% (recente factuur only)
 *   8. _uncertain opschoning: server haalt velden uit _uncertain die het zelf
 *      met confidence ≥ medium heeft kunnen invullen.
 */

// ─── DNB naam-mapping ─────────────────────────────────────────────────────────
const DNB_TO_TARIEF_KEY = {
  'Fluvius West': 'West',
  'Fluvius Antwerpen': 'Antwerpen',
  'Fluvius Halle-Vilvoorde': 'Halle-Vilv.',
  'Fluvius Imewo': 'Imewo',
  'Fluvius Kempen': 'Kempen',
  'Fluvius Limburg': 'Limburg',
  'Fluvius Midden-Vlaanderen': 'Midden-Vl.',
  'Fluvius Zenne-Dijle': 'Zenne-Dijle',
  'AIEG': 'AIEG',
  'ORES': 'ORES',
  'RESA': 'RESA',
  'REW': 'REW',
  'Sibelga': 'Sibelga',
  'AIESH': null,
};

// ─── Prompt builder ───────────────────────────────────────────────────────────
function buildSystemPrompt() {
  return `Je bent een expert in het lezen van Belgische elektriciteitsfacturen.

TAAK: Extraheer alle relevante velden en geef ze terug als VALIDE JSON volgens
het onderstaande schema. Geef GEEN extra tekst, GEEN markdown code-fences,
ALLEEN het JSON object.

ALGEMENE REGELS:
- Datumformaat: ALTIJD YYYY-MM-DD. Belgische facturen tonen meestal DD/MM/YYYY
  of DD-MM-YYYY — converteer correct.
- Eenheden: alle bedragen in EUR (excl BTW tenzij expliciet anders gevraagd),
  alle verbruiken in kWh (NIET MWh). Als de factuur MWh gebruikt: vermenigvuldig
  met 1000 voor kWh.
- Voor velden waar je NIET ZEKER bent: laat ze leeg (null) en voeg de veldnaam
  toe aan de "_uncertain" array. Maak NOOIT velden op.
- Indien meerdere EAN-nummers op de factuur staan: lijst ze allemaal in eanNrs.

LEVERANCIER (BIJZONDER):
Vul "leverancier" in met de COMMERCIELE MERKNAAM, niet de juridische entiteit.
  - "Eneco Belgium nv" → "Eneco"
  - "EDF Luminus nv" → "Luminus"
  - "Elindus NV" → "Elindus"
  - "Engie Electrabel" → "Engie"
  - "TotalEnergies Power & Gas Belgium" → "TotalEnergies"

KLANTNAAM CLEANUP:
- Verwijder dubbele rechtsvormen: "Vema Beton BV BV" → "Vema Beton BV"
- Verwijder "Tav. ..." prefixen wanneer ze de boekhouding/contactpersoon zijn
- Behoud rechtsvorm wel één keer: "Slagerij Janssens BV" blijft.

ADRES CLEANUP:
- Verwijder interne leverancier-codes tussen haakjes: "Land van Waaslaan(KAL) 3"
  → "Land van Waaslaan 3"
- Behoud postcode en gemeente.
- Indien klantadres en leveringsadres verschillen: vul beide in.

FACTUURTYPE:
Bepaal of de factuur een "afname", "injectie", "gemengd" of "regularisatie" is.
  - afname: klant heeft elektriciteit verbruikt, factuur toont kosten
  - injectie: factuur is een terugleververgoeding (PV-overschot, leverancier
    betaalt of compenseert klant)
  - gemengd: zowel afname als significante injectie (>5% van afname)
  - regularisatie: een correctie/herziening van een eerdere factuur ("Slot-
    factuur", "Rectificatie", "Regularisatie")

PERIODE EXTRACTIE:
Een Belgische elektriciteitsfactuur bevat meerdere soorten datums:
  - VERBRUIKSPERIODE (1 of meerdere) — DEZE GEBRUIKEN
  - Contractperiode/contractduur — NIET gebruiken voor periodeVan/Tot
  - Factuurdatum, betalingsdatum, vervaldatum — NIET gebruiken
Identificeer ALLE verbruiksperiodes. Indien meerdere aaneensluitende
deelperiodes (bv. tariefdetails per maand): neem het OMHULLENDE INTERVAL —
kleinste van-datum en grootste tot-datum.

AANSLUITVERMOGEN (BIJZONDER BELANGRIJK):
Zoek op de factuur ALLE voorkomens van vermogen-velden:
  a) "Toegangsvermogen" of "toegangsvermogen" (in kW — contractueel maximum)
  b) "Maandpiek" of "gemiddelde maandpiek" (in kW — gemeten piek deze maand)
  c) "(piek)capaciteit" of "capaciteitstarief basis" (in kW)
Vul deze in onder _aansluitvermogenBron als KW-waarden. Server berekent kVA.
Op MS/HS-facturen kan toegangsvermogen 1000+ kW zijn — dit is normaal.

SPANNINGSNIVEAU:
Zoek expliciete vermelding op factuur:
  - "MS", "MV", "HS", "Hoogspanning", "moyenne tension", tariefcodes met "HS"
    → spanningsniveau = "MS"
  - "LS", "Laagspanning", "BT", "basse tension"
    → spanningsniveau = "LS"
Als geen expliciete vermelding: laat null en flag in _uncertain. Server doet
post-extractie inferentie via beslisboom.

CAPACITEITSTARIEF EXTRACTIE:
Zoek naar regels met "Capaciteitstarief" / "Capaciteitsterm" als kostenpost.
Voor ELKE periode-regel:
  { van: "YYYY-MM-DD", tot: "YYYY-MM-DD", bedragExclBtw: <getal> }
Plaats deze als array in _capaciteitstariefRegels.

DAG/NACHT VERBRUIK:
- Als factuur expliciete kWh-totalen toont voor dag/nacht: vul direct in.
- Als alleen METERSTANDEN gegeven: bereken delta's. Vermeld in _provider_notes.
- Als enkelvoudige meter: laat afnameDagKwh/afnameNachtKwh null en flag in _uncertain.

INJECTIE EN PV:
- Als injectie-meterstanden onveranderd: injectieKwh = 0, pvKwpAanwezig = null
  (NIET 0 — onbekend, niet "geen PV").
- Als injectie ≠ 0 maar geen kWp gevonden: pvKwpAanwezig = null, voeg
  "injectie_zonder_kwp" toe aan _provider_flags.

pvKwpAanwezig is STRIKT een GETAL (kWp piekvermogen) of null. NOOIT een string.

TOTAALBEDRAGEN — VERMIJD DUBBELTELLING:
- totaalEnergieExclBtw = ALLEEN energie-leverancier kosten (commodity, markup,
  groene stroom, vergroening, vaste kost). NIET distributiekosten,
  NIET heffingen, NIET capaciteit.
- totaalDistributieExclBtw = nettarieven (proportioneel, transport, databeheer)
  excl capaciteitstarief.
- totaalCapaciteitExclBtw = bruto capaciteitstarief regels (vóór maximum-
  tarief correctie). Som van alle "Capaciteitstarief" regels zonder de "-Maximumtarief"
  correctie eraf te trekken.
- totaalHeffingenExclBtw = bijdragen, accijnzen, energiefonds, federale bijdragen.
- totaalExclBtw = de TOTALE KOST (excl BTW) van de verbruiksperiode: de som van
  energie + distributie + capaciteit + heffingen zoals de factuur die optelt,
  VÓÓR aftrek van reeds betaalde voorschotten. Dit is NIET het "netto te betalen"
  saldo.
- totaalInclBtw = diezelfde TOTALE KOST maar inclusief BTW (eveneens vóór aftrek
  van voorschotten).

AFREKENING / SLOTFACTUUR / REGULARISATIE — VOORSCHOTTEN (ZEER BELANGRIJK):
Veel facturen zijn een AFREKENING: ze tonen eerst de volledige "Totale kost" /
"Totaal energiekost" / "Totaal van deze afrekening" voor de periode, en trekken
daarna reeds gefactureerde VOORSCHOTTEN af. Wat overblijft is een klein bedrag
"Saldo", "Te betalen", "Netto te betalen" of "Terug te krijgen".
  - Gebruik voor totaalExclBtw en totaalInclBtw ALTIJD de volledige TOTALE KOST
    (onderaan de afrekening) — NOOIT het netto te betalen saldo na voorschotten.
    Zoek expliciet naar de regel die het periodetotaal geeft vóór de aftrek van
    voorschotten; die som moet ± gelijk zijn aan energie+distributie+capaciteit+
    heffingen.
  - Zet het reeds betaalde/afgetrokken voorschot-bedrag (positief getal, incl BTW
    zoals vermeld) in "voorschottenInclBtw". Meerdere voorschotten: som ze op.
  - Zet het uiteindelijke netto te betalen (of terug te krijgen, dan negatief)
    saldo in "totaalTeBetalenInclBtw". Dit veld is puur informatief en wordt NIET
    als factuurtotaal gebruikt.
  - Gewone maandfactuur zonder voorschotten: voorschottenInclBtw = 0 en
    totaalTeBetalenInclBtw = totaalInclBtw.

JSON SCHEMA (output):
{
  "factuurNummer": string,
  "factuurDatum": "YYYY-MM-DD",
  "factuurType": "afname" | "injectie" | "gemengd" | "regularisatie",
  "klantNaam": string,
  "klantBtw": string of null,
  "klantAdres": string,
  "leveringsadres": string,
  "eanNrs": [string],
  "aansluitVermogenKva": number of null,
  "spanningsniveau": "LS" | "MS" | null,
  "dnb": string of null,
  "periodeVan": "YYYY-MM-DD",
  "periodeTot": "YYYY-MM-DD",
  "afnameKwh": number,
  "afnameDagKwh": number of null,
  "afnameNachtKwh": number of null,
  "injectieKwh": number,
  "pvKwpAanwezig": number of null,
  "totaalEnergieExclBtw": number,
  "totaalDistributieExclBtw": number,
  "totaalHeffingenExclBtw": number,
  "totaalCapaciteitExclBtw": number,
  "totaalExclBtw": number,
  "totaalInclBtw": number,
  "totaalTeBetalenInclBtw": number of null,
  "voorschottenInclBtw": number of null,
  "leverancier": string,
  "leverancierTariefformule": string,
  "_aansluitvermogenBron": {
    "toegangsvermogenKw": number of null,
    "maandpiekKw": number of null,
    "capaciteitKw": number of null
  },
  "_capaciteitstariefRegels": [
    { "van": "YYYY-MM-DD", "tot": "YYYY-MM-DD", "bedragExclBtw": number }
  ],
  "_provider_flags": [string],
  "_uncertain": [string],
  "_provider_notes": string
}

Geef ALLEEN het JSON object terug, zonder enige andere tekst eromheen.`;
}

// ─── Anthropic API call ──────────────────────────────────────────────────────
async function callAnthropic({ apiKey, model, files, timeoutMs = 30000 }) {
  const contentBlocks = files.map(f => {
    if (f.mediaType === 'application/pdf') {
      return { type: 'document', source: { type: 'base64', media_type: 'application/pdf', data: f.base64 } };
    } else if (['image/jpeg', 'image/png', 'image/gif', 'image/webp'].includes(f.mediaType)) {
      return { type: 'image', source: { type: 'base64', media_type: f.mediaType, data: f.base64 } };
    }
    throw new Error(`Niet-ondersteund bestandstype: ${f.mediaType}`);
  });
  contentBlocks.push({
    type: 'text',
    text: 'Extraheer alle velden uit deze factuur volgens het schema. Geef alleen het JSON object terug.'
  });

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'x-api-key': apiKey,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json'
      },
      body: JSON.stringify({
        model,
        max_tokens: 4000,
        system: buildSystemPrompt(),
        messages: [{ role: 'user', content: contentBlocks }]
      }),
      signal: controller.signal
    });

    if (!r.ok) {
      const errBody = await r.text().catch(() => '');
      throw new Error(`Anthropic HTTP ${r.status}: ${errBody.slice(0, 300)}`);
    }
    const json = await r.json();
    return { rawText: json.content?.[0]?.text || '', usage: json.usage || {} };
  } finally {
    clearTimeout(timer);
  }
}

// ─── JSON parser ─────────────────────────────────────────────────────────────
function parseJsonResponse(rawText) {
  let cleaned = rawText.trim();
  cleaned = cleaned.replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/i, '');
  const firstBrace = cleaned.indexOf('{');
  const lastBrace = cleaned.lastIndexOf('}');
  if (firstBrace > 0 && lastBrace > firstBrace) {
    cleaned = cleaned.slice(firstBrace, lastBrace + 1);
  }
  return JSON.parse(cleaned);
}

// ─── Postcode → DNB lookup ────────────────────────────────────────────────────
function extractPostcode(adres) {
  if (!adres || typeof adres !== 'string') return null;
  const m = adres.match(/\b([1-9]\d{3})\b/);
  return m ? m[1] : null;
}

function lookupDnb(adres, postcodes) {
  const pc = extractPostcode(adres);
  if (!pc) return { postcode: null, dnb: null, gemeenten: [] };
  const entry = postcodes[pc];
  if (!entry) return { postcode: pc, dnb: null, gemeenten: [] };
  return { postcode: pc, dnb: entry.dnb, gemeenten: entry.gemeenten || [] };
}

// ─── Factuur leeftijds- en periode-analyse ───────────────────────────────────
//
// BELANGRIJK CONTEXT:
// data/tarieven.json bevat ÉÉN tariefset per DNB|niveau (geen historiek).
// We nemen aan dat dat de huidige (referentie)tarieven zijn — typisch 2025-2026.
// Voor facturen waarvan de VERBRUIKSPERIODE volledig in een ander tariefjaar
// valt, kan de capaciteitstarief reverse-engineering systematisch afwijken
// (Vlaamse nettarieven stegen ~33% van 2024 → 2025 volgens Vlaamse Nutsregulator).
//
// Strategie tot historische tarieven beschikbaar zijn:
//   - leeftijdMaanden > 18 → factuur "oud" → RE niet vertrouwen (medium ipv high)
//   - factuur dekt jaar(en) ≠ TARIEVEN_REFERENTIEJAAR → flag in _provider_flags
//   - factuur overspant jaargrens → confidence-downgrade en flag
const TARIEVEN_REFERENTIEJAAR = 2025;  // Bump dit wanneer tarieven.json wordt vernieuwd

function isFactuurOud(factuurDatum, maxMaanden = 18) {
  if (!factuurDatum) return { oud: false, leeftijdMaanden: null };
  const fd = new Date(factuurDatum);
  if (isNaN(fd)) return { oud: false, leeftijdMaanden: null };
  const nu = new Date();
  const verschilMs = nu.getTime() - fd.getTime();
  const leeftijdMaanden = verschilMs / (1000 * 60 * 60 * 24 * 30.44);
  return {
    oud: leeftijdMaanden > maxMaanden,
    leeftijdMaanden: Math.round(leeftijdMaanden * 10) / 10
  };
}

// Welke kalenderjaren raakt een verbruiksperiode, en met welk %?
function jarenOverspand(periodeVan, periodeTot) {
  if (!periodeVan || !periodeTot) return null;
  const v = new Date(periodeVan);
  const t = new Date(periodeTot);
  if (isNaN(v) || isNaN(t) || v > t) return null;
  const totaalDagen = Math.round((t.getTime() - v.getTime()) / 86400000) + 1;
  const jaren = {};
  let cur = new Date(v);
  while (cur <= t) {
    const jr = cur.getFullYear();
    const eindJaar = new Date(jr, 11, 31);
    const eind = eindJaar < t ? eindJaar : t;
    const dagenDitJaar = Math.round((eind.getTime() - cur.getTime()) / 86400000) + 1;
    jaren[jr] = (jaren[jr] || 0) + dagenDitJaar / totaalDagen;
    cur = new Date(eind.getTime() + 86400000);
  }
  // Rond percentages af op 1 decimaal
  const out = {};
  for (const [jr, pct] of Object.entries(jaren)) {
    out[jr] = Math.round(pct * 1000) / 10;  // % met 1 decimaal
  }
  return out;
}

// Tariefjaar-confidence: hoe goed matcht de factuurperiode met onze tariefset?
function evalTariefjaarMatch(periodeVan, periodeTot) {
  const jaren = jarenOverspand(periodeVan, periodeTot);
  if (!jaren) return { match: 'unknown', jaren: null, opmerking: 'Geen periode beschikbaar' };

  const jrSleutels = Object.keys(jaren).map(Number);
  const refJaar = TARIEVEN_REFERENTIEJAAR;

  // Volledig in referentiejaar?
  if (jrSleutels.length === 1 && jrSleutels[0] === refJaar) {
    return { match: 'exact', jaren, opmerking: `Periode volledig in ${refJaar} (referentiejaar tarieven.json).` };
  }
  // Volledig in aangrenzend jaar (referentiejaar ± 1)?
  if (jrSleutels.length === 1 && Math.abs(jrSleutels[0] - refJaar) <= 1) {
    return {
      match: 'aangrenzend_jaar',
      jaren,
      opmerking: `Periode in ${jrSleutels[0]} — referentiejaar tarieven.json is ${refJaar}. Tarieven kunnen lichtjes afwijken.`
    };
  }
  // Overspant jaargrens met meerderheid in referentiejaar of aangrenzend?
  if (jrSleutels.length === 2) {
    const dichtst = jrSleutels.reduce((a, b) =>
      Math.abs(a - refJaar) < Math.abs(b - refJaar) ? a : b);
    if (Math.abs(dichtst - refJaar) <= 1) {
      const pctRef = jaren[refJaar] || 0;
      return {
        match: 'overspant_jaargrens',
        jaren,
        opmerking: `Periode overspant jaargrens — ${pctRef}% in referentiejaar ${refJaar}, rest in aangrenzend jaar.`
      };
    }
  }
  // Volledig in een ander tariefjaar (>1 jaar verschil)
  return {
    match: 'ander_tariefjaar',
    jaren,
    opmerking: `Periode (${jrSleutels.join(', ')}) ligt buiten referentiejaar ${refJaar} ± 1 — capaciteitstarief reverse-engineering NIET betrouwbaar.`
  };
}

// ─── Spanningsniveau-detector — beslisboom v1.1 ───────────────────────────────
function dagenTussen(vanStr, totStr) {
  const van = new Date(vanStr);
  const tot = new Date(totStr);
  if (isNaN(van) || isNaN(tot)) return null;
  const ms = tot.getTime() - van.getTime();
  return Math.round(ms / 86400000) + 1;
}

// LS reverse engineering — geeft 'high' (<2%), 'medium' (<5%), of 'low'
function runLsReverseEngineering(capRegels, dnbTariefKey, tarieven, gekozenKw) {
  if (!capRegels || capRegels.length === 0 || !dnbTariefKey || !gekozenKw) return 'low';
  const lsTarief = tarieven[`${dnbTariefKey}|LS`];
  if (!lsTarief) return 'low';
  const afwijkingen = [];
  for (const r of capRegels) {
    const dagen = dagenTussen(r.van, r.tot);
    if (!dagen) continue;
    const implKw = r.bedragExclBtw / (lsTarief.maandpiek_eur_kw_jaar * dagen / 365);
    afwijkingen.push(Math.abs(implKw - gekozenKw) / gekozenKw);
  }
  if (afwijkingen.length === 0) return 'low';
  const gem = afwijkingen.reduce((a, b) => a + b, 0) / afwijkingen.length;
  if (gem < 0.02) return 'high';
  if (gem < 0.05) return 'medium';
  return 'low';
}

function buildPeriodeAnalyse(capRegels, dnbTariefKey, tarieven) {
  const periodes = [];
  const lsTarief = dnbTariefKey ? tarieven[`${dnbTariefKey}|LS`] : null;
  const msTarief = dnbTariefKey ? tarieven[`${dnbTariefKey}|MS`] : null;
  if (!capRegels || !Array.isArray(capRegels)) return periodes;
  for (const r of capRegels) {
    const dagen = dagenTussen(r.van, r.tot);
    const lsImplKw = (lsTarief && dagen) ? r.bedragExclBtw / (lsTarief.maandpiek_eur_kw_jaar * dagen / 365) : null;
    const msImplKw = (msTarief && dagen) ? r.bedragExclBtw / (msTarief.maandpiek_eur_kw_jaar * dagen / 365) : null;
    periodes.push({
      van: r.van, tot: r.tot, dagen,
      bedrag_eur: r.bedragExclBtw,
      ls_impliciet_kw: lsImplKw !== null ? Math.round(lsImplKw * 100) / 100 : null,
      ms_impliciet_kw: msImplKw !== null ? Math.round(msImplKw * 100) / 100 : null,
      jaartarief_ls_eur_kw: lsTarief ? lsTarief.maandpiek_eur_kw_jaar : null,
      jaartarief_ms_eur_kw: msTarief ? msTarief.maandpiek_eur_kw_jaar : null
    });
  }
  return periodes;
}

function detectSpanningsniveau({
  expliciet,                  // wat AI uit factuur las (kan al "MS"/"LS" zijn)
  capaciteitstariefRegels,
  dnbTariefKey,
  tarieven,
  toegangsvermogenKw,
  heeftProportioneelKwh,      // boolean: kenmerk LS
  factuurOud,
  tariefjaarMatch             // { match: 'exact'|'aangrenzend_jaar'|'overspant_jaargrens'|'ander_tariefjaar' }
}) {
  const periodes = buildPeriodeAnalyse(capaciteitstariefRegels, dnbTariefKey, tarieven);

  // RE alleen vertrouwen als tariefjaar matcht referentie binnen ± 1 jaar
  const reBetrouwbaar = !factuurOud && tariefjaarMatch && 
                        ['exact', 'aangrenzend_jaar', 'overspant_jaargrens'].includes(tariefjaarMatch.match);

  // Stap 1: Expliciet vermeld op factuur
  if (expliciet === 'MS' || expliciet === 'HS' || expliciet === 'MV' ||
      /hoogspann|moyenne.tension/i.test(expliciet || '')) {
    return mkResult('MS', 'high', 'expliciet_in_factuur', periodes, factuurOud, tariefjaarMatch);
  }
  if (expliciet === 'LS' || expliciet === 'BT' ||
      /laagspann|basse.tension/i.test(expliciet || '')) {
    return mkResult('LS', 'high', 'expliciet_in_factuur', periodes, factuurOud, tariefjaarMatch);
  }

  // Stap 2: Toegangsvermogen-indicator
  if (typeof toegangsvermogenKw === 'number') {
    if (toegangsvermogenKw >= 100) {
      return mkResult('MS', 'medium', 'toegangsvermogen_>=_100kW', periodes, factuurOud, tariefjaarMatch);
    }
    if (toegangsvermogenKw <= 56) {
      let conf = 'medium';
      let reden = 'toegangsvermogen_<=_56kW';
      if (heeftProportioneelKwh) reden += '_+_proportioneel_kwh';
      if (reBetrouwbaar) {
        const re = runLsReverseEngineering(capaciteitstariefRegels, dnbTariefKey, tarieven, toegangsvermogenKw);
        if (re === 'high') {
          conf = 'high';
          reden += '_+_LS_RE_match';
        }
      } else if (capaciteitstariefRegels && capaciteitstariefRegels.length > 0) {
        reden += '_(RE_skipped:_' + (factuurOud ? 'factuur_oud' : tariefjaarMatch.match) + ')';
      }
      return mkResult('LS', conf, reden, periodes, factuurOud, tariefjaarMatch);
    }
    // 56 < kW < 100: grijze zone, val door naar stap 3
  }

  // Stap 3: LS reverse engineering zonder toegangsvermogen — alleen bij betrouwbaar tariefjaar
  if (reBetrouwbaar && capaciteitstariefRegels && capaciteitstariefRegels.length > 0 && dnbTariefKey && toegangsvermogenKw) {
    const conf = runLsReverseEngineering(capaciteitstariefRegels, dnbTariefKey, tarieven, toegangsvermogenKw);
    if (conf !== 'low') {
      return mkResult('LS', conf, 'LS_reverse_engineering', periodes, factuurOud, tariefjaarMatch);
    }
  }

  // Stap 4: Niets sluitend
  return mkResult(null, 'low', 'onbepaald', periodes, factuurOud, tariefjaarMatch);
}

function mkResult(niveau, confidence, methode, periodes, factuurOud, tariefjaarMatch) {
  let opmerking;
  if (niveau === null) {
    opmerking = 'Spanningsniveau niet sluitend te bepalen — manuele validatie vereist';
  } else if (factuurOud && /reverse_engineering|RE_match/.test(methode)) {
    opmerking = `${niveau} bepaald via ${methode}. ⚠ Factuur > 18 mnd oud — RE niet 100% betrouwbaar tegen huidige tarieven.json.`;
  } else if (tariefjaarMatch && tariefjaarMatch.match === 'ander_tariefjaar' && /reverse_engineering|RE_match/.test(methode)) {
    opmerking = `${niveau} bepaald via ${methode}. ⚠ ${tariefjaarMatch.opmerking}`;
  } else {
    opmerking = `${niveau} bepaald via ${methode}.`;
  }
  return {
    spanningsniveau_gedetecteerd: niveau,
    detectie_methode: methode,
    detectie_confidence: confidence,
    periodes,
    consistentie_check: opmerking
  };
}

// ─── Sumcheck (consistentievalidatie) ─────────────────────────────────────────
function consistentieCheck(parsed) {
  const e = parseFloat(parsed.totaalEnergieExclBtw) || 0;
  const d = parseFloat(parsed.totaalDistributieExclBtw) || 0;
  const h = parseFloat(parsed.totaalHeffingenExclBtw) || 0;
  const c = parseFloat(parsed.totaalCapaciteitExclBtw) || 0;
  const totaal = parseFloat(parsed.totaalExclBtw) || 0;

  const som = e + d + h + c;
  const delta = som - totaal;
  const deltaPct = totaal > 0.01 ? Math.abs(delta) / totaal : 0;

  const result = {
    sum_componenten: Math.round(som * 100) / 100,
    totaal_factuur: totaal,
    delta_eur: Math.round(delta * 100) / 100,
    delta_pct: Math.round(deltaPct * 1000) / 10
  };

  if (totaal === 0 && som === 0) {
    result.status = 'leeg_geen_check';
    result.opmerking = 'Geen bedragen — sumcheck overgeslagen.';
    return result;
  }
  if (deltaPct < 0.01) {
    result.status = 'OK';
    result.opmerking = `Som componenten ≈ totaalExclBtw (${result.delta_pct}% afwijking).`;
  } else if (Math.abs(delta - c) < Math.max(0.5, c * 0.01)) {
    result.status = 'CAPACITEIT_DUBBEL';
    result.opmerking = `Som componenten = totaalExclBtw + capaciteitstarief — capaciteit zit waarschijnlijk al in distributie of energie verwerkt.`;
  } else {
    result.status = 'AFWIJKING';
    result.opmerking = `Som componenten wijkt €${result.delta_eur} (${result.delta_pct}%) af van totaal — mogelijk dubbeltelling, voorschotten of correcties.`;
  }
  return result;
}

// ─── Hoofdfunctie ────────────────────────────────────────────────────────────
async function run({ files, postcodes, tarieven, apiKey, model, retries = 1 }) {
  if (!apiKey) throw new Error('ANTHROPIC_API_KEY niet beschikbaar');
  if (!Array.isArray(files) || files.length === 0) throw new Error('Geen bestanden meegegeven');
  if (!postcodes || typeof postcodes !== 'object') throw new Error('postcodes lookup niet beschikbaar');
  if (!tarieven || typeof tarieven !== 'object') throw new Error('tarieven lookup niet beschikbaar');

  const usedModel = model || process.env.FACTUUR_MODEL || 'claude-sonnet-4-5';
  const t0 = Date.now();

  // Stap 1: AI-extractie met retry
  let aiResult, parsed, lastError;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      aiResult = await callAnthropic({ apiKey, model: usedModel, files });
      parsed = parseJsonResponse(aiResult.rawText);
      break;
    } catch (e) {
      lastError = e;
      if (attempt === retries) throw new Error(`Extractie mislukt na ${retries + 1} pogingen: ${e.message}`);
    }
  }

  // Stap 2: server-side validatie en aanrijking
  const _uncertain = Array.isArray(parsed._uncertain) ? [...parsed._uncertain] : [];
  const _provider_flags = Array.isArray(parsed._provider_flags) ? [...parsed._provider_flags] : [];

  // 2a. DNB lookup
  const lookupAdres = parsed.leveringsadres || parsed.klantAdres || '';
  const dnbLookup = lookupDnb(lookupAdres, postcodes);
  let dnbFinal = parsed.dnb || dnbLookup.dnb;
  let dnbDiscrepancy = null;
  if (parsed.dnb && dnbLookup.dnb && parsed.dnb.trim() !== dnbLookup.dnb.trim()) {
    dnbDiscrepancy = `Factuur vermeldt '${parsed.dnb}', postcode-lookup geeft '${dnbLookup.dnb}'`;
    if (!_uncertain.includes('dnb_discrepancy')) _uncertain.push('dnb_discrepancy');
  }

  // 2b. Aansluitvermogen consolidatie
  const bron = parsed._aansluitvermogenBron || {};
  ['toegangsvermogenKw', 'maandpiekKw', 'capaciteitKw'].forEach(k => {
    if (bron[k] !== null && bron[k] !== undefined && typeof bron[k] !== 'number') {
      const n = parseFloat(bron[k]);
      bron[k] = isNaN(n) ? null : n;
    }
  });
  const kandidaten = [bron.toegangsvermogenKw, bron.maandpiekKw, bron.capaciteitKw]
    .filter(v => typeof v === 'number' && v > 0);
  let gekozenKw = null;
  let aansluitVermogenKva = parsed.aansluitVermogenKva;
  if (kandidaten.length > 0) {
    gekozenKw = Math.max(...kandidaten);
    const berekendKva = Math.round((gekozenKw / 0.95) * 10) / 10;
    if (typeof aansluitVermogenKva !== 'number' || aansluitVermogenKva === null) {
      aansluitVermogenKva = berekendKva;
    }
    bron.gekozenMaxKw = gekozenKw;
    bron.berekendKva = berekendKva;
    bron.cosphiGebruikt = 0.95;
    const idx = _uncertain.indexOf('aansluitVermogenKva');
    if (idx >= 0) _uncertain.splice(idx, 1);
  } else if (aansluitVermogenKva === null || aansluitVermogenKva === undefined) {
    if (!_uncertain.includes('aansluitVermogenKva')) _uncertain.push('aansluitVermogenKva');
  }

  // 2c. Leeftijdscheck + tariefjaar-analyse
  const leeftijd = isFactuurOud(parsed.factuurDatum);
  if (leeftijd.oud) {
    _provider_flags.push(`factuur_oud_${leeftijd.leeftijdMaanden}_maanden`);
  }
  const tariefjaarMatch = evalTariefjaarMatch(parsed.periodeVan, parsed.periodeTot);
  if (tariefjaarMatch.match === 'ander_tariefjaar') {
    _provider_flags.push(`periode_ander_tariefjaar:${Object.keys(tariefjaarMatch.jaren || {}).join(',')}`);
  } else if (tariefjaarMatch.match === 'overspant_jaargrens') {
    _provider_flags.push(`periode_overspant_jaargrens:${Object.keys(tariefjaarMatch.jaren || {}).join(',')}`);
  }

  // 2d. Spanningsniveau-detectie
  const dnbTariefKey = dnbFinal ? DNB_TO_TARIEF_KEY[dnbFinal] : null;
  const heeftProportioneelKwh = (parseFloat(parsed.totaalDistributieExclBtw) || 0) > 0 &&
                                 (parseFloat(parsed.afnameKwh) || 0) > 0;
  const capaciteitAnalyse = detectSpanningsniveau({
    expliciet: parsed.spanningsniveau,
    capaciteitstariefRegels: parsed._capaciteitstariefRegels || [],
    dnbTariefKey,
    tarieven,
    toegangsvermogenKw: gekozenKw,
    heeftProportioneelKwh,
    factuurOud: leeftijd.oud,
    tariefjaarMatch
  });

  let spanningsniveauFinal = capaciteitAnalyse.spanningsniveau_gedetecteerd || parsed.spanningsniveau || null;
  if (spanningsniveauFinal && capaciteitAnalyse.detectie_confidence !== 'low') {
    const idx = _uncertain.indexOf('spanningsniveau');
    if (idx >= 0) _uncertain.splice(idx, 1);
  }

  // 2e. factuurType bepaling
  let factuurType = parsed.factuurType || null;
  if (!factuurType) {
    const afn = parseFloat(parsed.afnameKwh) || 0;
    const inj = parseFloat(parsed.injectieKwh) || 0;
    if (afn === 0 && inj > 0) factuurType = 'injectie';
    else if (afn > 0 && inj > 0 && inj / afn > 0.05) factuurType = 'gemengd';
    else if (afn > 0) factuurType = 'afname';
    else factuurType = 'afname';
  }

  // 2f. pvKwpAanwezig: strict number|null
  let pvKwp = parsed.pvKwpAanwezig;
  if (pvKwp !== null && pvKwp !== undefined && typeof pvKwp !== 'number') {
    if (typeof pvKwp === 'string' && !_provider_flags.includes(`pvKwpAanwezig_string:${pvKwp}`)) {
      _provider_flags.push(`pvKwpAanwezig_string:${pvKwp}`);
    }
    pvKwp = null;
  }

  // 2g. Sumcheck consistentie
  const consistentie = consistentieCheck(parsed);

  // 2g-bis. Afrekening/voorschot-reconciliatie (v1.3).
  // Op een AFREKENING/SLOTFACTUUR wordt soms het NETTO te betalen saldo (na
  // aftrek van reeds betaalde voorschotten) als totaalExclBtw/totaalInclBtw
  // gelezen i.p.v. de TOTALE kost. De som van de kost-componenten
  // (energie+distributie+heffingen+capaciteit) is de bruto periode-kost en is
  // onafhankelijk van voorschotten. Wanneer die som de gelezen totaalExclBtw
  // duidelijk overschrijdt (>5%) ÉN er een voorschot-indicatie is — en het géén
  // capaciteit-dubbeltelling betreft — corrigeren we naar de bruto totale kost.
  // De simulatie vergelijkt periodekost vs simulatiekost; een netto saldo (na
  // voorschotten) zou die vergelijking systematisch onderschatten.
  let totaalExclBtwFinal = parseFloat(parsed.totaalExclBtw);
  if (!isFinite(totaalExclBtwFinal)) totaalExclBtwFinal = 0;
  let totaalInclBtwFinal = (parsed.totaalInclBtw !== null && parsed.totaalInclBtw !== undefined && isFinite(parseFloat(parsed.totaalInclBtw)))
    ? parseFloat(parsed.totaalInclBtw) : null;
  let voorschotCorrectie = null;

  const somComponenten = consistentie.sum_componenten;            // e+d+h+c
  const voorschotIncl = parseFloat(parsed.voorschottenInclBtw) || 0;
  const heeftVoorschotIndicatie = voorschotIncl > 0 ||
    factuurType === 'regularisatie' ||
    /afreken|slotfactuur|regularisat|voorschot/i.test(parsed._provider_notes || '');
  const deltaSomTotaal = somComponenten - totaalExclBtwFinal;      // >0 => totaal te laag gelezen
  const relAfwijking = totaalExclBtwFinal > 0.01 ? (deltaSomTotaal / totaalExclBtwFinal) : 0;

  if (consistentie.status === 'AFWIJKING' &&
      deltaSomTotaal > 0 && relAfwijking > 0.05 &&
      heeftVoorschotIndicatie && somComponenten > 0) {
    const btwRatio = (totaalInclBtwFinal && totaalExclBtwFinal > 0.01)
      ? (totaalInclBtwFinal / totaalExclBtwFinal) : 1.21;
    voorschotCorrectie = {
      gelezen_totaalExclBtw: totaalExclBtwFinal,
      gecorrigeerd_totaalExclBtw: somComponenten,
      voorschottenInclBtw: voorschotIncl || null,
      netto_te_betalen_inclBtw: (parsed.totaalTeBetalenInclBtw !== null && parsed.totaalTeBetalenInclBtw !== undefined)
        ? parseFloat(parsed.totaalTeBetalenInclBtw) : totaalInclBtwFinal,
      reden: 'Gelezen totaal leek het netto te betalen saldo (na aftrek voorschotten); ' +
             'gecorrigeerd naar de bruto totale kost = som van de kost-componenten.'
    };
    totaalExclBtwFinal = somComponenten;
    totaalInclBtwFinal = Math.round(somComponenten * btwRatio * 100) / 100;
    if (!_provider_flags.includes('totaal_gecorrigeerd_voor_voorschotten')) {
      _provider_flags.push('totaal_gecorrigeerd_voor_voorschotten');
    }
    // Sumcheck bijwerken zodat de wizard geen valse AFWIJKING-waarschuwing toont.
    consistentie.status = 'OK_NA_VOORSCHOT_CORRECTIE';
    consistentie.totaal_factuur = totaalExclBtwFinal;
    consistentie.delta_eur = 0;
    consistentie.delta_pct = 0;
    consistentie.opmerking = `Totaal gecorrigeerd naar bruto totale kost (€${somComponenten}) ` +
      `na detectie van aftrek-voorschotten; som componenten klopt nu.`;
  }

  // Stap 3: response payload
  const baseCase = {
    ...parsed,
    factuurType,
    aansluitVermogenKva,
    totaalExclBtw: totaalExclBtwFinal,
    totaalInclBtw: totaalInclBtwFinal,
    _voorschot_correctie: voorschotCorrectie,
    spanningsniveau: spanningsniveauFinal,
    dnb: dnbFinal,
    pvKwpAanwezig: pvKwp,
    _aansluitvermogenBron: bron,
    _capaciteitstariefAnalyse: capaciteitAnalyse,
    _dnbLookup: {
      postcode: dnbLookup.postcode,
      dnb_uit_postcode: dnbLookup.dnb,
      dnb_op_factuur: parsed.dnb || null,
      gemeenten: dnbLookup.gemeenten,
      discrepancy: dnbDiscrepancy,
      tariefKey_gebruikt: dnbTariefKey
    },
    _consistentie: consistentie,
    _factuur_leeftijd: leeftijd,
    _tariefjaar_match: {
      ...tariefjaarMatch,
      tarieven_referentiejaar: TARIEVEN_REFERENTIEJAAR
    },
    _provider_flags,
    _uncertain
  };

  return {
    ok: true,
    baseCase,
    _meta: {
      model: usedModel,
      duration_ms: Date.now() - t0,
      input_tokens: aiResult.usage.input_tokens,
      output_tokens: aiResult.usage.output_tokens,
      n_files: files.length,
      version: '1.3'
    }
  };
}

module.exports = { run, DNB_TO_TARIEF_KEY };
