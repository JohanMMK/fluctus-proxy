'use strict';

/**
 * Fluctus Simulator — BaseCase factuur-extractie (Fase 1)
 * =======================================================
 * Module: factuur/extract.js
 * Versie: 1.0 (mei 2026)
 *
 * Doel: Belgische elektriciteitsfactuur (PDF base64) → gestructureerde JSON
 * volgens STATE.baseCase schema (zie BaseCase Uitbreiding §4).
 *
 * Architectuur-context:
 *   - Pure addition: geen wijziging aan bestaande server.js routes/helpers.
 *   - Geen nieuwe npm dependencies (gebruikt native fetch van Node 22).
 *   - Anthropic API call patroon overgenomen van /claude-explain-refresh
 *     route in server.js (regels 862-874) voor consistentie.
 *
 * Afwijkingen van BaseCase Uitbreiding v1 (gedocumenteerd):
 *   1. Body format: JSON met base64 i.p.v. multipart/form-data
 *      Reden: hergebruik van bestaande express.json({limit:'20mb'}) middleware,
 *      geen multer dependency, eenvoudiger curl-testen.
 *   2. Default model: claude-sonnet-4-5 i.p.v. claude-haiku-4-5-20251001
 *      Reden: Sonnet geeft hogere extractie-nauwkeurigheid voor complexe
 *      tabellen op MS-facturen (multi-EAN, multi-periode). Kostenverschil
 *      ~€0.012 per factuur is verwaarloosbaar bij verwacht volume.
 *      Override via env: FACTUUR_MODEL=claude-haiku-4-5-20251001
 *   3. STATE.baseCase uitgebreid met debug-velden _aansluitvermogenBron en
 *      _capaciteitstariefAnalyse. Reden: traceerbaarheid van afgeleide
 *      waarden (kVA conversie, spanningsniveau-detectie via reverse engineering).
 */

// ─── DNB naam-mapping ─────────────────────────────────────────────────────────
// postcodes.json gebruikt 'Fluvius West', tarieven.json gebruikt 'West'.
// Deze map normaliseert naar de tarieven.json key.
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
  'AIESH': null,   // geen tarieven in tarieven.json — flag in _uncertain
};

// ─── Prompt builder ───────────────────────────────────────────────────────────
function buildSystemPrompt() {
  return `Je bent een expert in het lezen van Belgische elektriciteitsfacturen.

TAAK: Extraheer alle relevante velden uit de aangeleverde factuur (PDF) en geef
ze terug als VALIDE JSON volgens het onderstaande schema. Geef GEEN extra tekst,
GEEN markdown code-fences, ALLEEN het JSON object.

REGELS:
- Datumformaat: ALTIJD YYYY-MM-DD. Belgische facturen tonen meestal DD/MM/YYYY
  of DD-MM-YYYY — converteer correct.
- Eenheden: alle bedragen in EUR (excl BTW tenzij expliciet anders gevraagd),
  alle verbruiken in kWh (NIET MWh). Als de factuur MWh gebruikt: vermenigvuldig
  met 1000 voor kWh.
- Voor velden waar je NIET ZEKER bent: laat ze leeg (null) en voeg de veldnaam
  toe aan de "_uncertain" array. Maak NOOIT velden op.
- Indien meerdere EAN-nummers op de factuur staan: lijst ze allemaal in eanNrs.

AANSLUITVERMOGEN (BIJZONDER BELANGRIJK):
Zoek op de factuur ALLE voorkomens van vermogen-velden, in deze volgorde van prioriteit:
  a) "toegangsvermogen" of "Toegangsvermogen" (meestal in kW)
  b) "maandpiek" of "gemiddelde maandpiek" (meestal in kW)
  c) "(piek)capaciteit" of "capaciteitstarief basis" (meestal in kW)

Vul deze als kW-waarden in onder _aansluitvermogenBron:
  _aansluitvermogenBron: {
    toegangsvermogenKw: <getal of null>,
    maandpiekKw: <getal of null>,
    capaciteitKw: <getal of null>
  }

Bereken aansluitVermogenKva = max(beschikbare kW-waarden) / 0.95 (cosφ=0.95),
afgerond op 1 decimaal. Als geen enkele kW-waarde gevonden: aansluitVermogenKva=null
en flag in _uncertain.

CAPACITEITSTARIEF EXTRACTIE:
Zoek naar regels die "Capaciteitstarief" als kostenpost bevatten. Voor ELKE
periode-regel, extraheer:
  { van: "YYYY-MM-DD", tot: "YYYY-MM-DD", bedragExclBtw: <getal> }
Plaats deze als array in _capaciteitstariefRegels. De server zal hieruit het
spanningsniveau valideren via reverse engineering tegen de DNB-tarieven.

DAG/NACHT VERBRUIK:
Als de factuur expliciete kWh-totalen toont voor dag en nacht: vul afnameDagKwh
en afnameNachtKwh rechtstreeks in.
Als alleen METERSTANDEN gegeven zijn (Beginmeterstand, Eindmeterstand voor Dag
en Nacht): bereken de delta's en vul ze in. Vermeld in _provider_notes dat ze
zijn afgeleid uit meterstanden.

INJECTIE:
Als injectie-meterstanden onveranderd zijn (begin = eind): injectieKwh = 0
en pvKwpAanwezig = null (NIET 0 — onbekend, niet "geen").
Als injectie-meterstanden delta hebben: bereken kWh-delta en flag pvKwpAanwezig
als "afgeleid_uit_injectie" in _provider_notes als geen expliciete kWp staat.

JSON SCHEMA (output):
{
  "factuurNummer": string,
  "factuurDatum": "YYYY-MM-DD",
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
  "_uncertain": [string],
  "_provider_notes": string
}

Geef ALLEEN het JSON object terug, zonder enige andere tekst eromheen.`;
}

// ─── Anthropic API call ──────────────────────────────────────────────────────
async function callAnthropic({ apiKey, model, files, timeoutMs = 30000 }) {
  // files: [{ filename, mediaType, base64 }]
  // Bouw content blocks: 1 document/image per file + 1 text instructie
  const contentBlocks = files.map(f => {
    if (f.mediaType === 'application/pdf') {
      return {
        type: 'document',
        source: { type: 'base64', media_type: 'application/pdf', data: f.base64 }
      };
    } else if (['image/jpeg', 'image/png', 'image/gif', 'image/webp'].includes(f.mediaType)) {
      return {
        type: 'image',
        source: { type: 'base64', media_type: f.mediaType, data: f.base64 }
      };
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
        model: model,
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
    const text = json.content?.[0]?.text || '';
    return { rawText: text, usage: json.usage || {} };
  } finally {
    clearTimeout(timer);
  }
}

// ─── JSON parser met retry-tolerantie ────────────────────────────────────────
function parseJsonResponse(rawText) {
  // Probeer rechtstreeks
  let cleaned = rawText.trim();
  // Verwijder eventuele markdown code-fences (```json ... ``` of ``` ... ```)
  cleaned = cleaned.replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/i, '');
  // Soms voegt het model een preamble toe vóór het { → trim tot eerste {
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

// ─── Spanningsniveau-detector via capaciteitstarief reverse engineering ──────
function dagenTussen(vanStr, totStr) {
  const van = new Date(vanStr);
  const tot = new Date(totStr);
  if (isNaN(van) || isNaN(tot)) return null;
  // Inclusief begindag, exclusief einddag → +1 voor inclusief beide
  const ms = tot.getTime() - van.getTime();
  return Math.round(ms / 86400000) + 1;
}

function detectSpanningsniveau({ capaciteitstariefRegels, dnbTariefKey, tarieven, gekozenKw }) {
  if (!dnbTariefKey || !capaciteitstariefRegels || capaciteitstariefRegels.length === 0) {
    return {
      spanningsniveau_gedetecteerd: null,
      detectie_methode: 'geen_data',
      detectie_confidence: 'low',
      periodes: [],
      consistentie_check: 'Onvoldoende data om spanningsniveau af te leiden'
    };
  }

  const lsKey = `${dnbTariefKey}|LS`;
  const msKey = `${dnbTariefKey}|MS`;
  const lsTarief = tarieven[lsKey];
  const msTarief = tarieven[msKey];
  if (!lsTarief && !msTarief) {
    return {
      spanningsniveau_gedetecteerd: null,
      detectie_methode: 'geen_tarieven_voor_dnb',
      detectie_confidence: 'low',
      periodes: [],
      consistentie_check: `Geen tarieven gevonden in tarieven.json voor DNB-key '${dnbTariefKey}'`
    };
  }

  // Bereken voor elke periode het impliciete kW-vermogen onder LS- en MS-hypothese
  const periodes = capaciteitstariefRegels.map(r => {
    const dagen = dagenTussen(r.van, r.tot);
    const lsImplKw = (lsTarief && dagen) ? r.bedragExclBtw / (lsTarief.maandpiek_eur_kw_jaar * dagen / 365) : null;
    const msImplKw = (msTarief && dagen) ? r.bedragExclBtw / (msTarief.maandpiek_eur_kw_jaar * dagen / 365) : null;
    return {
      van: r.van, tot: r.tot, dagen,
      bedrag_eur: r.bedragExclBtw,
      ls_impliciet_kw: lsImplKw !== null ? Math.round(lsImplKw * 100) / 100 : null,
      ms_impliciet_kw: msImplKw !== null ? Math.round(msImplKw * 100) / 100 : null,
      jaartarief_ls_eur_kw: lsTarief ? lsTarief.maandpiek_eur_kw_jaar : null,
      jaartarief_ms_eur_kw: msTarief ? msTarief.maandpiek_eur_kw_jaar : null
    };
  });

  // Welk niveau matcht het best met het gekozen kW-getal?
  if (gekozenKw === null || gekozenKw === undefined) {
    return {
      spanningsniveau_gedetecteerd: null,
      detectie_methode: 'geen_referentie_kw',
      detectie_confidence: 'low',
      periodes,
      consistentie_check: 'Kan spanningsniveau niet bepalen zonder referentie-kW (toegangsvermogen/maandpiek)'
    };
  }

  // Gemiddelde afwijking per hypothese
  const lsAfwijkingen = periodes.filter(p => p.ls_impliciet_kw !== null).map(p => Math.abs(p.ls_impliciet_kw - gekozenKw) / gekozenKw);
  const msAfwijkingen = periodes.filter(p => p.ms_impliciet_kw !== null).map(p => Math.abs(p.ms_impliciet_kw - gekozenKw) / gekozenKw);
  const lsGemAfwijking = lsAfwijkingen.length ? lsAfwijkingen.reduce((a, b) => a + b, 0) / lsAfwijkingen.length : Infinity;
  const msGemAfwijking = msAfwijkingen.length ? msAfwijkingen.reduce((a, b) => a + b, 0) / msAfwijkingen.length : Infinity;

  let niveau, confidence, methode, opmerking;
  if (lsGemAfwijking < 0.05 && lsGemAfwijking < msGemAfwijking) {
    niveau = 'LS';
    confidence = lsGemAfwijking < 0.02 ? 'high' : 'medium';
    methode = 'tarief_reverse_engineering';
    opmerking = `OK — impliciet vermogen LS-hypothese (${lsAfwijkingen.length ? (lsAfwijkingen.reduce((a,b)=>a+b,0)/lsAfwijkingen.length*100).toFixed(1) : '?'}% afwijking) matcht gekozen vermogen ${gekozenKw} kW`;
  } else if (msGemAfwijking < 0.05 && msGemAfwijking < lsGemAfwijking) {
    niveau = 'MS';
    confidence = msGemAfwijking < 0.02 ? 'high' : 'medium';
    methode = 'tarief_reverse_engineering';
    opmerking = `OK — impliciet vermogen MS-hypothese (${msAfwijkingen.length ? (msAfwijkingen.reduce((a,b)=>a+b,0)/msAfwijkingen.length*100).toFixed(1) : '?'}% afwijking) matcht gekozen vermogen ${gekozenKw} kW`;
  } else {
    niveau = null;
    confidence = 'low';
    methode = 'tarief_reverse_engineering_inconclusief';
    opmerking = `Geen van LS (${(lsGemAfwijking*100).toFixed(1)}%) of MS (${(msGemAfwijking*100).toFixed(1)}%) matcht binnen 5% van gekozen vermogen ${gekozenKw} kW`;
  }

  return {
    spanningsniveau_gedetecteerd: niveau,
    detectie_methode: methode,
    detectie_confidence: confidence,
    periodes,
    consistentie_check: opmerking
  };
}

// ─── Hoofdfunctie ────────────────────────────────────────────────────────────
async function run({ files, postcodes, tarieven, apiKey, model, retries = 1 }) {
  if (!apiKey) throw new Error('ANTHROPIC_API_KEY niet beschikbaar');
  if (!Array.isArray(files) || files.length === 0) throw new Error('Geen bestanden meegegeven');
  if (!postcodes || typeof postcodes !== 'object') throw new Error('postcodes lookup niet beschikbaar');
  if (!tarieven || typeof tarieven !== 'object') throw new Error('tarieven lookup niet beschikbaar');

  const usedModel = model || process.env.FACTUUR_MODEL || 'claude-sonnet-4-5';
  const t0 = Date.now();

  // Stap 1: AI-extractie met retry op JSON parse fout
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

  // 2a. DNB lookup via leveringsadres (fallback: klantAdres)
  const lookupAdres = parsed.leveringsadres || parsed.klantAdres || '';
  const dnbLookup = lookupDnb(lookupAdres, postcodes);

  // Cross-check met factuur-vermelde DNB indien aanwezig
  let dnbFinal = parsed.dnb || dnbLookup.dnb;
  let dnbDiscrepancy = null;
  if (parsed.dnb && dnbLookup.dnb && parsed.dnb.trim() !== dnbLookup.dnb.trim()) {
    dnbDiscrepancy = `Factuur vermeldt '${parsed.dnb}', postcode-lookup geeft '${dnbLookup.dnb}'`;
    _uncertain.push('dnb_discrepancy');
  }

  // 2b. Aansluitvermogen consolidatie
  const bron = parsed._aansluitvermogenBron || {};
  const kandidaten = [
    bron.toegangsvermogenKw,
    bron.maandpiekKw,
    bron.capaciteitKw
  ].filter(v => typeof v === 'number' && v > 0);

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
  } else if (aansluitVermogenKva === null || aansluitVermogenKva === undefined) {
    if (!_uncertain.includes('aansluitVermogenKva')) _uncertain.push('aansluitVermogenKva');
  }

  // 2c. Spanningsniveau-detectie via reverse engineering
  const dnbTariefKey = dnbFinal ? DNB_TO_TARIEF_KEY[dnbFinal] : null;
  const capaciteitAnalyse = detectSpanningsniveau({
    capaciteitstariefRegels: parsed._capaciteitstariefRegels || [],
    dnbTariefKey,
    tarieven,
    gekozenKw
  });

  // Als spanningsniveau op factuur ontbreekt en we hebben een detectie: gebruik die
  let spanningsniveauFinal = parsed.spanningsniveau;
  if (!spanningsniveauFinal && capaciteitAnalyse.spanningsniveau_gedetecteerd) {
    spanningsniveauFinal = capaciteitAnalyse.spanningsniveau_gedetecteerd;
  } else if (spanningsniveauFinal && capaciteitAnalyse.spanningsniveau_gedetecteerd
             && spanningsniveauFinal !== capaciteitAnalyse.spanningsniveau_gedetecteerd) {
    _uncertain.push('spanningsniveau_discrepancy');
  }

  // Stap 3: response-payload bouwen
  const baseCase = {
    ...parsed,
    aansluitVermogenKva,
    spanningsniveau: spanningsniveauFinal,
    dnb: dnbFinal,
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
      n_files: files.length
    }
  };
}

module.exports = { run, DNB_TO_TARIEF_KEY };
