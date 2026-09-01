'use strict';

/**
 * Fluctus EnergieKompas — Offerte-upload & heranalyse
 * ===================================================
 * Module: factuur/offerte.js
 * Versie: 1.0.0 (2026-09-01, Fase 6)
 *
 * Doel: een klant (of een prospect) laadt een OFFERTE op — een eigen offerte van een
 * installateur, of een CONCURRENT-offerte — en wij geven er een eerlijke "second opinion"
 * op tegen de EnergieKompas-studie. We LEZEN de offerte (Claude-vision op de PDF/foto),
 * halen de kerncijfers eruit (investering, jaarlijkse kost, PV/batterij/laadpalen, looptijd,
 * beloofde besparing) en vergelijken die met wat onze studie voor deze klant toont.
 *
 * BELANGRIJK — verdienmodel-kader (anti-regressie): het Fluctus-verhaal is UITSLUITEND
 * energy arbitrage (spot day-ahead / intraday) + imbalance settlement (passieve respons).
 * Geen enkele netbalanceringsdienst. De heranalyse benoemt de Fluctus-meerwaarde enkel in die termen.
 *
 * GEEN financieel advies: de heranalyse presenteert FEITEN + een vergelijking, en laat de
 * klant zelf beslissen. Geen "u moet kopen/tekenen"-taal.
 *
 * Exports:
 *   - extractOfferte({files, apiKey, model})  → { ok, offerte, _meta }
 *   - bouwHeranalyse(offerte, studie)         → { ... } (puur, testbaar)
 *   - _normaliseerOfferte(parsed)             → genormaliseerde offerte (puur, testbaar)
 */

const OFFERTE_SCHEMA_PROMPT = `Je bent een technisch analist bij Fluctus. Je krijgt een OFFERTE (of prijsvoorstel)
voor energie-infrastructuur: zonnepanelen (PV), een batterij (BESS), laadpalen, of een combinatie.
De offerte kan van een installateur komen of van een concurrent. Lees ze zorgvuldig en geef ALLEEN
een JSON-object terug met exact deze velden (gebruik null als een waarde niet in de offerte staat —
verzin nooit een getal):

{
  "installateur": string|null,            // naam van de aanbieder/installateur
  "is_concurrent": boolean,               // true als het duidelijk GEEN Fluctus/partner-offerte is
  "componenten": string[],                // subset van ["pv","batterij","laadpalen","laadpaal_thuis","omvormer","beheer","overig"]
  "investering_excl_btw": number|null,    // totale eenmalige investering excl. btw (EUR)
  "investering_incl_btw": number|null,    // totale eenmalige investering incl. btw (EUR)
  "jaarlijkse_kost_excl_btw": number|null,// terugkerende kost/jaar (onderhoud, abonnement, monitoring) excl. btw
  "looptijd_jaar": number|null,           // contract-/garantie-/afschrijvingslooptijd in jaren
  "pv_kwp": number|null,                  // geoffreerd PV-vermogen in kWp
  "batterij_kwh": number|null,            // batterijcapaciteit in kWh
  "batterij_kw": number|null,             // batterijvermogen in kW
  "laadpalen": [ { "aantal": number, "kw": number|null, "type": string|null } ],
  "belooft_besparing_eur_jaar": number|null, // als de offerte een jaarlijkse besparing claimt (EUR/jaar)
  "belooft_payback_jaar": number|null,       // als de offerte een terugverdientijd claimt (jaren)
  "prijs_kwh_eur": number|null,           // als de offerte een vaste energieprijs (EUR/kWh) vastlegt
  "energiecontract": boolean,             // true als de offerte ook een (vast) energie-/leveringscontract omvat
  "opmerkingen": string[],                // korte feitelijke observaties (bv. "geen dynamische sturing vermeld", "10 jaar garantie omvormer")
  "_onzeker": string[]                    // veldnamen die je onzeker uit de offerte hebt gehaald
}

Reken bedragen om naar hele euro's. Geef enkel het JSON-object, geen uitleg.`;

// ─── Anthropic vision-call (zelfstandig; zelfde patroon als de factuur-extractie) ─────────────────
async function _callClaude({ apiKey, model, files, timeoutMs = 120000 }) {
  const contentBlocks = files.map(f => {
    if (f.mediaType === 'application/pdf') {
      return { type: 'document', source: { type: 'base64', media_type: 'application/pdf', data: f.base64 } };
    } else if (['image/jpeg', 'image/png', 'image/gif', 'image/webp'].includes(f.mediaType)) {
      return { type: 'image', source: { type: 'base64', media_type: f.mediaType, data: f.base64 } };
    }
    throw new Error(`Niet-ondersteund bestandstype: ${f.mediaType}`);
  });
  contentBlocks.push({ type: 'text', text: 'Extraheer de offerte volgens het schema. Geef alleen het JSON-object terug.' });

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'anthropic-version': '2023-06-01', 'content-type': 'application/json' },
      body: JSON.stringify({ model, max_tokens: 4000, system: OFFERTE_SCHEMA_PROMPT, messages: [{ role: 'user', content: contentBlocks }] }),
      signal: controller.signal,
    });
    if (!r.ok) { const t = await r.text().catch(() => ''); throw new Error(`Anthropic HTTP ${r.status}: ${t.slice(0, 300)}`); }
    const json = await r.json();
    return { rawText: (json.content && json.content[0] && json.content[0].text) || '', usage: json.usage || {} };
  } finally { clearTimeout(timer); }
}

function _parseJson(rawText) {
  let s = String(rawText || '').trim().replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/i, '');
  const a = s.indexOf('{'), b = s.lastIndexOf('}');
  if (a >= 0 && b > a) s = s.slice(a, b + 1);
  return JSON.parse(s);
}

function _num(v) { const n = parseFloat(v); return isFinite(n) ? n : null; }

// Server-side normalisatie: types afdwingen, negatieven weg, laadpalen opschonen.
function _normaliseerOfferte(parsed) {
  parsed = parsed || {};
  const laad = Array.isArray(parsed.laadpalen) ? parsed.laadpalen
    .filter(p => p && (+p.aantal > 0))
    .map(p => ({ aantal: Math.round(+p.aantal), kw: _num(p.kw), type: p.type ? String(p.type).slice(0, 40) : null })) : [];
  const comp = Array.isArray(parsed.componenten) ? parsed.componenten.map(c => String(c).toLowerCase().slice(0, 20)).slice(0, 12) : [];
  const opm = Array.isArray(parsed.opmerkingen) ? parsed.opmerkingen.map(o => String(o).slice(0, 200)).slice(0, 12) : [];
  const onz = Array.isArray(parsed._onzeker) ? parsed._onzeker.map(o => String(o).slice(0, 40)).slice(0, 20) : [];
  const pos = v => { const n = _num(v); return (n != null && n >= 0) ? n : null; };
  return {
    installateur: parsed.installateur ? String(parsed.installateur).slice(0, 120) : null,
    is_concurrent: !!parsed.is_concurrent,
    componenten: comp,
    investering_excl_btw: pos(parsed.investering_excl_btw),
    investering_incl_btw: pos(parsed.investering_incl_btw),
    jaarlijkse_kost_excl_btw: pos(parsed.jaarlijkse_kost_excl_btw),
    looptijd_jaar: pos(parsed.looptijd_jaar),
    pv_kwp: pos(parsed.pv_kwp),
    batterij_kwh: pos(parsed.batterij_kwh),
    batterij_kw: pos(parsed.batterij_kw),
    laadpalen: laad,
    belooft_besparing_eur_jaar: pos(parsed.belooft_besparing_eur_jaar),
    belooft_payback_jaar: pos(parsed.belooft_payback_jaar),
    prijs_kwh_eur: pos(parsed.prijs_kwh_eur),
    energiecontract: !!parsed.energiecontract,
    opmerkingen: opm,
    _onzeker: onz,
  };
}

async function extractOfferte({ files, apiKey, model, retries = 1 }) {
  if (!apiKey) throw new Error('ANTHROPIC_API_KEY niet beschikbaar');
  if (!Array.isArray(files) || files.length === 0) throw new Error('Geen bestanden meegegeven');
  const usedModel = model || process.env.OFFERTE_MODEL || process.env.FACTUUR_MODEL || 'claude-sonnet-4-5';
  const t0 = Date.now();
  let raw, parsed, lastError;
  for (let att = 0; att <= retries; att++) {
    try {
      raw = await _callClaude({ apiKey, model: usedModel, files });
      parsed = _parseJson(raw.rawText);
      break;
    } catch (e) { lastError = e; console.warn(`[offerte] poging ${att + 1}/${retries + 1} mislukt: ${e.message}`); }
  }
  if (!parsed) throw new Error(`Offerte-extractie mislukt: ${lastError ? lastError.message : 'onbekende fout'}`);
  const offerte = _normaliseerOfferte(parsed);
  return { ok: true, offerte, _meta: { model: usedModel, duration_ms: Date.now() - t0, input_tokens: raw && raw.usage.input_tokens, output_tokens: raw && raw.usage.output_tokens, version: '1.0.0' } };
}

// ─── Heranalyse (puur) — vergelijk de offerte met de EnergieKompas-studie ─────────────────────────
// studie = { jaarvoordeel_eur, energiekost_nu_mwh, energiekost_dyn_mwh, bron } (best-effort uit de lead).
function _eur(n) { const v = Math.round(+n || 0); try { return '€ ' + v.toLocaleString('nl-BE'); } catch (e) { return '€ ' + v; } }

function bouwHeranalyse(offerte, studie) {
  offerte = offerte || {};
  studie = studie || {};
  const jaarvoordeel = +studie.jaarvoordeel_eur || 0;           // wat onze studie/jaar oplevert
  const capex = offerte.investering_excl_btw != null ? offerte.investering_excl_btw
    : (offerte.investering_incl_btw != null ? Math.round(offerte.investering_incl_btw / 1.21) : null);
  const recurring = +offerte.jaarlijkse_kost_excl_btw || 0;
  const nettoJaar = jaarvoordeel - recurring;

  // Samenvatting van wat de offerte biedt
  const biedt = [];
  if (offerte.pv_kwp) biedt.push(`${offerte.pv_kwp} kWp PV`);
  if (offerte.batterij_kwh) biedt.push(`batterij ${offerte.batterij_kwh} kWh${offerte.batterij_kw ? (' / ' + offerte.batterij_kw + ' kW') : ''}`);
  if (offerte.laadpalen && offerte.laadpalen.length) {
    const n = offerte.laadpalen.reduce((s, p) => s + (+p.aantal || 0), 0);
    biedt.push(`${n} laadpunt${n === 1 ? '' : 'en'}`);
  }

  // Vergelijkingstabel (feitelijk)
  const vergelijking = [];
  if (capex != null) vergelijking.push({ label: 'Investering (excl. btw)', waarde: _eur(capex) });
  if (recurring > 0) vergelijking.push({ label: 'Jaarlijkse kost', waarde: _eur(recurring) + '/jaar' });
  if (offerte.looptijd_jaar) vergelijking.push({ label: 'Looptijd / garantie', waarde: offerte.looptijd_jaar + ' jaar' });
  if (offerte.belooft_besparing_eur_jaar != null) vergelijking.push({ label: 'Beloofde besparing (offerte)', waarde: _eur(offerte.belooft_besparing_eur_jaar) + '/jaar' });
  if (offerte.belooft_payback_jaar != null) vergelijking.push({ label: 'Beloofde terugverdientijd (offerte)', waarde: offerte.belooft_payback_jaar + ' jaar' });
  if (jaarvoordeel > 0) vergelijking.push({ label: 'Jaarvoordeel volgens uw EnergieKompas-studie', waarde: _eur(jaarvoordeel) + '/jaar' });

  // Terugverdientijd op basis van ONZE studie (feitelijk, geen advies)
  let payback_studie_jaar = null;
  if (capex != null && capex > 0 && nettoJaar > 0) payback_studie_jaar = Math.round((capex / nettoJaar) * 10) / 10;

  // Aandachtspunten (feitelijk)
  const aandacht = [];
  if (capex == null) aandacht.push('De offerte vermeldt geen duidelijke totale investering — vraag een uitgesplitste prijs op.');
  if (recurring > 0 && offerte.looptijd_jaar) aandacht.push(`De jaarlijkse kost van ${_eur(recurring)} over ${offerte.looptijd_jaar} jaar is samen ${_eur(recurring * offerte.looptijd_jaar)} — tel die bij de investering.`);
  if (offerte.prijs_kwh_eur != null || offerte.energiecontract) aandacht.push('Deze offerte legt (deels) een vaste energieprijs vast. Dat is het tegenovergestelde van een dynamisch contract — vergelijk het met de spot-gebaseerde marge uit uw nota.');
  if (offerte.belooft_besparing_eur_jaar != null && jaarvoordeel > 0) {
    const d = offerte.belooft_besparing_eur_jaar - jaarvoordeel;
    if (Math.abs(d) / Math.max(jaarvoordeel, 1) > 0.25) {
      aandacht.push(d > 0
        ? `De offerte belooft een hogere besparing (${_eur(offerte.belooft_besparing_eur_jaar)}) dan onze voorzichtige studie (${_eur(jaarvoordeel)}) — vraag waarop die claim gebaseerd is.`
        : `Onze studie toont een hoger jaarvoordeel (${_eur(jaarvoordeel)}) dan de offerte belooft (${_eur(offerte.belooft_besparing_eur_jaar)}).`);
    }
  }

  // Fluctus-meerwaarde t.o.v. een pure hardware-offerte (ENKEL arbitrage + onbalans — anti-regressie)
  const meerwaarde = [];
  const heeftBatterij = !!(offerte.batterij_kwh || offerte.batterij_kw) || (offerte.componenten || []).indexOf('batterij') >= 0;
  const geenSturingVermeld = !(offerte.opmerkingen || []).some(o => /dynamisch|sturing|spot|arbitrage|onbalans/i.test(o));
  if (heeftBatterij && geenSturingVermeld) {
    meerwaarde.push('De offerte dimensioneert de hardware, maar vermeldt geen actieve marktsturing. Fluctus stuurt de batterij op de spotmarkt (day-ahead / intraday) en laat ze passief meebewegen met de onbalansprijs — dat is terugkerende opbrengst bovenop de installatie zelf.');
  } else if (offerte.is_concurrent) {
    meerwaarde.push('Vergelijk niet alleen de installatieprijs: de Fluctus-aanpak voegt aan dezelfde hardware een verdienlaag toe via spotmarkt-arbitrage en passieve onbalansrespons.');
  }
  if (offerte.pv_kwp && !offerte.batterij_kwh) {
    meerwaarde.push('De offerte is PV-only. Zonder opslag injecteert u overschot vaak tegen lage (of negatieve) prijzen; een batterij verschuift dat naar duurdere uren en opent de spotmarkt-arbitrage.');
  }

  // Eerlijk oordeel (feitelijk, geen koopadvies)
  let oordeel;
  if (capex == null) {
    oordeel = 'De offerte mist een heldere totaalprijs; op basis van wat we lazen kunnen we de terugverdientijd nog niet betrouwbaar leggen. Vraag een uitgesplitste offerte, dan rekenen we ze exact tegen uw studie.';
  } else if (payback_studie_jaar != null) {
    oordeel = `Met het jaarvoordeel uit uw EnergieKompas-studie (${_eur(nettoJaar)} netto/jaar) verdient deze investering van ${_eur(capex)} zich terug in ± ${payback_studie_jaar} jaar. ${meerwaarde.length ? 'Let vooral op de terugkerende marktopbrengst die in de offerte ontbreekt.' : ''}`.trim();
  } else if (jaarvoordeel <= 0) {
    oordeel = 'We hebben nog geen jaarvoordeel uit uw studie om de offerte tegen af te zetten — rond eerst de (exacte) studie af, dan leggen we de terugverdientijd.';
  } else {
    oordeel = `De offerte kost ${_eur(capex)} en de jaarlijkse kost overschrijdt voorlopig het jaarvoordeel uit uw studie — vraag een scherpere prijs of een grotere opstelling, of bekijk de exacte studie op uw echte profiel.`;
  }

  return {
    biedt, samenvatting_biedt: biedt.join(' + ') || 'onbepaald',
    is_concurrent: !!offerte.is_concurrent,
    installateur: offerte.installateur || null,
    vergelijking, meerwaarde, aandacht,
    payback_studie_jaar, jaarvoordeel_studie_eur: jaarvoordeel || null,
    oordeel,
  };
}

module.exports = { extractOfferte, bouwHeranalyse, _normaliseerOfferte };
