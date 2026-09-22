'use strict';
/**
 * Fluctus Simulator — Profiel-import (AI-formaatherkenning → gemeten profiel)
 * ===========================================================================
 * Module: profiel/import.js
 * Versie: 1.0.0 (2026-09-22)
 *
 * KERNPRINCIPE
 *   De AI ziet ENKEL een sample (headers + ~40 rijen + kolomtypes) en levert een DECLARATIEVE
 *   conversie-spec (JSON). De volledige transformatie draait deterministisch in deze module.
 *   Er wordt NOOIT door-AI-gegenereerde code uitgevoerd. Een formaat dat niet in de spec-velden
 *   te vatten is → "manuele review nodig" (geen vrije code, geen stille gok).
 *
 * BESLISSING (versie-header): converters worden bewaard als declaratieve spec in de bucket
 *   (converters/registry.json + converters/<fingerprint>.json), niet als opgeslagen JS.
 *   Veilig (geen code-execution), testbaar, en herbruikbaar zonder AI-call.
 *
 * DATACONTRACT (output, per type afname|injectie)
 *   kwh[35040]      kWh per kwartier op de vaste 2025-kalenderindex (doy*96+q, 29 feb → 28),
 *                   lokale Belgische kloktijd (zelfde semantiek als _parseProfielCsv).
 *   kwartier[35040] idem genormaliseerd (som = 1) — de vorm die simulator.py verwacht.
 *   herkomst[35040] 0=gemeten · 1=interpolatie (kort gat) · 2=typische-dag (eigen profiel) ·
 *                   3=SLP/PV-vorm-fallback · 4=uurresolutie (gelijk verdeeld over 4 kwartieren) ·
 *                   5=energiebalans (SLP-verbruik + PV-productie, gekalibreerd)
 *   coverage        % gemeten / % afgeleid per methode, afgeleide periodes, label, flags.
 *
 * Geen externe dependencies (enkel node:crypto + fetch voor de optionele AI-call).
 */

const crypto = require('crypto');

const VERSIE = '1.1.0';   // v1.1.0 (22-09): compacte datum/tijd (YYYYMMDD, DDMMYYYY, YYYYMMDDHHMM, HHMM) in de engine; buildSampleText geëxporteerd (review)
const N = 35040;
const CUMDAY = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
const MAAND_NAAM = ['jan', 'feb', 'mrt', 'apr', 'mei', 'jun', 'jul', 'aug', 'sep', 'okt', 'nov', 'dec'];
const HERKOMST_LABELS = ['gemeten', 'interpolatie', 'typische-dag', 'SLP-fallback', 'uurresolutie', 'energiebalans'];
const GAP_KORT = 4;        // ≤ 4 kwartieren → lineaire interpolatie
const GAP_DAG = 96;        // ≤ 1 dag → typische-dag
const DREMPEL_PCT = () => {
  const v = Number(process.env.PROFIEL_EXTRAPOLATIE_DREMPEL_PCT);
  return (isFinite(v) && v >= 0 && v <= 100) ? v : 10;
};
const PV_YIELD = 900;      // kWh/kWp/jaar — zelfde vorm/aanname als de rest van de simulator

// ─── 1. INLEZEN ─────────────────────────────────────────────────────────────────────────────
function _stripBom(s) { return s.charCodeAt(0) === 0xFEFF ? s.slice(1) : s; }

function detectDelimiter(text) {
  const lines = _stripBom(text).split(/\r?\n/).filter(l => l.trim()).slice(0, 60);
  let best = ';', bestScore = -1;
  for (const d of [';', ',', '\t', '|']) {
    const counts = lines.map(l => splitCsvLine(l, d).length);
    const tail = counts.slice(Math.min(5, Math.floor(counts.length / 3)));   // metadata-kop negeren
    if (!tail.length) continue;
    const freq = {}; tail.forEach(c => { freq[c] = (freq[c] || 0) + 1; });
    const [mode, n] = Object.entries(freq).sort((a, b) => b[1] - a[1])[0];
    const score = (+mode > 1 ? 1 : 0) * (n / tail.length) * Math.min(+mode, 8);
    if (score > bestScore) { bestScore = score; best = d; }
  }
  return best;
}

function splitCsvLine(line, d) {
  const out = []; let cur = '', q = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (q) {
      if (ch === '"') { if (line[i + 1] === '"') { cur += '"'; i++; } else q = false; }
      else cur += ch;
    } else if (ch === '"') q = true;
    else if (ch === d) { out.push(cur); cur = ''; }
    else cur += ch;
  }
  out.push(cur);
  return out.map(s => s.trim());
}

function parseCsv(text, delimiter) {
  const d = delimiter || detectDelimiter(text);
  const rows = [];
  for (const l of _stripBom(text).split(/\r?\n/)) { if (l.trim()) rows.push(splitCsvLine(l, d)); }
  return { rows, delimiter: d };
}

// ─── celtypes ──────────────────────────────────────────────────────────────────────────────
const RE_DT = /^\s*(\d{1,4})[-\/.](\d{1,2})[-\/.](\d{1,4})[ T]+(\d{1,2}):(\d{2})(?::(\d{2}))?/;
const RE_D = /^\s*(\d{1,4})[-\/.](\d{1,2})[-\/.](\d{1,4})\s*$/;
const RE_T = /^\s*(\d{1,2}):(\d{2})(?::(\d{2}))?(\s*[-–]\s*\d{1,2}:\d{2}(:\d{2})?)?\s*$/;
function celType(v) {
  if (v === null || v === undefined) return 'e';
  if (typeof v === 'number') return isFinite(v) ? 'n' : 'e';
  const s = String(v).trim();
  if (!s) return 'e';
  if (RE_DT.test(s)) return 'dt';
  if (RE_D.test(s)) return 'd';
  if (RE_T.test(s)) return 't';
  if (/^[-+]?[\d . ]*[\d][\d.,  ]*$/.test(s) && /\d/.test(s)) return 'n';
  return 's';
}
function shape(v) { return String(v == null ? '' : v).trim().replace(/\d+/g, m => 'd'.repeat(Math.min(m.length, 4))).slice(0, 24); }

// Zoek de headerrij: laatste rij vóór de eerste 'datarij' (rij met een datum/datetime-cel én een getal).
function detectHeader(rows) {
  const isData = r => r.some(c => { const t = celType(c); return t === 'dt' || t === 'd'; }) && r.some(c => celType(c) === 'n');
  const isNumRow = r => r.filter(c => celType(c) === 'n').length >= 1 && r.filter(c => celType(c) === 's').length === 0;
  for (let i = 0; i < Math.min(rows.length, 60); i++) {
    if (isData(rows[i]) || (isNumRow(rows[i]) && i + 1 < rows.length && isNumRow(rows[i + 1]))) {
      if (i === 0) return { has_header: false, skip_rows: 0, header_idx: -1 };
      const prev = rows[i - 1];
      const txt = prev.filter(c => celType(c) === 's').length;
      if (txt >= 1) return { has_header: true, skip_rows: i - 1, header_idx: i - 1 };
      return { has_header: false, skip_rows: i, header_idx: -1 };
    }
  }
  return { has_header: true, skip_rows: 0, header_idx: 0 };
}

function sniff(rows, kind, delimiter) {
  const h = detectHeader(rows);
  const header = h.header_idx >= 0 ? rows[h.header_idx].map(c => String(c == null ? '' : c)) : [];
  const start = h.has_header ? h.skip_rows + 1 : h.skip_rows;
  const data = rows.slice(start);
  const ncol = Math.max(header.length, ...data.slice(0, 50).map(r => r.length), 0);
  const sample = data.slice(0, 50);
  const types = [];
  const dateShapes = [];
  for (let c = 0; c < ncol; c++) {
    const freq = {};
    sample.forEach(r => { const t = celType(r[c]); freq[t] = (freq[t] || 0) + 1; });
    const nonE = Object.entries(freq).filter(([k]) => k !== 'e').sort((a, b) => b[1] - a[1]);
    const t = nonE.length ? nonE[0][0] : 'e';
    types.push(t);
    if (t === 'dt' || t === 'd' || t === 't') {
      const ex = sample.find(r => celType(r[c]) === t);
      dateShapes.push(c + ':' + shape(ex && ex[c]));
    }
  }
  // decimaalteken (enkel voor tekst-cellen; numerieke Excel-cellen zijn al getallen)
  let komma = 0, punt = 0;
  sample.forEach(r => r.forEach(c => {
    if (typeof c !== 'string' || celType(c) !== 'n') return;
    if (/\d,\d{1,3}$/.test(c) && !/\d\.\d{3},/.test(c)) komma++;
    else if (/\d\.\d+$/.test(c)) punt++;
  }));
  return {
    kind, delimiter: delimiter || null, has_header: h.has_header, skip_rows: h.skip_rows,
    header, ncol, types, dateShapes, decimal: komma > punt ? ',' : '.',
    n_rows: data.length, data_start: start,
  };
}

// ─── 2. FINGERPRINT ─────────────────────────────────────────────────────────────────────────
function normHeader(s) {
  return String(s || '').toLowerCase().replace(/["']/g, '').replace(/\d+/g, '#').replace(/\s+/g, ' ').trim().slice(0, 40);
}
function fingerprint(sn) {
  const basis = [
    'v1', sn.kind === 'rows' ? 'rows' : 'csv', sn.kind === 'rows' ? '' : (sn.delimiter || ''),
    sn.has_header ? '1' : '0', String(sn.skip_rows),
    sn.header.map(normHeader).join('|'), sn.types.join(','), sn.dateShapes.join(','),
  ].join('§');
  return crypto.createHash('sha1').update(basis).digest('hex').slice(0, 16);
}

// ─── 3. SPEC — validatie (whitelist) ────────────────────────────────────────────────────────
const UNITS = { kwh: 'kWh', kw: 'kW', wh: 'Wh', w: 'W', mwh: 'MWh' };
function validateSpec(spec, sn) {
  const fout = [];
  if (!spec || typeof spec !== 'object') return { ok: false, fout: ['spec ontbreekt'] };
  const ncol = sn ? sn.ncol : 999;
  const isCol = v => Number.isInteger(v) && v >= 0 && v < ncol;
  const s = {
    spec_versie: 1,
    naam: String(spec.naam || 'onbenoemd formaat').slice(0, 80),
    delimiter: [';', ',', '\t', '|'].includes(spec.delimiter) ? spec.delimiter : (sn && sn.delimiter) || ';',
    decimal: spec.decimal === ',' ? ',' : '.',
    has_header: !!spec.has_header,
    skip_rows: Number.isInteger(spec.skip_rows) && spec.skip_rows >= 0 && spec.skip_rows < 200 ? spec.skip_rows : 0,
    datetime: null,
    value_columns: { afname: null, injectie: null },
    sign_convention: spec.sign_convention === 'netto_signed' ? 'netto_signed' : 'positief',
    long_format: null,
    unit: UNITS[String(spec.unit || '').toLowerCase()] || null,
    unit_col: isCol(spec.unit_col) ? spec.unit_col : null,
    resolution: ['kwartier', 'uur', 'auto'].includes(spec.resolution) ? spec.resolution : 'auto',
    timezone: spec.timezone === 'utc' ? 'utc' : 'local',
    manuele_review: !!spec.manuele_review,
    opmerking: String(spec.opmerking || '').slice(0, 300),
  };
  const dt = spec.datetime || {};
  const order = ['DMY', 'YMD', 'MDY', 'ISO', 'EXCEL_SERIAL'].includes(dt.date_order) ? dt.date_order : null;
  if (!isCol(dt.col)) fout.push('datetime.col ongeldig');
  if (!order) fout.push('datetime.date_order ongeldig');
  s.datetime = { col: dt.col, time_col: isCol(dt.time_col) ? dt.time_col : null, date_order: order || 'DMY', label: dt.label === 'end' ? 'end' : 'start' };
  const cols = v => (Array.isArray(v) ? v : (v == null ? [] : [v])).filter(isCol);
  const vc = spec.value_columns || {};
  const a = cols(vc.afname), i = cols(vc.injectie);
  s.value_columns.afname = a.length ? a : null;
  s.value_columns.injectie = i.length ? i : null;
  if (spec.long_format && typeof spec.long_format === 'object') {
    const lf = spec.long_format;
    const arr = v => (Array.isArray(v) ? v : []).map(x => String(x).toLowerCase().trim()).filter(Boolean).slice(0, 12);
    if (isCol(lf.key_col) && isCol(lf.value_col)) {
      s.long_format = { key_col: lf.key_col, value_col: lf.value_col, afname: arr(lf.afname), injectie: arr(lf.injectie), match: lf.match === 'exact' ? 'exact' : 'contains' };
      if (!s.long_format.afname.length && !s.long_format.injectie.length) fout.push('long_format zonder afname/injectie-sleutels');
    } else fout.push('long_format.key_col/value_col ongeldig');
  }
  if (!s.long_format && !s.value_columns.afname && !s.value_columns.injectie) fout.push('geen waardekolom');
  if (!s.unit && s.unit_col == null) fout.push('eenheid onbekend (kWh/kW)');
  if (s.manuele_review) fout.push('AI markeerde dit formaat als "manuele review nodig"' + (s.opmerking ? ': ' + s.opmerking : ''));
  return { ok: fout.length === 0, spec: s, fout };
}

// ─── builtins (geen AI nodig) ───────────────────────────────────────────────────────────────
function builtinSpec(sn) {
  const hn = sn.header.map(normHeader);
  const find = (...keys) => hn.findIndex(h => keys.every(k => h.includes(k)));
  // (a) Fluvius-export kwartiertotalen (lang formaat: één rij per register per kwartier)
  const iVanD = find('van', 'datum'), iVanT = find('van', 'tijd'), iReg = find('register'), iVol = find('volume'), iEenh = find('eenheid');
  if (iVanD >= 0 && iReg >= 0 && iVol >= 0) {
    return { bron: 'builtin', spec: {
      naam: 'Fluvius kwartiertotalen (lang formaat, registers)', delimiter: sn.delimiter || ';', decimal: sn.decimal,
      has_header: true, skip_rows: sn.skip_rows,
      datetime: { col: iVanD, time_col: iVanT >= 0 ? iVanT : null, date_order: _guessOrderFromShape(sn, iVanD), label: 'start' },
      long_format: { key_col: iReg, value_col: iVol, afname: ['afname'], injectie: ['injectie'], match: 'contains' },
      unit: 'kWh', unit_col: iEenh >= 0 ? iEenh : null, resolution: 'auto', timezone: 'local',
    } };
  }
  // (b) Fluctus-canoniek / oude converter: "DD/MM/YYYY HH:MM;waarde" (2 kolommen)
  if (sn.ncol === 2 && sn.types[0] === 'dt' && sn.types[1] === 'n' && /^dd\/dd\/dddd dd:dd/.test((sn.dateShapes[0] || '').split(':').slice(1).join(':'))) {
    const h1 = hn[1] || '';
    return { bron: 'builtin', spec: {
      naam: 'Fluctus kwartier-CSV (DD/MM/YYYY HH:MM;waarde)', delimiter: sn.delimiter || ';', decimal: sn.decimal,
      has_header: sn.has_header, skip_rows: sn.skip_rows,
      datetime: { col: 0, time_col: null, date_order: 'DMY', label: 'start' },
      value_columns: { afname: [1], injectie: null }, unit: /\bkw\b(?!h)/.test(h1) ? 'kW' : 'kWh', resolution: 'auto', timezone: 'local',
    } };
  }
  return null;
}
function _guessOrderFromShape(sn, col) {
  const ds = (sn.dateShapes.find(x => x.startsWith(col + ':')) || '').split(':').slice(1).join(':');
  if (/T/.test(ds) && /(Z|[+-]dd:?dd)$/.test(ds)) return 'ISO';
  if (/^dddd/.test(ds)) return 'YMD';
  return 'DMY';
}

// Heuristische spec (fallback wanneer AI niet beschikbaar is) — altijd met "manuele review aanbevolen".
function heuristicSpec(sn) {
  const dtCol = sn.types.indexOf('dt');
  const dCol = sn.types.indexOf('d');
  const tCol = sn.types.indexOf('t');
  const numCols = sn.types.map((t, i) => t === 'n' ? i : -1).filter(i => i >= 0);
  if ((dtCol < 0 && dCol < 0) || !numCols.length) return null;
  const hn = sn.header.map(normHeader);
  const col = dtCol >= 0 ? dtCol : dCol;
  const pick = re => numCols.filter(i => re.test(hn[i] || ''));
  let afn = pick(/afname|verbruik|consum|import|levering|offtake|usage|load/);
  let inj = pick(/injectie|teruglever|export|productie|inject|feed/);
  if (!afn.length && !inj.length) afn = [numCols[numCols.length - 1]];
  const unitTxt = hn.join(' ');
  const unit = /\bkw\b(?!h)|\(kw\)/.test(unitTxt) && !/kwh/.test(unitTxt) ? 'kW' : 'kWh';
  return { bron: 'heuristiek', spec: {
    naam: 'Heuristisch herkend formaat', delimiter: sn.delimiter || ';', decimal: sn.decimal, has_header: sn.has_header, skip_rows: sn.skip_rows,
    datetime: { col, time_col: dtCol < 0 && tCol >= 0 ? tCol : null, date_order: _guessOrderFromShape(sn, col), label: 'start' },
    value_columns: { afname: afn.length ? afn : null, injectie: inj.length ? inj : null },
    unit, resolution: 'auto', timezone: 'local',
    opmerking: 'heuristiek (AI niet beschikbaar) — manuele review aanbevolen',
  } };
}

// ─── AI-spec (enkel sample) ─────────────────────────────────────────────────────────────────
function buildSampleText(rows, sn) {
  const lines = [];
  const start = Math.max(0, sn.skip_rows - 0);
  const head = rows.slice(0, Math.min(rows.length, start + (sn.has_header ? 1 : 0) + 40));
  head.forEach((r, i) => lines.push(`${String(i).padStart(3)}: ` + r.map(c => String(c == null ? '' : c)).join(' ¦ ')));
  if (rows.length > head.length + 10) {
    lines.push('...');
    const mid = Math.floor(rows.length / 2);
    rows.slice(mid, mid + 5).forEach((r, k) => lines.push(`${String(mid + k).padStart(3)}: ` + r.join(' ¦ ')));
    lines.push('...');
    rows.slice(-3).forEach((r, k) => lines.push(`${String(rows.length - 3 + k).padStart(3)}: ` + r.join(' ¦ ')));
  }
  return lines.join('\n').slice(0, 14000);
}

const AI_PROMPT = `Je herkent het formaat van een Belgisch energie-meetbestand (verbruiksprofiel) en levert ENKEL een JSON-conversiespec. Geen uitleg, geen code.
Je krijgt een SAMPLE van de rijen (0-geïndexeerde rijnummers; kolommen gescheiden door ' ¦ ') + automatische detectie.
Levert exact dit JSON-object:
{
 "naam": "korte omschrijving van de bron/het formaat",
 "delimiter": ";" | "," | "\\t" | "|",
 "decimal": "," | ".",
 "has_header": true|false,
 "skip_rows": <aantal rijen VÓÓR de headerrij (of vóór de eerste datarij als er geen header is)>,
 "datetime": { "col": <kolomindex tijdstempel of datum>, "time_col": <kolomindex tijd of null>, "date_order": "DMY"|"YMD"|"MDY"|"ISO"|"EXCEL_SERIAL", "label": "start"|"end" },
 "value_columns": { "afname": [<kolomindex(en) afname/verbruik — worden opgeteld, bv. dag+nacht>] | null, "injectie": [<kolomindex(en)>] | null },
 "sign_convention": "positief" | "netto_signed",
 "long_format": null | { "key_col": <kolom met registernaam>, "value_col": <kolom met waarde>, "afname": ["tekstfragment",...], "injectie": ["tekstfragment",...], "match": "contains"|"exact" },
 "unit": "kWh"|"kW"|"Wh"|"W"|"MWh",
 "unit_col": <kolomindex met eenheid per rij of null>,
 "resolution": "kwartier"|"uur"|"auto",
 "timezone": "local"|"utc",
 "manuele_review": false,
 "opmerking": ""
}
Regels:
- kW = gemiddeld vermogen over het interval; kWh = energie per interval. Twijfel? Kijk naar de header/eenheidkolom.
- "label":"end" als de tijdstempel het EINDE van het interval aanduidt (bv. eerste rij 00:15 voor het eerste kwartier, of een "tot"-kolom).
- ISO-tijdstempels met "Z" of offset → date_order "ISO". Tijdstempels in UTC zonder offset → timezone "utc". Anders "local" (Belgische kloktijd).
- "netto_signed" enkel als één kolom zowel afname (positief) als injectie (negatief) bevat.
- "long_format" als elke rij één register/richting bevat (bv. kolom "Register" met "Afname Dag", "Injectie Nacht").
- Bevat het bestand volgens de gebruiker enkel injectie, zet de kolom dan onder "injectie".
- Als het formaat niet in deze velden te vatten is (bv. dag- of maandtotalen, kruistabel met kwartieren als kolommen, meerdere meters door elkaar): "manuele_review": true + reden in "opmerking".`;

async function aiSpec(rows, sn, rol, opts) {
  const apiKey = (opts && opts.apiKey) || process.env.ANTHROPIC_API_KEY;
  if (!apiKey) throw new Error('ANTHROPIC_API_KEY niet beschikbaar');
  const model = (opts && opts.model) || process.env.PROFIEL_MODEL || process.env.FACTUUR_MODEL || 'claude-sonnet-4-5';
  const det = `Automatische detectie: kind=${sn.kind}, delimiter=${JSON.stringify(sn.delimiter)}, decimaal=${sn.decimal}, has_header=${sn.has_header}, skip_rows=${sn.skip_rows}, kolommen=${sn.ncol}\n` +
    `Header: ${JSON.stringify(sn.header)}\nKolomtypes (dt=datum+tijd, d=datum, t=tijd, n=getal, s=tekst, e=leeg): ${JSON.stringify(sn.types)}\n` +
    `Rol van dit bestand volgens de gebruiker: ${rol === 'injectie' ? 'INJECTIE-profiel' : 'AFNAME-profiel (kan ook injectie bevatten)'}\n` +
    (opts && opts.feedback ? `VORIGE POGING FAALDE de controle: ${opts.feedback}\nCorrigeer de spec.\n` : '');
  const body = {
    model, max_tokens: 1200,
    system: AI_PROMPT,
    messages: [{ role: 'user', content: det + '\nSAMPLE:\n' + buildSampleText(rows, sn) }],
  };
  const ctl = new AbortController(); const to = setTimeout(() => ctl.abort(), 60000);
  let r;
  try {
    r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST', signal: ctl.signal,
      headers: { 'x-api-key': apiKey, 'anthropic-version': '2023-06-01', 'content-type': 'application/json' },
      body: JSON.stringify(body),
    });
  } finally { clearTimeout(to); }
  if (!r.ok) throw new Error(`AI HTTP ${r.status}: ${(await r.text()).slice(0, 200)}`);
  const j = await r.json();
  const txt = (j.content || []).map(c => c.text || '').join('');
  const m = txt.match(/\{[\s\S]*\}/);
  if (!m) throw new Error('AI gaf geen JSON-spec');
  let spec; try { spec = JSON.parse(m[0]); } catch (e) { throw new Error('AI-spec is geen geldige JSON'); }
  if (spec.delimiter === '\\t') spec.delimiter = '\t';
  return { spec, model, usage: j.usage || null };
}

// ─── 4. ENGINE (vast, getest) ───────────────────────────────────────────────────────────────
function parseNum(v, decimal) {
  if (typeof v === 'number') return v;
  if (v === null || v === undefined) return NaN;
  let s = String(v).trim().replace(/[\s '"]/g, '');
  if (!s) return NaN;
  if (decimal === ',') s = s.replace(/\.(?=\d{3}(\D|$))/g, '').replace(',', '.');
  else s = s.replace(/,(?=\d{3}(\D|$))/g, '');
  if (!/^[-+]?\d*\.?\d+(e[-+]?\d+)?$/i.test(s)) return NaN;
  return parseFloat(s);
}

function _lastSundayUtc(y, m) { const d = new Date(Date.UTC(y, m + 1, 0)); d.setUTCDate(d.getUTCDate() - d.getUTCDay()); return d; }
function utcToBrusselsNaiveMs(msUtc) {
  const y = new Date(msUtc).getUTCFullYear();
  const s = _lastSundayUtc(y, 2).getTime() + 3600000, e = _lastSundayUtc(y, 9).getTime() + 3600000;   // 01:00 UTC
  return msUtc + (msUtc >= s && msUtc < e ? 2 : 1) * 3600000;
}

function _parseTime(v) {
  if (typeof v === 'number' && v >= 0 && v < 1.0001) { const mins = Math.round(v * 1440); return [Math.floor(mins / 60) % 24, mins % 60]; }
  const t = String(v == null ? '' : v).trim();
  const m = t.match(/(\d{1,2}):(\d{2})/);
  if (m) return [+m[1], +m[2]];
  const c = t.match(/^(\d{1,2})(\d{2})(\d{2})?$/);          // compact HHMM / HHMMSS (bv. "0015", "1345", "001500")
  if (c) return [+c[1], +c[2]];
  if (typeof v === 'number' && Number.isInteger(v) && v >= 0 && v <= 2400) return [Math.floor(v / 100), v % 100];
  return null;
}

// Retourneert { ms, utc } waarbij ms = naive ms (Date.UTC van de wall-clock-componenten) of echte UTC-ms (utc=true).
function parseStamp(row, dt) {
  const v = row[dt.col];
  if (v === null || v === undefined || v === '') return null;
  let y, mo, d, hh = 0, mi = 0;
  if (dt.date_order === 'EXCEL_SERIAL') {
    const n = typeof v === 'number' ? v : parseNum(v, '.');
    if (!(n > 20000 && n < 80000)) return null;
    const ms = Math.round((n - 25569) * 86400000 / 60000) * 60000;
    let out = ms;
    if (dt.time_col != null) { const t = _parseTime(row[dt.time_col]); if (t) out = Math.floor(ms / 86400000) * 86400000 + (t[0] * 60 + t[1]) * 60000; }
    return { ms: out, utc: false };
  }
  const s = String(v).trim();
  if (dt.date_order === 'ISO') {
    if (/[zZ]$|[+-]\d{2}:?\d{2}$/.test(s)) { const t = Date.parse(s); return isNaN(t) ? null : { ms: t, utc: true }; }
  }
  let g = s.match(/\d+/g);
  const ord = dt.date_order === 'ISO' ? 'YMD' : dt.date_order;
  // compacte datum (YYYYMMDD / DDMMYYYY, evt. gevolgd door HHMM[SS]) → splitsen in groepen
  if (g && g[0].length === 8) {
    const x = g[0];
    const dg = ord === 'YMD' ? [x.slice(0, 4), x.slice(4, 6), x.slice(6, 8)] : [x.slice(0, 2), x.slice(2, 4), x.slice(4, 8)];
    let rest = g.slice(1);
    if (rest.length && (rest[0].length === 4 || rest[0].length === 6)) rest = [rest[0].slice(0, 2), rest[0].slice(2, 4)].concat(rest.slice(1));
    g = dg.concat(rest);
  } else if (g && g[0].length === 12 && ord === 'YMD') {       // YYYYMMDDHHMM
    const x = g[0]; g = [x.slice(0, 4), x.slice(4, 6), x.slice(6, 8), x.slice(8, 10), x.slice(10, 12)].concat(g.slice(1));
  }
  if (!g || g.length < 3) return null;
  if (ord === 'YMD') { y = +g[0]; mo = +g[1]; d = +g[2]; }
  else if (ord === 'MDY') { mo = +g[0]; d = +g[1]; y = +g[2]; }
  else { d = +g[0]; mo = +g[1]; y = +g[2]; }
  if (y < 100) y += 2000;
  if (dt.time_col != null) { const t = _parseTime(row[dt.time_col]); if (!t) return null; hh = t[0]; mi = t[1]; }
  else if (g.length >= 5) { hh = +g[3]; mi = +g[4]; }
  if (!(mo >= 1 && mo <= 12 && d >= 1 && d <= 31 && hh >= 0 && hh <= 24 && mi >= 0 && mi < 60 && y > 1990 && y < 2100)) return null;
  return { ms: Date.UTC(y, mo - 1, d, hh, mi), utc: false };
}

function unitFactor(unitTxt) {
  const u = String(unitTxt || '').toLowerCase().replace(/[^a-z]/g, '');
  if (u === 'mwh') return { f: 1000, vermogen: false };
  if (u === 'kwh') return { f: 1, vermogen: false };
  if (u === 'wh') return { f: 0.001, vermogen: false };
  if (u === 'mw') return { f: 1000, vermogen: true };
  if (u === 'kw') return { f: 1, vermogen: true };
  if (u === 'w') return { f: 0.001, vermogen: true };
  return null;
}

function _idx2025(naiveMs) {
  const d = new Date(naiveMs);
  let mo = d.getUTCMonth(), day = d.getUTCDate();
  if (mo === 1 && day === 29) day = 28;
  const doy = CUMDAY[mo] + day - 1;
  const q = d.getUTCHours() * 4 + Math.floor(d.getUTCMinutes() / 15);
  return { idx: doy * 96 + q, y: d.getUTCFullYear(), mk: d.getUTCFullYear() * 100 + mo + 1 };
}
function fmtNaive(ms) {
  const d = new Date(ms), p = n => String(n).padStart(2, '0');
  return `${p(d.getUTCDate())}/${p(d.getUTCMonth() + 1)}/${d.getUTCFullYear()} ${p(d.getUTCHours())}:${p(d.getUTCMinutes())}`;
}

/**
 * Past een gevalideerde spec toe op alle rijen. rol = 'afname'|'injectie' (bepaalt de mapping als
 * het bestand maar één waardekolom heeft). Retourneert per type { kwh: Float64Array (NaN = ontbreekt),
 * uur: Uint8Array, stats }.
 */
function applySpec(rows, spec, rol) {
  const start = spec.has_header ? spec.skip_rows + 1 : spec.skip_rows;
  const dt = spec.datetime;
  // mapping afname/injectie-kolommen afhankelijk van de rol
  let colsA = spec.value_columns.afname, colsI = spec.value_columns.injectie;
  let lfA = spec.long_format ? spec.long_format.afname : [], lfI = spec.long_format ? spec.long_format.injectie : [];
  if (rol === 'injectie') {
    if (!colsI && colsA) { colsI = colsA; colsA = null; }
    if (spec.long_format && !lfI.length && lfA.length) { lfI = lfA; lfA = []; }
    colsA = null; lfA = [];                        // een injectiebestand levert nooit afname
  }
  const perType = { afname: new Map(), injectie: new Map() };   // t → Map(key → {s,c})
  const add = (type, t, key, v) => {
    let m = perType[type].get(t); if (!m) { m = new Map(); perType[type].set(t, m); }
    const e = m.get(key); if (e) { e.s += v; e.c++; } else m.set(key, { s: v, c: 1 });
  };
  let geparst = 0, fout = 0, negatief = 0, eenheidOnbekend = 0, utcSeen = false;
  const baseUnit = spec.unit ? unitFactor(spec.unit) : null;
  const units = new Map();   // t → unitFactor (voor vermogen-conversie na resolutiedetectie)
  let vermogenGlobaal = baseUnit ? baseUnit.vermogen : false;
  const matches = (txt, keys) => {
    const t = String(txt || '').toLowerCase().trim();
    return keys.some(k => spec.long_format.match === 'exact' ? t === k : t.includes(k));
  };
  for (let r = start; r < rows.length; r++) {
    const row = rows[r]; if (!row || !row.length) continue;
    const st = parseStamp(row, dt);
    if (!st) { fout++; continue; }
    let t = st.ms;
    if (st.utc) { utcSeen = true; }
    else if (spec.timezone === 'utc') { utcSeen = true; }
    const tIsUtc = st.utc || spec.timezone === 'utc';
    let uf = baseUnit;
    if (spec.unit_col != null) { const u2 = unitFactor(row[spec.unit_col]); if (u2) uf = u2; else if (!uf) { eenheidOnbekend++; continue; } }
    if (!uf) { eenheidOnbekend++; continue; }
    if (uf.vermogen) vermogenGlobaal = true;
    const key0 = (tIsUtc ? 'u' : 'n') + t;
    let ok = false;
    if (spec.long_format) {
      const lf = spec.long_format;
      const v = parseNum(row[lf.value_col], spec.decimal);
      if (isNaN(v)) { continue; }
      const k = String(row[lf.key_col] || '').toLowerCase().trim();
      if (lfA.length && matches(k, lfA)) { add('afname', key0, k, Math.abs(v) * uf.f); ok = true; }
      else if (lfI.length && matches(k, lfI)) { add('injectie', key0, k, Math.abs(v) * uf.f); ok = true; }
      else continue;
    } else {
      if (colsA) {
        let s = 0, any = false;
        for (const c of colsA) { const v = parseNum(row[c], spec.decimal); if (!isNaN(v)) { s += v; any = true; } }
        if (any) {
          if (spec.sign_convention === 'netto_signed' && rol !== 'injectie') {
            if (s >= 0) add('afname', key0, 'w', s * uf.f); else add('injectie', key0, 'w', -s * uf.f);
            // de andere richting is in dat kwartier 0 (gemeten)
            if (s >= 0) add('injectie', key0, 'w', 0); else add('afname', key0, 'w', 0);
          } else { if (s < 0) { negatief++; s = 0; } add('afname', key0, 'w', s * uf.f); }
          ok = true;
        }
      }
      if (colsI) {
        let s = 0, any = false;
        for (const c of colsI) { const v = parseNum(row[c], spec.decimal); if (!isNaN(v)) { s += Math.abs(v); any = true; } }
        if (any) { add('injectie', key0, 'w', s * uf.f); ok = true; }
      }
    }
    if (ok) { geparst++; units.set(key0, uf); } else fout++;
  }

  // resolutie: modus van de positieve deltas tussen unieke tijdstippen (in minuten)
  const allT = new Set();
  for (const type of ['afname', 'injectie']) for (const k of perType[type].keys()) allT.add(k);
  const tsSorted = [...allT].map(k => +k.slice(1)).sort((a, b) => a - b);
  const deltas = {};
  for (let i = 1; i < tsSorted.length && i < 20000; i++) { const dm = Math.round((tsSorted[i] - tsSorted[i - 1]) / 60000); if (dm > 0 && dm <= 1440) deltas[dm] = (deltas[dm] || 0) + 1; }
  const detRes = +((Object.entries(deltas).sort((a, b) => b[1] - a[1])[0] || [15])[0]);
  let R = spec.resolution === 'kwartier' ? 15 : spec.resolution === 'uur' ? 60 : detRes;
  if (![5, 10, 15, 30, 60].includes(R)) R = detRes;
  const waarschuwingen = [];
  if (spec.resolution !== 'auto' && detRes !== R && [5, 10, 15, 30, 60].includes(detRes)) waarschuwingen.push(`spec zegt ${spec.resolution}, data suggereert ${detRes} min — spec gevolgd`);
  if (R > 60) return { fout: `resolutie ${R} min (dag-/maandtotalen) — te grof voor een kwartierprofiel`, R };
  if (![5, 10, 15, 30, 60].includes(R)) return { fout: `onbekende resolutie (${R} min)`, R };

  const out = {};
  for (const type of ['afname', 'injectie']) {
    const m = perType[type];
    if (!m.size) { out[type] = null; continue; }
    const slot = new Map();   // y*N+idx → {s,c}
    const exp = Math.max(1, 15 / R);
    const uurFlag = new Uint8Array(N);
    const maanden = new Set();
    let tMin = Infinity, tMax = -Infinity, piekKwh = 0;
    for (const [key, regs] of m) {
      let v = 0; for (const e of regs.values()) v += e.s / e.c;   // DST-dubbel (zelfde t+register) → gemiddeld
      const uf = units.get(key);
      if ((uf && uf.vermogen) || (!uf && vermogenGlobaal)) v = v * R / 60;   // kW → kWh per interval
      let t = +key.slice(1);
      if (spec.datetime.label === 'end') t -= R * 60000;
      const naive = key[0] === 'u' ? utcToBrusselsNaiveMs(t) : t;
      if (naive < tMin) tMin = naive; if (naive > tMax) tMax = naive;
      const nq = R > 15 ? R / 15 : 1;
      for (let k = 0; k < nq; k++) {
        const ix = _idx2025(naive + k * 15 * 60000);
        if (ix.idx < 0 || ix.idx >= N) continue;
        maanden.add(ix.mk);
        const sk = ix.y * N + ix.idx;
        const e = slot.get(sk); const part = v / nq;
        if (e) { e.s += part; e.c++; } else slot.set(sk, { s: part, c: 1 });
        if (nq > 1) uurFlag[ix.idx] = 1;
      }
    }
    const acc = new Float64Array(N), cnt = new Uint16Array(N);
    for (const [sk, e] of slot) {
      const idx = sk % N;
      const val = e.s * (R < 15 ? exp / e.c : (e.c > 1 ? 1 / e.c : 1));   // sub-kwartier: opvullen tot volledig kwartier; dubbel: middelen
      acc[idx] += val; cnt[idx]++;
    }
    const kwh = new Float64Array(N);
    let nMeas = 0;
    for (let i = 0; i < N; i++) {
      if (cnt[i] > 0) { kwh[i] = acc[i] / cnt[i]; nMeas++; if (kwh[i] > piekKwh) piekKwh = kwh[i]; }
      else kwh[i] = NaN;
    }
    out[type] = {
      kwh, uur: uurFlag,
      stats: { unieke_tijdstippen: m.size, gemeten_kwartieren: nMeas, van: isFinite(tMin) ? fmtNaive(tMin) : null, tot: isFinite(tMax) ? fmtNaive(tMax) : null,
               van_ms: tMin, tot_ms: tMax, gemeten_maanden: maanden.size, maandpiek_kw_gemeten: Math.round(piekKwh * 4 * 10) / 10 },
    };
  }
  return { afname: out.afname, injectie: out.injectie, R, rijen: rows.length - start, geparst, fout_rijen: fout, negatief_genegeerd: negatief, eenheid_onbekend: eenheidOnbekend, utc: utcSeen, waarschuwingen };
}

// ─── 5. AANVULLING + HERKOMST ───────────────────────────────────────────────────────────────
function _weekdag(idx) { return (3 + Math.floor(idx / 96)) % 7; }      // 1 jan 2025 = woensdag (0 = zondag)
function _dagtype(idx) { const w = _weekdag(idx); return w === 0 ? 2 : w === 6 ? 1 : 0; }
function _maand(idx) { const doy = Math.floor(idx / 96); let m = 0; while (m < 11 && CUMDAY[m + 1] <= doy) m++; return m; }

// runs van ontbrekende (NaN) posities, circulair niet nodig voor korte gaten
function _gaps(arr) {
  const out = []; let s = -1;
  for (let i = 0; i <= N; i++) {
    const miss = i < N && isNaN(arr[i]);
    if (miss && s < 0) s = i;
    if (!miss && s >= 0) { out.push([s, i - 1]); s = -1; }
  }
  return out;
}

function fillShort(kwh, her) {
  for (const [a, b] of _gaps(kwh)) {
    const len = b - a + 1;
    if (len > GAP_KORT || a === 0 || b === N - 1) continue;
    const l = kwh[a - 1], r = kwh[b + 1];
    if (isNaN(l) || isNaN(r)) continue;
    for (let i = a; i <= b; i++) { kwh[i] = l + (r - l) * (i - a + 1) / (len + 1); her[i] = 1; }
  }
}

function fillTypicalDay(kwh, her, measuredMask) {
  // typische waarde per (maand, dagtype, kwartier) uit ENKEL gemeten posities
  const s = new Float64Array(12 * 3 * 96), c = new Uint32Array(12 * 3 * 96);
  const s2 = new Float64Array(3 * 96), c2 = new Uint32Array(3 * 96);   // jaar-breed (fallback)
  for (let i = 0; i < N; i++) {
    if (!measuredMask[i]) continue;
    const k = (_maand(i) * 3 + _dagtype(i)) * 96 + (i % 96);
    s[k] += kwh[i]; c[k]++;
    const k2 = _dagtype(i) * 96 + (i % 96); s2[k2] += kwh[i]; c2[k2]++;
  }
  const typ = i => {
    const k = (_maand(i) * 3 + _dagtype(i)) * 96 + (i % 96);
    if (c[k] >= 2) return s[k] / c[k];
    return null;
  };
  for (const [a, b] of _gaps(kwh)) {
    if (b - a + 1 > GAP_DAG) continue;
    // schaal op de gemeten kwartieren van dezelfde dag(en)
    const d0 = Math.floor(a / 96) * 96, d1 = Math.floor(b / 96) * 96 + 95;
    let sm = 0, st = 0, nm = 0;
    for (let i = d0; i <= d1; i++) { if (measuredMask[i]) { const t = typ(i); if (t != null) { sm += kwh[i]; st += t; nm++; } } }
    let k = (nm >= 8 && st > 0) ? sm / st : 1;
    k = Math.max(0.3, Math.min(3, k));
    const vals = [];
    for (let i = a; i <= b; i++) { const t = typ(i); if (t == null) { vals.length = 0; break; } vals.push(t * k); }
    if (vals.length !== b - a + 1) continue;
    for (let i = a; i <= b; i++) { kwh[i] = vals[i - a]; her[i] = 2; }
  }
}

// Lange gaten: vorm (SLP of PV) geschaald op het gemeten volume rond het gat (±30 d, circulair), anders globaal.
function fillShape(kwh, her, measuredMask, vorm, code) {
  let gm = 0, gv = 0;
  for (let i = 0; i < N; i++) if (measuredMask[i]) { gm += kwh[i]; gv += vorm[i]; }
  const kGlob = gv > 0 ? gm / gv : 0;
  for (const [a, b] of _gaps(kwh)) {
    let sm = 0, sv = 0, n = 0;
    const W = 30 * 96;
    for (let off = -W; off <= (b - a) + W; off++) {
      const i = ((a + off) % N + N) % N;
      if (measuredMask[i]) { sm += kwh[i]; sv += vorm[i]; n++; }
    }
    const k = (n >= 7 * 96 && sv > 0) ? sm / sv : kGlob;
    for (let i = a; i <= b; i++) { kwh[i] = Math.max(0, vorm[i] * k); her[i] = code; }
  }
}

// Synthetische PV-vorm (fallback wanneer MARKT.solar_norm ontbreekt): zonnehoogte (51° N, 4,5° O) × maandfactor BE.
function synthPvVorm() {
  const maandW = [0.28, 0.45, 0.75, 1.0, 1.1, 1.1, 1.1, 1.0, 0.85, 0.6, 0.35, 0.25];
  const lat = 51 * Math.PI / 180, out = new Float64Array(N);
  let sum = 0;
  for (let i = 0; i < N; i++) {
    const doy = Math.floor(i / 96), q = i % 96;
    const naive = Date.UTC(2025, 0, 1) + doy * 86400000 + q * 900000 + 450000;
    const off = utcToBrusselsNaiveMs(naive) - naive;               // lokale offset (≈ 1 of 2 u)
    const utcH = ((q * 15 + 7.5) / 60) - off / 3600000;
    const decl = 23.44 * Math.PI / 180 * Math.sin(2 * Math.PI * (284 + doy + 1) / 365);
    const ha = (utcH + 4.5 / 15 - 12) * 15 * Math.PI / 180;
    const sinEl = Math.sin(lat) * Math.sin(decl) + Math.cos(lat) * Math.cos(decl) * Math.cos(ha);
    const v = sinEl > 0 ? Math.pow(sinEl, 1.2) * maandW[_maand(i)] : 0;
    out[i] = v; sum += v;
  }
  for (let i = 0; i < N; i++) out[i] /= sum;
  return out;
}
function _norm(arr) { let s = 0; for (let i = 0; i < N; i++) s += (+arr[i] || 0); const o = new Float64Array(N); if (s > 0) for (let i = 0; i < N; i++) o[i] = (+arr[i] || 0) / s; return o; }

/**
 * 5b — energiebalans-reconstructie voor ontbrekende perioden bij een prosument.
 *   C = A + S, P = S + I ⇒ S = P − I ⇒ C = A + (P − I). (NIET A + I/(1−Z): dat telt I dubbel.)
 *   Per kwartier: S = min(C, P), Afname = C − S, Injectie = P − S. P geschaald zodat de
 *   gereconstrueerde injectie op de gemeten periode de gemeten I benadert.
 */
function energiebalans(A, I, maskA, maskI, slp, pv, opts) {
  const kwp0 = +opts.pv_kwp || 0, kva = +opts.omvormer_kva || 0;
  const both = new Uint8Array(N); let nBoth = 0;
  for (let i = 0; i < N; i++) if (maskA[i] && maskI[i]) { both[i] = 1; nBoth++; }
  if (nBoth < 7 * 96) return { ok: false, reden: 'te weinig overlap tussen gemeten afname en injectie (< 7 dagen)' };
  let Imeas = 0, Ameas = 0, Ipiek = 0;
  for (let i = 0; i < N; i++) if (both[i]) { Imeas += I[i]; Ameas += A[i]; if (I[i] * 4 > Ipiek) Ipiek = I[i] * 4; }
  if (!(Imeas > 0)) return { ok: false, reden: 'geen injectie in de gemeten periode' };
  // startschatting kWp: gegeven, anders 1,3 × injectiepiek (ondergrens bij AC-clipping)
  let kwpStart = kwp0 > 0 ? kwp0 : (kva > 0 ? 1.3 * kva : 1.3 * Ipiek);
  let s = Math.max(1, kwpStart * PV_YIELD);     // jaarproductie kWh (P_q = s · pv_q)
  let kC = 0, it = 0;
  const C = new Float64Array(N), P = new Float64Array(N);
  for (it = 0; it < 30; it++) {
    for (let i = 0; i < N; i++) P[i] = s * pv[i];
    let Cm = 0, slpM = 0;
    for (let i = 0; i < N; i++) if (both[i]) { Cm += A[i] + Math.max(0, P[i] - I[i]); slpM += slp[i]; }
    kC = slpM > 0 ? Cm / slpM : 0;
    let Irec = 0;
    for (let i = 0; i < N; i++) { C[i] = kC * slp[i]; if (both[i]) Irec += Math.max(0, P[i] - Math.min(C[i], P[i])); }
    if (!(Irec > 0)) { s *= 1.5; continue; }
    const r = Imeas / Irec;
    s *= Math.pow(r, 0.8);
    s = Math.max(kwpStart * PV_YIELD / 4, Math.min(kwpStart * PV_YIELD * 4, s));   // begrensd: max factor 4 t.o.v. de startschatting
    if (Math.abs(r - 1) < 0.002) break;
  }
  // convergentiecheck: lukt de kalibratie niet (bv. wintermaand met nauwelijks injectie) → startschatting behouden + flag
  let kalibratieOk = true;
  {
    for (let i = 0; i < N; i++) { P[i] = s * pv[i]; }
    let Cm = 0, slpM = 0; for (let i = 0; i < N; i++) if (both[i]) { Cm += A[i] + Math.max(0, P[i] - I[i]); slpM += slp[i]; }
    kC = slpM > 0 ? Cm / slpM : 0;
    let Ir = 0; for (let i = 0; i < N; i++) { C[i] = kC * slp[i]; if (both[i]) Ir += Math.max(0, P[i] - Math.min(C[i], P[i])); }
    if (!(Ir > 0) || Math.abs(Ir / Imeas - 1) > 0.1) {
      kalibratieOk = false;
      s = kwpStart * PV_YIELD;
      for (let i = 0; i < N; i++) P[i] = s * pv[i];
      Cm = 0; for (let i = 0; i < N; i++) if (both[i]) Cm += A[i] + Math.max(0, P[i] - I[i]);
      kC = slpM > 0 ? Cm / slpM : 0;
      for (let i = 0; i < N; i++) C[i] = kC * slp[i];
    }
  }
  // kwaliteitsindicatoren op de gemeten periode
  let Irec = 0, Arec = 0, dagOvertreding = 0, dagen = 0;
  const dagP = new Float64Array(365), dagI = new Float64Array(365), dagN = new Uint16Array(365);
  for (let i = 0; i < N; i++) {
    const S = Math.min(C[i], P[i]);
    if (both[i]) {
      Irec += P[i] - S; Arec += C[i] - S;
      const d = Math.floor(i / 96); dagP[d] += P[i]; dagI[d] += I[i]; dagN[d]++;
    }
  }
  for (let d = 0; d < 365; d++) if (dagN[d] >= 90) { dagen++; if (dagI[d] > dagP[d] * 1.05 + 0.5) dagOvertreding++; }
  const kwpEff = s / PV_YIELD;
  const flags = [];
  if (!kalibratieOk) flags.push(`PV-kalibratie op de gemeten periode lukt niet (weinig/geen injectie, bv. wintermaand) → kWp-schatting ${Math.round(kwpStart * 10) / 10} kWp ongekalibreerd` + (kwp0 > 0 ? '' : ' (1,3 × injectiepiek = ondergrens) — geef kWp op'));
  if (dagen && dagOvertreding / dagen > 0.1) flags.push(`injectie > theoretische PV-productie op ${dagOvertreding}/${dagen} dagen (kWp/oriëntatie?)`);
  if (kwp0 > 0) { const y = s / kwp0; if (y < 850 || y > 1050) flags.push(`jaarproductie ${Math.round(y)} kWh/kWp buiten 850–1050 (VL)`); }
  if (!(kwp0 > 0) && kalibratieOk && (kwpEff / kwpStart > 1.6 || kwpEff / kwpStart < 0.6)) flags.push(`gekalibreerde PV ${Math.round(kwpEff * 10) / 10} kWp wijkt sterk af van de schatting 1,3 × injectiepiek (${Math.round(kwpStart * 10) / 10} kWp) — geef het echte kWp op`);
  if (kva > 0 && Ipiek > kva * 1.05) flags.push(`injectiepiek ${Math.round(Ipiek)} kW > omvormer ${kva} kVA`);
  const Ptot = s; let Stot = 0, Ctot = 0;
  for (let i = 0; i < N; i++) { const S = Math.min(C[i], P[i]); Stot += S; Ctot += C[i]; }
  const Z = Ptot > 0 ? Stot / Ptot : null;
  if (Z != null && (Z < 0 || Z > 1)) flags.push('zelfconsumptiegraad buiten [0,1]');
  const afnRec = new Float64Array(N), injRec = new Float64Array(N);
  for (let i = 0; i < N; i++) { const S = Math.min(C[i], P[i]); afnRec[i] = C[i] - S; injRec[i] = P[i] - S; }
  return {
    ok: true, afname: afnRec, injectie: injRec, iteraties: it + 1,
    pv: { kwp_start: Math.round(kwpStart * 10) / 10, kwp_gekalibreerd: Math.round(kwpEff * 10) / 10, jaarproductie_mwh: Math.round(Ptot / 100) / 10,
          zelfconsumptie_pct: Z != null ? Math.round(Z * 1000) / 10 : null, verbruik_totaal_mwh: Math.round(Ctot / 100) / 10, injectiepiek_kw: Math.round(Ipiek * 10) / 10 },
    kwaliteit: { injectie_afwijking_pct: Math.round((Irec - Imeas) / Imeas * 1000) / 10, afname_afwijking_pct: Ameas > 0 ? Math.round((Arec - Ameas) / Ameas * 1000) / 10 : null },
    flags,
  };
}

function _spanTekst(nMeas) {
  const mnd = nMeas / (N / 12);
  if (mnd >= 0.95) { const m = Math.round(mnd); return m === 1 ? '1 maand metingen' : `${m} maanden metingen`; }
  const d = Math.max(1, Math.round(nMeas / 96));
  return d === 1 ? '1 dag metingen' : `${d} dagen metingen`;
}

function _periodes(her) {
  const out = []; let s = -1, code = 0;
  const idxDatum = i => { const doy = Math.floor(i / 96); const m = _maand(i); return `${String(doy - CUMDAY[m] + 1).padStart(2, '0')}/${String(m + 1).padStart(2, '0')}`; };
  for (let i = 0; i <= N; i++) {
    const c = i < N ? her[i] : 0;
    if (s >= 0 && c !== code) { if (code !== 4) out.push({ van: idxDatum(s), tot: idxDatum(i - 1), methode: HERKOMST_LABELS[code], kwartieren: i - s }); s = -1; }
    if (s < 0 && c !== 0) { s = i; code = c; }
  }
  // korte interpolaties niet oplijsten (ruis) — enkel samenvatten
  const lang = out.filter(p => p.methode !== 'interpolatie' || p.kwartieren > GAP_KORT);
  return lang.slice(0, 60);
}

function _summary(kwh, her, stats, extra) {
  const cnt = [0, 0, 0, 0, 0, 0];
  for (let i = 0; i < N; i++) cnt[her[i]]++;
  const pct = c => Math.round(c / N * 1000) / 10;
  let som = 0, piek = 0; const maandMwh = new Array(12).fill(0), maandGemeten = new Array(12).fill(0), maandN = new Array(12).fill(0);
  for (let i = 0; i < N; i++) { som += kwh[i]; if (kwh[i] * 4 > piek) piek = kwh[i] * 4; const m = _maand(i); maandMwh[m] += kwh[i]; maandN[m]++; if (her[i] === 0) maandGemeten[m]++; }
  const nMeas = cnt[0] + cnt[4];              // uurresolutie = gemeten volume, enkel vorm afgeleid
  const pctAfgeleid = Math.round((1 - nMeas / N) * 1000) / 10;
  const drempel = DREMPEL_PCT();
  const flags = (extra && extra.flags) || [];
  let label = pctAfgeleid <= drempel ? 'gemeten' : `12-maand profiel op basis van ${_spanTekst(nMeas)}`;
  if (flags.length) label += ' — onbetrouwbaar, controleer';
  return {
    label, label_type: pctAfgeleid <= drempel ? 'gemeten' : 'geextrapoleerd', drempel_pct: drempel,
    pct_gemeten: pct(cnt[0]), pct_uurresolutie: pct(cnt[4]), pct_interpolatie: pct(cnt[1]), pct_typische_dag: pct(cnt[2]),
    pct_slp: pct(cnt[3]), pct_energiebalans: pct(cnt[5]), pct_afgeleid: pctAfgeleid,
    gemeten_kwartieren: nMeas, gemeten_span: _spanTekst(nMeas), gemeten_maanden: stats.gemeten_maanden,
    van: stats.van, tot: stats.tot,
    maandpiek_kw: Math.round(piek * 10) / 10, maandpiek_kw_gemeten: stats.maandpiek_kw_gemeten,
    jaar_mwh: Math.round(som / 100) / 10, maand_mwh: maandMwh.map(v => Math.round(v / 10) / 100),
    maand_pct_gemeten: maandGemeten.map((v, m) => Math.round(v / maandN[m] * 100)),
    periodes: _periodes(her), flags,
  };
}

/**
 * Hoofdfunctie reconstructie. conv = output van applySpec (afname verplicht).
 * slp = 35040 (som 1) van het projecttype; pvVorm = 35040 (som 1) of null → synthetisch.
 */
function reconstrueer(conv, slp, pvVorm, opts) {
  opts = opts || {};
  if (!conv.afname) throw new Error('geen afname-data gevonden in het bestand');
  const slpN = _norm(slp && slp.length === N ? slp : new Array(N).fill(1));
  const pvN = (pvVorm && pvVorm.length === N) ? _norm(pvVorm) : synthPvVorm();
  const res = {};
  const types = ['afname', 'injectie'].filter(t => conv[t]);
  const masks = {};
  for (const t of types) {
    const c = conv[t];
    const kwh = Float64Array.from(c.kwh), her = new Uint8Array(N);
    const mask = new Uint8Array(N);
    for (let i = 0; i < N; i++) if (!isNaN(kwh[i])) { mask[i] = 1; her[i] = c.uur[i] ? 4 : 0; }
    fillShort(kwh, her);
    fillTypicalDay(kwh, her, mask);
    res[t] = { kwh, her, mask };
    masks[t] = mask;
  }
  let eb = null;
  const flags = [];
  const heeftLang = t => res[t] && res[t].kwh.some(v => isNaN(v));
  if (res.injectie && (heeftLang('afname') || heeftLang('injectie'))) {
    // maskers voor kalibratie = gemeten + kort geïnterpoleerd
    const mA = new Uint8Array(N), mI = new Uint8Array(N);
    for (let i = 0; i < N; i++) { mA[i] = res.afname.her[i] !== 2 && !isNaN(res.afname.kwh[i]) ? 1 : 0; mI[i] = res.injectie.her[i] !== 2 && !isNaN(res.injectie.kwh[i]) ? 1 : 0; }
    eb = energiebalans(res.afname.kwh, res.injectie.kwh, mA, mI, slpN, pvN, opts);
    if (eb.ok) {
      for (const t of ['afname', 'injectie']) {
        const r = res[t], rec = eb[t];
        for (let i = 0; i < N; i++) if (isNaN(r.kwh[i])) { r.kwh[i] = rec[i]; r.her[i] = 5; }
      }
      flags.push(...eb.flags);
    } else flags.push('energiebalans niet mogelijk: ' + eb.reden + ' → aparte SLP/PV-vorm-aanvulling');
  }
  for (const t of types) {
    const r = res[t];
    if (r.kwh.some(v => isNaN(v))) fillShape(r.kwh, r.her, r.mask, t === 'injectie' ? pvN : slpN, 3);
  }
  const out = { types: {}, energiebalans: eb && eb.ok ? { pv: eb.pv, kwaliteit: eb.kwaliteit, iteraties: eb.iteraties } : null, flags };
  for (const t of types) {
    const r = res[t];
    let som = 0; for (let i = 0; i < N; i++) { if (!(r.kwh[i] >= 0)) r.kwh[i] = 0; som += r.kwh[i]; }
    if (!(som > 0)) throw new Error(`${t}: totaal volume = 0`);
    const kwartier = new Array(N); for (let i = 0; i < N; i++) kwartier[i] = r.kwh[i] / som;
    const typeFlags = flags.slice();
    out.types[t] = { kwh: Array.from(r.kwh), kwartier, herkomst: Array.from(r.her), coverage: _summary(r.kwh, r.her, conv[t].stats, { flags: typeFlags }) };
  }
  return out;
}

// ─── sanity op het volledige bestand (spec-test) ────────────────────────────────────────────
function sanity(conv) {
  const fout = [];
  if (conv.fout) return { ok: false, fout: [conv.fout] };
  const parseRate = conv.rijen > 0 ? conv.geparst / conv.rijen : 0;
  if (parseRate < 0.6) fout.push(`slechts ${Math.round(parseRate * 100)}% van de rijen geparst`);
  if (!conv.afname && !conv.injectie) fout.push('geen afname of injectie herkend');
  for (const t of ['afname', 'injectie']) {
    const c = conv[t]; if (!c) continue;
    if (c.stats.gemeten_kwartieren < 96) fout.push(`${t}: minder dan 1 dag data`);
    let s = 0, n = 0, mx = 0; for (let i = 0; i < N; i++) if (!isNaN(c.kwh[i])) { s += c.kwh[i]; n++; if (c.kwh[i] > mx) mx = c.kwh[i]; }
    const jaarMwh = n ? s / n * N / 1000 : 0;
    if (t === 'afname' && !(jaarMwh > 0.05)) fout.push(`${t}: geannualiseerd volume ${jaarMwh.toFixed(3)} MWh onplausibel laag (eenheid Wh?)`);
    if (jaarMwh > 200000) fout.push(`${t}: geannualiseerd volume ${Math.round(jaarMwh)} MWh onplausibel hoog (eenheid?)`);
    const gem = n ? s / n : 0;
    if (t === 'afname' && gem > 0 && mx / gem > 400) fout.push(`${t}: piek/gemiddelde ${Math.round(mx / gem)} — uitschieters of eenheidsfout`);
  }
  return { ok: fout.length === 0, fout, parse_rate: Math.round(parseRate * 1000) / 10 };
}

// ─── preview ────────────────────────────────────────────────────────────────────────────────
function preview(kwh, her) {
  // typische week: week met de hoogste gemeten dekking (ma→zo), uurgemiddelde kW
  let best = 0, bestScore = -1;
  for (let d = 0; d + 7 <= 365; d++) {
    if (_weekdag(d * 96) !== 1) continue;
    let sc = 0; for (let i = d * 96; i < (d + 7) * 96; i++) if (her[i] === 0 || her[i] === 4) sc++;
    if (sc > bestScore) { bestScore = sc; best = d; }
  }
  const week = [];
  for (let h = 0; h < 168; h++) { let s = 0; for (let k = 0; k < 4; k++) s += kwh[best * 96 + h * 4 + k]; week.push(Math.round(s * 100) / 100); }   // kWh/uur = kW gem.
  const hm = []; const hmN = [];
  for (let m = 0; m < 12; m++) for (let h = 0; h < 24; h++) { hm.push(0); hmN.push(0); }
  for (let i = 0; i < N; i++) { const k = _maand(i) * 24 + Math.floor((i % 96) / 4); hm[k] += kwh[i] * 4; hmN[k]++; }
  const d0 = best; const m0 = _maand(d0 * 96);
  return { week_kw: week, week_van: `${String(d0 - CUMDAY[m0] + 1).padStart(2, '0')}/${String(m0 + 1).padStart(2, '0')}`, week_gemeten_pct: Math.round(bestScore / 672 * 100),
           heatmap_kw: hm.map((v, k) => Math.round(v / (hmN[k] || 1) * 100) / 100) };
}

// ─── canoniek CSV (compatibel met server.js _parseProfielCsv) ───────────────────────────────
function toCanonicalCsv(kwh) {
  const lines = ['Datum;kWh'];
  for (let i = 0; i < N; i++) {
    const doy = Math.floor(i / 96), q = i % 96, m = _maand(i), d = doy - CUMDAY[m] + 1;
    const p = n => String(n).padStart(2, '0');
    const v = kwh[i] > 0 ? Number(kwh[i].toPrecision(7)) : 0;
    lines.push(`${p(d)}/${p(m + 1)}/2025 ${p(Math.floor(q / 4))}:${p((q % 4) * 15)};${v}`);
  }
  return lines.join('\n') + '\n';
}

module.exports = {
  VERSIE, N, HERKOMST_LABELS, MAAND_NAAM,
  detectDelimiter, parseCsv, sniff, buildSampleText, fingerprint, validateSpec, builtinSpec, heuristicSpec, aiSpec,
  applySpec, sanity, reconstrueer, energiebalans, preview, toCanonicalCsv, synthPvVorm, parseNum, utcToBrusselsNaiveMs,
};
