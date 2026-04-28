const express = require('express');
const https = require('https');
const compression = require('compression');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');
const app = express();
const PORT = process.env.PORT || 3000;
const ELIA_BASE = 'https://opendata.elia.be/api/explore/v2.1/catalog/datasets';

// Pad naar lokale data-bestanden (mee gecommitteerd in repo)
const DATA_DIR = path.join(__dirname, 'data');

app.use(express.json({ limit: '25mb' }));

// gzip/brotli compressie — grote JSON responses (imb/wind/solar ~5 MB elk)
// comprimeren tot ~20-30% van origineel. Drastisch verschil in laadtijd.
app.use(compression({
  threshold: 1024, // compresseer alles >1 KB (bijna alles behalve meta)
  level: 6         // gebalanceerde CPU vs ratio (default)
}));

app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  // Standaard: geen cache. Individuele routes die van caching profiteren
  // (bijv. statische Elia data per dag) kunnen dit overschrijven.
  res.header('Cache-Control', 'no-store, no-cache, must-revalidate');
  res.header('Pragma', 'no-cache');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

// ─── HTTP hulpfunctie ──────────────────────────────────────────────────────
function httpGet(url, extraHeaders) {
  return new Promise((resolve, reject) => {
    const headers = Object.assign(
      { 'Accept': 'application/json', 'User-Agent': 'Fluctus-Dashboard/2.0' },
      extraHeaders || {}
    );
    https.get(url, { headers }, (resp) => {
      let data = '';
      resp.on('data', chunk => data += chunk);
      resp.on('end', () => resolve({ status: resp.statusCode, body: data }));
    }).on('error', reject);
  });
}

function httpPut(url, token, bodyObj) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify(bodyObj);
    const options = {
      method: 'PUT',
      headers: {
        'Authorization': 'token ' + token,
        'Content-Type': 'application/json',
        'User-Agent': 'Fluctus-Worker/2.0',
        'Content-Length': Buffer.byteLength(body),
      }
    };
    const req = https.request(url, options, (resp) => {
      let data = '';
      resp.on('data', chunk => data += chunk);
      resp.on('end', () => resolve({ status: resp.statusCode, body: data }));
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

function httpPostJson(url, headers, bodyObj) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify(bodyObj);
    const finalHeaders = Object.assign({
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(body),
    }, headers || {});
    const options = { method: 'POST', headers: finalHeaders };
    const req = https.request(url, options, (resp) => {
      let data = '';
      resp.on('data', chunk => data += chunk);
      resp.on('end', () => resolve({ status: resp.statusCode, body: data }));
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

// ─── GitHub helpers voor kleine JSON-bestanden ─────────────────────────────
// Lees een JSON-bestand uit de GitHub repo (via Contents API, niet de raw CDN,
// dus altijd vers). Retourneert {json, sha} of null als het bestand niet bestaat.
// Haal de rauwe inhoud op van een GitHub file. Werkt voor bestanden van alle
// groottes (ook >1 MB) door de raw media type te gebruiken. Returns:
// {raw, json, sha, parseError, exists}
async function githubReadJson(token, owner, repo, path) {
  const apiUrl = `https://api.github.com/repos/${owner}/${repo}/contents/${path}?t=${Date.now()}`;

  // Eerste call: krijg de file content + metadata in raw formaat.
  // application/vnd.github.raw+json werkt voor alle file-groottes.
  const rawHeaders = {
    'Authorization': 'token ' + token,
    'User-Agent': 'Fluctus-Worker/4.0',
    'Accept': 'application/vnd.github.raw+json',
  };
  const rawResp = await httpGet(apiUrl, rawHeaders);
  if (rawResp.status === 404) return null;
  if (rawResp.status !== 200) {
    throw new Error(`GitHub read HTTP ${rawResp.status}: ${rawResp.body.slice(0, 200)}`);
  }

  const rawContent = rawResp.body;

  // Tweede call: haal de SHA op via de standaard JSON media type.
  // Voor kleine bestanden bevat deze ook de base64 content (redundant hier),
  // voor grote bestanden alleen metadata. In beide gevallen: .sha is aanwezig.
  const jsonHeaders = {
    'Authorization': 'token ' + token,
    'User-Agent': 'Fluctus-Worker/4.0',
    'Accept': 'application/vnd.github+json',
  };
  const metaResp = await httpGet(apiUrl + '&_meta=1', jsonHeaders);
  let sha = null;
  if (metaResp.status === 200) {
    try {
      const metaData = JSON.parse(metaResp.body);
      sha = metaData.sha || null;
    } catch (_) { /* metadata parse fail is niet-kritiek; write zal sha later ophalen */ }
  }

  let parsedJson = null;
  let parseError = null;
  try {
    parsedJson = JSON.parse(rawContent);
  } catch (e) {
    parseError = e.message;
  }

  return { json: parsedJson, sha, raw: rawContent, parseError };
}

// Schrijf een JSON-bestand naar de GitHub repo. Retourneert {ok, action}.
async function githubWriteJson(token, owner, repo, path, payload, message) {
  const apiUrl = `https://api.github.com/repos/${owner}/${repo}/contents/${path}`;
  const contentBase64 = Buffer.from(JSON.stringify(payload)).toString('base64');
  const baseBody = {
    message: message || ('auto: ' + path + ' ' + new Date().toISOString().slice(0, 10)),
    content: contentBase64,
    committer: { name: 'Fluctus Bot', email: 'bot@fluctus.net' },
  };

  // Probeer tot 3 keer: bij 409 SHA-mismatch haal verse SHA op en retry.
  // Dit lost race-condities op tussen parallelle cache-update calls.
  const MAX_ATTEMPTS = 3;
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    // Haal huidige SHA op (null = bestand bestaat nog niet)
    let sha = null;
    const existing = await githubReadJson(token, owner, repo, path).catch(() => null);
    if (existing && existing.sha) sha = existing.sha;

    const putBody = { ...baseBody };
    if (sha) putBody.sha = sha;

    const putResp = await httpPut(apiUrl, token, putBody);

    if (putResp.status === 200 || putResp.status === 201) {
      return { ok: true, action: sha ? 'updated' : 'created', attempts: attempt };
    }

    // 409 = SHA mismatch (iemand anders heeft ondertussen geschreven).
    // 422 = content validation failed (kan ook over SHA gaan soms).
    // Bij beide: korte pauze + retry met verse SHA.
    if ((putResp.status === 409 || putResp.status === 422) && attempt < MAX_ATTEMPTS) {
      console.log(`GitHub write conflict (attempt ${attempt}/${MAX_ATTEMPTS}) — retry met verse SHA`);
      await new Promise(r => setTimeout(r, 200 + Math.random() * 300));
      continue;
    }

    throw new Error(`GitHub write HTTP ${putResp.status} na ${attempt} poging(en): ${putResp.body.slice(0, 200)}`);
  }

  throw new Error(`GitHub write faalde na ${MAX_ATTEMPTS} pogingen (conflicts)`);
}

function fmtIso(d) {
  return d.getUTCFullYear()
    + '-' + String(d.getUTCMonth() + 1).padStart(2, '0')
    + '-' + String(d.getUTCDate()).padStart(2, '0')
    + 'T' + String(d.getUTCHours()).padStart(2, '0')
    + ':' + String(d.getUTCMinutes()).padStart(2, '0')
    + ':00';
}

// Splits tijdsperiode in maandelijkse segmenten (voor parallelle calls)
function splitIntoMonths(start, end) {
  const segments = [];
  let cur = new Date(start);
  while (cur < end) {
    const segStart = new Date(cur);
    const segEnd = new Date(Date.UTC(cur.getUTCFullYear(), cur.getUTCMonth() + 1, 1));
    segments.push({ s: segStart, e: segEnd > end ? end : segEnd });
    cur = new Date(Date.UTC(cur.getUTCFullYear(), cur.getUTCMonth() + 1, 1));
  }
  return segments;
}

// ─── Elia Open Data — één pagina ophalen ──────────────────────────────────
async function eliaPage(dataset, timefield, selectfields, startStr, endStr, offset, limit) {
  const where = encodeURIComponent(`${timefield} >= "${startStr}" AND ${timefield} < "${endStr}"`);
  const select = encodeURIComponent(selectfields);
  const url = `https://opendata.elia.be/api/explore/v2.1/catalog/datasets/${dataset}/records`
    + `?where=${where}`
    + `&select=${select}`
    + `&order_by=${encodeURIComponent(timefield + ' ASC')}`
    + `&limit=${limit}`
    + `&offset=${offset}`;
  const r = await httpGet(url);
  if (r.status !== 200) throw new Error(`Elia ${dataset} HTTP ${r.status}`);
  return JSON.parse(r.body);
}

// ─── Elia Open Data — volledige maand ophalen (spot + imbalans tegelijk) ──
async function eliaMonth(dataset, spotfield, imbfield, segStart, segEnd) {
  const startStr = fmtIso(segStart);
  const endStr = fmtIso(segEnd);
  const selectfields = `datetime,${spotfield},${imbfield}`;
  const results = { spot: [], imb: [] };
  let offset = 0;
  const limit = 100;

  while (true) {
    const json = await eliaPage(dataset, 'datetime', selectfields, startStr, endStr, offset, limit);
    const records = json.results || [];

    records.forEach(rec => {
      const t = rec.datetime ? rec.datetime.substring(0, 19) + 'Z' : null;
      if (!t) return;
      const spotVal = parseFloat(rec[spotfield]);
      const imbVal = parseFloat(rec[imbfield]);
      if (!isNaN(spotVal)) results.spot.push({ t, v: Math.round(spotVal * 100) / 100 });
      if (!isNaN(imbVal))  results.imb.push({ t, v: Math.round(imbVal * 100) / 100 });
    });

    if (records.length < limit) break;
    offset += limit;
    // Kleine pauze om Elia rate limit te respecteren
    await new Promise(r => setTimeout(r, 100));
  }

  return results;
}

// ─── Elia Open Data — volledige periode ophalen (maanden parallel) ─────────
async function eliaFetch(dataset, spotfield, imbfield, startDate, endDate) {
  const segments = splitIntoMonths(startDate, endDate);
  const allSpot = [], allImb = [];

  // Max 4 maanden parallel (Elia rate limit)
  for (let i = 0; i < segments.length; i += 4) {
    const batch = segments.slice(i, i + 4);
    const batchResults = await Promise.all(
      batch.map(seg => eliaMonth(dataset, spotfield, imbfield, seg.s, seg.e))
    );
    batchResults.forEach(r => {
      allSpot.push(...r.spot);
      allImb.push(...r.imb);
    });
  }

  return { spot: allSpot, imb: allImb };
}

// ─── Hulpfunctie: geaggregeerde data (wind/zon) — één maand ─────────────────
async function eliaAggMonth(dataset, segStart, segEnd) {
  const startStr = fmtIso(segStart);
  const endStr   = fmtIso(segEnd);
  const results  = [];
  let offset = 0;
  const limit = 100;

  while (true) {
    const whereClause = `datetime >= "${startStr}" AND datetime < "${endStr}"`;
    const url = `${ELIA_BASE}/${dataset}/records`
      + `?where=${encodeURIComponent(whereClause)}`
      + `&select=${encodeURIComponent('datetime,sum(measured) as total_mw')}`
      + `&group_by=${encodeURIComponent('datetime')}`
      + `&order_by=${encodeURIComponent('datetime ASC')}`
      + `&limit=${limit}`
      + `&offset=${offset}`;

    const r = await httpGet(url);
    if (r.status !== 200) throw new Error(`${dataset} HTTP ${r.status}`);
    const json = JSON.parse(r.body);
    const records = (json.results || [])
      .filter(rec => rec.datetime && rec.total_mw != null)
      .map(rec => ({
        t: rec.datetime.substring(0, 19) + 'Z',
        v: Math.round((parseFloat(rec.total_mw) || 0) * 10) / 10
      }));

    results.push(...records);
    if (records.length < limit) break;
    offset += limit;
    await new Promise(r => setTimeout(r, 80));
  }
  return results;
}

// ═══════════════════════════════════════════════════════════════════════════
// ROUTE: GET /elia-data?from=YYYY-MM-DD&to=YYYY-MM-DD
//
// Geeft ONBALANSPRIJS terug voor de gevraagde periode uit Elia's ods047/ods134.
// Day-ahead spotprijs wordt via /entsoe-dayahead gehaald (ENTSO-E), NIET hier.
// (Het veld 'marginalincrementalprice' in Elia's datasets is een imbalance-
// component, geen spotprijs — vaak verward.)
//
// Response vorm blijft compatibel: { imb: [], spot: [], wind: [], solar: [] }
// maar spot/wind/solar zijn altijd leeg (voor backward-compat met oude client).
// ═══════════════════════════════════════════════════════════════════════════
app.get('/elia-data', async (req, res) => {
  const { from, to } = req.query;

  if (!from || !to) {
    return res.status(400).json({ error: 'from en to zijn verplicht (YYYY-MM-DD)' });
  }

  const fromDate = new Date(from + 'T00:00:00Z');
  const toDate   = new Date(to   + 'T23:59:59Z');
  const cutoff   = new Date('2024-05-22T00:00:00Z');

  // Hergebruik eliaFetch: die haalt ook marginalincrementalprice op, maar die
  // verdwijnt in resultaten negeren we. imbfield is wat we willen.
  const HIST   = { id: 'ods047', spotfield: 'marginalincrementalprice', imbfield: 'positiveimbalanceprice' };
  const RECENT = { id: 'ods134', spotfield: 'marginalincrementalprice', imbfield: 'imbalanceprice' };

  try {
    const allImb = [];
    const warnings = [];

    if (fromDate < cutoff) {
      try {
        const histEnd = toDate < cutoff ? toDate : cutoff;
        const d = await eliaFetch(HIST.id, HIST.spotfield, HIST.imbfield, fromDate, histEnd);
        allImb.push(...d.imb);
      } catch (e) { warnings.push('ods047: ' + e.message); }
    }
    if (toDate >= cutoff) {
      try {
        const recentStart = fromDate >= cutoff ? fromDate : cutoff;
        const d = await eliaFetch(RECENT.id, RECENT.spotfield, RECENT.imbfield, recentStart, toDate);
        allImb.push(...d.imb);
      } catch (e) { warnings.push('ods134: ' + e.message); }
    }

    allImb.sort((a, b) => a.t.localeCompare(b.t));

    const out = {
      spot:  [],   // Leeg — spot komt via /entsoe-dayahead
      imb:   allImb,
      wind:  [],
      solar: [],
      source: 'Elia Open Data (onbalans) — spot via ENTSO-E',
    };
    if (warnings.length) out.warnings = warnings;
    res.json(out);

  } catch (err) {
    console.error('elia-data fout:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════
// ROUTE: GET /elia-renewable?dataset=ods031&from=YYYY-MM-DD&to=YYYY-MM-DD
// ═══════════════════════════════════════════════════════════════════════════
app.get('/elia-renewable', async (req, res) => {
  const { dataset, from, to } = req.query;

  if (!dataset || !from || !to) {
    return res.status(400).json({ error: 'dataset, from en to zijn verplicht' });
  }
  if (!['ods031', 'ods032'].includes(dataset)) {
    return res.status(400).json({ error: 'dataset moet ods031 (wind) of ods032 (zon) zijn' });
  }

  const fromDate = new Date(from + 'T00:00:00Z');
  const toDate   = new Date(to   + 'T23:59:59Z');

  try {
    const segments = splitIntoMonths(fromDate, toDate);
    const all = [];

    // Max 3 maanden parallel om timeout te vermijden
    for (let i = 0; i < segments.length; i += 3) {
      const batch = segments.slice(i, i + 3);
      const batchResults = await Promise.all(
        batch.map(seg => eliaAggMonth(dataset, seg.s, seg.e))
      );
      batchResults.forEach(r => all.push(...r));
    }

    all.sort((a, b) => a.t.localeCompare(b.t));
    res.json({ data: all, count: all.length, dataset, source: 'Elia Open Data' });

  } catch (err) {
    console.error('elia-renewable fout:', err.message);
    res.status(500).json({ error: err.message });
  }
});


// ═══════════════════════════════════════════════════════════════════════════
// ROUTE: GET /entsoe-dayahead?from=YYYY-MM-DD&to=YYYY-MM-DD
//
// Haalt de échte day-ahead spotprijs (BELPEX/EPEX) op via de ENTSO-E
// Transparency Platform API. Dit is de correcte prijs die Elindus en andere
// platforms tonen — niet het 'marginalincrementalprice' veld uit Elia.
//
// DocumentType: A44 (Price Document)
// BusinessType: A62 (workaround voor ENTSO-E REST API bug sinds jan 2026)
// In/Out Domain: 10YBE----------2 (Belgische bidding zone)
// ═══════════════════════════════════════════════════════════════════════════

const ENTSOE_BASE = 'https://web-api.tp.entsoe.eu/api';
const ENTSOE_DOMAIN_BE = '10YBE----------2';
const ENTSOE_MAX_RANGE_DAYS = 365; // ENTSO-E max 1 jaar per call voor A44

// Parse ENTSO-E XML response (Publication_MarketDocument) naar {t, v} array.
// We gebruiken simpele regex — geen volledige XML parser nodig voor dit schema.
function parseEntsoePriceXml(xml) {
  const results = [];
  // Elke TimeSeries bevat een Period met een start-tijdstip en Point-reeksen
  // met position + price.amount. Resolution is PT60M (uurlijks) of PT15M.
  const timeSeriesMatches = xml.match(/<TimeSeries>[\s\S]*?<\/TimeSeries>/g) || [];

  timeSeriesMatches.forEach(ts => {
    const periodMatch = ts.match(/<Period>([\s\S]*?)<\/Period>/);
    if (!periodMatch) return;
    const period = periodMatch[1];

    const startMatch = period.match(/<start>([^<]+)<\/start>/);
    const resMatch = period.match(/<resolution>([^<]+)<\/resolution>/);
    if (!startMatch || !resMatch) return;

    const start = new Date(startMatch[1]); // ISO UTC
    const resolution = resMatch[1]; // bv "PT60M" of "PT15M"

    // Resolution in minuten
    let stepMin = 60;
    const resMin = resolution.match(/PT(\d+)M/);
    if (resMin) stepMin = parseInt(resMin[1], 10);

    // Points
    const pointRe = /<Point>\s*<position>(\d+)<\/position>\s*<price\.amount>([^<]+)<\/price\.amount>\s*<\/Point>/g;
    let m;
    let lastPos = 0;
    let lastPrice = null;
    const points = [];
    while ((m = pointRe.exec(period)) !== null) {
      points.push({ pos: parseInt(m[1], 10), price: parseFloat(m[2]) });
    }
    // ENTSO-E omits repeated values: als position 1 = 50, position 3 = 60,
    // dan is position 2 ook 50 (gap = fill forward). Maar typisch zien we
    // 24 opeenvolgende points voor uurlijkse data.
    if (!points.length) return;

    // Expand eventuele gaps
    const expanded = [];
    for (let i = 0; i < points.length; i++) {
      expanded.push(points[i]);
      if (i + 1 < points.length) {
        const gap = points[i + 1].pos - points[i].pos - 1;
        for (let g = 1; g <= gap; g++) {
          expanded.push({ pos: points[i].pos + g, price: points[i].price });
        }
      }
    }

    expanded.forEach(p => {
      const t = new Date(start.getTime() + (p.pos - 1) * stepMin * 60000);
      const tStr = t.toISOString().substring(0, 19) + 'Z';
      if (!isNaN(p.price)) {
        results.push({ t: tStr, v: Math.round(p.price * 100) / 100 });
      }
    });
  });

  return results;
}

function fmtEntsoeDate(d) {
  // Format: YYYYMMDDhhmm in UTC
  return d.getUTCFullYear()
    + String(d.getUTCMonth() + 1).padStart(2, '0')
    + String(d.getUTCDate()).padStart(2, '0')
    + String(d.getUTCHours()).padStart(2, '0')
    + String(d.getUTCMinutes()).padStart(2, '0');
}

async function entsoeDayAhead(token, startDate, endDate) {
  const url = ENTSOE_BASE
    + '?securityToken=' + encodeURIComponent(token)
    + '&documentType=A44'
    + '&businessType=A62'  // workaround voor REST API bug sinds jan 2026
    + '&in_Domain=' + ENTSOE_DOMAIN_BE
    + '&out_Domain=' + ENTSOE_DOMAIN_BE
    + '&periodStart=' + fmtEntsoeDate(startDate)
    + '&periodEnd=' + fmtEntsoeDate(endDate);

  const r = await httpGet(url, {
    'Accept': 'application/xml',
    'User-Agent': 'Fluctus-Dashboard/3.1',
  });

  if (r.status === 401) throw new Error('ENTSO-E 401: ongeldig securityToken');
  if (r.status === 429) throw new Error('ENTSO-E 429: rate limit bereikt');
  if (r.status !== 200) {
    // ENTSO-E geeft soms 400 met een XML body die een Reason bevat
    const reasonMatch = r.body.match(/<text>([^<]+)<\/text>/);
    const detail = reasonMatch ? reasonMatch[1] : r.body.slice(0, 200);
    throw new Error(`ENTSO-E HTTP ${r.status}: ${detail}`);
  }

  // Als er geen data is voor de periode: ENTSO-E geeft 200 met een Acknowledgement_MarketDocument
  if (r.body.indexOf('Acknowledgement_MarketDocument') !== -1) {
    return []; // Geen data, geen fout
  }

  return parseEntsoePriceXml(r.body);
}

// Splits een periode in segmenten van maximaal N dagen (voor ENTSO-E 1-jaar limiet)
function splitIntoYears(start, end, maxDays) {
  maxDays = maxDays || ENTSOE_MAX_RANGE_DAYS;
  const segments = [];
  let cur = new Date(start);
  while (cur < end) {
    const segEnd = new Date(Math.min(cur.getTime() + maxDays * 86400000, end.getTime()));
    segments.push({ s: new Date(cur), e: segEnd });
    cur = new Date(segEnd);
  }
  return segments;
}

app.get('/entsoe-dayahead', async (req, res) => {
  const { from, to } = req.query;

  if (!from || !to) {
    return res.status(400).json({ error: 'from en to zijn verplicht (YYYY-MM-DD)' });
  }

  const token = process.env.ENTSOE_TOKEN;
  if (!token) {
    return res.status(500).json({ error: 'ENTSOE_TOKEN niet ingesteld op de server' });
  }

  const fromDate = new Date(from + 'T00:00:00Z');
  const toDate   = new Date(to   + 'T23:59:59Z');

  try {
    // Split in 1-jaar segmenten, sequentieel opvragen (ENTSO-E rate limit is strict)
    const segments = splitIntoYears(fromDate, toDate);
    const all = [];
    const warnings = [];

    for (const seg of segments) {
      try {
        const data = await entsoeDayAhead(token, seg.s, seg.e);
        all.push(...data);
      } catch (e) {
        warnings.push(`${seg.s.toISOString().slice(0,10)} → ${seg.e.toISOString().slice(0,10)}: ${e.message}`);
      }
      // Kleine pauze tussen segmenten — ENTSO-E rate limit respecteren
      if (segments.length > 1) await new Promise(r => setTimeout(r, 300));
    }

    // Dedupe en sorteer (overlapping periodes kunnen dubbele punten geven)
    const seen = {};
    const deduped = [];
    all.forEach(p => {
      if (!seen[p.t]) {
        seen[p.t] = true;
        deduped.push(p);
      }
    });
    deduped.sort((a, b) => a.t.localeCompare(b.t));

    const out = {
      data: deduped,
      count: deduped.length,
      source: 'ENTSO-E Transparency Platform (BELPEX day-ahead)',
      domain: ENTSOE_DOMAIN_BE
    };
    if (warnings.length) out.warnings = warnings;
    res.json(out);

  } catch (err) {
    console.error('entsoe-dayahead fout:', err.message);
    res.status(500).json({ error: err.message });
  }
});


// ═══════════════════════════════════════════════════════════════════════════
// CACHE ROUTES — multi-bestand architectuur
//
// De cache is opgesplitst in 5 GitHub-bestanden om onder de GitHub API
// onbetrouwbaarheidsgrens (~10 MB) te blijven:
//   data/fluctus-cache-meta.json    (meta: lastDate, cacheVersion, ...)
//   data/fluctus-cache-spot.json    (ENTSO-E day-ahead, uurlijks)
//   data/fluctus-cache-imb.json     (Elia onbalans, kwartierlijks)
//   data/fluctus-cache-wind.json    (wind productie, kwartierlijks)
//   data/fluctus-cache-solar.json   (zon productie, kwartierlijks)
//
// Elk bestand is een plain JSON array met {t,v} objecten. Meta is een object.
// ═══════════════════════════════════════════════════════════════════════════

const CACHE_DATASETS = ['meta', 'spot', 'imb', 'wind', 'solar'];
const CACHE_BASE = 'data/fluctus-cache';

function cachePathFor(dataset) {
  if (!CACHE_DATASETS.includes(dataset)) return null;
  return `${CACHE_BASE}-${dataset}.json`;
}

// ═══════════════════════════════════════════════════════════════════════════
// ROUTE: GET /cache-read?dataset=meta|spot|imb|wind|solar
// Leest één cache-bestand via de GitHub API (no-CDN).
// Als het bestand niet bestaat of corrupt is, retourneert het een lege default
// i.p.v. een fout — zo blijft de snippet eenvoudig.
// ═══════════════════════════════════════════════════════════════════════════
app.get('/cache-read', async (req, res) => {
  const token = process.env.GITHUB_TOKEN;
  const owner = process.env.GITHUB_OWNER;
  const repo  = process.env.GITHUB_REPO;

  if (!token || !owner || !repo) {
    return res.status(500).json({ error: 'GitHub omgevingsvariabelen niet ingesteld' });
  }

  const dataset = req.query.dataset || 'meta';
  const path = cachePathFor(dataset);
  if (!path) {
    return res.status(400).json({ error: `Ongeldige dataset: ${dataset}. Geldig: ${CACHE_DATASETS.join(', ')}` });
  }

  res.set('Cache-Control', 'no-store, no-cache, must-revalidate');
  res.set('Pragma', 'no-cache');

  try {
    const result = await githubReadJson(token, owner, repo, path);
    if (!result) {
      // Bestand bestaat niet — retourneer default lege structuur
      return res.json(dataset === 'meta'
        ? { cacheVersion: 'entsoe-v1', lastDate: null, lastUpdated: null, _missing: true }
        : []);
    }
    if (!result.json) {
      // Corrupt JSON — retourneer lege default en markeer als corrupt
      console.error(`cache-read ${dataset}: corrupt JSON — ${result.parseError}`);
      return res.json(dataset === 'meta'
        ? { cacheVersion: 'entsoe-v1', lastDate: null, lastUpdated: null, _corrupted: true, _error: result.parseError }
        : []);
    }
    res.json(result.json);
  } catch (err) {
    console.error(`cache-read ${dataset} fout:`, err.message);
    res.status(500).json({ error: err.message });
  }
});


// ═══════════════════════════════════════════════════════════════════════════
// ROUTE: POST /cache-init
// Initialiseert alle 5 cache-bestanden met lege structuur. Idempotent.
// Gebruikt om corrupte cache te resetten.
// ═══════════════════════════════════════════════════════════════════════════
app.post('/cache-init', async (req, res) => {
  const token = process.env.GITHUB_TOKEN;
  const owner = process.env.GITHUB_OWNER;
  const repo  = process.env.GITHUB_REPO;

  if (!token || !owner || !repo) {
    return res.status(500).json({ error: 'GitHub omgevingsvariabelen niet ingesteld' });
  }

  const results = {};
  const now = new Date().toISOString();

  try {
    // Meta bestand
    const metaPath = cachePathFor('meta');
    const metaContent = {
      cacheVersion: 'entsoe-v1',
      lastDate: null,
      lastUpdated: now,
      _initializedAt: now
    };
    const metaRes = await githubWriteJson(token, owner, repo, metaPath, metaContent,
      'init: reset ' + metaPath);
    results.meta = metaRes;

    // Data bestanden (allemaal lege arrays)
    for (const ds of ['spot', 'imb', 'wind', 'solar']) {
      const dsPath = cachePathFor(ds);
      const dsRes = await githubWriteJson(token, owner, repo, dsPath, [],
        'init: reset ' + dsPath);
      results[ds] = dsRes;
      // Kleine pauze om concurrent writes te vermijden
      await new Promise(r => setTimeout(r, 150));
    }

    res.json({ ok: true, initialized: true, results });
  } catch (err) {
    console.error('cache-init fout:', err.message);
    res.status(500).json({ error: err.message, partialResults: results });
  }
});


// ═══════════════════════════════════════════════════════════════════════════
// ROUTE: POST /cache-update?dataset=meta|spot|imb|wind|solar
// Schrijft één cache-bestand. Body = de volledige nieuwe inhoud (array voor
// data-sets, object voor meta). Gebruikt 409-retry mechanisme van githubWriteJson.
// ═══════════════════════════════════════════════════════════════════════════
app.post('/cache-update', async (req, res) => {
  const token = process.env.GITHUB_TOKEN;
  const owner = process.env.GITHUB_OWNER;
  const repo  = process.env.GITHUB_REPO;

  if (!token || !owner || !repo) {
    return res.status(500).json({ error: 'GitHub omgevingsvariabelen niet ingesteld' });
  }

  const dataset = req.query.dataset || 'meta';
  const path = cachePathFor(dataset);
  if (!path) {
    return res.status(400).json({ error: `Ongeldige dataset: ${dataset}` });
  }

  try {
    const payload = req.body;
    // Validatie: arrays voor data-sets, object voor meta
    if (dataset === 'meta') {
      if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
        return res.status(400).json({ error: 'meta body moet een object zijn' });
      }
    } else {
      if (!Array.isArray(payload)) {
        return res.status(400).json({ error: `${dataset} body moet een array zijn` });
      }
    }

    const result = await githubWriteJson(token, owner, repo, path, payload,
      `auto: ${dataset} ${new Date().toISOString().slice(0, 10)}`);

    const sizeKb = Math.round(JSON.stringify(payload).length / 1024);
    res.json({
      ok: true,
      dataset,
      size_kb: sizeKb,
      action: result.action,
      attempts: result.attempts
    });

  } catch (err) {
    console.error(`cache-update ${dataset} fout:`, err.message);
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════
// ROUTE: GET /   (status pagina)
// ═══════════════════════════════════════════════════════════════════════════
app.get('/', (req, res) => res.json({
  status: 'ok',
  versie: '15.2',
  model: 'claude-opus-4-7',
  tools: ['web_search_20250305'],
  cache: 'multi-bestand (5 datasets) + gzip compressie',
  routes: [
    '/elia-data?from=YYYY-MM-DD&to=YYYY-MM-DD   (onbalans uit Elia)',
    '/elia-renewable?dataset=ods031|ods032&...  (wind/zon uit Elia)',
    '/entsoe-dayahead?from=...&to=...           (BELPEX spot uit ENTSO-E)',
    'GET  /cache-read?dataset=meta|spot|imb|wind|solar',
    'POST /cache-update?dataset=...             (body = array, behalve meta=object)',
    'POST /cache-init                           (reset alle 5 cache-bestanden)',
    'GET  /explanation?chartId=c1',
    'POST /claude-explain-refresh',
    '── SIMULATOR (v15) ──',
    'GET  /api/profielen-lijst                  (25 profielen + beschrijvingen)',
    'GET  /api/profiel?naam=Slager              (35040-kwartier-array)',
    'GET  /api/postcode-grd?postcode=8500       (GRD lookup)',
    'GET  /api/regio-tarieven?grd=...&spanning=MS|LS',
    'GET  /api/leveringscontract-staffel        (default markup/markdown per MWh)',
    'GET  /api/batterijen                       (lijst beschikbare batterijen)',
    'POST /api/batterij-toevoegen               (voeg batterij toe)',
    'POST /api/nominatie-sim                    (run simulator.py met JSON-input)',
    'GET  /api/projecten                        (lijst projecten in fluctus-scenarios)',
    'GET  /api/scenarios?project=X              (lijst scenarios in project X)',
    'GET  /api/scenario?project=X&scenario=Y    (lees scenario JSON)',
    'POST /api/scenario-bewaren                 (body = {project, scenario, data})',
  ]
}));

// ═══════════════════════════════════════════════════════════════════════════
// ROUTE: GET /explanation?chartId=c1
//
// Leest de gecachede uitleg voor een chart uit de GitHub repo.
// Retourneert {cached: true, date, text, citations} of {cached: false}
// als er nog geen uitleg van vandaag bestaat.
// ═══════════════════════════════════════════════════════════════════════════
app.get('/explanation', async (req, res) => {
  const chartId = req.query.chartId;
  if (!chartId || !/^[a-zA-Z0-9_-]+$/.test(chartId)) {
    return res.status(400).json({ error: 'chartId is verplicht (alleen a-z, 0-9, _, -)' });
  }

  const token = process.env.GITHUB_TOKEN;
  const owner = process.env.GITHUB_OWNER;
  const repo  = process.env.GITHUB_REPO;
  if (!token || !owner || !repo) {
    return res.status(500).json({ error: 'GitHub omgevingsvariabelen niet ingesteld.' });
  }

  const path = `data/explanations/${chartId}.json`;

  try {
    const result = await githubReadJson(token, owner, repo, path);
    if (!result) {
      return res.json({ cached: false, reason: 'nog niet gegenereerd' });
    }

    const today = new Date().toISOString().slice(0, 10);
    const cachedDate = (result.json && result.json.date) ? result.json.date : null;

    if (cachedDate !== today) {
      return res.json({
        cached: false,
        reason: 'cache van andere dag',
        stale_date: cachedDate
      });
    }

    return res.json({
      cached: true,
      date: cachedDate,
      text: result.json.text || '',
      citations: result.json.citations || [],
      generated_at: result.json.generated_at || null,
      model: result.json.model || null
    });

  } catch (err) {
    console.error('GET /explanation fout:', err.message);
    return res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════
// Anthropic API helpers
// ═══════════════════════════════════════════════════════════════════════════

// Haal alle tekst + citaties uit de content blocks van een Claude response.
// Strategie: alleen het LAATSTE substantiële tekstblok is het eigenlijke antwoord.
// Eerdere tekstblokken zijn "ik ga zoeken..." redeneringen tussen tool-calls door.
// Citaties verzamelen we uit ALLE text blocks (niet enkel het laatste) zodat
// bronnen niet verloren gaan als Claude ze in een vroeg blok vermeldde.
function extractTextAndCitations(content) {
  const allTexts = [];
  const citations = [];
  const seenUrls = {};

  (content || []).forEach(block => {
    if (block.type === 'text' && typeof block.text === 'string' && block.text.trim().length > 0) {
      allTexts.push(block.text);
      (block.citations || []).forEach(c => {
        const url = c.url;
        const title = c.title || c.cited_text || url;
        if (url && !seenUrls[url]) {
          seenUrls[url] = true;
          citations.push({ url, title });
        }
      });
    }
  });

  // Filter: het finale antwoord kan uit meerdere text-blocks bestaan verdeeld
  // rond tool_use calls. We nemen ALLE substantiële blocks (>50 chars) in
  // volgorde en voegen ze slim samen.
  //
  // Slim samenvoegen: als een block eindigt midden in een zin (geen . ! ? ) en
  // het volgende block begint met een kleine letter, dan voegen we samen met
  // één spatie. Anders met een dubbele newline (paragraaf-break).
  //
  // Let op: vroeger namen we alleen een achterwaartse reeks. Dat was fout
  // want een kort block tussen twee lange blocks zorgde voor het weglaten
  // van het eerste lange block (zoals de header "**1) Algemeen beeld**").
  let finalText = '';
  if (allTexts.length === 0) {
    finalText = '';
  } else if (allTexts.length === 1) {
    finalText = allTexts[0];
  } else {
    const substantialBlocks = allTexts
      .map(t => t.trim())
      .filter(t => t.length > 50);
    if (substantialBlocks.length > 0) {
      finalText = substantialBlocks[0];
      for (let i = 1; i < substantialBlocks.length; i++) {
        const prev = finalText;
        const next = substantialBlocks[i];
        // Eindigt vorig block in midden van zin? (geen punt/vraag/uitroep aan einde
        // en geen dubbele newline)
        const endsMidSentence = !/[.!?:]["')]*\s*$/.test(prev) && !prev.endsWith('\n\n');
        // Begint volgende block met kleine letter of leesteken? (vervolg van zin)
        const startsMidSentence = /^[a-z,;)\-–—]/.test(next);
        if (endsMidSentence && startsMidSentence) {
          finalText = prev + ' ' + next;
        } else if (endsMidSentence) {
          // Midden in zin maar volgende start nieuwe zin/kop → spatie
          finalText = prev + ' ' + next;
        } else {
          // Complete zin → paragraaf-break
          finalText = prev + '\n\n' + next;
        }
      }
    } else {
      // Fallback: geen enkel block >50 chars — neem gewoon alles samen
      finalText = allTexts.map(t => t.trim()).filter(Boolean).join(' ');
    }
  }

  // Strip Engelse meta-prefixes die Claude soms uitspreekt vóór het echte antwoord.
  // Deze patronen zijn overleg-tekst die onbedoeld in het antwoord belandt.
  // We strippen regel-voor-regel tot we een regel vinden die geen meta is.
  const metaPrefixes = [
    /^i have (enough |now |)\s*(gathered |collected |gotten |)\s*(enough |sufficient |)?\s*(information|data|context).*$/i,
    /^i (now |)\s*have (what i need|sufficient|enough).*$/i,
    /^i'?ll now (compose|write|put together|structure|draft|provide).*$/i,
    /^let me (now |)\s*(compose|write|put together|structure|draft|provide|analyze).*$/i,
    /^based on (the |my |)(research|search|data|analysis), (i|let me).*$/i,
    /^(now |)\s*i can (compose|write|provide|give|analyze).*$/i,
    /^(now |)\s*let'?s (compose|analyze|look).*$/i,
    /^here'?s (the|my|a) (analysis|breakdown|summary|overview).*$/i,
    /^i'?ll (structure|format|organize).*$/i,
  ];
  const lines = finalText.split('\n');
  let skipUntil = 0;
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) { skipUntil = i + 1; continue; } // lege regels meenemen in skip
    const isMeta = metaPrefixes.some(re => re.test(line));
    if (isMeta) {
      skipUntil = i + 1;
    } else {
      break; // eerste niet-meta regel → stop
    }
  }
  if (skipUntil > 0) {
    finalText = lines.slice(skipUntil).join('\n').trimStart();
  }

  return { text: finalText.trim(), citations };
}

async function callClaudeMessages(apiKey, requestBody) {
  const headers = {
    'x-api-key': apiKey,
    'anthropic-version': '2023-06-01',
    'User-Agent': 'Fluctus-Dashboard/3.0',
  };
  const r = await httpPostJson('https://api.anthropic.com/v1/messages', headers, requestBody);
  if (r.status !== 200) {
    throw new Error(`Anthropic HTTP ${r.status}: ${r.body.slice(0, 300)}`);
  }
  return JSON.parse(r.body);
}

// ═══════════════════════════════════════════════════════════════════════════
// ROUTE: POST /claude-explain-refresh
// Body: { chartId: 'c1', prompt: '...' }
//
// Genereert een nieuwe uitleg via Claude + web search, schrijft ze naar
// GitHub cache, en retourneert het resultaat. Beschermt tegen race-condities:
// als er tussen read en generatie al een cache van vandaag bestaat wordt die
// teruggegeven zonder tweede API call.
// ═══════════════════════════════════════════════════════════════════════════
app.post('/claude-explain-refresh', async (req, res) => {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  const token = process.env.GITHUB_TOKEN;
  const owner = process.env.GITHUB_OWNER;
  const repo  = process.env.GITHUB_REPO;

  if (!apiKey) return res.status(500).json({ error: 'ANTHROPIC_API_KEY niet ingesteld' });
  if (!token || !owner || !repo) return res.status(500).json({ error: 'GitHub omgevingsvariabelen niet ingesteld' });

  const { chartId, prompt } = req.body || {};
  if (!chartId || !/^[a-zA-Z0-9_-]+$/.test(chartId)) {
    return res.status(400).json({ error: 'chartId is verplicht (alleen a-z, 0-9, _, -)' });
  }
  if (!prompt) return res.status(400).json({ error: 'prompt is verplicht' });

  const path = `data/explanations/${chartId}.json`;
  const today = new Date().toISOString().slice(0, 10);

  try {
    // Race-condition check: misschien heeft een andere klik net gegenereerd
    const existing = await githubReadJson(token, owner, repo, path).catch(() => null);
    if (existing && existing.json && existing.json.date === today) {
      return res.json({
        text: existing.json.text || '',
        citations: existing.json.citations || [],
        stop_reason: 'from_cache',
        model: existing.json.model || null,
        from_cache: true,
        generated_at: existing.json.generated_at || null
      });
    }

    // Genereer nieuwe uitleg
    const MODEL = 'claude-opus-4-7';
    const MAX_CONTINUATIONS = 3;

    const baseRequest = {
      model: MODEL,
      max_tokens: 1500,
      tools: [{
        type: 'web_search_20250305',
        name: 'web_search',
        max_uses: 4,
        user_location: { type: 'approximate', country: 'BE', timezone: 'Europe/Brussels' }
      }],
      messages: [{ role: 'user', content: prompt }]
    };

    let response = await callClaudeMessages(apiKey, baseRequest);
    let conversation = [
      { role: 'user', content: prompt },
      { role: 'assistant', content: response.content }
    ];

    let continuations = 0;
    while (response.stop_reason === 'pause_turn' && continuations < MAX_CONTINUATIONS) {
      continuations++;
      const continueReq = Object.assign({}, baseRequest, { messages: conversation });
      response = await callClaudeMessages(apiKey, continueReq);
      conversation.push({ role: 'assistant', content: response.content });
    }

    const { text, citations } = extractTextAndCitations(response.content);

    // Schrijf naar GitHub cache
    const payload = {
      chartId,
      date: today,
      generated_at: new Date().toISOString(),
      model: response.model || MODEL,
      text: text || '',
      citations,
      stop_reason: response.stop_reason
    };

    try {
      await githubWriteJson(
        token, owner, repo, path, payload,
        `auto: uitleg ${chartId} ${today}`
      );
    } catch (writeErr) {
      // Cache-schrijven mislukt, maar we hebben wel tekst — log en stuur terug
      console.error('Cache write fout (niet-fataal):', writeErr.message);
    }

    res.json({
      text: payload.text || 'Geen antwoord.',
      citations: payload.citations,
      stop_reason: payload.stop_reason,
      model: payload.model,
      from_cache: false,
      generated_at: payload.generated_at
    });

  } catch (err) {
    console.error('claude-explain-refresh fout:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.options('/explanation', (req, res) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  res.sendStatus(200);
});
app.options('/claude-explain-refresh', (req, res) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  res.sendStatus(200);
});





// ═══════════════════════════════════════════════════════════════════════════
// ═══════════════════════════════════════════════════════════════════════════
//                         SIMULATOR ENDPOINTS (v15)
// ═══════════════════════════════════════════════════════════════════════════
// ═══════════════════════════════════════════════════════════════════════════

// ─── Helpers ───────────────────────────────────────────────────────────────

// Cache voor data-bestanden (geladen bij eerste call)
const dataCache = {};

function loadDataFile(filename) {
  if (dataCache[filename]) return dataCache[filename];
  const fullPath = path.join(DATA_DIR, filename);
  if (!fs.existsSync(fullPath)) {
    throw new Error(`Data bestand niet gevonden: ${filename}`);
  }
  const content = fs.readFileSync(fullPath, 'utf8');
  const parsed = JSON.parse(content);
  dataCache[filename] = parsed;
  return parsed;
}

function safeFilename(naam) {
  return String(naam).toLowerCase().replace(/\//g, '_').replace(/\s+/g, '_').replace(/[^a-z0-9_-]/g, '');
}

// ─── ROUTE: GET /api/profielen-lijst ──────────────────────────────────────
// Retourneert: [{naam, beschrijving}, ...] (25 profielen)
app.get('/api/profielen-lijst', (req, res) => {
  try {
    const lijst = loadDataFile('profielen-lijst.json');
    res.json(lijst);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── ROUTE: GET /api/profiel?naam=Slager ──────────────────────────────────
// Retourneert: {naam, beschrijving, kwartier: [35040 floats genormaliseerd]}
app.get('/api/profiel', (req, res) => {
  const naam = req.query.naam;
  if (!naam) return res.status(400).json({ error: 'naam is verplicht' });

  try {
    const lijst = loadDataFile('profielen-lijst.json');
    const meta = lijst.find(p => p.naam === naam);
    if (!meta) return res.status(404).json({ error: `Profiel '${naam}' niet gevonden` });

    const safe = safeFilename(naam);
    const fullPath = path.join(DATA_DIR, 'profielen', `${safe}.json`);
    if (!fs.existsSync(fullPath)) {
      return res.status(404).json({ error: `Profiel-bestand niet gevonden: ${safe}.json` });
    }
    const kwartier = JSON.parse(fs.readFileSync(fullPath, 'utf8'));
    res.json({
      naam: meta.naam,
      beschrijving: meta.beschrijving,
      kwartier: kwartier,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GRD-normalisatie: postcode-tabel naam → tarieven-tabel naam
const GRD_NORMALISATIE = {
  'Fluvius Antwerpen': 'Antwerpen',
  'Fluvius Halle-Vilvoorde': 'Halle-Vilv.',
  'Fluvius Imewo': 'Imewo',
  'Fluvius Kempen': 'Kempen',
  'Fluvius Limburg': 'Limburg',
  'Fluvius Midden-Vlaanderen': 'Midden-Vl.',
  'Fluvius West': 'West',
  'Fluvius Zenne-Dijle': 'Zenne-Dijle',
  'ORES': 'ORES',
  'RESA': 'RESA',
  'AIEG': 'AIEG',
  'AIESH': 'ORES',  // fallback: AIESH gebruikt ORES-tarieven
  'REW': 'ORES',    // fallback: REW gebruikt ORES-tarieven (proxy)
  'Sibelga': 'Sibelga',
};

// ─── ROUTE: GET /api/postcode-grd?postcode=8500 ──────────────────────────
// Retourneert: {postcode, grd_origineel, grd, fallback_naar?}
app.get('/api/postcode-grd', (req, res) => {
  const pc = String(req.query.postcode || '').trim();
  if (!/^\d{4}$/.test(pc)) {
    return res.status(400).json({ error: 'postcode moet 4 cijfers zijn' });
  }
  try {
    const map = loadDataFile('postcodes.json');
    const grdOrigineel = map[pc];
    let grd, fallback = null;

    if (grdOrigineel) {
      grd = GRD_NORMALISATIE[grdOrigineel] || grdOrigineel;
      if (grd !== grdOrigineel) {
        if (grdOrigineel === 'AIESH' || grdOrigineel === 'REW') {
          fallback = `${grdOrigineel} gebruikt ORES-tarieven als proxy`;
        }
      }
    } else {
      // Onbekende postcode: gewest-fallback op basis van eerste cijfer
      const eerste = pc[0];
      if (eerste === '1' && pc[1] === '0' && parseInt(pc) >= 1000 && parseInt(pc) <= 1299) {
        grd = 'Sibelga';
        fallback = `Postcode ${pc} niet exact bekend, fallback naar Sibelga (Brussel)`;
      } else if (['1', '2', '3', '8', '9'].includes(eerste)) {
        grd = 'West';
        fallback = `Postcode ${pc} niet exact bekend, fallback naar Fluvius West (Vlaanderen)`;
      } else {
        grd = 'ORES';
        fallback = `Postcode ${pc} niet exact bekend, fallback naar ORES (Wallonië)`;
      }
    }
    res.json({
      postcode: pc,
      grd_origineel: grdOrigineel || null,
      grd,
      fallback_naar: fallback,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── ROUTE: GET /api/regio-tarieven?grd=West&spanning=MS ─────────────────
// Retourneert: alle tarief-velden voor die GRD/spanning combinatie
app.get('/api/regio-tarieven', (req, res) => {
  const grd = req.query.grd;
  const spanning = (req.query.spanning || 'MS').toUpperCase();
  if (!grd) return res.status(400).json({ error: 'grd is verplicht' });
  if (!['MS', 'LS'].includes(spanning)) {
    return res.status(400).json({ error: 'spanning moet MS of LS zijn' });
  }
  try {
    const tarieven = loadDataFile('tarieven.json');
    const key = `${grd}|${spanning}`;
    const tar = tarieven[key];
    if (!tar) {
      return res.status(404).json({
        error: `Geen tarieven voor ${key}. Beschikbaar: ${Object.keys(tarieven).join(', ')}`
      });
    }

    // Construeer accijns-staffel in juiste formaat voor simulator
    const accijnzen_staffel = [
      [3, tar.accijns_schijf1_3mwh || 14.21],
      [20, tar.accijns_schijf2_20mwh || 14.21],
      [50, tar.accijns_schijf3_50mwh || 12.09],
      [1000, tar.accijns_schijf4_1000mwh || 11.39],
      [9999999, tar.accijns_schijf5_inf || 10.00],
    ];

    res.json({
      grd: grd,
      spanning: spanning,
      tarieven: tar,
      accijnzen_staffel: accijnzen_staffel,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── ROUTE: GET /api/leveringscontract-staffel ────────────────────────────
app.get('/api/leveringscontract-staffel', (req, res) => {
  try {
    const lc = loadDataFile('leveringscontract.json');
    res.json(lc);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── ROUTE: GET /api/batterijen ───────────────────────────────────────────
app.get('/api/batterijen', (req, res) => {
  try {
    const list = loadDataFile('batterijen.json');
    res.json(list);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── ROUTE: POST /api/batterij-toevoegen ──────────────────────────────────
// Body: {Battery, "Vermogen omvormer kW", "Capaciteit kWh", ...}
// Append in batterijen.json (lokaal cache + GitHub commit)
app.post('/api/batterij-toevoegen', async (req, res) => {
  const nieuwe = req.body;
  if (!nieuwe || !nieuwe.Battery) {
    return res.status(400).json({ error: 'body moet Battery-veld bevatten' });
  }
  try {
    const list = loadDataFile('batterijen.json');
    list.push(nieuwe);
    // Schrijf lokaal (overleeft niet container restart, maar voor sessie OK)
    fs.writeFileSync(path.join(DATA_DIR, 'batterijen.json'), JSON.stringify(list, null, 2));
    dataCache['batterijen.json'] = list;

    // Commit naar GitHub indien mogelijk (best effort)
    const token = process.env.GITHUB_TOKEN;
    const owner = process.env.GITHUB_OWNER;
    const proxyRepo = process.env.GITHUB_PROXY_REPO || 'fluctus-proxy';
    if (token && owner) {
      try {
        await githubWriteJson(token, owner, proxyRepo, 'data/batterijen.json', list,
          `add: batterij ${nieuwe.Battery}`);
      } catch (e) {
        console.warn('GitHub commit batterij gefaald:', e.message);
      }
    }
    res.json({ ok: true, totaal: list.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── ROUTE: POST /api/nominatie-sim ──────────────────────────────────────
// SMART ENDPOINT v15.1
// Body kan één van twee shapes hebben:
//   1. UI-payload (high-level): { profielNaam, jaarverbruik_mwh, postcode, grd,
//        spanning, pv_kwp, batterijId, contract: { staffel, modus, ... },
//        aansluiting_kva, simulatieperiode: { van, tot } }
//   2. Volledige sim-input (low-level): wordt direct doorgepiped naar simulator.py
//
// We detecteren shape via aanwezigheid van 'profiel_kwartier' (low-level) of
// 'profielNaam' (high-level). High-level → buildSimulatorInput() draait alle
// data-loads en mappings, dan spawn.

// Helper: bouw de complete simulator-input vanaf high-level UI keys
async function buildSimulatorInput(uiInput) {
  const errors = [];
  const out = {};

  // --- 1. Profiel ---
  if (!uiInput.profielNaam) errors.push('profielNaam ontbreekt');
  let profielKwartier = null;
  if (uiInput.profielNaam) {
    const safe = safeFilename(uiInput.profielNaam);
    const profPath = path.join(DATA_DIR, 'profielen', `${safe}.json`);
    if (!fs.existsSync(profPath)) {
      errors.push(`Profiel '${uiInput.profielNaam}' niet gevonden (bestand ${safe}.json)`);
    } else {
      profielKwartier = JSON.parse(fs.readFileSync(profPath, 'utf8'));
      if (!Array.isArray(profielKwartier) || profielKwartier.length !== 35040) {
        errors.push(`Profiel ${uiInput.profielNaam} heeft ${profielKwartier?.length || 0} elementen, verwacht 35040`);
      }
    }
  }
  out.profiel_kwartier = profielKwartier || [];
  out.aanvullingen = { laadinfra: null, elektrificatie: null };
  out.jaarverbruik_mwh = parseFloat(uiInput.jaarverbruik_mwh) || 0;
  if (out.jaarverbruik_mwh <= 0) errors.push('jaarverbruik_mwh moet > 0');

  // --- 2. PV ---
  const pv_kwp = parseFloat(uiInput.pv_kwp) || 0;
  // Generieke Belgische PV-vorm (35040 floats, som=1) — sinusoidale dagcurve × seizoensmodulatie
  // Lazy-laad of genereer een keer en cache
  if (!dataCache._pvShape) {
    dataCache._pvShape = generateBelgianPvShape();
  }
  out.pv = {
    kwp: pv_kwp,
    specifiek_rendement_kwh_per_kwp: 950,
    vorm_kwartier: dataCache._pvShape,
    capex_eur: 0
  };

  // --- 3. Batterij ---
  if (uiInput.batterijId) {
    const batterijen = loadDataFile('batterijen.json');
    const list = batterijen.batterijen || batterijen;
    const b = list.find(x => (x.id || x.naam) === uiInput.batterijId);
    if (!b) {
      errors.push(`Batterij '${uiInput.batterijId}' niet gevonden`);
    } else {
      out.batterij = {
        kw: parseFloat(b.kw) || 0,
        kwh: parseFloat(b.kwh) || 0,
        dod_pct: parseFloat(b.dod_pct || b.dod) || 0.95,
        rte_pct: parseFloat(b.eta || b.rte_pct) || 0.92,
        capex_eur: parseFloat(b.capex || b.capex_eur) || 0,
        max_cycli: parseFloat(b.max_cycli) || 5000
      };
    }
  } else {
    // geen batterij — kw=0, kwh=0
    out.batterij = { kw: 0, kwh: 0, dod_pct: 0.95, rte_pct: 0.92, capex_eur: 0, max_cycli: 5000 };
  }

  // --- 4. Aansluiting ---
  const kva = parseFloat(uiInput.aansluiting_kva) || 50;
  const cosphi = 0.95;  // typisch
  const max_kw = kva * cosphi;
  out.aansluiting = {
    max_afname_kw_zacht: max_kw,
    max_afname_kw_hard: max_kw * 1.5,
    max_injectie_kw_zacht: max_kw,
    max_injectie_kw_hard: max_kw * 1.5,
    tarief_overschrijding_afname_eur_per_kw_jaar: 50,
    tarief_overschrijding_injectie_eur_per_kw_jaar: 30
  };

  // --- 5. Contract (passthrough vanuit UI met fallback defaults) ---
  out.contract = uiInput.contract || {};
  // Zorg dat staffel een lijst is
  if (!Array.isArray(out.contract.staffel)) {
    try {
      const lc = loadDataFile('leveringscontract.json');
      out.contract.staffel = lc.schijven || lc.staffel || [];
      if (typeof out.contract.vergroening_eur_per_mwh !== 'number') {
        out.contract.vergroening_eur_per_mwh = lc.vergroening_eur_per_mwh || 2.5;
      }
      if (typeof out.contract.vaste_kost_eur_maand !== 'number') {
        out.contract.vaste_kost_eur_maand = lc.vast_eur_per_maand || 10;
      }
      if (!out.contract.leverancier) out.contract.leverancier = lc.leverancier || 'Enwyse';
    } catch (e) {
      errors.push('leveringscontract.json niet leesbaar: ' + e.message);
    }
  }
  if (!out.contract.modus) out.contract.modus = 'passthrough';
  if (typeof out.contract.injectie_toegelaten !== 'boolean') out.contract.injectie_toegelaten = true;
  // backwards-compat keys voor simulator
  out.contract.gsc_eur_mwh = out.contract.gsc_eur_mwh || 0;
  out.contract.wkk_eur_mwh = out.contract.wkk_eur_mwh || 0;

  // --- 6. Netbeheer (tarieven) ---
  const grd = uiInput.grd || 'West';
  const spanning = uiInput.spanning || 'MS';
  let tariefset = null;
  try {
    const tarieven = loadDataFile('tarieven.json');
    // tarieven.json shape: flat { "West|MS": {...}, "West|LS": {...}, ... }
    // (NIET nested { "West": { "MS": {...}, ... } })
    const key = `${grd}|${spanning}`;
    if (tarieven[key]) {
      tariefset = tarieven[key];
    } else {
      // Fallback op eerste beschikbare GRD voor deze spanning
      const matchingKeys = Object.keys(tarieven).filter(k => k.endsWith('|' + spanning));
      if (matchingKeys.length > 0) {
        const fallbackKey = matchingKeys[0];
        tariefset = tarieven[fallbackKey];
        errors.push(`WAARSCHUWING: GRD '${grd}' niet gevonden voor ${spanning}, fallback op '${fallbackKey}'`);
      }
    }
  } catch (e) {
    errors.push('tarieven.json niet leesbaar: ' + e.message);
  }
  if (!tariefset) {
    errors.push(`Geen tarieven voor GRD=${grd} spanning=${spanning}`);
    tariefset = {};
  }
  out.netbeheer = { grd, spanning, tarieven: tariefset };

  // --- 7. Forecast (defaults) ---
  out.forecast = uiInput.forecast || {
    sigma_da: 0,
    sigma_imb: 0,
    sigma_volume_verbruik_pct: 0,
    sigma_volume_pv_pct: 0
  };

  // --- 8. Simulatieperiode ---
  out.simulatieperiode = uiInput.simulatieperiode || { van: '2024-01-01', tot: '2024-12-31' };

  // --- 9. Marktdata (BELPEX spot + imbalance) ---
  // Lees uit GitHub cache via githubReadJson, projecteer op simulatieperiode
  const markt = await loadMarktData(out.simulatieperiode.van, out.simulatieperiode.tot);
  out.markt = markt;
  if (markt._warning) errors.push(markt._warning);

  // --- 10. Random seed ---
  out.random_seed = parseInt(uiInput.random_seed) || 42;

  return { input: out, errors };
}

// PV-vorm generator: 35040 kwartiers, sinus-curve × seizoens-modulatie
// Genormaliseerd op som=1 (zodat × jaarproductie_kwh × 4 = kw per kwartier)
function generateBelgianPvShape() {
  const N = 35040;
  const arr = new Array(N).fill(0);
  for (let i = 0; i < N; i++) {
    // Dag van het jaar (0-364), uur van de dag (0-23.75)
    const day = Math.floor(i / 96);
    const quartUur = i % 96;
    const hour = quartUur / 4;  // 0..23.75
    // Daglengte en zonnemax: zomer ~16h, winter ~8h
    const seasonRad = ((day - 80) / 365) * 2 * Math.PI;  // 0 op equinox
    const dayLength = 12 + 4 * Math.sin(seasonRad);  // 8..16
    const noon = 13;  // CET zonnehoogte
    const sunStart = noon - dayLength / 2;
    const sunEnd = noon + dayLength / 2;
    if (hour < sunStart || hour > sunEnd) continue;
    // Sinus-piek tijdens daglicht
    const t = (hour - sunStart) / (sunEnd - sunStart);
    const dailyShape = Math.sin(t * Math.PI);
    // Seizoens-amplitude: zomer × 1.5, winter × 0.5
    const seasonScale = 1.0 + 0.5 * Math.sin(seasonRad);
    arr[i] = Math.max(0, dailyShape * seasonScale);
  }
  // Normaliseer som=1
  const som = arr.reduce((a, b) => a + b, 0);
  if (som > 0) {
    for (let i = 0; i < N; i++) arr[i] /= som;
  }
  return arr;
}

// Laad BELPEX spot + imbalance data uit GitHub cache, projecteer op periode
async function loadMarktData(vanISO, totISO) {
  const token = process.env.GITHUB_TOKEN;
  const owner = process.env.GITHUB_OWNER;
  const repo  = process.env.GITHUB_REPO;
  if (!token || !owner || !repo) {
    return generateDummyMarkt(vanISO, totISO, 'GitHub env-vars niet ingesteld');
  }
  let spotRaw = null, imbRaw = null;
  try {
    const r1 = await githubReadJson(token, owner, repo, cachePathFor('spot'));
    if (r1 && r1.json && Array.isArray(r1.json)) spotRaw = r1.json;
  } catch (e) { /* leeg */ }
  try {
    const r2 = await githubReadJson(token, owner, repo, cachePathFor('imb'));
    if (r2 && r2.json && Array.isArray(r2.json)) imbRaw = r2.json;
  } catch (e) { /* leeg */ }
  if (!spotRaw || !imbRaw || spotRaw.length === 0 || imbRaw.length === 0) {
    return generateDummyMarkt(vanISO, totISO, 'Marktdata-cache leeg of niet beschikbaar — dummy data gebruikt');
  }
  // Spot/imb data shape (uit ENTSO-E + Elia): [{datum: 'YYYY-MM-DD', uur: 0-23, prijs_eur_mwh: ...}]
  // OF kwartier: [{datum, kwartier: 0-95, ...}]. We reconstrueren een 1-uur of 15-min array.
  // Simpele projectie: bouw timestamps array op kwartierbasis tussen vanISO en totISO.
  return buildMarktArrayFromCache(spotRaw, imbRaw, vanISO, totISO);
}

function generateDummyMarkt(vanISO, totISO, warning) {
  const start = new Date(vanISO + 'T00:00:00');
  const end = new Date(totISO + 'T23:45:00');
  const N = Math.floor((end - start) / (15 * 60 * 1000)) + 1;
  const spot = new Array(N), imb = new Array(N), timestamps = new Array(N);
  for (let i = 0; i < N; i++) {
    const t = new Date(start.getTime() + i * 15 * 60 * 1000);
    timestamps[i] = t.toISOString();
    // Dummy: ~80 €/MWh + sinus daycycle
    const hour = t.getHours() + t.getMinutes() / 60;
    const daily = 80 + 30 * Math.sin((hour - 7) * Math.PI / 12);
    spot[i] = daily;
    imb[i] = daily + (Math.random() - 0.5) * 20;
  }
  return { spot_kwartier: spot, imb_kwartier: imb, timestamps, _warning: warning };
}

function buildMarktArrayFromCache(spotRaw, imbRaw, vanISO, totISO) {
  // CACHE SHAPE: [{t: 1619215200000, v: 68.51}, ...]
  //   t = Unix milliseconds (UTC), 15-minute resolution
  //   v = prijs in €/MWh
  // Sim-periode: vanISO/totISO zijn YYYY-MM-DD (lokale dag, behandelen we als UTC)
  const start = new Date(vanISO + 'T00:00:00Z').getTime();
  const end = new Date(totISO + 'T23:45:00Z').getTime();
  const N = Math.floor((end - start) / (15 * 60 * 1000)) + 1;

  // Bouw lookup map: timestamp_ms → prijs
  // We rounden de cache-timestamps naar de dichtsbijzijnde 15-min grid om missende
  // datapunten op te vangen (sommige bronnen geven uurdata, sommige kwartierdata).
  function buildLookup(arr) {
    const map = new Map();
    for (const r of arr) {
      const t = r.t;
      const v = r.v;
      if (typeof t !== 'number' || typeof v !== 'number') continue;
      // Round naar dichtsbijzijnde 15-min boundary (kwartier-grid)
      const rounded = Math.round(t / (15 * 60 * 1000)) * (15 * 60 * 1000);
      map.set(rounded, v);
    }
    return map;
  }
  const spotMap = buildLookup(spotRaw);
  const imbMap = buildLookup(imbRaw);

  const spot = new Array(N), imb = new Array(N), timestamps = new Array(N);
  let missing = 0;
  // Track laatste bekende waarde voor forward-fill (bij uurdata: 4 kwartieren krijgen zelfde uur-prijs)
  let lastSpot = null, lastImb = null;

  for (let i = 0; i < N; i++) {
    const t = start + i * 15 * 60 * 1000;
    timestamps[i] = new Date(t).toISOString();

    // Probeer exacte 15-min match, anders zoek naar uur-grid (00,15,30,45 → 00)
    let sp = spotMap.get(t);
    let im = imbMap.get(t);

    // Fallback: probeer uur-grid (rond af naar uur)
    if (sp == null) {
      const hourT = Math.floor(t / (60 * 60 * 1000)) * (60 * 60 * 1000);
      sp = spotMap.get(hourT);
    }
    if (im == null) {
      const hourT = Math.floor(t / (60 * 60 * 1000)) * (60 * 60 * 1000);
      im = imbMap.get(hourT);
    }

    // Forward-fill bij gat
    if (sp != null) lastSpot = sp; else if (lastSpot != null) sp = lastSpot;
    if (im != null) lastImb = im; else if (lastImb != null) im = lastImb;

    if (sp == null || im == null) {
      missing++;
      sp = sp != null ? sp : 80;
      im = im != null ? im : (sp != null ? sp : 80);
    }
    spot[i] = sp;
    imb[i] = im;
  }
  const result = { spot_kwartier: spot, imb_kwartier: imb, timestamps };
  if (missing > N * 0.5) {
    result._warning = `Marktdata: ${missing}/${N} kwartieren ontbraken — fallback 80 €/MWh gebruikt`;
  }
  return result;
}

app.post('/api/nominatie-sim', async (req, res) => {
  let input = req.body;
  if (!input) return res.status(400).json({ error: 'body is verplicht' });

  // SHAPE-DETECTIE: high-level UI payload (profielNaam aanwezig) vs low-level (profiel_kwartier)
  let buildErrors = [];
  if (!Array.isArray(input.profiel_kwartier) && input.profielNaam) {
    try {
      const built = await buildSimulatorInput(input);
      input = built.input;
      buildErrors = built.errors || [];
    } catch (e) {
      return res.status(400).json({ error: 'Input-build fout: ' + e.message });
    }
  }

  const startTime = Date.now();
  const simulatorPath = path.join(__dirname, 'simulator.py');
  if (!fs.existsSync(simulatorPath)) {
    return res.status(500).json({ error: 'simulator.py niet gevonden op server' });
  }

  // Spawn Python process
  const proc = spawn('python3', [simulatorPath], {
    cwd: __dirname,
    env: { ...process.env, PYTHONUNBUFFERED: '1' },
  });

  let stdoutData = '';
  let stderrData = '';
  let timedOut = false;

  // Timeout: 90s
  const timeout = setTimeout(() => {
    timedOut = true;
    proc.kill('SIGKILL');
  }, 90000);

  proc.stdout.on('data', chunk => stdoutData += chunk.toString());
  proc.stderr.on('data', chunk => stderrData += chunk.toString());

  proc.on('close', (code) => {
    clearTimeout(timeout);
    const elapsed = Date.now() - startTime;

    if (timedOut) {
      return res.status(504).json({
        error: 'simulator.py time-out na 90s',
        log: stderrData.slice(-2000),
        build_errors: buildErrors,
      });
    }
    if (code !== 0) {
      return res.status(500).json({
        error: `simulator.py exit code ${code}`,
        log: stderrData.slice(-2000),
        build_errors: buildErrors,
      });
    }
    try {
      const out = JSON.parse(stdoutData);
      res.json({
        ok: true,
        elapsed_ms: elapsed,
        log: stderrData.slice(-3000),  // laatste 3KB van log
        build_errors: buildErrors,
        result: out,
      });
    } catch (e) {
      res.status(500).json({
        error: 'simulator.py output niet parseerbaar als JSON: ' + e.message,
        stdout_preview: stdoutData.slice(0, 500),
        log: stderrData.slice(-2000),
        build_errors: buildErrors,
      });
    }
  });

  proc.on('error', (err) => {
    clearTimeout(timeout);
    res.status(500).json({ error: 'spawn fout: ' + err.message });
  });

  // Schrijf JSON naar stdin
  try {
    proc.stdin.write(JSON.stringify(input));
    proc.stdin.end();
  } catch (e) {
    clearTimeout(timeout);
    proc.kill();
    res.status(500).json({ error: 'stdin write fout: ' + e.message });
  }
});

// ─── ROUTE: GET /api/profiel-aanvulling-genereren ─────────────────────────
// Stub voor toekomst: synthetisch profiel uit beschrijving genereren
app.post('/api/profiel-aanvulling-genereren', (req, res) => {
  res.status(501).json({
    error: 'Nog niet geïmplementeerd in v15',
    geplant_voor: 'v16',
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// SCENARIO STORAGE — via fluctus-scenarios GitHub repo
// ═══════════════════════════════════════════════════════════════════════════

const SCEN_OWNER = process.env.GITHUB_SCENARIOS_OWNER || 'JohanMMK';
const SCEN_REPO = process.env.GITHUB_SCENARIOS_REPO || 'fluctus-scenarios';

function safeProjectName(s) {
  return String(s || '').trim().replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 60);
}

// Lijst alle directories onder /projecten via GitHub-API
async function githubListDirs(token, owner, repo, parentPath) {
  const apiUrl = `https://api.github.com/repos/${owner}/${repo}/contents/${parentPath}?t=${Date.now()}`;
  const resp = await httpGet(apiUrl, {
    'Authorization': 'token ' + token,
    'User-Agent': 'Fluctus-Worker/15.0',
    'Accept': 'application/vnd.github+json',
  });
  if (resp.status === 404) return [];
  if (resp.status !== 200) {
    throw new Error(`GitHub list HTTP ${resp.status}: ${resp.body.slice(0, 200)}`);
  }
  const arr = JSON.parse(resp.body);
  return Array.isArray(arr) ? arr : [];
}

// ─── ROUTE: GET /api/projecten ────────────────────────────────────────────
app.get('/api/projecten', async (req, res) => {
  const token = process.env.GITHUB_TOKEN;
  if (!token) return res.status(500).json({ error: 'GITHUB_TOKEN niet ingesteld' });
  try {
    const items = await githubListDirs(token, SCEN_OWNER, SCEN_REPO, 'projecten');
    const projecten = items
      .filter(i => i.type === 'dir')
      .map(i => ({ naam: i.name, path: i.path }));
    res.json({ projecten });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── ROUTE: GET /api/scenarios?project=X ─────────────────────────────────
app.get('/api/scenarios', async (req, res) => {
  const token = process.env.GITHUB_TOKEN;
  if (!token) return res.status(500).json({ error: 'GITHUB_TOKEN niet ingesteld' });
  const project = safeProjectName(req.query.project);
  if (!project) return res.status(400).json({ error: 'project is verplicht' });
  try {
    const items = await githubListDirs(token, SCEN_OWNER, SCEN_REPO, `projecten/${project}`);
    const scenarios = items
      .filter(i => i.type === 'file' && i.name.endsWith('.json') && i.name !== '.gitkeep')
      .map(i => ({
        naam: i.name.replace(/\.json$/, ''),
        size: i.size,
        sha: i.sha,
      }));
    res.json({ project, scenarios });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── ROUTE: GET /api/scenario?project=X&scenario=Y ───────────────────────
app.get('/api/scenario', async (req, res) => {
  const token = process.env.GITHUB_TOKEN;
  if (!token) return res.status(500).json({ error: 'GITHUB_TOKEN niet ingesteld' });
  const project = safeProjectName(req.query.project);
  const scenario = safeProjectName(req.query.scenario);
  if (!project || !scenario) {
    return res.status(400).json({ error: 'project en scenario zijn verplicht' });
  }
  try {
    const filePath = `projecten/${project}/${scenario}.json`;
    const result = await githubReadJson(token, SCEN_OWNER, SCEN_REPO, filePath);
    if (!result) return res.status(404).json({ error: `Scenario niet gevonden: ${filePath}` });
    if (!result.json) {
      return res.status(500).json({ error: 'Scenario JSON corrupt: ' + result.parseError });
    }
    res.json({ project, scenario, data: result.json });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── ROUTE: POST /api/scenario-bewaren ───────────────────────────────────
// Body: {project: 'X', scenario: 'Y', data: {...}}
app.post('/api/scenario-bewaren', async (req, res) => {
  const token = process.env.GITHUB_TOKEN;
  if (!token) return res.status(500).json({ error: 'GITHUB_TOKEN niet ingesteld' });
  const project = safeProjectName(req.body.project);
  const scenario = safeProjectName(req.body.scenario);
  const data = req.body.data;
  if (!project || !scenario || !data) {
    return res.status(400).json({ error: 'project, scenario en data zijn verplicht' });
  }
  try {
    const filePath = `projecten/${project}/${scenario}.json`;
    const result = await githubWriteJson(token, SCEN_OWNER, SCEN_REPO, filePath, data,
      `save: ${project}/${scenario}`);
    res.json({ ok: true, project, scenario, action: result.action });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// CORS preflight voor nieuwe endpoints
['/api/profielen-lijst', '/api/profiel', '/api/postcode-grd', '/api/regio-tarieven',
 '/api/leveringscontract-staffel', '/api/batterijen', '/api/batterij-toevoegen',
 '/api/nominatie-sim', '/api/profiel-aanvulling-genereren',
 '/api/projecten', '/api/scenarios', '/api/scenario', '/api/scenario-bewaren']
  .forEach(route => {
    app.options(route, (req, res) => {
      res.header('Access-Control-Allow-Origin', '*');
      res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
      res.header('Access-Control-Allow-Headers', 'Content-Type');
      res.sendStatus(200);
    });
  });


app.listen(PORT, () => console.log('Fluctus Worker v15.2 (smart sim + cache fix + tarief fix) draait op poort ' + PORT));
