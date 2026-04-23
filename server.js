const express = require('express');
const https = require('https');
const app = express();
const PORT = process.env.PORT || 3000;
const ELIA_BASE = 'https://opendata.elia.be/api/explore/v2.1/catalog/datasets';

app.use(express.json({ limit: '25mb' }));

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
  versie: '4.1',
  model: 'claude-opus-4-7',
  tools: ['web_search_20250305'],
  cache: 'multi-bestand (5 datasets) — fix voor files >1 MB',
  routes: [
    '/elia-data?from=YYYY-MM-DD&to=YYYY-MM-DD   (onbalans uit Elia)',
    '/elia-renewable?dataset=ods031|ods032&...  (wind/zon uit Elia)',
    '/entsoe-dayahead?from=...&to=...           (BELPEX spot uit ENTSO-E)',
    'GET  /cache-read?dataset=meta|spot|imb|wind|solar',
    'POST /cache-update?dataset=...             (body = array, behalve meta=object)',
    'POST /cache-init                           (reset alle 5 cache-bestanden)',
    'GET  /explanation?chartId=c1',
    'POST /claude-explain-refresh',
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



app.listen(PORT, () => console.log('Fluctus Worker v4.1 (multi-file cache, raw media type voor >1 MB) draait op poort ' + PORT));
