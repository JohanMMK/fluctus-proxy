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
  res.header('Cache-Control', 'public, max-age=900');
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
async function githubReadJson(token, owner, repo, path) {
  const apiUrl = `https://api.github.com/repos/${owner}/${repo}/contents/${path}?t=${Date.now()}`;
  const authHeaders = {
    'Authorization': 'token ' + token,
    'User-Agent': 'Fluctus-Worker/2.1',
    'Accept': 'application/json',
  };
  const resp = await httpGet(apiUrl, authHeaders);
  if (resp.status === 404) return null;
  if (resp.status !== 200) throw new Error(`GitHub read HTTP ${resp.status}: ${resp.body.slice(0, 200)}`);
  const data = JSON.parse(resp.body);
  // content is base64-encoded
  const decoded = Buffer.from(data.content, 'base64').toString('utf8');
  try {
    return { json: JSON.parse(decoded), sha: data.sha };
  } catch (e) {
    throw new Error(`GitHub file ${path} niet geldig JSON: ${e.message}`);
  }
}

// Schrijf een JSON-bestand naar de GitHub repo. Retourneert {ok, action}.
async function githubWriteJson(token, owner, repo, path, payload, message) {
  const apiUrl = `https://api.github.com/repos/${owner}/${repo}/contents/${path}`;

  // Haal SHA op (nodig om overschrijven toe te laten)
  let sha = null;
  const existing = await githubReadJson(token, owner, repo, path).catch(() => null);
  if (existing && existing.sha) sha = existing.sha;

  const putBody = {
    message: message || ('auto: ' + path + ' ' + new Date().toISOString().slice(0, 10)),
    content: Buffer.from(JSON.stringify(payload)).toString('base64'),
    committer: { name: 'Fluctus Bot', email: 'bot@fluctus.net' },
  };
  if (sha) putBody.sha = sha;

  const putResp = await httpPut(apiUrl, token, putBody);
  if (putResp.status !== 200 && putResp.status !== 201) {
    throw new Error(`GitHub write HTTP ${putResp.status}: ${putResp.body.slice(0, 200)}`);
  }
  return { ok: true, action: sha ? 'updated' : 'created' };
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
// ROUTE: GET /cache-read
//
// Leest fluctus-cache.json rechtstreeks via de GitHub API (niet via
// raw.githubusercontent.com, dat door Fastly CDN ~5 min gecached wordt).
// Gebruikt door de snippet om verse cache-data op te halen na een save.
// ═══════════════════════════════════════════════════════════════════════════
app.get('/cache-read', async (req, res) => {
  const token = process.env.GITHUB_TOKEN;
  const owner = process.env.GITHUB_OWNER;
  const repo  = process.env.GITHUB_REPO;
  const path  = process.env.GITHUB_PATH || 'data/fluctus-cache.json';

  if (!token || !owner || !repo) {
    return res.status(500).json({
      error: 'GitHub omgevingsvariabelen niet ingesteld.'
    });
  }

  try {
    let result;
    try {
      result = await githubReadJson(token, owner, repo, path);
    } catch (parseErr) {
      // Corrupt JSON of andere parse-fout: behandel als "leeg" in plaats van 500.
      // Zo kan de snippet gewoon opnieuw opbouwen i.p.v. te crashen.
      console.error('cache-read: corrupt JSON bestand op GitHub — terug als leeg:', parseErr.message);
      res.set('Cache-Control', 'no-store, no-cache, must-revalidate');
      res.set('Pragma', 'no-cache');
      return res.json({
        spot: [], imb: [], wind: [], solar: [],
        lastDate: null,
        cacheVersion: 'entsoe-v1',
        _corrupted: true,
        _error: parseErr.message
      });
    }
    if (!result) {
      return res.status(404).json({ error: 'Cache bestand niet gevonden' });
    }
    // No-cache headers zodat browsers deze response ook niet cachen
    res.set('Cache-Control', 'no-store, no-cache, must-revalidate');
    res.set('Pragma', 'no-cache');
    res.json(result.json);
  } catch (err) {
    console.error('cache-read fout:', err.message);
    res.status(500).json({ error: err.message });
  }
});


app.post('/cache-update', async (req, res) => {
  const token = process.env.GITHUB_TOKEN;
  const owner = process.env.GITHUB_OWNER;
  const repo  = process.env.GITHUB_REPO;
  const path  = process.env.GITHUB_PATH || 'data/fluctus-cache.json';

  if (!token || !owner || !repo) {
    return res.status(500).json({
      error: 'GitHub omgevingsvariabelen niet ingesteld. '
           + 'Controleer GITHUB_TOKEN, GITHUB_OWNER en GITHUB_REPO in Railway Variables.'
    });
  }

  try {
    const payload    = req.body;
    const payloadStr = JSON.stringify(payload);
    const apiUrl     = `https://api.github.com/repos/${owner}/${repo}/contents/${path}`;
    const authHeaders = {
      'Authorization': 'token ' + token,
      'User-Agent': 'Fluctus-Worker/2.0',
      'Accept': 'application/json',
    };

    // Stap 1: haal huidige SHA op (nodig om bestaand bestand te overschrijven).
    // Cache-bust via unieke query parameter om GitHub's CDN te omzeilen.
    let sha = null;
    try {
      const shaResp = await httpGet(apiUrl + '?t=' + Date.now(), authHeaders);
      if (shaResp.status === 200) {
        const shaData = JSON.parse(shaResp.body);
        sha = shaData.sha;
      }
      // status 404 = bestand bestaat nog niet, sha blijft null (dat is ok)
    } catch (_) {
      // Netwerk error bij SHA ophalen — doorgaan met sha=null, PUT zal falen als bestand bestaat
    }

    // Stap 2: schrijf het bestand naar GitHub
    const putBody = {
      message:   'auto: marktdata ' + new Date().toISOString().slice(0, 10),
      content:   Buffer.from(payloadStr).toString('base64'),
      committer: { name: 'Fluctus Bot', email: 'bot@fluctus.net' },
    };
    if (sha) putBody.sha = sha;

    const putResp = await httpPut(apiUrl, token, putBody);

    if (putResp.status !== 200 && putResp.status !== 201) {
      return res.status(putResp.status).json({
        error: `GitHub write mislukt (HTTP ${putResp.status}): ${putResp.body.slice(0, 200)}`
      });
    }

    const sizeKb = Math.round(payloadStr.length / 1024);
    res.json({ ok: true, size_kb: sizeKb, action: sha ? 'updated' : 'created' });

  } catch (err) {
    console.error('cache-update fout:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════
// ROUTE: GET /   (status pagina)
// ═══════════════════════════════════════════════════════════════════════════
app.get('/', (req, res) => res.json({
  status: 'ok',
  versie: '3.2',
  model: 'claude-opus-4-7',
  tools: ['web_search_20250305'],
  routes: [
    '/elia-data?from=YYYY-MM-DD&to=YYYY-MM-DD   (onbalans uit Elia; spot = [])',
    '/elia-renewable?dataset=ods031|ods032&...  (wind/zon uit Elia)',
    '/entsoe-dayahead?from=YYYY-MM-DD&to=...    (BELPEX day-ahead uit ENTSO-E)',
    'GET  /cache-read                           (lees cache via GitHub API, no-CDN)',
    'POST /cache-update                         (schrijf marktdata cache naar GitHub)',
    'GET  /explanation?chartId=c1               (lees gecachede uitleg)',
    'POST /claude-explain-refresh               (genereer + cache uitleg — 1x per dag)',
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

  // Filter: het finale antwoord bestaat vaak uit meerdere aaneensluitende
  // tekstblokken (Claude kan z'n antwoord in stukken genereren). Korte blokken
  // tussen tool-calls ("Ik ga zoeken...", "Nu controleer ik...") moeten eruit,
  // maar we willen het volledige finale antwoord behouden.
  //
  // Strategie: vind de eerste INDEX waarna alle blokken substantieel zijn,
  // en concateneer die tot het einde. "Substantieel" = >80 chars OF het
  // laatste blok in de reeks.
  let finalText = '';
  if (allTexts.length === 0) {
    finalText = '';
  } else if (allTexts.length === 1) {
    finalText = allTexts[0];
  } else {
    // Loop van achter naar voor, en blijf blokken meenemen zolang ze substantieel zijn
    const finalBlocks = [];
    for (let i = allTexts.length - 1; i >= 0; i--) {
      const block = allTexts[i].trim();
      if (block.length > 80 || finalBlocks.length === 0) {
        finalBlocks.unshift(block);
      } else {
        // Eerste kort blok dat geen deel is van finale antwoord → stop
        break;
      }
    }
    finalText = finalBlocks.join('\n\n');

    // Fallback: als niets gevonden, neem gewoon het laatste blok
    if (!finalText) finalText = allTexts[allTexts.length - 1];
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



app.listen(PORT, () => console.log('Fluctus Worker v3.2 (ENTSO-E spot + Elia onbalans + cache-read via GitHub API) draait op poort ' + PORT));
