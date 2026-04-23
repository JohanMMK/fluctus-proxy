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
// ═══════════════════════════════════════════════════════════════════════════
app.get('/elia-data', async (req, res) => {
  const { from, to } = req.query;

  if (!from || !to) {
    return res.status(400).json({ error: 'from en to zijn verplicht (YYYY-MM-DD)' });
  }

  const fromDate = new Date(from + 'T00:00:00Z');
  const toDate   = new Date(to   + 'T23:59:59Z');
  const cutoff   = new Date('2024-05-22T00:00:00Z');

  const HIST   = { id: 'ods047', spotfield: 'marginalincrementalprice', imbfield: 'positiveimbalanceprice' };
  const RECENT = { id: 'ods134', spotfield: 'marginalincrementalprice', imbfield: 'imbalanceprice' };

  try {
    const allSpot = [], allImb = [];
    const warnings = [];

    // Spot + onbalans
    if (fromDate < cutoff) {
      try {
        const histEnd = toDate < cutoff ? toDate : cutoff;
        const d = await eliaFetch(HIST.id, HIST.spotfield, HIST.imbfield, fromDate, histEnd);
        allSpot.push(...d.spot);
        allImb.push(...d.imb);
      } catch (e) { warnings.push('ods047: ' + e.message); }
    }
    if (toDate >= cutoff) {
      try {
        const recentStart = fromDate >= cutoff ? fromDate : cutoff;
        const d = await eliaFetch(RECENT.id, RECENT.spotfield, RECENT.imbfield, recentStart, toDate);
        allSpot.push(...d.spot);
        allImb.push(...d.imb);
      } catch (e) { warnings.push('ods134: ' + e.message); }
    }

    allSpot.sort((a, b) => a.t.localeCompare(b.t));
    allImb.sort((a, b) => a.t.localeCompare(b.t));

    const out = {
      spot:  allSpot,
      imb:   allImb,
      wind:  [],
      solar: [],
      source: 'Elia Open Data',
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
// ROUTE: GET /imbalance?days=N   (bestaande route — behouden voor compatibiliteit)
// ═══════════════════════════════════════════════════════════════════════════
app.get('/imbalance', async (req, res) => {
  const days = Math.min(parseInt(req.query.days || '7'), 366);
  const now = new Date(); now.setUTCMinutes(0, 0, 0);
  const start = new Date(now.getTime() - days * 86400000);
  const cutoff = new Date('2024-05-22T00:00:00Z');

  async function fetchOdsMonth(ds, imbfield, s, e) {
    const startStr = fmtIso(s);
    const endStr = fmtIso(e);
    const allResults = [];
    let offset = 0;
    const limit = 100;
    while (true) {
      const json = await eliaPage(ds, 'datetime', `datetime,${imbfield}`, startStr, endStr, offset, limit);
      const records = (json.results || [])
        .filter(x => x[imbfield] != null)
        .map(x => ({ t: x.datetime.substring(0, 19) + 'Z', v: Math.round(parseFloat(x[imbfield]) * 100) / 100 }));
      allResults.push(...records);
      if (records.length < limit) break;
      offset += limit;
      await new Promise(r => setTimeout(r, 100));
    }
    return allResults;
  }

  async function fetchOdsParallel(ds, imbfield, s, e) {
    const segments = splitIntoMonths(s, e);
    const results = [];
    for (let i = 0; i < segments.length; i += 4) {
      const batch = segments.slice(i, i + 4);
      const batchResults = await Promise.all(batch.map(seg => fetchOdsMonth(ds, imbfield, seg.s, seg.e)));
      batchResults.forEach(r => results.push(...r));
    }
    return results;
  }

  const results = [], warnings = [];

  if (start < cutoff) {
    try {
      const segEnd = now < cutoff ? now : cutoff;
      const d = await fetchOdsParallel('ods047', 'positiveimbalanceprice', start, segEnd);
      results.push(...d);
    } catch (e) { warnings.push('ods047: ' + e.message); }
  }
  if (now >= cutoff) {
    try {
      const segStart = start >= cutoff ? start : cutoff;
      const d = await fetchOdsParallel('ods134', 'imbalanceprice', segStart, now);
      results.push(...d);
    } catch (e) { warnings.push('ods134: ' + e.message); }
  }

  results.sort((a, b) => a.t.localeCompare(b.t));
  const out = { data: results, count: results.length, source: 'Elia Open Data' };
  if (warnings.length) out.warnings = warnings;
  res.json(out);
});

// ═══════════════════════════════════════════════════════════════════════════
// ROUTE: GET /spot?days=N
// ═══════════════════════════════════════════════════════════════════════════
app.get('/spot', async (req, res) => {
  const days = Math.min(parseInt(req.query.days || '7'), 1825);
  const now = new Date(); now.setUTCMinutes(0, 0, 0);
  const start = new Date(now.getTime() - days * 86400000);
  const cutoff = new Date('2024-05-22T00:00:00Z');

  async function fetchSpotMonth(ds, spotfield, s, e) {
    const startStr = fmtIso(s);
    const endStr = fmtIso(e);
    const allResults = [];
    let offset = 0;
    const limit = 100;
    while (true) {
      const json = await eliaPage(ds, 'datetime', `datetime,${spotfield}`, startStr, endStr, offset, limit);
      const records = (json.results || [])
        .filter(x => x[spotfield] != null)
        .map(x => ({ t: x.datetime.substring(0, 19) + 'Z', v: Math.round(parseFloat(x[spotfield]) * 100) / 100 }));
      allResults.push(...records);
      if (records.length < limit) break;
      offset += limit;
      await new Promise(r => setTimeout(r, 100));
    }
    return allResults;
  }

  async function fetchSpotParallel(ds, spotfield, s, e) {
    const segments = splitIntoMonths(s, e);
    const results = [];
    for (let i = 0; i < segments.length; i += 4) {
      const batch = segments.slice(i, i + 4);
      const batchResults = await Promise.all(batch.map(seg => fetchSpotMonth(ds, spotfield, seg.s, seg.e)));
      batchResults.forEach(r => results.push(...r));
    }
    return results;
  }

  const results = [], warnings = [];

  if (start < cutoff) {
    try {
      const segEnd = now < cutoff ? now : cutoff;
      const d = await fetchSpotParallel('ods047', 'marginalincrementalprice', start, segEnd);
      results.push(...d);
    } catch (e) { warnings.push('ods047: ' + e.message); }
  }
  if (now >= cutoff) {
    try {
      const segStart = start >= cutoff ? start : cutoff;
      const d = await fetchSpotParallel('ods134', 'marginalincrementalprice', segStart, now);
      results.push(...d);
    } catch (e) { warnings.push('ods134: ' + e.message); }
  }

  results.sort((a, b) => a.t.localeCompare(b.t));
  const out = { data: results, count: results.length, source: 'Elia Open Data' };
  if (warnings.length) out.warnings = warnings;
  res.json(out);
});

// ═══════════════════════════════════════════════════════════════════════════
// ROUTE: POST /cache-update
// Schrijft de volledige cache als JSON naar GitHub.
// ═══════════════════════════════════════════════════════════════════════════
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
  versie: '2.3',
  model: 'claude-opus-4-7',
  tools: ['web_search_20250305'],
  routes: [
    '/elia-data?from=YYYY-MM-DD&to=YYYY-MM-DD  (spot + imbalans tegelijk)',
    '/elia-renewable?dataset=ods031|ods032&from=...&to=...  (wind/zon)',
    '/spot?days=N                               (spot proxy via Elia)',
    '/imbalance?days=N                          (onbalans via Elia)',
    'POST /cache-update                         (schrijf marktdata cache naar GitHub)',
    'GET  /explanation?chartId=c1               (lees gecachede uitleg)',
    'POST /claude-explain-refresh               (genereer + cache uitleg — 1x per dag)',
    'POST /share-create                         (PNG + OG-pagina naar GitHub Pages)',
    'POST /claude-explain                       (legacy: AI uitleg zonder cache)',
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
// ROUTE: POST /share-create
// Body: { imageBase64: '...', title: '...', description: '...' }
//
// Schrijft de grafiek-PNG naar data/shares/<hash>.png en maakt een HTML-pagina
// met OG-tags op data/shares/<hash>.html. LinkedIn fetcht de HTML, leest de
// OG-tags en toont de PNG als preview.
//
// Retourneert { shareUrl, imageUrl, hash } — shareUrl is wat in de LinkedIn
// share-link gaat, imageUrl is direct naar de PNG.
// ═══════════════════════════════════════════════════════════════════════════
function makeShareHash(imageBase64) {
  // Kleine stabiele hash uit timestamp + eerste chunk van de PNG.
  // Geen crypto nodig — uniek genoeg voor share-URLs.
  const base = Date.now().toString(36) + (imageBase64 || '').substring(0, 64);
  let h = 0;
  for (let i = 0; i < base.length; i++) h = ((h << 5) - h + base.charCodeAt(i)) | 0;
  return Math.abs(h).toString(36) + '-' + Date.now().toString(36);
}

function escapeHtml(s) {
  return String(s || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function buildSharePage(title, description, imageUrl, pagesUrl) {
  const safeTitle = escapeHtml(title);
  const safeDesc = escapeHtml(description).substring(0, 300);
  const safeImg = escapeHtml(imageUrl);
  const safePage = escapeHtml(pagesUrl);
  return `<!DOCTYPE html>
<html lang="nl">
<head>
<meta charset="UTF-8">
<title>${safeTitle}</title>
<meta name="description" content="${safeDesc}">
<meta property="og:type" content="article">
<meta property="og:title" content="${safeTitle}">
<meta property="og:description" content="${safeDesc}">
<meta property="og:image" content="${safeImg}">
<meta property="og:image:width" content="1200">
<meta property="og:image:height" content="630">
<meta property="og:url" content="${safePage}">
<meta property="og:site_name" content="Fluctus">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="${safeTitle}">
<meta name="twitter:description" content="${safeDesc}">
<meta name="twitter:image" content="${safeImg}">
<meta http-equiv="refresh" content="0; url=https://fluctus.net/energie">
<style>
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;max-width:900px;margin:40px auto;padding:20px;color:#1F3864;background:#fff;}
h1{color:#1F3864;border-bottom:3px solid #05B050;padding-bottom:8px;}
.meta{color:#666;font-size:13px;margin-bottom:20px;}
img{max-width:100%;height:auto;border:1px solid #ddd;border-radius:4px;}
.desc{margin:20px 0;line-height:1.6;white-space:pre-wrap;}
.cta{display:inline-block;margin-top:16px;background:#05B050;color:#fff;padding:10px 20px;text-decoration:none;border-radius:4px;font-weight:600;}
.cta:hover{background:#048a40;}
footer{margin-top:30px;padding-top:16px;border-top:1px solid #eee;color:#888;font-size:12px;}
</style>
</head>
<body>
<h1>${safeTitle}</h1>
<div class="meta">Fluctus · ${new Date().toLocaleDateString('nl-BE')}</div>
<img src="${safeImg}" alt="${safeTitle}">
<div class="desc">${safeDesc}</div>
<a class="cta" href="https://fluctus.net/energie">Bekijk het live dashboard op fluctus.net</a>
<footer>
Gedeeld vanuit het Fluctus energiemarkt dashboard · Verdien anders · Onze kennis, uw macht<br>
Data bron: Elia Open Data Licence · Elia Transmission Belgium SA
</footer>
</body>
</html>`;
}

async function githubWriteBinary(token, owner, repo, path, base64Content, message) {
  const apiUrl = `https://api.github.com/repos/${owner}/${repo}/contents/${path}`;

  // Haal SHA op als bestand bestaat
  let sha = null;
  try {
    const authHeaders = {
      'Authorization': 'token ' + token,
      'User-Agent': 'Fluctus-Worker/2.2',
      'Accept': 'application/json',
    };
    const existing = await httpGet(apiUrl + '?t=' + Date.now(), authHeaders);
    if (existing.status === 200) {
      const d = JSON.parse(existing.body);
      sha = d.sha;
    }
  } catch (_) {}

  const putBody = {
    message: message || ('auto: share ' + new Date().toISOString().slice(0, 10)),
    content: base64Content,
    committer: { name: 'Fluctus Bot', email: 'bot@fluctus.net' },
  };
  if (sha) putBody.sha = sha;

  const putResp = await httpPut(apiUrl, token, putBody);
  if (putResp.status !== 200 && putResp.status !== 201) {
    throw new Error(`GitHub write HTTP ${putResp.status}: ${putResp.body.slice(0, 200)}`);
  }
  return { ok: true, action: sha ? 'updated' : 'created' };
}

app.post('/share-create', async (req, res) => {
  const token = process.env.GITHUB_TOKEN;
  const owner = process.env.GITHUB_OWNER;
  const repo  = process.env.GITHUB_REPO;

  if (!token || !owner || !repo) {
    return res.status(500).json({ error: 'GitHub omgevingsvariabelen niet ingesteld' });
  }

  const { imageBase64, title, description } = req.body || {};
  if (!imageBase64 || typeof imageBase64 !== 'string') {
    return res.status(400).json({ error: 'imageBase64 is verplicht' });
  }
  if (imageBase64.length > 8 * 1024 * 1024) {
    return res.status(400).json({ error: 'Afbeelding te groot (max 8MB base64)' });
  }
  if (!title) {
    return res.status(400).json({ error: 'title is verplicht' });
  }

  try {
    const hash = makeShareHash(imageBase64);
    const imgPath = `data/shares/${hash}.png`;
    const htmlPath = `data/shares/${hash}.html`;

    // GitHub Pages URL — lowercase repo naam
    const pagesBase = `https://${owner.toLowerCase()}.github.io/${repo}`;
    const imageUrl = `${pagesBase}/${imgPath}`;
    const pageUrl = `${pagesBase}/${htmlPath}`;

    // Stap 1: upload PNG
    // Verwijder "data:image/png;base64," prefix als die aanwezig is
    let pngData = imageBase64;
    const comma = pngData.indexOf(',');
    if (pngData.startsWith('data:') && comma > 0) {
      pngData = pngData.substring(comma + 1);
    }
    await githubWriteBinary(token, owner, repo, imgPath, pngData, `auto: share PNG ${hash}`);

    // Stap 2: upload HTML met OG-tags
    const html = buildSharePage(title, description || '', imageUrl, pageUrl);
    const htmlBase64 = Buffer.from(html, 'utf8').toString('base64');
    await githubWriteBinary(token, owner, repo, htmlPath, htmlBase64, `auto: share page ${hash}`);

    res.json({
      ok: true,
      hash,
      shareUrl: pageUrl,
      imageUrl
    });

  } catch (err) {
    console.error('share-create fout:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.options('/share-create', (req, res) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  res.sendStatus(200);
});

// ═══════════════════════════════════════════════════════════════════════════
// ROUTE: POST /claude-explain
//
// (legacy — blijft beschikbaar maar cache NIET via GitHub; elke klik = API call)
// Nieuwe frontend gebruikt /explanation + /claude-explain-refresh.
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

  // Filter: neem alleen het laatste substantiële blok (=> finale antwoord).
  // Een blok telt als substantieel als het >150 tekens is OF als het ENIGE blok is.
  let finalText = '';
  if (allTexts.length === 0) {
    finalText = '';
  } else if (allTexts.length === 1) {
    finalText = allTexts[0];
  } else {
    // Zoek vanachter naar voor naar eerste substantiële blok
    for (let i = allTexts.length - 1; i >= 0; i--) {
      if (allTexts[i].trim().length > 150) {
        finalText = allTexts[i];
        break;
      }
    }
    // Fallback: als niets >150 chars, neem het laatste
    if (!finalText) finalText = allTexts[allTexts.length - 1];
  }

  return {
    text: finalText.trim(),
    citations
  };
}

async function callClaudeMessages(apiKey, requestBody) {
  const headers = {
    'x-api-key': apiKey,
    'anthropic-version': '2023-06-01',
    'User-Agent': 'Fluctus-Dashboard/2.1',
  };
  const r = await httpPostJson('https://api.anthropic.com/v1/messages', headers, requestBody);
  if (r.status !== 200) {
    throw new Error(`Anthropic HTTP ${r.status}: ${r.body.slice(0, 300)}`);
  }
  return JSON.parse(r.body);
}

app.post('/claude-explain', async (req, res) => {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) {
    return res.status(500).json({ error: 'ANTHROPIC_API_KEY niet ingesteld op de server' });
  }

  const { prompt } = req.body;
  if (!prompt) {
    return res.status(400).json({ error: 'prompt is verplicht' });
  }

  const MODEL = 'claude-opus-4-7';
  const MAX_CONTINUATIONS = 3; // pause_turn loop veiligheid

  const baseRequest = {
    model: MODEL,
    max_tokens: 1500,
    tools: [{
      type: 'web_search_20250305',
      name: 'web_search',
      max_uses: 4,
      user_location: {
        type: 'approximate',
        country: 'BE',
        timezone: 'Europe/Brussels'
      }
    }],
    messages: [{ role: 'user', content: prompt }]
  };

  try {
    let response = await callClaudeMessages(apiKey, baseRequest);
    let conversation = [
      { role: 'user', content: prompt },
      { role: 'assistant', content: response.content }
    ];

    // pause_turn = Claude wil meer searches doen dan in één turn passen
    let continuations = 0;
    while (response.stop_reason === 'pause_turn' && continuations < MAX_CONTINUATIONS) {
      continuations++;
      const continueReq = Object.assign({}, baseRequest, { messages: conversation });
      response = await callClaudeMessages(apiKey, continueReq);
      conversation.push({ role: 'assistant', content: response.content });
    }

    const { text, citations } = extractTextAndCitations(response.content);

    res.json({
      text: text || 'Geen antwoord.',
      citations,
      stop_reason: response.stop_reason,
      model: response.model || MODEL
    });

  } catch (err) {
    console.error('claude-explain fout:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// OPTIONS preflight voor claude-explain
app.options('/claude-explain', (req, res) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  res.sendStatus(200);
});

app.listen(PORT, () => console.log('Fluctus Worker v2.3 (Opus 4.7 + web search + explanation cache + share pages) draait op poort ' + PORT));
