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
  versie: '2.1',
  model: 'claude-opus-4-7',
  tools: ['web_search_20250305'],
  routes: [
    '/elia-data?from=YYYY-MM-DD&to=YYYY-MM-DD  (spot + imbalans tegelijk)',
    '/elia-renewable?dataset=ods031|ods032&from=...&to=...  (wind/zon)',
    '/spot?days=N                               (spot proxy via Elia)',
    '/imbalance?days=N                          (onbalans via Elia)',
    'POST /cache-update                         (schrijf cache naar GitHub)',
    'POST /claude-explain                       (AI uitleg met web search)',
  ]
}));

// ═══════════════════════════════════════════════════════════════════════════
// ROUTE: POST /claude-explain
//
// Roept Anthropic API aan met web search tool. Bouwt de citaten om tot
// leesbare bronvermelding en behandelt pause_turn als Claude meer zoek-
// opdrachten nodig heeft dan in één ronde passen.
// ═══════════════════════════════════════════════════════════════════════════

// Haal alle tekst + citaties uit de content blocks van een Claude response
function extractTextAndCitations(content) {
  const texts = [];
  const citations = []; // array van {url, title}
  const seenUrls = {};

  (content || []).forEach(block => {
    if (block.type === 'text' && typeof block.text === 'string') {
      texts.push(block.text);
      // Citaties binnen dit text block
      (block.citations || []).forEach(c => {
        // web_search_result_location: heeft url en title
        const url = c.url;
        const title = c.title || c.cited_text || url;
        if (url && !seenUrls[url]) {
          seenUrls[url] = true;
          citations.push({ url, title });
        }
      });
    }
    // web_search_tool_result blocks tellen we niet als tekst, maar kunnen ook sources bevatten
    if (block.type === 'web_search_tool_result' && Array.isArray(block.content)) {
      block.content.forEach(r => {
        if (r && r.type === 'web_search_result' && r.url && !seenUrls[r.url]) {
          // Deze worden pas citaat als Claude ernaar verwijst in tekst blocks,
          // dus hier niet toevoegen — we laten extractie via text.citations lopen.
        }
      });
    }
  });

  return {
    text: texts.join('\n\n').trim(),
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

app.listen(PORT, () => console.log('Fluctus Worker v2.1 (Opus 4.7 + web search) draait op poort ' + PORT));
