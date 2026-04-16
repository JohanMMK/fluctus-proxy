const express = require('express');
const https = require('https');
const app = express();
const PORT = process.env.PORT || 3000;

const ENTSOE_TOKEN = process.env.ENTSOE_TOKEN || '0d0d352a-8548-49f0-9c8e-498d6e198e3b';

app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  res.header('Cache-Control', 'public, max-age=900');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

function httpGet(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'Accept': 'application/json, text/xml', 'User-Agent': 'Fluctus-Dashboard/1.0' } }, (resp) => {
      let data = '';
      resp.on('data', chunk => data += chunk);
      resp.on('end', () => resolve({ status: resp.statusCode, body: data }));
    }).on('error', reject);
  });
}

function fmtEntsoe(d) {
  return d.getUTCFullYear()
    + String(d.getUTCMonth() + 1).padStart(2, '0')
    + String(d.getUTCDate()).padStart(2, '0')
    + String(d.getUTCHours()).padStart(2, '0')
    + String(d.getUTCMinutes()).padStart(2, '0');
}

function fmtIso(d) {
  return d.getUTCFullYear()
    + '-' + String(d.getUTCMonth() + 1).padStart(2, '0')
    + '-' + String(d.getUTCDate()).padStart(2, '0')
    + 'T' + String(d.getUTCHours()).padStart(2, '0')
    + ':' + String(d.getUTCMinutes()).padStart(2, '0')
    + ':00';
}

function parseXml(xml) {
  const results = [];
  const pReg = /<Period>([\s\S]*?)<\/Period>/g;
  let pm;
  while ((pm = pReg.exec(xml)) !== null) {
    const p = pm[1];
    const sm = p.match(/<start>(.*?)<\/start>/);
    const rm = p.match(/<resolution>(.*?)<\/resolution>/);
    if (!sm) continue;
    const t0 = new Date(sm[1]);
    const step = rm && rm[1] === 'PT15M' ? 15 : 60;
    const ptReg = /<Point>[\s\S]*?<position>(\d+)<\/position>[\s\S]*?<price\.amount>([\d.]+)<\/price\.amount>[\s\S]*?<\/Point>/g;
    let ptm;
    while ((ptm = ptReg.exec(p)) !== null) {
      const pos = parseInt(ptm[1]);
      const price = Math.round(parseFloat(ptm[2]) * 100) / 100;
      const t = new Date(t0.getTime() + (pos - 1) * step * 60000);
      if (step === 60) {
        for (let q = 0; q < 4; q++) {
          results.push({ t: new Date(t.getTime() + q * 900000).toISOString().substring(0, 19) + 'Z', v: price });
        }
      } else {
        results.push({ t: t.toISOString().substring(0, 19) + 'Z', v: price });
      }
    }
  }
  return results.sort((a, b) => a.t.localeCompare(b.t));
}

// Splits tijdsperiode in maandelijkse segmenten
function splitIntoMonths(start, end) {
  const segments = [];
  let cur = new Date(start);
  while (cur < end) {
    const segStart = new Date(cur);
    const segEnd = new Date(cur.getUTCFullYear(), cur.getUTCMonth() + 1, 1);
    segments.push({ s: segStart, e: segEnd > end ? end : segEnd });
    cur = new Date(cur.getUTCFullYear(), cur.getUTCMonth() + 1, 1);
  }
  return segments;
}

app.get('/spot', async (req, res) => {
  const days = Math.min(parseInt(req.query.days || '7'), 365);
  const now = new Date(); now.setUTCMinutes(0, 0, 0);
  const start = new Date(now.getTime() - days * 86400000);
  const url = 'https://web-api.tp.entsoe.eu/api'
    + '?securityToken=' + ENTSOE_TOKEN
    + '&documentType=A44'
    + '&in_Domain=10YBE----------2'
    + '&out_Domain=10YBE----------2'
    + '&periodStart=' + fmtEntsoe(start)
    + '&periodEnd=' + fmtEntsoe(now);
  try {
    const r = await httpGet(url);
    if (r.status !== 200 || r.body.includes('Acknowledgement_MarketDocument')) {
      const match = r.body.match(/<text>(.*?)<\/text>/);
      return res.json({ error: 'ENTSO-E: ' + (match ? match[1] : 'HTTP ' + r.status) });
    }
    const data = parseXml(r.body);
    res.json({ data, count: data.length, source: 'ENTSO-E' });
  } catch (e) {
    res.json({ error: 'ENTSO-E fout: ' + e.message });
  }
});

app.get('/imbalance', async (req, res) => {
  const days = Math.min(parseInt(req.query.days || '7'), 366);
  const now = new Date(); now.setUTCMinutes(0, 0, 0);
  const start = new Date(now.getTime() - days * 86400000);
  const cutoff = new Date('2024-05-22T00:00:00Z');

  async function fetchOdsMonth(ds, s, e) {
    const startStr = fmtIso(s);
    const endStr = fmtIso(e);
    const allResults = [];
    let offset = 0;
    const limit = 100;
    while (true) {
      const whereEnc = encodeURIComponent('datetime >= "' + startStr + '" AND datetime < "' + endStr + '"');
      const url = 'https://opendata.elia.be/api/explore/v2.1/catalog/datasets/' + ds
        + '/records?where=' + whereEnc
        + '&select=datetime%2Cimbalanceprice'
        + '&order_by=datetime%20ASC'
        + '&limit=' + limit
        + '&offset=' + offset;
      const r = await httpGet(url);
      if (r.status !== 200) throw new Error(ds + ' HTTP ' + r.status);
      const json = JSON.parse(r.body);
      const records = (json.results || [])
        .filter(x => x.imbalanceprice != null)
        .map(x => ({ t: x.datetime.substring(0, 19) + 'Z', v: Math.round(parseFloat(x.imbalanceprice) * 100) / 100 }));
      allResults.push(...records);
      if (records.length < limit) break;
      offset += limit;
    }
    return allResults;
  }

  async function fetchOdsParallel(ds, s, e) {
    const segments = splitIntoMonths(s, e);
    // Haal maandelijkse segmenten parallel op (max 6 tegelijk)
    const results = [];
    for (let i = 0; i < segments.length; i += 6) {
      const batch = segments.slice(i, i + 6);
      const batchResults = await Promise.all(batch.map(seg => fetchOdsMonth(ds, seg.s, seg.e)));
      batchResults.forEach(r => results.push(...r));
    }
    return results;
  }

  const results = [];
  const warnings = [];

  if (start < cutoff) {
    try {
      const segEnd = now < cutoff ? now : cutoff;
      const d = await fetchOdsParallel('ods047', start, segEnd);
      results.push(...d);
    } catch (e) { warnings.push('ods047: ' + e.message); }
  }
  if (now >= cutoff) {
    try {
      const segStart = start >= cutoff ? start : cutoff;
      const d = await fetchOdsParallel('ods134', segStart, now);
      results.push(...d);
    } catch (e) { warnings.push('ods134: ' + e.message); }
  }

  results.sort((a, b) => a.t.localeCompare(b.t));
  const out = { data: results, count: results.length, source: 'Elia Open Data' };
  if (warnings.length) out.warnings = warnings;
  res.json(out);
});

app.get('/', (req, res) => res.json({ status: 'ok', routes: ['/spot?days=N', '/imbalance?days=N'] }));

app.listen(PORT, () => console.log('Fluctus proxy running on port ' + PORT));
