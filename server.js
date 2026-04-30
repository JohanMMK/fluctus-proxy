'use strict';

const express    = require('express');
const compression = require('compression');
const { spawn }  = require('child_process');
const path       = require('path');
const fs         = require('fs');

const app  = express();
const PORT = process.env.PORT || 3000;

// ─── MIDDLEWARE ───────────────────────────────────────────────────────────────
app.use(compression());
app.use(express.json({ limit: '20mb' }));
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

// ─── HEALTH ──────────────────────────────────────────────────────────────────
app.get('/', (req, res) => {
  res.json({ status: 'ok', version: '15.6', ts: new Date().toISOString() });
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

// ─── ROUTE: POST /api/nominatie-sim ──────────────────────────────────────────
// Body: volledige simulator-input JSON
// Spawn Python simulator.py, lever stdout terug als JSON
app.post('/api/nominatie-sim', (req, res) => {
  const input = req.body;
  if (!input || typeof input !== 'object') {
    return res.status(400).json({ error: 'body is verplicht en moet JSON zijn' });
  }

  const simulatorPath = path.join(__dirname, 'simulator.py');
  if (!fs.existsSync(simulatorPath)) {
    return res.status(500).json({ error: 'simulator.py niet gevonden op server' });
  }

  const startTime = Date.now();
  const inputJson = JSON.stringify(input);

  // Spawn Python — leest van stdin, schrijft JSON naar stdout
  const proc = spawn('python3', [simulatorPath], {
    env: { ...process.env, PYTHONUNBUFFERED: '1' },
  });

  let stdout = '';
  let stderr = '';

  proc.stdout.on('data', (chunk) => { stdout += chunk.toString(); });
  proc.stderr.on('data', (chunk) => { stderr += chunk.toString(); });

  proc.on('close', (code) => {
    const elapsed = Date.now() - startTime;
    console.log(`[sim] exit=${code} elapsed=${elapsed}ms stderr_lines=${stderr.split('\n').length - 1}`);

    if (code !== 0) {
      console.error('[sim] stderr:', stderr.slice(-2000));
      return res.status(500).json({
        error: 'Simulator gefaald',
        exit_code: code,
        detail: stderr.slice(-1000),
      });
    }

    // Zoek het JSON-object in stdout (simulator schrijft log naar stderr)
    const jsonStart = stdout.indexOf('{');
    const jsonEnd   = stdout.lastIndexOf('}');
    if (jsonStart === -1 || jsonEnd === -1) {
      return res.status(500).json({
        error: 'Geen geldige JSON output van simulator',
        raw: stdout.slice(0, 500),
      });
    }

    let result;
    try {
      result = JSON.parse(stdout.slice(jsonStart, jsonEnd + 1));
    } catch (e) {
      return res.status(500).json({
        error: 'JSON parse fout',
        detail: e.message,
        raw: stdout.slice(jsonStart, jsonStart + 500),
      });
    }

    result._meta = { elapsed_ms: elapsed, server_version: '15.6' };
    res.json(result);
  });

  proc.on('error', (err) => {
    console.error('[sim] spawn error:', err);
    res.status(500).json({ error: 'Kon simulator niet starten: ' + err.message });
  });

  // Stuur input via stdin
  proc.stdin.write(inputJson);
  proc.stdin.end();
});

// ─── ROUTE: GET /api/marktdata ────────────────────────────────────────────────
// Serveert de gecachte marktdata JSON-bestanden uit de repo
app.get('/api/marktdata/:bestand', (req, res) => {
  const allowed = ['fluctus-cache-spot.json', 'fluctus-cache-imb.json', 'fluctus-cache-solar.json'];
  const { bestand } = req.params;
  if (!allowed.includes(bestand)) {
    return res.status(404).json({ error: 'Bestand niet gevonden of niet toegestaan' });
  }
  const filePath = path.join(__dirname, 'data', bestand);
  if (!fs.existsSync(filePath)) {
    return res.status(404).json({ error: `${bestand} niet aanwezig op server` });
  }
  res.setHeader('Content-Type', 'application/json');
  fs.createReadStream(filePath).pipe(res);
});

// ─── START ────────────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`Fluctus proxy v15.6 luistert op poort ${PORT}`);
  console.log(`simulator.py aanwezig: ${fs.existsSync(path.join(__dirname, 'simulator.py'))}`);
});
