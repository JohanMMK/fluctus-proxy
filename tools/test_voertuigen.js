// Test voertuigtabel: geslaagde update, fallback bij mislukte download / afgekeurde release / mislukte opslag,
// herstart (pointer), werkdagenlogica + eindmelding, rollback. Draaien: node tools/test_voertuigen.js
'use strict';
const assert = require('assert');
const path = require('path');
const V = require('../voertuigen.js');

// Belgische feestdagen (zelfde regels als server.js _beFeestdagen)
function pasen(j) { const a = j % 19, b = Math.floor(j / 100), c = j % 100, d = Math.floor(b / 4), e = b % 4, f = Math.floor((b + 8) / 25), g = Math.floor((b - f + 1) / 3),
  h = (19 * a + b - d - g + 15) % 30, i = Math.floor(c / 4), k = c % 4, l = (32 + 2 * e + 2 * i - h - k) % 7, m = Math.floor((a + 11 * h + 22 * l) / 451), mnd = Math.floor((h + l - 7 * m + 114) / 31), dag = ((h + l - 7 * m + 114) % 31) + 1;
  return Date.UTC(j, mnd - 1, dag); }
function feestdagen(j) { const p = pasen(j), D = 864e5;
  return new Set([Date.UTC(j, 0, 1), p + D, p + 39 * D, p + 50 * D, Date.UTC(j, 4, 1), Date.UTC(j, 6, 21), Date.UTC(j, 7, 15), Date.UTC(j, 10, 1), Date.UTC(j, 10, 11), Date.UTC(j, 11, 25)].map(x => new Date(x).toISOString().slice(0, 10))); }

function fixture(nAuto, opt) {
  const veh = [];
  for (let i = 0; i < nAuto; i++) veh.push({ schema_version: '1.0.0', make: { slug: 'merk' + (i % 12), name: 'Merk ' + (i % 12) }, model: { slug: 'm' + i, name: 'Model ' + i },
    year: 2025, trim: { slug: 'base', name: 'Base' }, vehicle_type: i % 10 === 0 ? 'van' : (i % 3 ? 'passenger_car' : 'suv'),
    battery: { pack_capacity_kwh_net: 50 + (i % 40) }, range: { rated: [{ cycle: 'wltp', range_km: 350 + (i % 150) }] },
    charging: { ac: { max_power_kw: 11 }, dc: { max_power_kw: 100 + (i % 150) } }, sources: [{ url: 'https://example.org/' + i }] });
  // een conflict met onze tabel: OpenEV kent ook de eSprinter 113 kWh (andere waarden) → onze tabel moet winnen
  veh.push({ make: { name: 'Mercedes-Benz' }, model: { name: 'eSprinter' }, vehicle_type: 'van', variant: { slug: 'x', name: '113 kWh' },
    battery: { pack_capacity_kwh_net: 100 }, range: { rated: [{ cycle: 'wltp', range_km: 300 }] }, charging: { ac: { max_power_kw: 11 }, dc: { max_power_kw: 80 } } });
  if (opt && opt.slecht) for (const v of veh.slice(0, Math.floor(veh.length * 0.6))) v.battery = {};
  return { version: '1.0.0', vehicles: veh };
}

function memStore() {
  const m = new Map(); let failPut = null;
  return { m, failOn: (re) => { failPut = re; },
    get: async (p) => (m.has(p) ? m.get(p) : null),
    put: async (p, s) => { if (failPut && failPut.test(p)) throw new Error('storage upload ' + p + ': HTTP 503'); m.set(p, s); } };
}
function mockFetch(mode) {
  return async (url) => {
    if (url.includes('api.github.com')) return { ok: true, json: async () => ({ tag_name: 'v9.9.9', assets: [{ name: 'open-ev-data-v9.9.9.json', browser_download_url: 'https://download/openev.json' }] }) };
    if (mode.dl === 'fout') return { ok: false, status: 502, text: async () => '' };
    if (mode.dl === 'te_weinig') return { ok: true, status: 200, text: async () => JSON.stringify(fixture(40)) };
    if (mode.dl === 'slecht') return { ok: true, status: 200, text: async () => JSON.stringify(fixture(300, { slecht: true })) };
    return { ok: true, status: 200, text: async () => JSON.stringify(fixture(300)) };
  };
}

(async () => {
  const mails = [];
  const store = memStore();
  const mode = { dl: 'ok' };
  let klok = Date.UTC(2026, 11, 1, 8, 0);   // di 1 dec 2026 09:00 Brussel = 1ste werkdag
  const mk = () => V.createVoertuigen({ store, fetch: mockFetch(mode), mail: async (to, s, t) => mails.push({ to, s, t }), now: () => klok,
    feestdagen, aanvPad: path.join(__dirname, '..', 'data', 'voertuigen_aanvullend.json'), alertTo: 'alert@test.local', minRecords: 150, log: { log() {}, warn() {} } });
  let vt = mk();

  // 1. Zonder opgeslagen versie: basisversie (aanvullende tabel) is actief
  await vt.init();
  assert.ok(vt.huidig().versie.startsWith('basis-'), 'basisversie verwacht');
  assert.ok(vt.lijst('vrachtwagen').merken.length >= 5, 'vrachtwagens in de basis');
  console.log('✔ 1 basisversie actief zonder opgeslagen versie:', vt.huidig());

  // 2. Geslaagde update via de werkdag-tick
  let r = await vt.tick();
  assert.strictEqual(r.actie, 'update'); assert.ok(r.r.ok, 'update moet slagen');
  assert.strictEqual(vt.huidig().versie, '2026.12-1');
  const es = vt.lijst('bestelwagen').merken.find(m => m.merk === 'Mercedes-Benz').modellen.find(x => x.model === 'eSprinter');
  assert.strictEqual(es.batt_kwh, 113, 'onze tabel wint bij een conflict');
  console.log('✔ 2 update geslaagd → actief', vt.huidig().versie, '| eSprinter uit onze tabel:', es.batt_kwh, 'kWh');
  assert.strictEqual(mails.length, 0);
  r = await vt.tick(); assert.strictEqual(r.reden, 'al geslaagd deze maand');

  // 3. Volgende maand, 1ste werkdag (ma 4 jan 2027; 1 jan = feestdag): download faalt → vorige versie blijft
  klok = Date.UTC(2027, 0, 4, 8, 0); mode.dl = 'fout';
  r = await vt.tick();
  assert.strictEqual(r.r.ok, false); assert.strictEqual(r.r.stap, 'download');
  assert.strictEqual(vt.huidig().versie, '2026.12-1', 'vorige versie moet actief blijven');
  assert.ok(mails.at(-1).s.includes('download') && mails.at(-1).t.includes('2026.12-1'));
  console.log('✔ 3 download mislukt → actief blijft', vt.huidig().versie, '| mail:', mails.at(-1).s);

  // 4. 2de werkdag: release afgekeurd (te weinig records)
  klok = Date.UTC(2027, 0, 5, 8, 0); mode.dl = 'te_weinig';
  r = await vt.tick(); assert.strictEqual(r.r.stap, 'validatie'); assert.strictEqual(vt.huidig().versie, '2026.12-1');
  console.log('✔ 4 validatie keurt af →', r.r.fout);

  // 5. 3de werkdag: te veel lege kernvelden
  klok = Date.UTC(2027, 0, 6, 8, 0); mode.dl = 'slecht';
  r = await vt.tick(); assert.strictEqual(r.r.stap, 'validatie'); assert.strictEqual(vt.huidig().versie, '2026.12-1');
  console.log('✔ 5 validatie keurt af →', r.r.fout.slice(0, 90));

  // 6. 4de werkdag: opslag van de nieuwe versie faalt → geen switch
  klok = Date.UTC(2027, 0, 7, 8, 0); mode.dl = 'ok'; store.failOn(/voertuigen\/v2027/);
  r = await vt.tick(); assert.strictEqual(r.r.stap, 'activatie'); assert.strictEqual(vt.huidig().versie, '2026.12-1');
  assert.strictEqual(JSON.parse(store.m.get('voertuigen/actief.json')).versie, '2026.12-1', 'pointer onveranderd');
  console.log('✔ 6 opslag faalt → pointer blijft op', JSON.parse(store.m.get('voertuigen/actief.json')).versie);

  // 7. Herstart van de server: nieuwe instantie leest de pointer
  vt = mk(); await vt.init(); assert.strictEqual(vt.huidig().versie, '2026.12-1');
  console.log('✔ 7 na herstart actief', vt.huidig().versie);

  // 8. 5de werkdag (vr 8 jan): nog altijd mislukt → foutmail + eindmelding
  klok = Date.UTC(2027, 0, 8, 8, 0); mode.dl = 'fout'; store.failOn(null);
  const voor = mails.length;
  r = await vt.tick(); assert.strictEqual(r.wd, 5);
  const nieuw = mails.slice(voor);
  assert.ok(nieuw.some(m => m.s.includes('maandelijkse update niet gelukt – vorige versie blijft actief')), 'eindmelding verwacht');
  console.log('✔ 8 5de werkdag mislukt → eindmelding:', nieuw.map(m => m.s).join(' | '));
  klok = Date.UTC(2027, 0, 11, 8, 0); r = await vt.tick(); assert.strictEqual(r.actie, 'niets', 'na de 5de werkdag geen nieuwe pogingen');
  assert.strictEqual(mails.length, voor + 2, 'eindmelding maar één keer');

  // 9. Weekend en vóór 6 u: geen poging
  klok = Date.UTC(2027, 1, 1, 3, 0); mode.dl = 'ok'; r = await vt.tick(); assert.strictEqual(r.actie, 'niets'); // ma 1 feb 04:00 Brussel
  klok = Date.UTC(2027, 1, 1, 8, 0); r = await vt.tick(); assert.ok(r.r.ok); assert.strictEqual(vt.huidig().versie, '2027.02-1');
  console.log('✔ 9 niet vóór 6 u; daarna geslaagd →', vt.huidig().versie);

  // 10. Rollback naar de vorige versie
  await vt.rollback('2026.12-1'); assert.strictEqual(vt.huidig().versie, '2026.12-1');
  console.log('✔ 10 rollback naar', vt.huidig().versie);

  // 11. Werkdagen: 1–5 dec 2026 / jan 2027 (1 jan feestdag) / mei 2027 (1 mei = zaterdag)
  assert.strictEqual(V.werkdagNr('2026-12-01', feestdagen), 1);
  assert.strictEqual(V.werkdagNr('2026-12-07', feestdagen), 5);
  assert.strictEqual(V.werkdagNr('2027-01-01', feestdagen), 0);
  assert.strictEqual(V.werkdagNr('2027-01-08', feestdagen), 5);
  assert.strictEqual(V.werkdagNr('2026-11-11', feestdagen), 0);
  console.log('✔ 11 werkdagenlogica (feestdagen) ok');
  console.log('\nALLE TESTS GESLAAGD');
})().catch(e => { console.error('✘ TEST MISLUKT:', e && e.stack || e); process.exit(1); });
