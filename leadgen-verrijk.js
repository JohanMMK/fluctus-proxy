'use strict';
// ─── LEAD GENERATOR — VERRIJKEN via Google Places (module leadgen-verrijk.js) ─────────────────────────────────────
// v1.1.0 (Johan 28-11): SOLAR — dezelfde klik vraagt ook Google Solar API buildingInsights:findClosest op (dichtste
//   gebouw bij de parking): max. aantal panelen, kWp, dakoppervlak, zonuren, geschatte jaaropbrengst (AC ≈ DC × 0,85).
//   SKU Solar API Building Insights (Essentials). NOT_FOUND = gratis. Eigen maandlimiet LEADGEN_SOLAR_MAAND (standaard
//   1.000); LEADGEN_SOLAR=0 zet enkel het zon-deel uit. Google ziet geen bestaande panelen: «potentieel» = heel het dak.
// v1.0.0 (Johan 28-11): knop «📞 Verrijken» in de Lead Generator (gemeentekaart) → telefoon + website + Google-naam
//   van het bedrijf bij een parking. Registreert WIE verrijkt (app_activity_log, actie 'lead_verrijkt', server-side).
//
// POST /api/leadgen/verrijk  { lat, lng, zoek, site?, site_id?, gemeente?, nis?, slug? }   (Bearer = Supabase-sessie)
//   → { ok, gevonden, resultaat:{ naam, adres, telefoon, telefoon_int, website, maps, status, afstand_m, ver }, cache, bron,
//       teller:{ dag, dag_max, maand, maand_max } }
// GET  /api/leadgen/verrijk/status  (manager) → tellers + limieten + of de key gezet is
//
// Werkwijze (kostenbewust):
//   1. Text Search (New) met field mask «places.id» = SKU Text Search Essentials (IDs Only) — gratis, onbeperkt.
//      Zoekterm = naam van de parking/het bedrijf, met locationBias (cirkel 250 m) rond de parking;
//      generieke naam → locationRestriction (rechthoek ± 200 m) zodat we enkel iets op die plek vinden.
//   2. Place Details (New) voor de BESTE kandidaat met field mask telefoon/website/naam/adres/locatie
//      = SKU Place Details Enterprise ($20 / 1.000, 1.000 gratis per maand). Max. 1 betaalde call per klik.
//   3. Terugval op de Legacy-API (Find Place ID-only + Details) als «Places API (New)» niet ingeschakeld is.
//
// Google-voorwaarden: Places-inhoud niet blijvend bewaren (place_id wel). Daarom: enkel een geheugen-cache van max.
//   30 dagen (zelfde plek = geen tweede betaalde call), en in de log enkel place_id + ja/nee-vlaggen, geen telefoonnummer.
//
// ENV: GOOGLE_PLACES_KEY (terugval GOOGLE_MAPS_KEY) · LEADGEN_VERRIJK_DAG (per adviseur per dag, standaard 40; managers
//   onbeperkt) · LEADGEN_VERRIJK_MAAND (betaalde calls per kalendermaand, iedereen samen, standaard 950 = binnen het
//   gratis quotum; 0 = geen limiet) · LEADGEN_VERRIJK=0 zet de knop server-side uit.

const VERSIE = '1.1.0';
const APP_ID = 'gemeenteplan';
const CACHE_TTL = 30 * 24 * 3600 * 1000;
const CACHE_MAX = 5000;

module.exports = function mountLeadgenVerrijk(app, deps) {
  const { resolveUser, heeftAppToegang, isManager, sbRest } = deps;
  const doFetch = deps.fetch || ((u, o) => fetch(u, o));

  const CACHE = new Map();             // sleutel → { t, val }
  const TEL = { solar: 0, maand: null, maandKey: '', dag: {}, dagKey: '', geladen: false, laden: null };

  const key = () => process.env.GOOGLE_PLACES_KEY || process.env.GOOGLE_MAPS_KEY || '';
  const dagMax = () => { const v = parseInt(process.env.LEADGEN_VERRIJK_DAG, 10); return isFinite(v) && v >= 0 ? v : 40; };
  const maandMax = () => { const v = parseInt(process.env.LEADGEN_VERRIJK_MAAND, 10); return isFinite(v) && v >= 0 ? v : 950; };
  const uit = () => String(process.env.LEADGEN_VERRIJK || '1') === '0';
  const solarUit = () => String(process.env.LEADGEN_SOLAR || '1') === '0';
  const solarMax = () => { const v = parseInt(process.env.LEADGEN_SOLAR_MAAND, 10); return isFinite(v) && v >= 0 ? v : 1000; };

  function nuBrussel() {
    const s = new Date().toLocaleString('sv-SE', { timeZone: 'Europe/Brussels' });   // «2026-11-28 17:10:00»
    return { dag: s.slice(0, 10), maand: s.slice(0, 7) };
  }
  function maandStartIso(maand) { return new Date(maand + '-01T00:00:00+01:00').toISOString(); }

  // Tellers één keer per (her)start uit de log halen, daarna in het geheugen bijhouden.
  async function tellersLaden() {
    const n = nuBrussel();
    if (TEL.maandKey !== n.maand) { TEL.maandKey = n.maand; TEL.maand = null; TEL.geladen = false; }
    if (TEL.dagKey !== n.dag) { TEL.dagKey = n.dag; TEL.dag = {}; TEL.geladen = false; }
    if (TEL.geladen) return;
    if (!TEL.laden) {
      TEL.laden = (async () => {
        let maand = 0, solar = 0; const dag = {};
        try {
          const rijen = await sbRest(`app_activity_log?app_id=eq.${APP_ID}&actie=eq.lead_verrijkt&ts=gte.${encodeURIComponent(maandStartIso(n.maand))}&select=user_id,ts,details&limit=20000`) || [];
          for (const r of rijen) {
            const d = r.details || {};
            if (d.betaald) maand++;
            if (d.solar_betaald) solar++;
            const rd = new Date(r.ts).toLocaleString('sv-SE', { timeZone: 'Europe/Brussels' }).slice(0, 10);
            if (rd === n.dag && !d.cache) dag[r.user_id] = (dag[r.user_id] || 0) + 1;
          }
        } catch (e) { console.warn('[leadgen-verrijk] tellers laden faalde:', e.message); }
        TEL.maand = maand; TEL.solar = solar; TEL.dag = dag; TEL.geladen = true;
      })().finally(() => { TEL.laden = null; });
    }
    await TEL.laden;
  }

  function cacheGet(k) {
    const h = CACHE.get(k);
    if (!h) return null;
    if (Date.now() - h.t > CACHE_TTL) { CACHE.delete(k); return null; }
    return h.val;
  }
  function cacheSet(k, val) {
    if (CACHE.size >= CACHE_MAX) { const eerste = CACHE.keys().next().value; CACHE.delete(eerste); }
    CACHE.set(k, { t: Date.now(), val });
  }

  function afstand(aLat, aLon, bLat, bLon) {
    const R = 6371000, t = Math.PI / 180;
    const dLa = (bLat - aLat) * t, dLo = (bLon - aLon) * t;
    const h = Math.sin(dLa / 2) ** 2 + Math.cos(aLat * t) * Math.cos(bLat * t) * Math.sin(dLo / 2) ** 2;
    return Math.round(2 * R * Math.asin(Math.sqrt(h)));
  }

  // Generieke namen (OSM «Parking», leeg, …) → zoeken we op de plek zelf i.p.v. op naam.
  const GENERIEK = /^(parking|parkeerplaats|parkeerterrein|car ?park|p\d*|bedrijf|bedrijventerrein|onderneming|winkel|kantoor|garage|magazijn|industrie)$/i;
  function isGeneriek(z) { return !z || z.length < 3 || GENERIEK.test(z.trim()); }

  class NewApiUit extends Error {}

  // ── Places API (New)
  async function zoekNew(k, lat, lng, zoek, generiek) {
    const body = { textQuery: zoek, languageCode: 'nl', regionCode: 'BE', pageSize: 5 };
    if (generiek) {
      const dLa = 200 / 111320, dLo = 200 / (111320 * Math.cos(lat * Math.PI / 180));
      body.locationRestriction = { rectangle: { low: { latitude: lat - dLa, longitude: lng - dLo }, high: { latitude: lat + dLa, longitude: lng + dLo } } };
    } else {
      body.locationBias = { circle: { center: { latitude: lat, longitude: lng }, radius: 250 } };
    }
    const r = await doFetch('https://places.googleapis.com/v1/places:searchText', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Goog-Api-Key': k, 'X-Goog-FieldMask': 'places.id' },
      body: JSON.stringify(body),
    });
    if (r.status === 403 || r.status === 400) {
      const t = await r.text();
      if (/SERVICE_DISABLED|PERMISSION_DENIED|API_KEY_SERVICE_BLOCKED|has not been used|not enabled/i.test(t)) throw new NewApiUit(t.slice(0, 200));
      throw new Error(`searchText HTTP ${r.status}: ${t.slice(0, 200)}`);
    }
    if (!r.ok) throw new Error(`searchText HTTP ${r.status}`);
    const j = await r.json();
    return ((j && j.places) || []).map(p => p.id).filter(Boolean);
  }
  async function detailsNew(k, id) {
    const r = await doFetch(`https://places.googleapis.com/v1/places/${encodeURIComponent(id)}?languageCode=nl&regionCode=BE`, {
      headers: {
        'X-Goog-Api-Key': k,
        'X-Goog-FieldMask': 'id,displayName,formattedAddress,location,nationalPhoneNumber,internationalPhoneNumber,websiteUri,googleMapsUri,businessStatus',
      },
    });
    if (!r.ok) throw new Error(`details HTTP ${r.status}: ${(await r.text()).slice(0, 200)}`);
    const p = await r.json();
    return {
      place_id: p.id, naam: (p.displayName && p.displayName.text) || null, adres: p.formattedAddress || null,
      lat: p.location ? p.location.latitude : null, lng: p.location ? p.location.longitude : null,
      telefoon: p.nationalPhoneNumber || null, telefoon_int: p.internationalPhoneNumber || null,
      website: p.websiteUri || null, maps: p.googleMapsUri || null, status: p.businessStatus || null,
    };
  }

  // ── Legacy-API (terugval)
  async function zoekLegacy(k, lat, lng, zoek, generiek) {
    const bias = generiek ? `circle:200@${lat},${lng}` : `circle:250@${lat},${lng}`;
    const u = `https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input=${encodeURIComponent(zoek)}&inputtype=textquery&fields=place_id&language=nl&locationbias=${encodeURIComponent(bias)}&key=${k}`;
    const r = await doFetch(u); if (!r.ok) throw new Error(`findplace HTTP ${r.status}`);
    const j = await r.json();
    if (j.status && j.status !== 'OK' && j.status !== 'ZERO_RESULTS') throw new Error(`findplace ${j.status} ${j.error_message || ''}`);
    return ((j && j.candidates) || []).map(c => c.place_id).filter(Boolean);
  }
  async function detailsLegacy(k, id) {
    const f = 'place_id,name,formatted_address,geometry/location,formatted_phone_number,international_phone_number,website,url,business_status';
    const r = await doFetch(`https://maps.googleapis.com/maps/api/place/details/json?place_id=${encodeURIComponent(id)}&fields=${f}&language=nl&key=${k}`);
    if (!r.ok) throw new Error(`details HTTP ${r.status}`);
    const j = await r.json(); if (j.status !== 'OK') throw new Error(`details ${j.status} ${j.error_message || ''}`);
    const p = j.result || {}, loc = p.geometry && p.geometry.location;
    return {
      place_id: p.place_id || id, naam: p.name || null, adres: p.formatted_address || null,
      lat: loc ? loc.lat : null, lng: loc ? loc.lng : null,
      telefoon: p.formatted_phone_number || null, telefoon_int: p.international_phone_number || null,
      website: p.website || null, maps: p.url || null, status: p.business_status || null,
    };
  }

  let _newUit = false;   // onthouden zodat we niet elke klik eerst de New-API proberen
  async function opzoeken(k, lat, lng, zoek) {
    const generiek = isGeneriek(zoek);
    const term = generiek ? 'bedrijf' : zoek;
    if (!_newUit) {
      try {
        const ids = await zoekNew(k, lat, lng, term, generiek);
        if (!ids.length) return { gevonden: false, bron: 'places_new', betaald: false };
        return { gevonden: true, bron: 'places_new', betaald: true, p: await detailsNew(k, ids[0]) };
      } catch (e) {
        if (!(e instanceof NewApiUit)) throw e;
        _newUit = true;
        console.warn('[leadgen-verrijk] Places API (New) niet ingeschakeld op de key — terugval Legacy:', e.message);
      }
    }
    const ids = await zoekLegacy(k, lat, lng, term, generiek);
    if (!ids.length) return { gevonden: false, bron: 'places_legacy', betaald: false };
    return { gevonden: true, bron: 'places_legacy', betaald: true, p: await detailsLegacy(k, ids[0]) };
  }


  // ── Solar API — dichtste gebouw bij de parking
  async function solarOpzoeken(k, lat, lng) {
    for (const q of ['MEDIUM', 'LOW']) {
      const u = `https://solar.googleapis.com/v1/buildingInsights:findClosest?location.latitude=${lat}&location.longitude=${lng}&requiredQuality=${q}&key=${k}`;
      const r = await doFetch(u);
      if (r.status === 404) continue;                       // NOT_FOUND = gratis → lagere kwaliteit proberen
      if (!r.ok) throw new Error(`solar HTTP ${r.status}: ${(await r.text()).slice(0, 200)}`);
      const j = await r.json();
      const sp = j.solarPotential || {};
      const nPan = +sp.maxArrayPanelsCount || 0, wPan = +sp.panelCapacityWatts || 400;
      const cfgs = Array.isArray(sp.solarPanelConfigs) ? sp.solarPanelConfigs : [];
      const maxCfg = cfgs.length ? cfgs[cfgs.length - 1] : null;
      const kwp = Math.round(nPan * wPan / 100) / 10;
      const dcKwh = maxCfg ? +maxCfg.yearlyEnergyDcKwh || 0 : 0;
      const acKwh = Math.round(dcKwh * 0.85);
      const c = j.center || {};
      const d = j.imageryDate || {};
      return {
        gevonden: nPan > 0, betaald: true, kwaliteit: j.imageryQuality || q,
        panelen: nPan, paneel_w: wPan, kwp, dak_m2: sp.wholeRoofStats ? Math.round(+sp.wholeRoofStats.areaMeters2 || 0) : null,
        paneel_m2: sp.maxArrayAreaMeters2 != null ? Math.round(+sp.maxArrayAreaMeters2) : null,
        zonuren: sp.maxSunshineHoursPerYear != null ? Math.round(+sp.maxSunshineHoursPerYear) : null,
        kwh_jaar: acKwh || null, kwh_per_kwp: kwp && acKwh ? Math.round(acKwh / kwp) : null,
        afstand_m: (c.latitude != null) ? afstand(lat, lng, c.latitude, c.longitude) : null,
        beeld: d.year ? `${d.year}-${String(d.month || 1).padStart(2, '0')}` : null,
      };
    }
    return { gevonden: false, betaald: false };
  }

  async function logRij(u, details, klantNaam) {
    try {
      await sbRest('app_activity_log', {
        method: 'POST', headers: { 'Prefer': 'return=minimal' },
        body: { user_id: u.id, app_id: APP_ID, actie: 'lead_verrijkt', klant_naam: klantNaam ? String(klantNaam).slice(0, 200) : null, details },
      });
    } catch (e) { console.warn('[leadgen-verrijk] log faalde:', e.message); }
  }

  app.post('/api/leadgen/verrijk', async (req, res) => {
    try {
      const u = await resolveUser(req);
      if (!u) return res.status(401).json({ ok: false, error: 'niet_ingelogd' });
      if (!(await heeftAppToegang(u, APP_ID))) return res.status(403).json({ ok: false, error: 'geen_toegang' });
      if (uit()) return res.status(503).json({ ok: false, error: 'uitgeschakeld', melding: 'Verrijken staat tijdelijk uit.' });
      const k = key();
      if (!k) return res.status(503).json({ ok: false, error: 'geen_key', melding: 'Google Places-sleutel ontbreekt op de server.' });

      const b = req.body || {};
      const lat = +b.lat, lng = +b.lng;
      if (!isFinite(lat) || !isFinite(lng) || lat < 49.4 || lat > 51.6 || lng < 2.4 || lng > 6.5)
        return res.status(400).json({ ok: false, error: 'coordinaten_buiten_belgie' });
      const zoek = String(b.zoek || b.site || '').replace(/\s+/g, ' ').trim().slice(0, 120);
      const ctx = {
        site: b.site ? String(b.site).slice(0, 200) : '', site_id: b.site_id ? String(b.site_id).slice(0, 80) : null,
        gemeente: b.gemeente ? String(b.gemeente).slice(0, 80) : null, nis: b.nis ? String(b.nis).slice(0, 10) : null,
        slug: b.slug ? String(b.slug).slice(0, 80) : null, lat: Math.round(lat * 1e5) / 1e5, lng: Math.round(lng * 1e5) / 1e5,
      };

      await tellersLaden();
      const mgr = isManager(u);
      const ck = `${lat.toFixed(4)},${lng.toFixed(4)}|${zoek.toLowerCase()}`;
      const hit = cacheGet(ck);
      const tellers = () => ({ dag: TEL.dag[u.id] || 0, dag_max: mgr ? null : dagMax(), maand: TEL.maand || 0, maand_max: maandMax() || null, solar: TEL.solar, solar_max: solarMax() || null });

      let uitkomst, cache = false;
      if (hit) { uitkomst = hit; cache = true; }
      else {
        if (!mgr && dagMax() && (TEL.dag[u.id] || 0) >= dagMax())
          return res.status(429).json({ ok: false, error: 'daglimiet', melding: `Daglimiet bereikt (${dagMax()} verrijkingen per dag).`, teller: tellers() });
        if (maandMax() && (TEL.maand || 0) >= maandMax())
          return res.status(429).json({ ok: false, error: 'maandlimiet', melding: `Maandlimiet van ${maandMax()} verrijkingen bereikt (gratis Google-quotum). Vraag een beheerder om LEADGEN_VERRIJK_MAAND te verhogen.`, teller: tellers() });
        TEL.dag[u.id] = (TEL.dag[u.id] || 0) + 1;
        // Places en Solar tegelijk; een fout in het ene deel laat het andere deel staan.
        const solarMag = !solarUit() && !(solarMax() && TEL.solar >= solarMax());
        const [pl, so] = await Promise.all([
          opzoeken(k, lat, lng, zoek).catch(e => ({ fout: e.message })),
          solarMag ? solarOpzoeken(k, lat, lng).catch(e => ({ fout: e.message })) : Promise.resolve({ overgeslagen: true }),
        ]);
        if (pl.fout) console.warn('[leadgen-verrijk] places fout:', pl.fout);
        if (so.fout) console.warn('[leadgen-verrijk] solar fout:', so.fout);
        if (pl.fout && (so.fout || so.overgeslagen)) {
          TEL.dag[u.id]--;
          return res.status(502).json({ ok: false, error: 'places_fout', melding: 'Google gaf een fout. Probeer later opnieuw.' });
        }
        uitkomst = Object.assign({}, pl.fout ? { gevonden: false, bron: 'places_fout', betaald: false, places_fout: true } : pl, { solar: so });
        if (uitkomst.betaald) TEL.maand = (TEL.maand || 0) + 1;
        if (so.betaald) TEL.solar++;
        if (!pl.fout && !so.fout) cacheSet(ck, uitkomst);
      }

      let resultaat = null;
      if (uitkomst.gevonden && uitkomst.p) {
        const p = uitkomst.p;
        const af = (p.lat != null && p.lng != null) ? afstand(lat, lng, p.lat, p.lng) : null;
        resultaat = { naam: p.naam, adres: p.adres, telefoon: p.telefoon, telefoon_int: p.telefoon_int, website: p.website,
                      maps: p.maps, status: p.status, afstand_m: af, ver: af != null && af > 400, place_id: p.place_id };
      }
      const so = uitkomst.solar || {};
      const solar = so.gevonden ? { kwp: so.kwp, panelen: so.panelen, paneel_w: so.paneel_w, dak_m2: so.dak_m2, paneel_m2: so.paneel_m2,
                                    zonuren: so.zonuren, kwh_jaar: so.kwh_jaar, kwh_per_kwp: so.kwh_per_kwp, afstand_m: so.afstand_m,
                                    ver: so.afstand_m != null && so.afstand_m > 150, kwaliteit: so.kwaliteit, beeld: so.beeld }
                                : { gevonden: false, fout: !!so.fout, overgeslagen: !!so.overgeslagen };
      if (so.gevonden) solar.gevonden = true;

      // Registratie: wie, waar, wat gevonden — geen telefoonnummer of website in de log (Google-voorwaarden).
      await logRij(u, Object.assign({}, ctx, {
        zoek, gevonden: !!resultaat, place_id: resultaat ? resultaat.place_id : null,
        heeft_tel: !!(resultaat && resultaat.telefoon), heeft_web: !!(resultaat && resultaat.website),
        afstand_m: resultaat ? resultaat.afstand_m : null, bron: uitkomst.bron, betaald: !cache && !!uitkomst.betaald, cache,
        solar_gevonden: !!so.gevonden, solar_betaald: !cache && !!so.betaald,
      }), ctx.site || ctx.gemeente);

      return res.json({ ok: true, gevonden: !!resultaat, resultaat, solar, cache, bron: uitkomst.bron, teller: tellers(), versie: VERSIE });
    } catch (e) {
      console.warn('[leadgen-verrijk] fout:', e.message);
      return res.status(502).json({ ok: false, error: 'places_fout', melding: 'Google Places gaf een fout. Probeer later opnieuw.' });
    }
  });

  app.get('/api/leadgen/verrijk/status', async (req, res) => {
    const u = await resolveUser(req);
    if (!isManager(u)) return res.status(403).json({ ok: false, error: 'alleen voor managers' });
    await tellersLaden();
    res.json({ ok: true, versie: VERSIE, key: !!key(), uit: uit(), api: _newUit ? 'legacy' : 'new',
               maand: TEL.maandKey, betaald_deze_maand: TEL.maand || 0, maand_max: maandMax() || null,
               dag_max: dagMax(), per_gebruiker_vandaag: TEL.dag, cache: CACHE.size,
               solar_uit: solarUit(), solar_deze_maand: TEL.solar, solar_max: solarMax() || null });
  });

  console.log(`[leadgen-verrijk] v${VERSIE} gemount — POST /api/leadgen/verrijk`);
  return { VERSIE };
};
