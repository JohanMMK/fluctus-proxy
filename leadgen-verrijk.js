'use strict';
// ─── LEAD GENERATOR — VERRIJKEN via Google Places (module leadgen-verrijk.js) ─────────────────────────────────────
// v1.3.0 (Johan 28-11): CRM-begin — kolommen volgende_actie + actie_datum op leadgen_leads, tabel leadgen_lead_log
//   (logboek per lead: notities per datum + automatische regels bij status/actie-wijziging). GET/POST /api/leadgen/lead/:id/log.
//   Lijst: ?todo=1 = actie gepland t/m vandaag (sortering op actie_datum). Verrijken met {force:true} (+ eigen zoekterm)
//   negeert cache/opslag → opnieuw zoeken bij Google (betaald), bv. als Google het verkeerde bedrijf koos.
// v1.2.3: verrijken van een parking die al in leadgen_leads staat met geldige Google-inhoud (≤ 30 d) gebruikt die opslag
//   (bron «opslag», gratis) — ook als een andere adviseur ze verrijkte. Geen dubbele Google-kost.
// v1.2.2: kwp_bevestigd:true (knop «✅ Bevestigd») zet de eigen kWp op bron «adviseur», ook als het getal niet wijzigde.
// v1.2.1: bewaren schrijft enkel gewijzigde velden (ongewijzigde GRB-kWp blijft «grb»).
// v1.2.0 (Johan 28-11): OPSLAG + BELLIJST — tabel leadgen_leads (Supabase, uniek per slug+site_id).
//   Google-inhoud (naam/adres/telefoon/website/kWp van Google) in kolom «google» met google_tot = +30 dagen; een uurlijkse
//   opruimer zet «google» op null zodra google_tot voorbij is. place_id en EIGEN gegevens blijven: tel_bevestigd (door de klant
//   bevestigd bij het bellen), contactpersoon, belstatus, notitie, eigen_naam, kwp_eigen (GRB-dak × LEADGEN_KWP_PER_M2 = 0,12, of
//   ingevuld door de adviseur), dak_m2_grb. Endpoints: GET /api/leadgen/leads (filters), POST /api/leadgen/lead/:id (eigen velden),
//   POST /api/leadgen/lead/:id/ververs (Google opnieuw, telt mee in de limieten), GET /api/leadgen/leads.csv (standaard enkel
//   eigen gegevens; ?google=1 = tijdelijke werklijst). Adviseur ziet de leads die hij verrijkte; managers alles.
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

const VERSIE = '1.3.0';
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

  async function logRij(u, details, klantNaam, actie) {
    try {
      await sbRest('app_activity_log', {
        method: 'POST', headers: { 'Prefer': 'return=minimal' },
        body: { user_id: u.id, app_id: APP_ID, actie: actie || 'lead_verrijkt', klant_naam: klantNaam ? String(klantNaam).slice(0, 200) : null, details },
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
      const force = b.force === true;
      const hit = force ? null : cacheGet(ck);
      const tellers = () => ({ dag: TEL.dag[u.id] || 0, dag_max: mgr ? null : dagMax(), maand: TEL.maand || 0, maand_max: maandMax() || null, solar: TEL.solar, solar_max: solarMax() || null });

      let uitkomst, cache = false;
      const dbh = (hit || force) ? null : await dbTreffer(ctx).catch(() => null);   // v1.2.3: al verrijkt (door wie dan ook) en Google-data nog geldig → gratis uit de opslag
      if (hit || dbh) { uitkomst = hit || dbh; cache = true; }
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
        uitkomst = Object.assign({}, pl.fout ? { gevonden: false, bron: 'places_fout', betaald: false, places_fout: true } : pl, { solar: so, t: Date.now() });
        if (uitkomst.betaald) TEL.maand = (TEL.maand || 0) + 1;
        if (so.betaald) TEL.solar++;
        if (!pl.fout && !so.fout) cacheSet(ck, uitkomst);
      }

      let resultaat = null;
      if (uitkomst.gevonden && uitkomst.p) {
        const p = uitkomst.p;
        const af = (p.lat != null && p.lng != null) ? afstand(lat, lng, p.lat, p.lng) : (p._afstand != null ? p._afstand : null);
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

      let lead_id = null;
      try { lead_id = await leadBewaar(u, ctx, resultaat, solar, uitkomst.t); } catch (e) { console.warn('[leadgen-verrijk] lead bewaren faalde:', e.message); }

      return res.json({ ok: true, gevonden: !!resultaat, resultaat, solar, cache, bron: uitkomst.bron, teller: tellers(), lead_id, versie: VERSIE });
    } catch (e) {
      console.warn('[leadgen-verrijk] fout:', e.message);
      return res.status(502).json({ ok: false, error: 'places_fout', melding: 'Google Places gaf een fout. Probeer later opnieuw.' });
    }
  });


  // ════════════════════════════════════ v1.2.0 OPSLAG + BELLIJST ════════════════════════════════════
  const TABEL = 'leadgen_leads';
  const LOGTABEL = 'leadgen_lead_log';
  const GOOGLE_DAGEN = 30;
  const kwpPerM2 = () => { const v = parseFloat(process.env.LEADGEN_KWP_PER_M2); return isFinite(v) && v > 0 ? v : 0.12; };
  const ACTIES = ['bellen', 'terugbellen', 'mail_sturen', 'offerte_maken', 'offerte_opvolgen', 'bezoek', 'afspraak', 'factuur_opvragen'];
  const BELSTATUS = ['nieuw', 'geen_antwoord', 'terugbellen', 'interesse', 'afspraak', 'geen_interesse', 'ongeldig_nummer'];
  const naamVan = (u) => (u && (u.naam || u.email)) || null;
  const nuIso = () => new Date().toISOString();
  const totIso = () => new Date(Date.now() + GOOGLE_DAGEN * 864e5).toISOString();
  const enc = encodeURIComponent;

  function googleBlok(resultaat, solar) {
    const g = {};
    if (resultaat) Object.assign(g, { naam: resultaat.naam, adres: resultaat.adres, telefoon: resultaat.telefoon, telefoon_int: resultaat.telefoon_int,
                                      website: resultaat.website, maps: resultaat.maps, status: resultaat.status, afstand_m: resultaat.afstand_m, ver: resultaat.ver });
    if (solar && solar.gevonden) g.solar = solar;
    return Object.keys(g).length ? g : null;
  }

  // Eigen dakschatting uit het GRB (Vlaanderen): grootste gebouw (hoofd- of bijgebouw) ≤ 100 m van het parkingpunt.
  function _ringOpp(ring) {
    const R = 6371000, t = Math.PI / 180, c = Math.cos(ring[0][1] * t);
    let s = 0; for (let i = 0; i < ring.length - 1; i++) { const [x1, y1] = ring[i], [x2, y2] = ring[i + 1]; s += (x1 * c) * y2 - (x2 * c) * y1; }
    return Math.abs(s / 2) * (R * t) * (R * t);
  }
  async function grbDak(lat, lng) {
    const d = 0.0011, dl = 0.0016;
    const url = 'https://geo.api.vlaanderen.be/GRB/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=GRB:GBG&outputFormat=application/json&count=80&srsName=EPSG:4326'
      + `&bbox=${lat - d},${lng - dl},${lat + d},${lng + dl},urn:ogc:def:crs:EPSG::4326`;
    const r = await doFetch(url); if (!r.ok) return null;
    const j = await r.json(); const fs = (j && j.features) || [];
    let best = null;
    for (const f of fs) {
      const g = f.geometry; if (!g) continue;
      const rings = g.type === 'Polygon' ? [g.coordinates[0]] : g.type === 'MultiPolygon' ? g.coordinates.map(p => p[0]) : [];
      if (!rings.length || !rings[0] || rings[0].length < 4) continue;
      const opp = rings.reduce((a, ri) => a + _ringOpp(ri), 0);
      const r0 = rings[0]; const cx = r0.reduce((a, q) => a + q[0], 0) / r0.length, cy = r0.reduce((a, q) => a + q[1], 0) / r0.length;
      const af = afstand(lat, lng, cy, cx);
      if (af > 100) continue;
      if (!best || opp > best.opp) best = { opp, af };
    }
    if (!best || best.opp < 80) return null;
    const dak = Math.round(best.opp);
    return { dak_m2_grb: dak, kwp_eigen: Math.round(dak * kwpPerM2()), kwp_bron: 'grb' };
  }

  async function leadBewaar(u, ctx, resultaat, solar, opgehaaldOp) {
    if (!ctx.slug || !ctx.site_id) return null;
    const bestaand = await sbRest(`${TABEL}?slug=eq.${enc(ctx.slug)}&site_id=eq.${enc(ctx.site_id)}&select=id,kwp_eigen,kwp_bron,eerst_door&limit=1`).catch(() => null);
    const oud = bestaand && bestaand[0];
    const rij = {
      slug: ctx.slug, site_id: ctx.site_id, site: ctx.site || null, gemeente: ctx.gemeente, nis: ctx.nis, lat: ctx.lat, lng: ctx.lng,
      place_id: resultaat ? resultaat.place_id : null, google: googleBlok(resultaat, solar),
      google_tot: new Date((opgehaaldOp || Date.now()) + GOOGLE_DAGEN * 864e5).toISOString(),   // 30 dagen vanaf het échte ophalen bij Google
      laatst_door: u.id, laatst_door_naam: naamVan(u), laatst_op: nuIso(),
    };
    let id;
    if (oud) {
      if (!rij.place_id) delete rij.place_id;
      await sbRest(`${TABEL}?id=eq.${oud.id}`, { method: 'PATCH', headers: { Prefer: 'return=minimal' }, body: rij });
      id = oud.id;
    } else {
      Object.assign(rij, { eerst_door: u.id, eerst_door_naam: naamVan(u), eerst_op: nuIso() });
      const out = await sbRest(TABEL, { method: 'POST', headers: { Prefer: 'return=representation' }, body: rij });
      id = out && out[0] && out[0].id;
    }
    // Eigen kWp uit het GRB (enkel als er nog geen eigen schatting van een adviseur is) — op de achtergrond.
    if (id && (!oud || oud.kwp_bron !== 'adviseur')) {
      grbDak(ctx.lat, ctx.lng).then(g => g && sbRest(`${TABEL}?id=eq.${id}`, { method: 'PATCH', headers: { Prefer: 'return=minimal' }, body: g }))
        .catch(e => console.warn('[leadgen-verrijk] GRB-dak faalde:', e.message));
    }
    return id;
  }

  // v1.2.3: bestaande lead met geldige Google-inhoud → zelfde vorm als een opzoeking, zonder Google-call.
  async function dbTreffer(ctx) {
    if (!ctx.slug || !ctx.site_id) return null;
    const rij = (await sbRest(`${TABEL}?slug=eq.${enc(ctx.slug)}&site_id=eq.${enc(ctx.site_id)}&select=place_id,google,google_tot&limit=1`) || [])[0];
    if (!rij || !rij.google || !rij.google_tot || new Date(rij.google_tot) <= new Date()) return null;
    const g = rij.google;
    const heeftP = !!(g.naam || g.telefoon || g.website);
    return {
      gevonden: heeftP, bron: 'opslag', betaald: false, t: new Date(rij.google_tot).getTime() - GOOGLE_DAGEN * 864e5,
      p: heeftP ? { place_id: rij.place_id, naam: g.naam, adres: g.adres, telefoon: g.telefoon, telefoon_int: g.telefoon_int, website: g.website,
                    maps: g.maps, status: g.status, lat: null, lng: null, _afstand: g.afstand_m } : null,
      solar: g.solar ? Object.assign({ gevonden: true, betaald: false }, g.solar) : { gevonden: false },
    };
  }

  function magZien(u, rij) { return isManager(u) || rij.eerst_door === u.id || rij.laatst_door === u.id; }
  function rijUit(rij, metGoogle) {
    const geldig = rij.google && rij.google_tot && new Date(rij.google_tot) > new Date();
    const o = Object.assign({}, rij);
    o.google = (geldig && metGoogle !== false) ? rij.google : null;
    o.google_verlopen = !geldig;
    return o;
  }
  async function vraagGebruiker(req, res) {
    const u = await resolveUser(req);
    if (!u) { res.status(401).json({ ok: false, error: 'niet_ingelogd' }); return null; }
    if (!(await heeftAppToegang(u, APP_ID))) { res.status(403).json({ ok: false, error: 'geen_toegang' }); return null; }
    return u;
  }
  function filterQuery(u, q) {
    const f = [];
    if (!isManager(u)) f.push(`or=(eerst_door.eq.${u.id},laatst_door.eq.${u.id})`);
    else if (q.door && /^[0-9a-f-]{36}$/i.test(q.door)) f.push(`or=(eerst_door.eq.${q.door},laatst_door.eq.${q.door})`);
    if (q.slug) f.push(`slug=eq.${enc(String(q.slug).slice(0, 80))}`);
    if (q.van && /^\d{4}-\d{2}-\d{2}$/.test(q.van)) f.push(`laatst_op=gte.${q.van}T00:00:00`);
    if (q.tot && /^\d{4}-\d{2}-\d{2}$/.test(q.tot)) f.push(`laatst_op=lte.${q.tot}T23:59:59`);
    if (q.status && BELSTATUS.includes(q.status)) f.push(`belstatus=eq.${q.status}`);
    if (q.open === '1') f.push('belstatus=in.(nieuw,geen_antwoord,terugbellen,interesse)');
    if (q.todo === '1') f.push(`actie_datum=lte.${new Date().toLocaleString('sv-SE', { timeZone: 'Europe/Brussels' }).slice(0, 10)}`);
    return f;
  }
  async function leadsOphalen(u, q) {
    const f = filterQuery(u, q);
    const ord = q.todo === '1' || q.sort === 'actie' ? 'actie_datum.asc.nullslast,laatst_op.desc' : 'laatst_op.desc';
    const rijen = await sbRest(`${TABEL}?select=*&order=${ord}&limit=2000${f.length ? '&' + f.join('&') : ''}`) || [];
    const naam = String(q.naam || '').trim().toLowerCase();
    const minKwp = parseFloat(q.minkwp);
    return rijen.map(r => rijUit(r)).filter(r => {
      if (naam) {
        const hooi = [r.site, r.eigen_naam, r.gemeente, r.contactpersoon, r.google && r.google.naam].filter(Boolean).join(' ').toLowerCase();
        if (!hooi.includes(naam)) return false;
      }
      if (q.tel === '1' && !(r.tel_bevestigd || (r.google && r.google.telefoon))) return false;
      if (isFinite(minKwp) && minKwp > 0) {
        const k = r.kwp_eigen != null ? +r.kwp_eigen : (r.google && r.google.solar ? +r.google.solar.kwp : null);
        if (k == null || k < minKwp) return false;
      }
      return true;
    });
  }

  app.get('/api/leadgen/leads', async (req, res) => {
    try {
      const u = await vraagGebruiker(req, res); if (!u) return;
      const leads = await leadsOphalen(u, req.query || {});
      const adviseurs = isManager(u) ? Object.values(leads.reduce((m, r) => {
        if (r.eerst_door) m[r.eerst_door] = { id: r.eerst_door, naam: r.eerst_door_naam };
        if (r.laatst_door) m[r.laatst_door] = { id: r.laatst_door, naam: r.laatst_door_naam };
        return m; }, {})) : null;
      res.json({ ok: true, leads, adviseurs, belstatus: BELSTATUS, acties: ACTIES, manager: isManager(u), google_dagen: GOOGLE_DAGEN });
    } catch (e) { console.warn('[leadgen-verrijk] leads:', e.message); res.status(502).json({ ok: false, error: 'opslag_fout', melding: 'Kon de verrijkte leads niet laden.' }); }
  });

  app.post('/api/leadgen/lead/:id', async (req, res) => {
    try {
      const u = await vraagGebruiker(req, res); if (!u) return;
      const id = parseInt(req.params.id, 10); if (!id) return res.status(400).json({ ok: false, error: 'id' });
      const rij = (await sbRest(`${TABEL}?id=eq.${id}&select=*&limit=1`) || [])[0];
      if (!rij) return res.status(404).json({ ok: false, error: 'niet_gevonden' });
      if (!magZien(u, rij)) return res.status(403).json({ ok: false, error: 'geen_toegang' });
      const b = req.body || {}, upd = {};
      const txt = (v, n) => (v == null ? null : String(v).trim().slice(0, n) || null);
      if ('belstatus' in b) { if (!BELSTATUS.includes(b.belstatus)) return res.status(400).json({ ok: false, error: 'belstatus' }); upd.belstatus = b.belstatus; }
      if ('notitie' in b) upd.notitie = txt(b.notitie, 1000);
      if ('volgende_actie' in b) { const a = txt(b.volgende_actie, 40); if (a && !ACTIES.includes(a)) return res.status(400).json({ ok: false, error: 'actie' }); upd.volgende_actie = a; }
      if ('actie_datum' in b) { const d = txt(b.actie_datum, 10); if (d && !/^\d{4}-\d{2}-\d{2}$/.test(d)) return res.status(400).json({ ok: false, error: 'datum' }); upd.actie_datum = d; }
      if ('contactpersoon' in b) upd.contactpersoon = txt(b.contactpersoon, 120);
      if ('eigen_naam' in b) upd.eigen_naam = txt(b.eigen_naam, 160);
      if ('tel_bevestigd' in b) { const t = txt(b.tel_bevestigd, 40); if (t && !/^[+\d][\d\s./()-]{5,}$/.test(t)) return res.status(400).json({ ok: false, error: 'telefoon' }); upd.tel_bevestigd = t; }
      if ('kwp_eigen' in b) { const k = b.kwp_eigen === '' || b.kwp_eigen == null ? null : Math.round(+b.kwp_eigen);
        if (k != null && !(k >= 0 && k < 100000)) return res.status(400).json({ ok: false, error: 'kwp' });
        upd.kwp_eigen = k; upd.kwp_bron = k == null ? null : 'adviseur'; }
      // Enkel echte wijzigingen bewaren (ongewijzigde GRB-kWp blijft bron «grb»).
      Object.keys(upd).forEach(kk => {
        if (kk === 'kwp_bron') return;
        const oud = rij[kk] == null ? null : (kk === 'kwp_eigen' ? Math.round(+rij[kk]) : String(rij[kk]));
        const nw = upd[kk] == null ? null : (kk === 'kwp_eigen' ? upd[kk] : String(upd[kk]));
        if (oud === nw) { delete upd[kk]; if (kk === 'kwp_eigen') delete upd.kwp_bron; }
      });
      // v1.2.2: «✅ Bevestigd» — adviseur bevestigt de eigen kWp (ook ongewijzigd) → bron «adviseur».
      if (b.kwp_bevestigd === true) {
        const kv = upd.kwp_eigen !== undefined ? upd.kwp_eigen : (rij.kwp_eigen == null ? null : Math.round(+rij.kwp_eigen));
        if (kv != null && rij.kwp_bron !== 'adviseur') { upd.kwp_eigen = kv; upd.kwp_bron = 'adviseur'; }
      }
      const logTekst = txt(b.log_tekst, 2000);
      if (!Object.keys(upd).length && !logTekst) return res.json({ ok: true, lead: rijUit(rij) });
      // Logboek: automatische regels bij status/actie + vrije notitie
      const regels = [];
      if (upd.belstatus) regels.push({ soort: 'status', tekst: `Status: ${rij.belstatus || 'nieuw'} → ${upd.belstatus}` });
      if ('volgende_actie' in upd || 'actie_datum' in upd) {
        const a = 'volgende_actie' in upd ? upd.volgende_actie : rij.volgende_actie, d = 'actie_datum' in upd ? upd.actie_datum : rij.actie_datum;
        regels.push({ soort: 'actie', tekst: a ? `Volgende actie: ${a.replace(/_/g, ' ')}${d ? ' op ' + d.split('-').reverse().join('/') : ''}` : 'Volgende actie gewist' });
      }
      if ('tel_bevestigd' in upd && upd.tel_bevestigd) regels.push({ soort: 'bevestigd', tekst: `Telefoon bevestigd: ${upd.tel_bevestigd}` });
      if (upd.kwp_bron === 'adviseur') regels.push({ soort: 'bevestigd', tekst: `Eigen kWp bevestigd: ${upd.kwp_eigen}` });
      if (logTekst) regels.push({ soort: 'notitie', tekst: logTekst, datum: /^\d{4}-\d{2}-\d{2}$/.test(String(b.log_datum || '')) ? b.log_datum : undefined });
      if (regels.length) await sbRest(LOGTABEL, { method: 'POST', headers: { Prefer: 'return=minimal' },
        body: regels.map(r => Object.assign({ lead_id: id, door: u.id, door_naam: naamVan(u) }, r)) }).catch(e => console.warn('[leadgen-verrijk] logboek:', e.message));
      if (!Object.keys(upd).length) { const nu = (await sbRest(`${TABEL}?id=eq.${id}&select=*&limit=1`) || [])[0] || rij; return res.json({ ok: true, lead: rijUit(nu) }); }
      Object.assign(upd, { bijgewerkt_door: u.id, bijgewerkt_door_naam: naamVan(u), bijgewerkt_op: nuIso() });
      const out = await sbRest(`${TABEL}?id=eq.${id}`, { method: 'PATCH', headers: { Prefer: 'return=representation' }, body: upd });
      await logRij(u, { site: rij.site, site_id: rij.site_id, slug: rij.slug, gemeente: rij.gemeente, lead_id: id,
                        velden: Object.keys(upd).filter(k => !/^bijgewerkt/.test(k)), belstatus: upd.belstatus || null,
                        tel_bevestigd: 'tel_bevestigd' in upd ? !!upd.tel_bevestigd : undefined, kwp_bevestigd: upd.kwp_bron === 'adviseur' || undefined }, rij.site || rij.gemeente, 'lead_belresultaat');
      res.json({ ok: true, lead: rijUit((out && out[0]) || Object.assign(rij, upd)) });
    } catch (e) { console.warn('[leadgen-verrijk] lead bijwerken:', e.message); res.status(502).json({ ok: false, error: 'opslag_fout', melding: 'Bewaren mislukt.' }); }
  });

  app.post('/api/leadgen/lead/:id/ververs', async (req, res) => {
    try {
      const u = await vraagGebruiker(req, res); if (!u) return;
      const k = key(); if (!k) return res.status(503).json({ ok: false, error: 'geen_key', melding: 'Google-sleutel ontbreekt op de server.' });
      const id = parseInt(req.params.id, 10);
      const rij = (await sbRest(`${TABEL}?id=eq.${id}&select=*&limit=1`) || [])[0];
      if (!rij) return res.status(404).json({ ok: false, error: 'niet_gevonden' });
      if (!magZien(u, rij)) return res.status(403).json({ ok: false, error: 'geen_toegang' });
      await tellersLaden();
      const mgr = isManager(u);
      if (!mgr && dagMax() && (TEL.dag[u.id] || 0) >= dagMax()) return res.status(429).json({ ok: false, error: 'daglimiet', melding: `Daglimiet bereikt (${dagMax()} per dag).` });
      if (maandMax() && (TEL.maand || 0) >= maandMax()) return res.status(429).json({ ok: false, error: 'maandlimiet', melding: `Maandlimiet van ${maandMax()} bereikt.` });
      TEL.dag[u.id] = (TEL.dag[u.id] || 0) + 1;
      const solarMag = !solarUit() && !(solarMax() && TEL.solar >= solarMax());
      const [pl, so] = await Promise.all([
        rij.place_id ? (_newUit ? detailsLegacy(k, rij.place_id) : detailsNew(k, rij.place_id)).then(p => ({ gevonden: true, betaald: true, p })).catch(e => ({ fout: e.message }))
                     : opzoeken(k, rij.lat, rij.lng, rij.site || '').catch(e => ({ fout: e.message })),
        solarMag ? solarOpzoeken(k, rij.lat, rij.lng).catch(e => ({ fout: e.message })) : Promise.resolve({ overgeslagen: true }),
      ]);
      if (pl.betaald) TEL.maand = (TEL.maand || 0) + 1;
      if (so.betaald) TEL.solar++;
      let resultaat = null;
      if (pl.gevonden && pl.p) { const p = pl.p; const af = (p.lat != null) ? afstand(rij.lat, rij.lng, p.lat, p.lng) : null;
        resultaat = { naam: p.naam, adres: p.adres, telefoon: p.telefoon, telefoon_int: p.telefoon_int, website: p.website, maps: p.maps, status: p.status, afstand_m: af, ver: af != null && af > 400, place_id: p.place_id }; }
      const solar = so.gevonden ? { gevonden: true, kwp: so.kwp, panelen: so.panelen, paneel_w: so.paneel_w, dak_m2: so.dak_m2, paneel_m2: so.paneel_m2, zonuren: so.zonuren,
                                    kwh_jaar: so.kwh_jaar, kwh_per_kwp: so.kwh_per_kwp, afstand_m: so.afstand_m, ver: so.afstand_m != null && so.afstand_m > 150, kwaliteit: so.kwaliteit, beeld: so.beeld } : null;
      const upd = { google: googleBlok(resultaat, solar), google_tot: totIso(), laatst_door: u.id, laatst_door_naam: naamVan(u), laatst_op: nuIso() };
      if (resultaat && resultaat.place_id) upd.place_id = resultaat.place_id;
      const out = await sbRest(`${TABEL}?id=eq.${id}`, { method: 'PATCH', headers: { Prefer: 'return=representation' }, body: upd });
      await logRij(u, { site: rij.site, site_id: rij.site_id, slug: rij.slug, gemeente: rij.gemeente, lat: rij.lat, lng: rij.lng, lead_id: id, ververs: true,
                        gevonden: !!resultaat, place_id: resultaat ? resultaat.place_id : null, heeft_tel: !!(resultaat && resultaat.telefoon), heeft_web: !!(resultaat && resultaat.website),
                        bron: pl.fout ? 'places_fout' : (_newUit ? 'places_legacy' : 'places_new'), betaald: !!pl.betaald, cache: false,
                        solar_gevonden: !!so.gevonden, solar_betaald: !!so.betaald }, rij.site || rij.gemeente);
      res.json({ ok: true, lead: rijUit((out && out[0]) || Object.assign(rij, upd)) });
    } catch (e) { console.warn('[leadgen-verrijk] ververs:', e.message); res.status(502).json({ ok: false, error: 'places_fout', melding: 'Verversen mislukt.' }); }
  });

  app.get('/api/leadgen/lead/:id/log', async (req, res) => {
    try {
      const u = await vraagGebruiker(req, res); if (!u) return;
      const id = parseInt(req.params.id, 10);
      const rij = (await sbRest(`${TABEL}?id=eq.${id}&select=id,eerst_door,laatst_door&limit=1`) || [])[0];
      if (!rij) return res.status(404).json({ ok: false, error: 'niet_gevonden' });
      if (!magZien(u, rij)) return res.status(403).json({ ok: false, error: 'geen_toegang' });
      const log = await sbRest(`${LOGTABEL}?lead_id=eq.${id}&select=*&order=datum.desc,ts.desc&limit=500`) || [];
      res.json({ ok: true, log, acties: ACTIES });
    } catch (e) { console.warn('[leadgen-verrijk] log lezen:', e.message); res.status(502).json({ ok: false, error: 'opslag_fout' }); }
  });

  // CSV: standaard ENKEL eigen gegevens; ?google=1 voegt de Google-velden toe als tijdelijke werklijst (gelogd).
  app.get('/api/leadgen/leads.csv', async (req, res) => {
    try {
      const u = await vraagGebruiker(req, res); if (!u) return;
      const q = req.query || {}, metG = q.google === '1';
      const leads = await leadsOphalen(u, q);
      const kol = ['gemeente', 'parking', 'bedrijf (eigen)', 'telefoon (bevestigd door klant)', 'contactpersoon', 'belstatus', 'notitie', 'volgende actie', 'actie op', 'kWp (eigen)', 'kWp-bron', 'dak m² (GRB)', 'verrijkt op', 'door', 'lat', 'lng'];
      if (metG) kol.push('Google: bedrijf', 'Google: telefoon', 'Google: website', 'Google: kWp', 'Google: geldig tot');
      const cel = v => { const t = v == null ? '' : String(v); return /[;"\n\r]/.test(t) ? '"' + t.replace(/"/g, '""') + '"' : t; };
      const lijnen = [kol.join(';')];
      for (const r of leads) {
        const g = r.google || {};
        const rij = [r.gemeente, r.site, r.eigen_naam, r.tel_bevestigd, r.contactpersoon, r.belstatus, r.notitie, r.volgende_actie ? r.volgende_actie.replace(/_/g, ' ') : '', r.actie_datum || '', r.kwp_eigen, r.kwp_bron, r.dak_m2_grb,
                     r.laatst_op ? String(r.laatst_op).slice(0, 10) : '', r.laatst_door_naam, r.lat, r.lng];
        if (metG) rij.push(g.naam, g.telefoon_int || g.telefoon, g.website, g.solar ? g.solar.kwp : '', r.google ? String(r.google_tot).slice(0, 10) : '');
        lijnen.push(rij.map(cel).join(';'));
      }
      await logRij(u, { export: true, met_google: metG, aantal: leads.length }, null, 'lead_export');
      const naam = `bellijst_${new Date().toISOString().slice(0, 10)}${metG ? '_TIJDELIJK' : ''}.csv`;
      res.setHeader('Content-Type', 'text/csv; charset=utf-8');
      res.setHeader('Content-Disposition', `attachment; filename="${naam}"`);
      res.send('﻿' + (metG ? '# Tijdelijke werklijst met Google-gegevens: na gebruik verwijderen (max. 30 dagen).\n' : '') + lijnen.join('\r\n'));
    } catch (e) { console.warn('[leadgen-verrijk] csv:', e.message); res.status(502).json({ ok: false, error: 'opslag_fout' }); }
  });

  // Opruimer: Google-inhoud weg na 30 dagen (één PATCH per uur, enkel rijen die nog Google-inhoud hebben).
  async function opruimen() {
    try { await sbRest(`${TABEL}?google=not.is.null&google_tot=lt.${enc(nuIso())}`, { method: 'PATCH', headers: { Prefer: 'return=minimal' }, body: { google: null } }); }
    catch (e) { console.warn('[leadgen-verrijk] opruimen:', e.message); }
  }
  if (!deps.geenTimers) { setTimeout(opruimen, 60 * 1000); const tm = setInterval(opruimen, 3600 * 1000); if (tm.unref) tm.unref(); }

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
