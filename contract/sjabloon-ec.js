'use strict';
// contract/sjabloon-ec.js v2.0.0 — 2026-10-19 (Johan): NIEUW FORMAT «ec-samenwerking-v2» (tekst zoals herwerkt in het partnercontract-document):
//   voorwerp particulier + bedrijven LS/MS, NCNP-vergoeding in art. 2, rapport «al dan niet» white-label, melding op het platform,
//   feedback-clausule (Fluctus mag na X dagen zonder feedback de eindklant contacteren), logbestand als rechtsgeldig bewijs,
//   referenties bij de melding, EV-opbrengst = vermeden kosten, geen claims, NON-CIRCUMVENTIE (beide richtingen, termijn + forfait).
//   Nieuwe velden: feedback_dagen (60), nc_maanden (18), nc_boete_pct (50). Bestaande contracten met sjabloon v1 houden exact hun tekst
//   (inhoudV1 ongewijzigd → zelfde hash). Ondertekeningsblok: handtekening van Fluctus (afbeelding) als die bij de verzending werd meegegeven.
// contract/sjabloon-ec.js v1.2.0 — 2026-10-17 (Johan): FIX ontbrekende getallen — een numeriek veld dat niet in de aanvraag zit
//   (of leeg is) krijgt nu de sjabloon-default i.p.v. 0/het minimum (Number('') = 0 → bv. ncnp_maanden 1, tarieven 0).
//   Ingevulde waarden en bestaande contracten: ongewijzigd (zelfde tekst, zelfde hash).
// contract/sjabloon-ec.js v1.1.0 — 2026-10-16 (Johan): TOEGANG VIA E-MAILDOMEIN — veld `toegang_domein` (website/URL of
//   e-maildomein van de partner, genormaliseerd naar bv. 'brelco.be'). Iedereen met een e-mailadres op dat domein krijgt
//   toegang als adviseur zodra het contract bestaat (ook vóór ondertekening); de aan/uit-schakelaar zit BUITEN de velden
//   (geen invloed op de hash). Leeg domein → tekst byte-identiek aan v1.0.0 (lopende teken-links blijven geldig).
//   Generieke maildomeinen (gmail, outlook, telenet …) en eigen domeinen worden geweigerd.
// v1.0.0 — 2026-10-16 (Johan): sjabloon "Energie-Compas samenwerkingsovereenkomst" (No Cure No Pay
//   + optioneel business development), gebaseerd op de Brelco-afspraken van 9/14-10-2026. Eén bron van waarheid: de server
//   rendert de tekst; de SHA-256 van de inhoud (zonder ondertekeningsblok) wordt bij verzending vastgelegd en bij het
//   tekenen opnieuw berekend → bewijs dat de getekende tekst exact de verzonden tekst is.
//   Batterijwaarde in de tekst: enkel zelfconsumptie, piekshaving, energy arbitrage (day-ahead/intraday) en passieve
//   respons op de onbalansprijs (imbalance settlement).

const crypto = require('crypto');

const SJABLOON_ID_V1 = 'ec-samenwerking-v1';
const SJABLOON_ID = 'ec-samenwerking-v2';   // v2.0.0: huidig sjabloon voor nieuwe contracten
const SJABLOON_NAAM = 'Energie-Compas samenwerking (No Cure No Pay) — v2';

const DEFAULTS = {
  partner_naam: '', partner_adres: '', partner_btw: '', partner_vert_naam: '', partner_vert_functie: '',
  partner_mail: '', contactpersonen: '', gebruikers: '', toegang_domein: '',
  afspraken_ref: '',
  tarief_part: 400, tarief_ls: 900, tarief_ms: 1600,
  ncnp_maanden: 18, melding_dagen: 14, betaal_dagen: 15,
  whitelabel: true, offerte_optie: true, imby: true,
  bd_aan: true, bd_tarief: 100, bd_betaal_dagen: 60,
  nda_datum: '',
  evaluatie_maanden: 3, opzeg_maanden: 3, kickoff: true,
  geldigheid_dagen: 30,
  extra_bepalingen: '',
  feedback_dagen: 60, nc_maanden: 18, nc_boete_pct: 50,   // v2.0.0
  fluctus_vert_naam: 'Johan Konings', fluctus_vert_functie: 'bestuurder',
};

const TEKST_MAX = { extra_bepalingen: 4000, contactpersonen: 600, gebruikers: 600, afspraken_ref: 300, partner_adres: 300 };

function _getal(x, def, min, max) { if (x === undefined || x === null || String(x).trim() === '') return def; const n = Number(String(x).trim().replace(',', '.')); if (!isFinite(n)) return def; return Math.min(max, Math.max(min, Math.round(n * 100) / 100)); }
function _bool(x, def) { if (x === undefined || x === null || x === '') return def; return x === true || x === 1 || x === '1' || x === 'true' || x === 'ja' || x === 'on'; }
function _tekst(x, max) { return String(x == null ? '' : x).replace(/\r/g, '').trim().slice(0, max || 200); }

function normaliseer(v) {
  v = v || {};
  const o = {};
  for (const k of Object.keys(DEFAULTS)) {
    const d = DEFAULTS[k];
    if (typeof d === 'number') {
      const grens = { tarief_part: [0, 100000], tarief_ls: [0, 100000], tarief_ms: [0, 100000], ncnp_maanden: [1, 60], melding_dagen: [1, 90],
        betaal_dagen: [0, 120], bd_tarief: [0, 1000], bd_betaal_dagen: [0, 120], evaluatie_maanden: [1, 24], opzeg_maanden: [0, 24], geldigheid_dagen: [1, 180], feedback_dagen: [1, 365], nc_maanden: [0, 60], nc_boete_pct: [0, 500] }[k] || [0, 1e9];
      o[k] = _getal(v[k], d, grens[0], grens[1]);
    } else if (typeof d === 'boolean') o[k] = _bool(v[k], d);
    else o[k] = _tekst(v[k] == null ? d : v[k], TEKST_MAX[k] || 200);
  }
  o.partner_mail = o.partner_mail.toLowerCase();
  o.toegang_domein = normDomein(o.toegang_domein);
  return o;
}

// v1.1.0: 'https://www.Brelco.be/contact' / '@brelco.be' / 'jan@brelco.be' → 'brelco.be'. Ongeldig of generiek → ''.
const GENERIEKE_DOMEINEN = new Set(['gmail.com', 'googlemail.com', 'hotmail.com', 'hotmail.be', 'hotmail.nl', 'outlook.com', 'outlook.be', 'live.com', 'live.be',
  'msn.com', 'yahoo.com', 'yahoo.fr', 'icloud.com', 'me.com', 'mac.com', 'telenet.be', 'skynet.be', 'proximus.be', 'scarlet.be', 'base.be', 'orange.be', 'voo.be',
  'edpnet.be', 'pandora.be', 'gmx.com', 'gmx.de', 'proton.me', 'protonmail.com', 'aol.com', 'mail.com', 'yandex.com', 'zoho.com', 'free.fr', 'orange.fr', 'ziggo.nl',
  'kpnmail.nl', 'fluctus.net', 'energie-compas.eu', 'energie-compas.be']);
function normDomein(x) {
  let d = String(x == null ? '' : x).trim().toLowerCase();
  if (!d) return '';
  if (d.indexOf('@') >= 0) d = d.slice(d.lastIndexOf('@') + 1);
  d = d.replace(/^[a-z][a-z0-9+.-]*:\/\//, '').split(/[\/?#:\s]/)[0].replace(/^www\./, '').replace(/\.+$/, '');
  if (!/^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+$/.test(d) || d.length > 120) return '';
  if (GENERIEKE_DOMEINEN.has(d)) return '';
  return d;
}
function domeinFout(ruw) {   // uitleg waarom een ingevuld domein niet aanvaard wordt (of null)
  if (!String(ruw || '').trim()) return null;
  if (normDomein(ruw)) return null;
  let d = String(ruw).trim().toLowerCase(); if (d.indexOf('@') >= 0) d = d.slice(d.lastIndexOf('@') + 1);
  d = d.replace(/^[a-z][a-z0-9+.-]*:\/\//, '').split(/[\/?#:\s]/)[0].replace(/^www\./, '');
  return GENERIEKE_DOMEINEN.has(d) ? `«${d}» is een algemeen of eigen maildomein — dat geeft te veel mensen toegang.` : 'Geen geldig domein (bv. brelco.be of https://www.brelco.be).';
}
function mailOpDomein(mail, domein) {
  const m = String(mail || '').trim().toLowerCase(); const d = normDomein(domein); if (!m || !d) return false;
  const md = m.slice(m.lastIndexOf('@') + 1); return md === d || md.endsWith('.' + d);
}

function ontbrekend(v) {
  const m = [];
  if (!v.partner_naam) m.push('partner_naam');
  if (!v.partner_adres) m.push('partner_adres');
  if (!v.partner_vert_naam) m.push('partner_vert_naam');
  if (!v.partner_vert_functie) m.push('partner_vert_functie');
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(v.partner_mail || '')) m.push('partner_mail');
  if (!v.fluctus_vert_naam) m.push('fluctus_vert_naam');
  return m;
}

function esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])); }
function eur(n) { const v = Math.round(+n || 0); return 'EUR ' + v.toLocaleString('nl-BE'); }
function nl(n) { return String(n).replace('.', ','); }
const MAANDEN = ['januari', 'februari', 'maart', 'april', 'mei', 'juni', 'juli', 'augustus', 'september', 'oktober', 'november', 'december'];
function datumNL(ts) { const d = new Date(ts || Date.now()); const p = new Intl.DateTimeFormat('nl-BE', { timeZone: 'Europe/Brussels', day: 'numeric', month: 'numeric', year: 'numeric' }).formatToParts(d); const g = t => +((p.find(x => x.type === t) || {}).value); return `${g('day')} ${MAANDEN[g('month') - 1]} ${g('year')}`; }
function tijdNL(ts) { return new Intl.DateTimeFormat('nl-BE', { timeZone: 'Europe/Brussels', day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' }).format(new Date(ts)); }
function regels(t) { return esc(t).replace(/\n/g, '<br>'); }
function lijst(t) { const L = String(t || '').split(/\n|;/).map(s => s.trim()).filter(Boolean); return L; }

const CSS = `:root{--navy:#003A5D;--green:#6AAF42;--ink:#1d2730;--muted:#5b6770;--line:#d8dee3;--soft:#f3f7f9}
@page{size:A4;margin:16mm 15mm 18mm}
*{box-sizing:border-box}
html,body{margin:0;background:#fff;color:var(--ink);font:10.2pt/1.5 "Segoe UI",Helvetica,Arial,sans-serif}
.page{max-width:180mm;margin:0 auto;padding:10mm 4mm}
header{display:flex;justify-content:space-between;align-items:flex-end;gap:4mm;border-bottom:3px solid var(--navy);padding-bottom:4mm;margin-bottom:6mm}
header img{height:15mm;max-width:60%}
header .ref{text-align:right;font-size:8.6pt;color:var(--muted)}
header .ref b{color:var(--navy)}
h1{font-size:17pt;color:var(--navy);margin:0 0 1mm;letter-spacing:.3px}
.sub{color:var(--green);font-weight:600;margin:0 0 5mm}
h2{font-size:11.5pt;color:var(--navy);margin:6mm 0 2mm;padding-bottom:1mm;border-bottom:1px solid var(--line);break-after:avoid}
h2 span{display:inline-block;min-width:7mm;color:var(--green)}
p{margin:0 0 2mm}
ul,ol{margin:0 0 2mm;padding-left:5.5mm}
li{margin:0 0 1mm}
.parties{display:grid;grid-template-columns:1fr 1fr;gap:4mm;margin-bottom:2mm}
.party{background:var(--soft);border-left:3px solid var(--navy);padding:3mm 4mm;font-size:9.4pt}
.party .lbl{font-size:8pt;text-transform:uppercase;letter-spacing:.8px;color:var(--green);font-weight:700}
.party b{color:var(--navy)}
table{width:100%;border-collapse:collapse;margin:2mm 0 3mm;font-size:9.6pt}
th{background:var(--navy);color:#fff;text-align:left;padding:2mm 3mm;font-weight:600}
td{padding:2mm 3mm;border-bottom:1px solid var(--line);vertical-align:top}
td.n{text-align:right;white-space:nowrap;font-weight:700;color:var(--navy)}
.box{border:1px solid var(--green);background:#f4faf0;padding:3mm 4mm;margin:2mm 0 3mm;break-inside:avoid}
.box b{color:var(--navy)}
.steps{counter-reset:s;list-style:none;padding:0}
.steps li{counter-increment:s;position:relative;padding-left:8mm}
.steps li::before{content:counter(s);position:absolute;left:0;top:.2mm;width:5.5mm;height:5.5mm;border-radius:50%;background:var(--navy);color:#fff;font-size:8pt;font-weight:700;text-align:center;line-height:5.5mm}
.small{font-size:8.8pt;color:var(--muted)}
.sign{display:grid;grid-template-columns:1fr 1fr;gap:8mm;margin-top:6mm;break-inside:avoid}
.sign>div{border-top:1px solid var(--ink);padding-top:2mm;font-size:9.4pt;min-height:30mm}
.sign img{max-height:22mm;max-width:100%;display:block;margin:2mm 0}
.sign .leeg{height:20mm}
.audit{margin-top:6mm;border:1px dashed var(--line);padding:3mm 4mm;font-size:8.4pt;color:var(--muted);break-inside:avoid;word-break:break-all}
.audit b{color:var(--ink)}
.stempel{display:inline-block;margin-top:1mm;padding:1mm 3mm;border-radius:99px;background:#e6f6ec;color:#0a7d3c;font-weight:700;font-size:8.6pt}
.concept{position:fixed;top:40%;left:0;right:0;text-align:center;font-size:60pt;font-weight:800;color:rgba(0,58,93,.06);transform:rotate(-20deg);pointer-events:none}
footer{margin-top:8mm;padding-top:2mm;border-top:1px solid var(--line);font-size:8pt;color:var(--muted);text-align:center}
.keep{break-inside:avoid}
@media (max-width:640px){.parties,.sign{grid-template-columns:1fr}.page{padding:6mm 4mm}h1{font-size:14pt;overflow-wrap:anywhere}header{flex-direction:column;align-items:flex-start}header .ref{text-align:left}.concept{font-size:34pt}}
@media print{.page{padding:0}}`;

// Inhoud (de eigenlijke overeenkomst) — hier wordt de hash op berekend. Geen datum van ondertekening of status in deze tekst.
function inhoud(c) { return (c && c.sjabloon === SJABLOON_ID) ? inhoudV2(c) : inhoudV1(c); }   // v2.0.0: per contract het sjabloon waarmee het werd opgemaakt

function inhoudV1(c) {
  const v = normaliseer(c.velden);
  const ref = esc(c.referentie || '');
  const datum = datumNL(c.datum_ts || c.aangemaakt_ts);
  let n = 0; const H = (t) => `<h2><span>${++n}</span>${t}</h2>`;
  const contact = lijst(v.contactpersonen), gebr = lijst(v.gebruikers);
  const naam = esc(v.partner_naam);
  const art = {};
  let h = '';
  h += `<header><img src="${esc(c.logo_url || '')}" alt="Energie-Compas"><div class="ref">Referentie <b>${ref}</b><br>Datum: ${datum}<br>Geldigheid aanbod: ${v.geldigheid_dagen} dagen</div></header>`;
  h += `<h1>SAMENWERKINGSOVEREENKOMST</h1><p class="sub">Energie-Compas · No Cure No Pay${v.bd_aan ? ' · Business development' : ''} — ${naam}</p>`;
  h += H('Partijen'); art.partijen = n;
  h += `<div class="parties"><div class="party"><div class="lbl">Aanbieder</div><b>Fluctus.net CVSO</b><br>Dumolinlaan 1 bus 13, 8500 Kortrijk<br>BTW BE 0757.494.180<br>admin@fluctus.net<br>vertegenwoordigd door ${esc(v.fluctus_vert_naam)}, ${esc(v.fluctus_vert_functie)}<br>hierna “Fluctus”</div>`
    + `<div class="party"><div class="lbl">Opdrachtgever</div><b>${naam}</b><br>${regels(v.partner_adres)}<br>BTW ${esc(v.partner_btw || '……………………')}<br>vertegenwoordigd door ${esc(v.partner_vert_naam)}, ${esc(v.partner_vert_functie)}`
    + (contact.length ? `<br>contactpersonen: ${contact.map(esc).join(', ')}` : '') + `<br>hierna “${naam}”</div></div>`;
  if (v.afspraken_ref) h += `<p class="small">Deze overeenkomst legt de afspraken vast zoals besproken in ${esc(v.afspraken_ref)}.</p>`;

  h += H('Voorwerp'); art.voorwerp = n;
  h += `<p>Fluctus stelt ${naam} het platform Energie-Compas ter beschikking om voor haar klanten (particulier, laagspanning en middenspanning) studies te maken voor zonnepanelen, batterijopslag en laadinfrastructuur${v.bd_aan ? ', en ondersteunt ' + naam + ' optioneel met business development op uurbasis' : ''}.</p>`
    + `<ul><li><b>Input:</b> energiefactuur en/of verbruiksprofiel (kwartierdata), gegevens van de site en bestaande installaties.</li>`
    + `<li><b>Resultaat:</b> een onderbouwd rapport met dimensionering, energiebalans, besparing, terugverdientijd en financieringsplan per site.</li></ul>`
    + `<p>De vergoeding voor Energie-Compas betaalt het versnelde verkoopproces (studies + platform) en is enkel verschuldigd wanneer er een verkoop volgt.</p>`;

  h += H('Energie-Compas: toegang' + (v.whitelabel ? ' en white-label' : '')); art.toegang = n;
  if (v.toegang_domein) h += `<ul><li>Alle medewerkers van ${naam} met een e-mailadres op <b>@${esc(v.toegang_domein)}</b> krijgen toegang tot Energie-Compas als adviseur, vanaf het opmaken van deze overeenkomst en ook vóór de ondertekening. Zij melden zich aan met hun e-mailadres en een eenmalige code.</li>`
    + `<li>Fluctus kan deze toegang op elk moment in één keer opschorten of beëindigen, onder meer wanneer deze overeenkomst niet binnen de geldigheidstermijn wordt ondertekend of wanneer ze eindigt. ${naam} verwittigt Fluctus wanneer een medewerker het e-mailadres niet langer mag gebruiken voor Energie-Compas.</li>`;
  else h += `<ul><li>${naam} krijgt toegang op uitnodiging.${gebr.length ? ' Bij de start worden uitgenodigd: ' + gebr.map(esc).join(', ') + '.' : ''} Bijkomende gebruikers op vraag van ${naam}.</li>`;
  if (v.whitelabel) h += `<li><b>White-label:</b> de rapporten verschijnen op naam van ${naam}, in de huisstijl en branding van ${naam}, en worden door ${naam} onder eigen naam aan haar klanten voorgesteld.</li>`;
  if (v.offerte_optie) h += `<li><b>Optie offerte (inbegrepen in dezelfde vergoeding per site):</b> vanuit het platform kan een offerte op naam van ${naam} worden opgemaakt, onder de opschortende voorwaarde van een technische inspectie ter plaatse.</li>`;
  h += `<li>Het platform, de rekenmethode, de modellen en de software blijven eigendom van Fluctus. ${naam} verkrijgt een gebruiksrecht voor de duur van deze overeenkomst; de opgemaakte rapporten mag ${naam} onbeperkt gebruiken voor haar klanten.</li></ul>`;

  h += H('Traject per dossier'); art.traject = n;
  h += `<ol class="steps"><li>${naam} start een dossier in Energie-Compas (factuur en/of profiel opladen, of doorsturen naar het dossier-adres).</li>`
    + `<li>Het platform leest de gegevens in; ${naam} bevestigt afname/injectie en de site-gegevens.</li>`
    + `<li>Simulatie van zonnepanelen, batterij en/of laadpalen in Energie-Compas.</li>`
    + `<li>Rapport${v.whitelabel ? ' op naam en in de huisstijl van ' + naam : ''}, klaar om aan de klant voor te stellen.</li>`
    + (v.offerte_optie ? `<li>Optioneel: offerte vanuit het platform op naam van ${naam}, onder opschortende voorwaarde van technische inspectie.</li>` : '')
    + `<li>Bij akkoord van de klant meldt ${naam} de verkoop via de knop «Offertes» in het platform.</li>`
    + (v.imby ? `<li>Onboarding bij imby: registratie van de offerte en opstart — inbegrepen.</li>` : '') + `</ol>`;

  h += H('No Cure No Pay — wanneer is de vergoeding verschuldigd?'); art.ncnp = n;
  h += `<div class="box"><p><b>Verschuldigd</b> wanneer de klant met ${naam} een akkoord sluit (getekende offerte of bestelbon) voor zonnepanelen, een batterij en/of laadpalen, voor een site die in Energie-Compas werd doorgerekend, en dit <b>binnen ${v.ncnp_maanden} maanden na de laatste simulatie</b> van die site.</p>`
    + `<p style="margin:0"><b>Niet verschuldigd</b> wanneer er geen verkoop volgt. Er is geen vergoeding per studie, per gebruiker of per maand.</p></div>`
    + `<ul><li><b>Eén vergoeding per site</b> (aansluiting / EAN), ongeacht de omvang van de bestelling of het aantal technologieën.</li>`
    + `<li>Latere uitbreidingen op dezelfde site zijn inbegrepen, tenzij daarvoor een nieuwe simulatie wordt gemaakt; dan geldt die nieuwe simulatie als nieuw dossier.</li>`
    + `<li>${naam} meldt een verkoop binnen <b>${v.melding_dagen} kalenderdagen</b> na het akkoord van de klant via de knop «Offertes» in het platform.</li></ul>`;

  h += H('Vergoeding per site'); art.vergoeding = n;
  h += `<table class="keep"><tr><th>Type site</th><th>Omvang studie</th><th style="text-align:right">Vergoeding (excl. btw)</th></tr>`
    + `<tr><td>Particulier</td><td>Residentiële aansluiting</td><td class="n">${eur(v.tarief_part)}</td></tr>`
    + `<tr><td>Laagspanning (LS)</td><td>Situatie vóór én na installatie</td><td class="n">${eur(v.tarief_ls)}</td></tr>`
    + `<tr><td>Middenspanning (MS)</td><td>Situatie vóór óf na installatie</td><td class="n">${eur(v.tarief_ms)}</td></tr></table>`
    + `<p class="small">Bedragen exclusief btw. De vergoeding omvat het gebruik van Energie-Compas${v.whitelabel ? ', het white-label rapport' : ', het rapport'}${v.offerte_optie ? ', de optionele offerte vanuit het platform' : ''}${v.imby ? ' en de onboarding bij imby' : ''} voor die site.</p>`;

  h += H('Facturatie en betaling'); art.facturatie = n;
  h += `<ul><li>Fluctus factureert de vergoeding op het moment van de melding van de verkoop (artikel ${art.ncnp}).</li><li>Betaling binnen ${v.betaal_dagen} dagen na factuurdatum.</li></ul>`;

  if (v.bd_aan) {
    h += H('Business development (optioneel, op uurbasis)'); art.bd = n;
    h += `<p>Buiten het Energie-Compas-traject ondersteunt Fluctus ${naam} op projectbasis, onder meer bij:</p>`
      + `<ul><li>analyse, dimensionering en verdediging van dossiers;</li><li>een fast track voor kleinere projecten;</li><li>coaching in verkopen op waarde;</li><li>de customer journey elektrificatie;</li><li>dossiers rond financiering en verhoogde investeringsaftrek.</li></ul>`
      + `<ul><li>Per opdracht worden de scope en een tijdsinschatting vooraf afgesproken en door ${naam} goedgekeurd. De inschatting geldt als referentie; dreigt ze overschreden te worden, dan verwittigt Fluctus ${naam} vooraf.</li>`
      + `<li>Tarief: <b>${eur(v.bd_tarief)} excl. btw per begonnen uur</b>.</li>`
      + `<li>Maandelijkse facturatie op basis van een gedetailleerde timesheet; betaling binnen ${v.bd_betaal_dagen} dagen.</li>`
      + `<li>Contact met de eindklant enkel op vraag van ${naam}.</li></ul>`
      + `<p class="small">Binnen het Energie-Compas-traject (artikel ${art.traject}) worden geen uren in regie aangerekend.</p>`;
  }

  h += H('Grondslag sturing en EMS'); art.ems = n;
  h += `<p>De simulaties in Energie-Compas rekenen de waarde van een batterij en de sturing door op basis van zelfconsumptie, piekshaving en energy arbitrage op de spotmarkt (day-ahead / intraday). Opbrengsten uit imbalance settlement (passieve respons op de onbalansprijs) worden indicatief weergegeven; de effectieve opbrengst hangt af van de sturing en het energiecontract na installatie bij de leverancier. Resultaten zijn een onderbouwde raming, geen gewaarborgde opbrengst.</p>`;

  h += H('Vertrouwelijkheid'); art.vertrouwelijk = n;
  h += v.nda_datum
    ? `<p>De geheimhoudingsovereenkomst (NDA) die partijen op ${esc(v.nda_datum)} ondertekenden, is van toepassing op alle informatie die in het kader van deze overeenkomst wordt uitgewisseld, inclusief klant- en verbruiksgegevens in het platform. Fluctus verwerkt klantgegevens enkel om de diensten onder deze overeenkomst te leveren.</p>`
    : `<p>Partijen behandelen alle informatie die in het kader van deze overeenkomst wordt uitgewisseld, inclusief klant- en verbruiksgegevens in het platform, als vertrouwelijk en gebruiken ze enkel voor de uitvoering van deze overeenkomst. Fluctus verwerkt klantgegevens enkel om de diensten onder deze overeenkomst te leveren.</p>`;

  h += H('Samenwerking, duur en evaluatie'); art.duur = n;
  h += `<ul>${v.kickoff ? '<li>Pragmatische start met een kick-off via Teams van 1 uur.</li>' : ''}`
    + `<li>Partijen evalueren de samenwerking minstens om de ${v.evaluatie_maanden} maanden en passen de afspraken zo nodig in onderling akkoord aan.</li>`
    + `<li>Deze overeenkomst geldt voor onbepaalde duur en kan door elke partij schriftelijk worden opgezegd met een opzegtermijn van ${v.opzeg_maanden} maanden. Voor sites die vóór het einde van de overeenkomst werden doorgerekend, blijft artikel ${art.ncnp} van toepassing gedurende de termijn van ${v.ncnp_maanden} maanden.</li>`
    + `<li>Op deze overeenkomst is het Belgisch recht van toepassing.</li></ul>`;

  if (v.extra_bepalingen) { h += H('Bijzondere bepalingen'); h += `<p>${regels(v.extra_bepalingen)}</p>`; }

  h += H('Elektronische ondertekening'); art.teken = n;
  h += `<p>Partijen aanvaarden dat deze overeenkomst elektronisch wordt ondertekend via het Energie-Compas-platform. De elektronische handtekening heeft voor partijen dezelfde bewijskracht als een handgeschreven handtekening. Het platform registreert per ondertekening de naam, de functie, het e-mailadres, het tijdstip en het IP-adres van de ondertekenaar, samen met een digitale vingerafdruk (SHA-256) van de ondertekende tekst.</p>`;
  return h;
}

// v2.0.0 (Johan 19-10): tekst van het herwerkte partnercontract.
function inhoudV2(c) {
  const v = normaliseer(c.velden);
  const ref = esc(c.referentie || '');
  const datum = datumNL(c.datum_ts || c.aangemaakt_ts);
  let n = 0; const H = (t) => `<h2><span>${++n}</span>${t}</h2>`;
  const contact = lijst(v.contactpersonen), gebr = lijst(v.gebruikers);
  const naam = esc(v.partner_naam);
  const art = {};
  let h = '';
  h += `<header><img src="${esc(c.logo_url || '')}" alt="Energie-Compas"><div class="ref">Referentie <b>${ref}</b><br>Datum: ${datum}<br>Geldigheid aanbod: ${v.geldigheid_dagen} dagen</div></header>`;
  h += `<h1>SAMENWERKINGSOVEREENKOMST</h1><p class="sub">Energie-Compas · No Cure No Pay${v.bd_aan ? ' · Business development' : ''} — ${naam}</p>`;
  h += H('Partijen'); art.partijen = n;
  h += `<div class="parties"><div class="party"><div class="lbl">Aanbieder</div><b>Fluctus.net CVSO</b><br>Dumolinlaan 1 bus 13, 8500 Kortrijk<br>BTW BE 0757.494.180<br>admin@fluctus.net<br>vertegenwoordigd door ${esc(v.fluctus_vert_naam)}, ${esc(v.fluctus_vert_functie)}<br>hierna “Fluctus”</div>`
    + `<div class="party"><div class="lbl">Opdrachtgever</div><b>${naam}</b><br>${regels(v.partner_adres)}<br>BTW ${esc(v.partner_btw || '……………………')}<br>vertegenwoordigd door ${esc(v.partner_vert_naam)}, ${esc(v.partner_vert_functie)}`
    + (contact.length ? `<br>contactpersonen: ${contact.map(esc).join(', ')}` : '') + `<br>hierna “${naam}”</div></div>`;
  h += `<p class="small">Deze overeenkomst legt de afspraken vast tussen partijen zoals besproken tot op de datum van ondertekening${v.afspraken_ref ? ', onder meer in ' + esc(v.afspraken_ref) : ''}.</p>`;

  h += H('Voorwerp'); art.voorwerp = n;
  h += `<p>Fluctus stelt ${naam} het platform Energie-Compas ter beschikking om voor haar klanten (particulier en bedrijven met laagspanning of middenspanning) studies te maken voor zonnepanelen, batterijopslag en laadinfrastructuur${v.bd_aan ? ', en ondersteunt ' + naam + ' optioneel met business development op uurbasis' : ''}.</p>`
    + `<ul><li><b>Input:</b> energiefactuur en/of verbruiksprofiel (kwartierdata), gegevens van de site en bestaande installaties.</li>`
    + `<li><b>Resultaat:</b> een onderbouwd rapport met dimensionering, energiebalans, besparing, terugverdientijd en financieringsplan per site.</li></ul>`
    + `<p>Een No Cure No Pay-vergoeding is verschuldigd voor het gebruik van Energie-Compas en wordt aangerekend voor de versnelling van het verkoopproces van ${naam}; ze is enkel verschuldigd wanneer er een verkoop door ${naam} volgt.`
    + (v.bd_aan ? `<br>De vergoeding voor business development wordt op basis van een uurtarief aangerekend, volgens een maandelijkse rapportering van de geleverde prestaties.` : '') + `</p>`;

  h += H('Energie-Compas: toegang' + (v.whitelabel ? ' en white-label' : '')); art.toegang = n;
  if (v.toegang_domein) h += `<ul><li>Alle medewerkers van ${naam} met een e-mailadres op <b>@${esc(v.toegang_domein)}</b> krijgen toegang tot Energie-Compas als adviseur, vanaf het opmaken van deze overeenkomst en ook vóór de ondertekening. Zij melden zich aan met hun e-mailadres en een eenmalige code.</li>`
    + `<li>Fluctus kan deze toegang op elk moment in één keer opschorten of beëindigen, onder meer wanneer deze overeenkomst niet binnen de geldigheidstermijn wordt ondertekend of wanneer ze eindigt. ${naam} verwittigt Fluctus wanneer een medewerker het e-mailadres niet langer mag gebruiken voor Energie-Compas.</li>`;
  else h += `<ul><li>${naam} krijgt toegang op uitnodiging.${gebr.length ? ' Bij de start worden uitgenodigd: ' + gebr.map(esc).join(', ') + '.' : ''} Bijkomende gebruikers op vraag van ${naam}.</li>`;
  if (v.whitelabel) h += `<li><b>Optie white-label:</b> de rapporten verschijnen op naam van ${naam}, in de huisstijl en branding van ${naam}, en worden door ${naam} onder eigen naam aan haar klanten voorgesteld.</li>`;
  if (v.offerte_optie) h += `<li><b>Optie offerte (inbegrepen in dezelfde vergoeding per site):</b> vanuit het platform kan een offerte op naam van ${naam} worden opgemaakt, onder de opschortende voorwaarde van een technische inspectie ter plaatse.</li>`;
  h += `<li>Het platform, de rekenmethode, de modellen en de software blijven eigendom van Fluctus. ${naam} verkrijgt een gebruiksrecht voor de duur van deze overeenkomst; de opgemaakte rapporten mag ${naam} onbeperkt gebruiken voor haar klanten.</li></ul>`;

  h += H('Traject per dossier'); art.traject = n;
  h += `<ol class="steps"><li>${naam} start een dossier in Energie-Compas (factuur en/of profiel opladen, of doorsturen naar het dossier-adres).</li>`
    + `<li>Het platform leest de gegevens in; ${naam} bevestigt afname/injectie en de site-gegevens.</li>`
    + `<li>Simulatie van zonnepanelen, batterij en/of laadpalen in Energie-Compas.</li>`
    + `<li>Rapport, al dan niet op naam en in de huisstijl van ${naam}, klaar om aan de klant voor te stellen.</li>`
    + (v.offerte_optie ? `<li>Optioneel: offerte vanuit het platform op naam van ${naam}, onder opschortende voorwaarde van technische inspectie.</li>` : '')
    + `<li>Bij akkoord van de klant meldt ${naam} de verkoop op het platform.</li>`
    + `<li>Indien Fluctus binnen ${v.feedback_dagen} dagen na oplevering van de analyse geen feedback van ${naam} via het platform krijgt, heeft Fluctus het recht om contact te leggen met de eindklant/verbruiker op de site.</li></ol>`
    + `<p>Alle transacties en bewegingen worden in een logbestand op het platform bijgehouden om de dienstverlening aan ${naam} en haar prospects en klanten te verbeteren. De loggegevens van het platform gelden als rechtsgeldig bewijs tussen partijen.</p>`;

  h += H('No Cure No Pay — wanneer is de vergoeding verschuldigd?'); art.ncnp = n;
  h += `<div class="box"><p><b>Verschuldigd</b> wanneer de klant met ${naam} een akkoord sluit (getekende offerte of bestelbon) voor zonnepanelen, een batterij en/of laadpalen, voor een site die in Energie-Compas werd doorgerekend, en dit <b>binnen ${v.ncnp_maanden} maanden na de laatste simulatie</b> van die site.`
    + `<br>Dit geldt voor elke site die door een medewerker van ${naam}, zoals gedefinieerd in artikel ${art.toegang}, werd doorgerekend.`
    + `<br>${naam} meldt een verkoop binnen <b>${v.melding_dagen} kalenderdagen</b> na het akkoord van de klant via de knop «Offertes» in het platform.</p>`
    + `<p style="margin:0"><b>Niet verschuldigd</b> wanneer er geen verkoop volgt. Er is geen vergoeding per studie, per gebruiker of per maand.</p></div>`
    + `<ul><li><b>Eén vergoeding per site</b> (aansluiting / EAN), ongeacht de omvang van de bestelling of het aantal technologieën.</li>`
    + `<li>Latere uitbreidingen op dezelfde site zijn inbegrepen, tenzij daarvoor een nieuwe simulatie wordt gemaakt; dan geldt die nieuwe simulatie als nieuw dossier.</li></ul>`;

  h += H('Vergoeding per site'); art.vergoeding = n;
  h += `<table class="keep"><tr><th>Type site</th><th>Omvang studie</th><th style="text-align:right">Vergoeding (excl. btw)</th></tr>`
    + `<tr><td>Particulier</td><td>Residentiële aansluiting</td><td class="n">${eur(v.tarief_part)}</td></tr>`
    + `<tr><td>Laagspanning (LS)</td><td>Situatie vóór én na installatie</td><td class="n">${eur(v.tarief_ls)}</td></tr>`
    + `<tr><td>Middenspanning (MS)</td><td>Situatie vóór óf na installatie</td><td class="n">${eur(v.tarief_ms)}</td></tr></table>`
    + `<p class="small">Bedragen exclusief btw. De vergoeding omvat het gebruik van Energie-Compas${v.whitelabel ? ', het white-label rapport' : ', het rapport'}${v.offerte_optie ? ', de optionele offerte vanuit het platform' : ''}${v.imby ? ' en de onboarding bij imby' : ''} voor die site.</p>`;

  h += H('Facturatie en betaling'); art.facturatie = n;
  h += `<ul><li>Fluctus factureert de vergoeding op het moment van de melding van de verkoop (artikel ${art.ncnp}).</li>`
    + `<li>Bij de melding zal ${naam} de nodige referenties meegeven zoals die moeten worden vermeld op de factuur.</li>`
    + `<li>Betaling binnen ${v.betaal_dagen} dagen na factuurdatum.</li></ul>`;

  if (v.bd_aan) {
    h += H('Business development (optioneel, op uurbasis)'); art.bd = n;
    h += `<p>Buiten het Energie-Compas-traject ondersteunt Fluctus ${naam} op projectbasis, onder meer bij:</p>`
      + `<ul><li>analyse, dimensionering en verdediging van dossiers;</li><li>een fast track voor kleinere projecten;</li><li>coaching in verkopen op waarde;</li><li>de customer journey elektrificatie;</li><li>dossiers rond financiering en verhoogde investeringsaftrek.</li></ul>`
      + `<p>Voorwaarden:</p><ul><li>Per opdracht worden de scope en een tijdsinschatting vooraf afgesproken en door ${naam} goedgekeurd. De inschatting geldt als referentie; dreigt ze overschreden te worden, dan verwittigt Fluctus ${naam} vooraf.</li>`
      + `<li>Tarief: <b>${eur(v.bd_tarief)} excl. btw per begonnen uur</b>.</li>`
      + `<li>Maandelijkse facturatie op basis van een gedetailleerde timesheet; betaling binnen ${v.bd_betaal_dagen} dagen.</li>`
      + `<li>Contact met de eindklant enkel op vraag van ${naam}.</li></ul>`
      + `<p class="small">Binnen het Energie-Compas-traject (artikel ${art.traject}) worden geen uren in regie aangerekend.</p>`;
  }

  h += H('Grondslag sturing en EMS'); art.ems = n;
  h += `<p>De simulaties in Energie-Compas rekenen de waarde van een batterij en de sturing door op basis van zelfconsumptie, piekshaving en energy arbitrage op de spotmarkt (day-ahead / intraday). Opbrengsten door het laden van EV's op de locaties van de klant worden berekend aan de hand van de vermeden kosten door elders te laden of te tanken. Opbrengsten uit onbalans worden indicatief weergegeven; die opbrengst hangt af van de sturing en het energiecontract <u><i>na</i></u> installatie bij de leverancier. Resultaten zijn een onderbouwde raming, geen gewaarborgde opbrengst en kunnen nooit aanleiding geven tot claims.</p>`;

  h += H('Vertrouwelijkheid en non-circumventie'); art.vertrouwelijk = n;
  h += v.nda_datum
    ? `<p>De geheimhoudingsovereenkomst (NDA) die partijen op ${esc(v.nda_datum)} ondertekenden, is van toepassing op alle informatie die in het kader van deze overeenkomst wordt uitgewisseld, inclusief klant- en verbruiksgegevens in het platform. Fluctus verwerkt klantgegevens enkel om de diensten onder deze overeenkomst te leveren.</p>`
    : `<p>Partijen behandelen alle informatie die in het kader van deze overeenkomst wordt uitgewisseld, inclusief klant- en verbruiksgegevens in het platform, als vertrouwelijk en gebruiken ze enkel voor de uitvoering van deze overeenkomst. Fluctus verwerkt klantgegevens enkel om de diensten onder deze overeenkomst te leveren.</p>`;
  h += `<p><b>Non-circumventie.</b> Partijen omzeilen elkaar niet. Concreet:</p><ul>`
    + `<li>${naam} sluit voor een site die in Energie-Compas werd doorgerekend geen akkoord met de klant buiten het platform om, rechtstreeks of via een verbonden vennootschap, onderaannemer of derde, met als doel de vergoeding van artikel ${art.vergoeding} te ontlopen. Zo'n akkoord geldt als verkoop in de zin van artikel ${art.ncnp} en de vergoeding blijft verschuldigd.</li>`
    + `<li>${naam} gebruikt de rapporten, berekeningen en gegevens uit Energie-Compas niet om dezelfde studies buiten het platform te laten maken of door te geven aan derden, behalve aan de eigen klant voor wie het rapport werd opgemaakt.</li>`
    + `<li>${naam} contacteert partners en leveranciers die Fluctus in het kader van deze overeenkomst introduceerde (zoals imby of EMS-leveranciers) niet rechtstreeks om de diensten van Fluctus te vervangen, zonder voorafgaand schriftelijk akkoord van Fluctus.</li>`
    + `<li>Fluctus benadert de klanten en prospects van ${naam} die via het platform bekend werden niet rechtstreeks om er zelf of via een andere partner aan te verkopen, behalve in het geval van artikel ${art.traject} (geen feedback van ${naam} via het platform binnen ${v.feedback_dagen} dagen) of op vraag van ${naam}.</li></ul>`
    + `<p>Deze verplichting geldt tijdens de duur van de overeenkomst en tot <b>${v.nc_maanden} maanden</b> na het einde ervan. Bij een inbreuk is per betrokken site de vergoeding van artikel ${art.vergoeding} verschuldigd${v.nc_boete_pct > 0 ? `, verhoogd met een forfaitaire schadevergoeding van <b>${nl(v.nc_boete_pct)} %</b>` : ''}, onverminderd het recht om een hogere werkelijke schade te bewijzen.</p>`;

  h += H('Samenwerking, duur en evaluatie'); art.duur = n;
  h += `<ul>${v.kickoff ? '<li>Pragmatische start met een kick-off via Teams van 1 uur.</li>' : ''}`
    + `<li>Partijen evalueren de samenwerking minstens om de ${v.evaluatie_maanden} maanden en passen de afspraken zo nodig in onderling akkoord aan.</li>`
    + `<li>Deze overeenkomst geldt voor onbepaalde duur en kan door elke partij schriftelijk worden opgezegd met een opzegtermijn van ${v.opzeg_maanden} maanden. Voor sites die vóór het einde van de overeenkomst werden doorgerekend, blijft artikel ${art.ncnp} van toepassing gedurende de termijn van ${v.ncnp_maanden} maanden.</li>`
    + `<li>Op deze overeenkomst is het Belgisch recht van toepassing.</li></ul>`;

  if (v.extra_bepalingen) { h += H('Bijzondere bepalingen'); h += `<p>${regels(v.extra_bepalingen)}</p>`; }

  h += H('Elektronische ondertekening'); art.teken = n;
  h += `<p>Partijen aanvaarden dat deze overeenkomst elektronisch wordt ondertekend via het Energie-Compas-platform. De elektronische handtekening heeft voor partijen dezelfde bewijskracht als een handgeschreven handtekening. Het platform registreert per ondertekening de naam, de functie, het e-mailadres, het tijdstip en het IP-adres van de ondertekenaar, samen met een digitale vingerafdruk (SHA-256) van de ondertekende tekst.</p>`;
  return h;
}

function hashInhoud(c) { return crypto.createHash('sha256').update(inhoud(c), 'utf8').digest('hex'); }

// Volledig document: inhoud + ondertekeningsblok + audit (status-afhankelijk).
function document(c, opts) {
  opts = opts || {};
  const v = normaliseer(c.velden);
  const naam = esc(v.partner_naam);
  const F = c.fluctus_tekening || null, P = c.getekend || null;
  let s = '';
  s += `<div class="sign">`
    + `<div><b>Voor Fluctus.net CVSO</b>${F ? `${F.handtekening ? `<img src="${esc(F.handtekening)}" alt="handtekening">` : ''}<div class="stempel">✔ elektronisch ondertekend</div><br>${esc(F.naam)}, ${esc(F.functie)}<br><span class="small">${tijdNL(F.ts)}</span>` : `<div class="leeg"></div>${esc(v.fluctus_vert_naam)}, ${esc(v.fluctus_vert_functie)}`}</div>`
    + `<div><b>Voor ${naam}</b>${P ? `${P.handtekening ? `<img src="${esc(P.handtekening)}" alt="handtekening">` : ''}<div class="stempel">✔ elektronisch ondertekend</div><br>${esc(P.naam)}, ${esc(P.functie)}<br><span class="small">${tijdNL(P.ts)}</span>` : `<div class="leeg"></div>${esc(v.partner_vert_naam)}, ${esc(v.partner_vert_functie)}`}</div>`
    + `</div>`;
  if (c.hash) {
    s += `<div class="audit"><b>Ondertekeningsbewijs</b><br>Referentie ${esc(c.referentie)} · sjabloon ${esc(c.sjabloon || SJABLOON_ID)}<br>Vingerafdruk tekst (SHA-256): <b>${esc(c.hash)}</b>`
      + (F ? `<br>Fluctus: ${esc(F.naam)} (${esc(F.mail || '')}) — verzonden ter ondertekening op ${tijdNL(F.ts)}` : '')
      + (c.verstuurd_naar ? ` naar ${esc(c.verstuurd_naar)}` : '')
      + (P ? `<br>${naam}: ${esc(P.naam)}, ${esc(P.functie)} (${esc(P.mail || '')}) — ondertekend op ${tijdNL(P.ts)} · IP ${esc(P.ip || '?')} · ${esc(String(P.ua || '').slice(0, 140))}`
        + `<br>Vingerafdruk bij ondertekening: <b>${esc(P.hash || '')}</b> ${P.hash === c.hash ? '(identiek aan de verzonden tekst)' : '(AFWIJKEND)'}` : '')
      + `</div>`;
  }
  const concept = !P && opts.watermerk !== false && c.status !== 'getekend';
  return `<!doctype html><html lang="nl"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>${esc(c.referentie || 'Overeenkomst')} — ${naam}</title><style>${CSS}</style></head><body>`
    + (concept ? `<div class="concept">${c.status === 'verstuurd' ? 'TER ONDERTEKENING' : 'CONCEPT'}</div>` : '')
    + `<div class="page">${inhoud(c)}${s}<footer>Fluctus.net CVSO · Dumolinlaan 1 bus 13, 8500 Kortrijk · BTW BE 0757.494.180 · admin@fluctus.net · Energie-Compas is een platform van Fluctus.net CVSO</footer></div></body></html>`;
}

function referentieSlug(naam) { return String(naam || 'PARTNER').normalize('NFD').replace(/[̀-ͯ]/g, '').toUpperCase().replace(/[^A-Z0-9]+/g, '').slice(0, 10) || 'PARTNER'; }

module.exports = { SJABLOON_ID, SJABLOON_ID_V1, SJABLOON_NAAM, inhoudV1, inhoudV2, DEFAULTS, normaliseer, ontbrekend, inhoud, hashInhoud, document, referentieSlug, normDomein, domeinFout, mailOpDomein, VERSIE: '2.0.0' };
