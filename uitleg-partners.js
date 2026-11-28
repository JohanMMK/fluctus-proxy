'use strict';
// Uitlegvideo «Drie deuren, twee sporen» (ewb.js v1.6.0) — samenwerking installateur (EPC) × energie-adviseur (Elindus).
// Zelfde opbouw en stijl als uitleg-flows.js: elk beeld = 1120×630 HTML (campagnefoto + infografiek) + één stuk spreektekst (rol «docu»).
// Inhoud = brochure «Go-to-market — van factuur naar bindende EPC-offerte» + videoprompt 28-11 (KB).
// Leadkanalen: Elindus-profiel (vóór de call), mail (vóór of na het bezoek), WhatsApp (tijdens het bezoek).
// Spelregel: elk spoor blijft bij zijn vak; over het andere vak enkel bevestigen, niet brengen.
// Adressen en telefoonnummer staan in beeld en worden niet uitgesproken.

const { BW, BH } = require('./uitleg-flows.js');
const GROEN = '#05B050', BL_GROEN = '#6BAE41', NAVY = '#101a30', NAVY2 = '#1a2846', HW = '#5b9bd5';
const esc = s => String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

function basis(inhoud, o = {}) {
  const AB = o.AB;
  const bg = o.bg ? `url('${AB}/ec-img/campagne/${o.bg}.jpg') ${o.pos || 'center'}/${o.maat || 'cover'} no-repeat` : `radial-gradient(ellipse at 50% 40%,#22345c 0%,#14203a 65%)`;
  const rechts = !!o.rechts;
  const sluier = o.bg ? (rechts
    ? `linear-gradient(270deg,${NAVY} 0%,${NAVY} 50%,rgba(16,26,48,.86) 60%,rgba(16,26,48,.25) 80%,rgba(16,26,48,.15) 100%)`
    : `linear-gradient(90deg,${NAVY} 0%,${NAVY} 50%,rgba(16,26,48,.86) 60%,rgba(16,26,48,.25) 80%,rgba(16,26,48,.15) 100%)`) : 'none';
  return `<!doctype html><html><head><meta charset="utf-8"><link href="https://fonts.googleapis.com/css2?family=Poppins:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<style>*{margin:0;padding:0;box-sizing:border-box}
body{width:${BW}px;height:${BH}px;overflow:hidden;font-family:Poppins,Arial,sans-serif;color:#fff;background:${bg};position:relative}
.sl{position:absolute;inset:0;background:${sluier}}
.pn{position:absolute;top:0;bottom:0;${rechts ? 'right:0' : 'left:0'};width:${o.breed || 600}px;padding:40px 46px 34px 54px;display:flex;flex-direction:column;justify-content:center}
.lg{height:26px;display:block}.lgr{display:flex;gap:18px;align-items:center;margin-bottom:20px}
.ey{display:flex;align-items:center;gap:10px;color:${GROEN};font-weight:600;letter-spacing:2.5px;text-transform:uppercase;font-size:13px;margin-bottom:12px}
.ey:before{content:'';width:3px;height:16px;background:${GROEN};display:block}
h1{font-size:34px;line-height:1.14;font-weight:700;letter-spacing:-.3px}
h1 em{font-style:normal;color:${GROEN}}
.sub{font-size:16px;color:#b9c4d6;line-height:1.5;margin-top:12px}
.band{position:absolute;left:0;right:0;bottom:0;height:6px;background:${GROEN}}
.merk{position:absolute;${rechts ? 'right:46px' : 'left:54px'};bottom:22px;font-size:11px;font-weight:700;letter-spacing:1.5px;color:#9fb0c8}
.kaart{background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.13);border-radius:14px;padding:14px 16px}
.chip{display:inline-flex;align-items:center;gap:8px;padding:7px 14px;border-radius:999px;background:rgba(5,176,80,.14);border:1px solid rgba(5,176,80,.55);color:#dff7e8;font-size:14px;font-weight:600}
.n{font-variant-numeric:tabular-nums}
.wm{font-weight:700;font-size:15px;letter-spacing:.3px;color:#fff}
${o.css || ''}</style></head><body><div class="sl"></div>${o.los || ''}<div class="pn">${inhoud}</div>${o.merk === false ? '' : `<div class="merk">ENERGIE-COMPAS</div>`}<div class="band"></div></body></html>`;
}
const ecLogo = (AB, h = 26) => `<img class="lg" style="height:${h}px" src="${AB}/apps/ec-logo-dark.png">`;
const blLogo = (AB, h = 30) => `<img class="lg" style="height:${h}px" src="${AB}/apps/bl/betere-lader-logo-wit.svg">`;

// eenvoudige lijn-iconen (geen emoji: Chromium op de server heeft geen emoji-lettertype)
const IC = {
  stekker: '<path d="M9 2v6M15 2v6M6 8h12v4a6 6 0 0 1-12 0zM12 18v4" />',
  batterij: '<rect x="3" y="7" width="16" height="10" rx="2"/><path d="M21 11v2M7 10v4M11 10v4"/>',
  zon: '<circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/>',
  mail: '<rect x="3" y="5" width="18" height="14" rx="2"/><path d="M3 7l9 6 9-6"/>',
  check: '<path d="M5 12l5 5 9-10"/>',
  pijl: '<path d="M5 12h14M13 6l6 6-6 6"/>',
  tel: '<path d="M5 4h4l2 5-2.5 1.5a11 11 0 0 0 5 5L15 13l5 2v4a2 2 0 0 1-2 2A16 16 0 0 1 3 6a2 2 0 0 1 2-2"/>',
  kaart: '<path d="M9 4L3 6v14l6-2 6 2 6-2V4l-6 2z"/><path d="M9 4v14M15 6v14"/>',
  data: '<path d="M4 19V9M10 19V5M16 19v-7M22 19H2"/>',
  contract: '<path d="M7 3h8l4 4v14H7z"/><path d="M15 3v4h4M10 12h6M10 16h6"/>'
};
const ic = (k, s = 22, kl = '#fff', w = 1.8) => `<svg width="${s}" height="${s}" viewBox="0 0 24 24" fill="none" stroke="${kl}" stroke-width="${w}" stroke-linecap="round" stroke-linejoin="round">${IC[k]}</svg>`;

// tijdlijn vóór · tijdens · na het bezoek (actief = indexen)
function tijdlijn(actief, labels = ['Vóór het bezoek', 'Tijdens het bezoek', 'Na het bezoek']) {
  const punt = (t, i) => { const a = actief.includes(i); return `<div style="flex:1;text-align:center;position:relative;z-index:1">
    <div style="width:${a ? 22 : 14}px;height:${a ? 22 : 14}px;border-radius:50%;margin:${a ? 0 : 4}px auto 0;background:${a ? GROEN : '#4a5a74'};box-shadow:${a ? '0 0 0 6px rgba(5,176,80,.22)' : 'none'}"></div>
    <div style="font-size:12.5px;margin-top:8px;font-weight:${a ? 700 : 500};color:${a ? '#fff' : '#8193ad'}">${t}</div></div>`; };
  return `<div style="position:relative;display:flex;margin-top:22px;width:470px">
    <div style="position:absolute;left:16%;right:16%;top:10px;height:2px;background:#3a4a63"></div>${labels.map(punt).join('')}</div>`;
}
const regel = (k, t, kl = GROEN) => `<div style="display:flex;gap:11px;align-items:center;font-size:15.5px;line-height:1.35">${ic(k, 20, kl, 2.2)}<span>${t}</span></div>`;

function beelden({ AB, qr }) {
  const L = [];

  // 1 · Aankondiging
  L.push({ titel: 'Aankondiging', tekst: 'In drie minuten zie je hoe de installateur en de energie-adviseur samen sneller tot een bindende offerte komen, elk vanuit hun eigen vak.',
    html: basis(`<div style="margin:auto 0">
      <div class="lgr">${ecLogo(AB, 30)}<span style="width:1px;height:30px;background:rgba(255,255,255,.3)"></span>${blLogo(AB, 34)}</div>
      <div class="ey">Uitleg · voor verkopers en belsters</div>
      <h1 style="font-size:44px">Drie deuren,<br><em>twee sporen</em></h1>
      <div class="sub">Van lead naar bindende offerte. De installateur en de energie-adviseur, elk vanuit hun eigen vak.</div>
      <div style="display:flex;gap:10px;margin-top:22px;flex-wrap:wrap"><span class="chip">Installateur</span><span class="chip">Energie-adviseur</span><span class="chip">Eén platform</span></div></div>`, { AB, bg: 'b00-basis', pos: 'center' }) });

  // 2 · Twee sporen, één platform
  const kol = (kop, kl, items, sub) => `<div class="kaart" style="flex:1;padding:14px 12px;border-color:${kl}">
      <div style="font-size:15px;font-weight:700;color:#fff">${kop}</div><div style="font-size:11.5px;color:#9fb0c8;margin-bottom:10px">${sub}</div>
      ${items.map(t => `<div style="display:flex;gap:7px;align-items:center;font-size:13px;margin-top:6px">${ic('check', 14, kl, 2.6)}<span>${t}</span></div>`).join('')}</div>`;
  L.push({ titel: 'Twee sporen, één platform', tekst: 'Twee vakmensen, één klant. De installateur zorgt voor laadpalen, een batterij en zonnepanelen. De energie-adviseur zorgt voor het juiste contract. Energie-Compas en Betere Lader werken achter de schermen voor allebei: de data, de studie en het ontwerp. De klant ziet twee aparte gesprekken.',
    html: basis(`<div class="ey">Twee sporen, één platform</div><h1>Twee vakmensen,<br>één klant</h1>
      <div style="display:flex;gap:10px;margin-top:22px">
        ${kol('Hardware', HW, ['Laadpalen', 'Batterij', 'Zonnepanelen'], 'installateur')}
        ${kol('Platform', 'rgba(255,255,255,.35)', ['Data', 'Studie', 'Ontwerp'], 'achter de schermen')}
        ${kol('Energie', GROEN, ['Dynamisch contract', 'Intraday', 'Onbalans'], 'energie-adviseur')}</div>
      <div style="margin-top:18px"><span class="chip">De klant ziet twee aparte gesprekken</span></div>`, { AB, bg: 'kmo-transitie-1', pos: 'right center' }) });

  // 3 · Drie deuren
  const deur = (n, wat, vraag, mid) => `<div class="kaart" style="flex:1;padding:16px 12px;text-align:center;${mid ? `border:2px solid ${BL_GROEN};background:rgba(107,174,65,.12)` : ''}">
      <div style="font-size:11.5px;letter-spacing:1.5px;font-weight:700;color:${mid ? BL_GROEN : '#9fb0c8'}">DEUR ${n}</div>
      <div style="width:44px;height:58px;margin:10px auto 8px;border:2px solid ${mid ? BL_GROEN : '#8193ad'};border-radius:4px 4px 0 0;position:relative"><div style="position:absolute;right:7px;top:28px;width:5px;height:5px;border-radius:50%;background:${mid ? BL_GROEN : '#8193ad'}"></div></div>
      <div style="font-size:14.5px;font-weight:700;line-height:1.25">${wat}</div><div style="font-size:12.5px;color:#cdd6e3;margin-top:6px;line-height:1.3">${vraag}</div></div>`;
  L.push({ titel: 'Drie deuren', tekst: 'Elk gesprek vertrekt van waar de klant naar kijkt. Naar zijn aansluiting: kan ze meer dragen? Naar zijn elektrische wagens: kunnen we ze laden? Of naar zijn contract en zijn factuur: kan het goedkoper? De deur in het midden opent beide gesprekken.',
    html: basis(`<div class="ey">Waar kijkt de klant naar?</div><h1>Drie deuren naar<br>hetzelfde gesprek</h1>
      <div style="display:flex;gap:10px;margin-top:22px;align-items:stretch">
        ${deur(1, 'Zijn aansluiting', 'Kan ze meer dragen?')}
        ${deur(2, "Zijn EV's", 'Kunnen we ze laden?', true)}
        ${deur(3, 'Zijn contract en factuur', 'Kan het goedkoper?')}</div>
      <div style="display:flex;align-items:center;gap:12px;margin-top:18px">${blLogo(AB, 26)}<span style="font-size:14px;color:#dff7e8;font-weight:600">Deur 2 opent beide gesprekken</span></div>`, { AB, bg: 'w01-di', pos: 'right center', breed: 640 }) });

  // 4 · Leads en belsters
  const belster = (wie, deuren, kl) => `<div class="kaart" style="flex:1;border-color:${kl}"><div style="display:flex;gap:10px;align-items:center">${ic('tel', 20, kl, 2)}<span style="font-size:14px;font-weight:700">${wie}</span></div>
      <div style="font-size:13px;color:#cdd6e3;margin-top:8px">opent met</div><div style="font-size:22px;font-weight:800;color:${kl}">deur ${deuren}</div></div>`;
  L.push({ titel: 'Leads en belsters', tekst: 'De leads komen uit de Lead Generator: bedrijven met een eigen cabine en een grote parking, met het telefoonnummer erbij. En uit elkaars klantenlijsten, altijd via een intromail met het dossieradres in kopie. De belster van de installateur opent met deur één of twee, die van de energie-adviseur met deur twee of drie. Het doel van elke call: een bezoek, en liefst de factuur vooraf.',
    html: basis(`<div class="ey">Leads en belsters</div><h1 style="font-size:30px">Twee bronnen,<br>twee belsters</h1>
      <div style="display:flex;flex-direction:column;gap:8px;margin-top:18px">
        ${regel('kaart', '<b>Lead Generator</b> · cabine, parking, telefoon')}
        ${regel('mail', '<b>Elkaars klanten</b> · intromail, dossier@ in kopie')}</div>
      <div style="display:flex;gap:10px;margin-top:18px">${belster('Belster installateur', '1 of 2', HW)}${belster('Belster energie-adviseur', '2 of 3', GROEN)}</div>
      <div style="margin-top:16px"><span class="chip">Doel: een bezoek + de factuur vooraf</span></div>`, { AB, bg: 'kmo-transitie-2', pos: 'right center' }) });

  // 5 · Kanaal 1 — Elindus-profiel, vóór de call
  L.push({ titel: 'Kanaal 1: Elindus-profiel', tekst: 'Is de klant al bij Elindus? Dan rekenen we zijn verbruiksprofiel door nog vóór er gebeld wordt. De belster weet dan al waar de piek zit en hoeveel kilometer er op de aansluiting past. Met de input van de accountmanager wordt de pitch scherp, en wordt het bezoek een gesprek over zijn eigen cijfers.',
    html: basis(`<div class="ey">Kanaal 1 · Elindus-profiel</div><h1>Klant bij Elindus?<br>Analyse <em>vóór de call</em>.</h1>
      <div style="display:flex;flex-direction:column;gap:11px;margin-top:20px">
        ${regel('data', 'Verbruiksprofiel doorgerekend')}
        ${regel('check', 'Piek en km per wagen al gekend')}
        ${regel('contract', 'Input van de accountmanager')}</div>
      ${tijdlijn([0], ['Vóór de call', 'Tijdens het bezoek', 'Na het bezoek'])}`, { AB, bg: 'vm-ems', pos: 'left center', rechts: true }) });

  // 6 · Kanaal 2 — Mail, vóór of na het bezoek
  const env = (adres, logo, wat, kl) => `<div class="kaart" style="display:flex;gap:14px;align-items:center;border-color:${kl};padding:11px 14px">
      <div style="flex:none;width:44px;height:44px;border-radius:12px;background:rgba(255,255,255,.08);display:flex;align-items:center;justify-content:center">${ic('mail', 24, kl)}</div>
      <div style="flex:1"><div class="n" style="font-size:17px;font-weight:700">${adres}</div><div style="display:flex;align-items:center;gap:10px;margin-top:4px">${ic('pijl', 14, kl, 2.2)}${logo}<span style="font-size:13px;color:#cdd6e3">${wat}</span></div></div></div>`;
  L.push({ titel: 'Kanaal 2: mail', tekst: 'Stuurt de klant zijn factuur vooraf naar het laadadres of het dossieradres, dan ligt de simulatie klaar als je aanbelt. Komt de factuur pas na het bezoek, dan heb je binnen enkele minuten de link, en volg je op met een studie in plaats van met een belletje.',
    html: basis(`<div class="ey">Kanaal 2 · mail</div><h1 style="font-size:30px">Factuur vooraf,<br>of na het bezoek</h1>
      <div style="display:flex;flex-direction:column;gap:10px;margin-top:18px">
        ${env('laden@energie-compas.eu', blLogo(AB, 20), 'laadplan', 'rgba(107,174,65,.7)')}
        ${env('dossier@energie-compas.eu', ecLogo(AB, 14), 'volledig dossier', 'rgba(5,176,80,.7)')}</div>
      ${tijdlijn([0, 2])}
      <div style="display:flex;gap:16px;margin-top:10px;font-size:12.5px;color:#cdd6e3;width:470px"><span style="flex:1">Vooraf: de simulatie ligt klaar</span><span style="flex:1;text-align:right">Achteraf: opvolgen met een studie</span></div>`, { AB, bg: 'w02-di-v2', pos: 'right center' }) });

  // 7 · Kanaal 3 — WhatsApp, tijdens het bezoek
  const bub = (t, uit, ex = '') => `<div style="align-self:${uit ? 'flex-end' : 'flex-start'};max-width:84%;background:${uit ? '#d9fdd3' : '#fff'};color:#111b21;border-radius:9px;padding:5px 8px;font-size:11px;line-height:1.3;box-shadow:0 1px 1px rgba(0,0,0,.12)">${t}${ex}</div>`;
  const knop = t => `<div style="margin-top:4px;border-top:1px solid #e3e6e8;padding-top:3px;text-align:center;color:#027eb5;font-weight:600">${t}</div>`;
  const pag = `<div style="display:flex;gap:4px">${[1, 2, 3].map(() => `<div style="width:32px;height:42px;background:#fff;border-radius:3px;padding:4px;display:flex;flex-direction:column;gap:3px">${'<div style="height:2px;background:#c7ced6"></div>'.repeat(6)}</div>`).join('')}</div>`;
  const telefoon = `<div style="width:266px;height:556px;border-radius:36px;background:#0b0f14;padding:10px;box-shadow:0 20px 50px rgba(0,0,0,.55),0 0 0 2px #2c3644">
    <div style="width:100%;height:100%;border-radius:28px;overflow:hidden;background:#efeae2;display:flex;flex-direction:column">
      <div style="background:#075e54;color:#fff;padding:20px 12px 8px;display:flex;align-items:center;gap:8px"><div style="width:28px;height:28px;border-radius:50%;background:${GROEN};display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:800">EC</div><div><div style="font-size:12.5px;font-weight:600">Energie-Compas</div><div style="font-size:9.5px;opacity:.8">zakelijk account</div></div></div>
      <div style="flex:1;padding:8px 8px;display:flex;flex-direction:column;gap:5px">
        <div style="align-self:flex-end;background:#d9fdd3;border-radius:9px;padding:5px">${pag}</div>
        ${bub('Betere Lader of Energie-Compas?', false, knop('Betere Lader') + knop('Energie-Compas'))}
        ${bub('Betere Lader', true)}
        ${bub('Waar wordt er geladen?', false, knop('Thuis') + knop('KMO') + knop('Logistiek'))}
        ${bub('KMO', true)}
        ${bub('Hoeveel wagens?', false)}
        ${bub('10 3', true)}
        ${bub('<b>Laadcheck</b> · km per wagen · prijs per 100 km<br><span style="color:#027eb5">Laadplan: app.energie-compas.eu/…</span>', false)}
      </div></div></div>`;
  L.push({ titel: 'Kanaal 3: WhatsApp', tekst: 'Ligt de factuur op tafel? Fotografeer elke pagina en stuur ze via WhatsApp naar Energie-Compas. Terwijl jullie verder praten, wordt ze gelezen. Enkele seconden later staat de laadcheck op je scherm, en praat je over zijn aansluiting, zijn wagens en zijn kost per kilometer.',
    html: basis(`<div style="display:flex;gap:30px;height:100%;align-items:center">${telefoon}
      <div style="width:330px"><div class="ey">Kanaal 3 · WhatsApp</div><h1 style="font-size:29px">Factuur op tafel?<br>Foto per pagina.</h1>
        <div style="display:flex;flex-direction:column;gap:9px;margin-top:16px">${regel('check', 'Gelezen terwijl jullie praten')}${regel('check', 'Laadcheck in enkele seconden')}${regel('check', 'Aansluiting, wagens, prijs per km')}</div>
        <div class="kaart" style="margin-top:16px;padding:10px 14px"><div style="font-size:11px;letter-spacing:1.5px;color:#9fb0c8;font-weight:600">WHATSAPP ENERGIE-COMPAS</div><div class="n" style="font-size:22px;font-weight:700;margin-top:2px">+32 483 62 79 58</div></div>
        <div style="transform:scale(.7);transform-origin:left top;margin-top:4px">${tijdlijn([1])}</div>
      </div></div>`, { AB, bg: 'factuurcheck-maart', pos: 'right top', maat: 'auto 125%', breed: 740, css: '.pn{padding:34px 30px 34px 50px}' }) });

  // 8 · Het bezoek: elk zijn verhaal
  const spoor = (kop, wie, kl, items) => `<div class="kaart" style="flex:1;border-color:${kl}"><div style="font-size:15px;font-weight:700">${kop}</div><div style="font-size:12px;color:#9fb0c8;margin-bottom:8px">${wie}</div>
      ${items.map(t => `<div style="display:flex;gap:8px;align-items:flex-start;font-size:13px;margin-top:6px;line-height:1.3">${ic('check', 14, kl, 2.6)}<span>${t}</span></div>`).join('')}</div>`;
  L.push({ titel: 'Het bezoek', tekst: 'Zo heb je bij elk bezoek meer dan een doodgewoon verhaal. De installateur toont wat de aansluiting kan dragen, en wanneer een batterij of zonnepanelen echt iets opleveren. De energie-adviseur toont wat een dynamisch contract met toegang tot intraday en onbalans opbrengt. Vraagt de klant naar het andere vak, dan bevestig je. Je verkoopt het niet.',
    html: basis(`<div class="ey">Het bezoek</div><h1 style="font-size:30px">Meer dan een<br>doodgewoon verhaal</h1>
      <div style="display:flex;gap:10px;margin-top:20px">
        ${spoor('Hardware', 'installateur', HW, ['Wat de aansluiting kan dragen', 'Wanneer batterij of PV echt opbrengt'])}
        ${spoor('Energie', 'energie-adviseur', GROEN, ['Dynamisch contract', 'Intraday en onbalans'])}</div>
      <div style="margin-top:18px"><span class="chip">Ander vak? Bevestigen, niet brengen.</span></div>`, { AB, bg: 'laden-onder-5-euro-v2', pos: 'right center' }) });

  // 9 · Naar bindende offerte
  const stap = (n, t, laatste) => `<div style="display:flex;gap:12px;align-items:center"><span style="flex:none;width:28px;height:28px;border-radius:50%;background:${laatste ? GROEN : 'rgba(255,255,255,.12)'};border:1px solid ${GROEN};display:flex;align-items:center;justify-content:center;font-weight:700;font-size:13px">${n}</span><span style="font-size:15.5px;${laatste ? 'font-weight:700' : ''}">${t}</span></div>`;
  L.push({ titel: 'Naar bindende offerte', tekst: 'Kiest de klant zijn volgende stap, dan gaat het ontwerp rechtstreeks naar EME. Die bouwt het bord, met het eendraadschema en de datasheets, en de sturing van Voltmaster zit er standaard in. Op het platform wordt dat één bindende offerte, met de analyse erbij.',
    html: basis(`<div class="ey">Naar bindende offerte</div><h1 style="font-size:30px">Van keuze<br>naar offerte</h1>
      <div style="display:flex;flex-direction:column;gap:10px;margin-top:20px">
        ${stap(1, 'Studie: ontwerprooster en groeipad')}${stap(2, 'De klant kiest zijn volgende stap')}${stap(3, 'Ontwerp rechtstreeks naar <b>EME</b>')}${stap(4, 'Bord, eendraadschema, datasheets · sturing <b>Voltmaster</b>')}${stap(5, 'Eén bindende offerte, analyse erbij', true)}</div>
      <div style="display:flex;align-items:center;gap:14px;margin-top:20px">${ecLogo(AB, 18)}${blLogo(AB, 22)}<span class="wm">EME</span><span class="wm">Voltmaster</span></div>`, { AB, bg: 'kmo-transitie-3', pos: 'right center' }) });

  // 10 · Boodschap
  L.push({ titel: 'Boodschap', tekst: 'Twee vakmensen, één dossier, één doel: goedkopere megawatturen en goedkopere kilometers, all-in. Elk bij zijn vak. Samen sneller.',
    html: basis(`<div style="margin:auto 0"><div class="lgr">${ecLogo(AB, 28)}</div>
      <div style="font-size:40px;font-weight:800;line-height:1.18">Twee vakmensen.<br>Eén dossier.</div>
      <div style="margin-top:18px;font-size:24px;font-weight:700;color:${GROEN};line-height:1.3">Goedkopere MWh en km,<br>all-in.</div>
      <div style="margin-top:18px;font-size:20px;color:#dfe6f0">Elk bij zijn vak. Samen sneller.</div></div>`, { AB, bg: 'w12-do', pos: 'left center', rechts: true }) });

  // 11 · Eindbeeld
  L.push({ titel: 'Slot', eind: true, tekst: 'Probeer het zelf. Start met een demo.',
    html: basis(`<div style="display:flex;height:100%;align-items:center;justify-content:space-between;gap:40px">
      <div><div class="lgr">${ecLogo(AB, 30)}<span style="width:1px;height:30px;background:rgba(255,255,255,.3)"></span>${blLogo(AB, 34)}</div>
        <div class="ey">Probeer het zelf</div><div style="font-size:56px;font-weight:800;line-height:1.05">Start met<br>een <span style="color:${GROEN}">demo</span></div>
        <div style="font-size:17px;color:#cdd6e3;margin-top:18px;line-height:1.5">Stuur <b style="color:#fff">demo</b> via WhatsApp naar<br><span class="n" style="font-size:24px;font-weight:700;color:#fff">+32 483 62 79 58</span></div>
        <div style="display:flex;align-items:center;gap:16px;margin-top:20px;flex-wrap:wrap"><span style="font-size:11px;letter-spacing:2px;color:#8193ad;font-weight:600">POWERED BY</span><span class="wm">Energie-Compas</span><span class="wm">Betere Lader</span><span class="wm">EME</span><span class="wm">Voltmaster</span><span class="wm">Elindus</span></div></div>
      <div style="text-align:center"><div style="background:#fff;border-radius:16px;padding:14px;width:236px;height:236px"><img src="${qr}" style="width:208px;height:208px;display:block"></div><div style="font-size:13px;color:#b9c4d6;margin-top:10px">Scan · WhatsApp demo</div></div></div>
      <div style="position:absolute;left:54px;bottom:22px;font-size:11px;color:#8193ad;letter-spacing:.5px">Stem gegenereerd met AI · Fluctus.net CVSO</div>`, { AB, breed: BW, merk: false, css: '.pn{padding:40px 90px 50px 90px}' }) });
  return L;
}
module.exports = { beelden, BW, BH };
