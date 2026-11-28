'use strict';
// Uitlegvideo «Van factuur naar groeipad» (ewb.js v1.5.0) — eigen beelden i.p.v. schermafbeeldingen van een app.
// Elk beeld = 1120×630 HTML (campagnefoto + infografiek) + één stuk spreektekst (rol «docu»).
// Cijfers Betere Lader = laadcheck van de KMO-demofactuur (DEMO Productie bv, 130 kW, 399,5 MWh/jaar;
// 10 personeelswagens + 3 bestelwagens, binnen een jaar 20 + 6), zodat de video toont wat de demo geeft.
// Adressen en telefoonnummer staan in beeld en worden niet uitgesproken.

const BW = 1120, BH = 630;
const GROEN = '#05B050', BL_GROEN = '#6BAE41', NAVY = '#101a30', NAVY2 = '#1a2846';
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
.lg{height:26px;display:block}.lgr{display:flex;gap:18px;align-items:center;margin-bottom:22px}
.ey{display:flex;align-items:center;gap:10px;color:${GROEN};font-weight:600;letter-spacing:2.5px;text-transform:uppercase;font-size:13px;margin-bottom:12px}
.ey:before{content:'';width:3px;height:16px;background:${GROEN};display:block}
.ey.bl{color:${BL_GROEN}}.ey.bl:before{background:${BL_GROEN}}
h1{font-size:38px;line-height:1.14;font-weight:700;letter-spacing:-.3px}
h1 em{font-style:normal;color:${GROEN}}
.sub{font-size:16px;color:#b9c4d6;line-height:1.5;margin-top:12px}
.band{position:absolute;left:0;right:0;bottom:0;height:6px;background:${GROEN}}
.merk{position:absolute;${rechts ? 'right:46px' : 'left:54px'};bottom:22px;font-size:11px;font-weight:700;letter-spacing:1.5px;color:#9fb0c8}
.kaart{background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.13);border-radius:14px;padding:16px 18px}
.chip{display:inline-flex;align-items:center;gap:8px;padding:7px 14px;border-radius:999px;background:rgba(5,176,80,.14);border:1px solid rgba(5,176,80,.55);color:#dff7e8;font-size:14px;font-weight:600}
.n{font-variant-numeric:tabular-nums}
${o.css || ''}</style></head><body><div class="sl"></div>${o.los || ''}<div class="pn">${inhoud}</div>${o.merk === false ? '' : `<div class="merk">ENERGIE-COMPAS</div>`}<div class="band"></div></body></html>`;
}
const ecLogo = (AB, h = 26) => `<img class="lg" style="height:${h}px" src="${AB}/apps/ec-logo-dark.png">`;
const blLogo = (AB, h = 30) => `<img class="lg" style="height:${h}px" src="${AB}/apps/bl/betere-lader-logo-wit.svg">`;

// eenvoudige lijn-iconen (geen emoji: Chromium op de server heeft geen emoji-lettertype)
const IC = {
  stekker: '<path d="M9 2v6M15 2v6M6 8h12v4a6 6 0 0 1-12 0zM12 18v4" />',
  batterij: '<rect x="3" y="7" width="16" height="10" rx="2"/><path d="M21 11v2M7 10v4M11 10v4"/>',
  zon: '<circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/>',
  sturing: '<path d="M4 6h10M18 6h2M4 12h4M12 12h8M4 18h12M20 18h0"/><circle cx="16" cy="6" r="2"/><circle cx="10" cy="12" r="2"/><circle cx="18" cy="18" r="2"/>',
  mail: '<rect x="3" y="5" width="18" height="14" rx="2"/><path d="M3 7l9 6 9-6"/>',
  check: '<path d="M5 12l5 5 9-10"/>',
  pijl: '<path d="M5 12h14M13 6l6 6-6 6"/>',
  foto: '<rect x="3" y="5" width="18" height="14" rx="2"/><circle cx="9" cy="10" r="2"/><path d="M21 16l-5-5-8 8"/>',
  wagen: '<path d="M3 13l2-5h14l2 5v4H3z"/><circle cx="7.5" cy="17" r="1.6"/><circle cx="16.5" cy="17" r="1.6"/>'
};
const ic = (k, s = 22, kl = '#fff', w = 1.8) => `<svg width="${s}" height="${s}" viewBox="0 0 24 24" fill="none" stroke="${kl}" stroke-width="${w}" stroke-linecap="round" stroke-linejoin="round">${IC[k]}</svg>`;

function beelden({ AB, qr, demo }) {
  const D = Object.assign({ km: '32.000', nodig: '15.000', dom: '7,33', slim: '2,54', pct: '65', lf0: '35,1', lf1: '42,7', toeg: '130', lf0n: 35.1, lf1n: 42.7, kmN: 32000, nodigN: 15000, domN: 7.33, slimN: 2.54 }, demo || {});
  const L = [];

  // 1 · Aankondiging
  L.push({ titel: 'Aankondiging', tekst: 'In drie minuten zie je hoe je met één energiefactuur vertrekt naar een helder plan: wat er vandaag al kan met je aansluiting, en wat je morgen nodig hebt.',
    html: basis(`<div style="margin-top:auto;margin-bottom:auto">
      <div class="lgr">${ecLogo(AB, 30)}<span style="width:1px;height:30px;background:rgba(255,255,255,.3)"></span>${blLogo(AB, 34)}</div>
      <div class="ey">Uitleg · voor adviseurs</div>
      <h1>Van factuur<br>naar <em>groeipad</em></h1>
      <div class="sub">Eén energiefactuur. Wat kan er vandaag al met de aansluiting, en wat is er morgen nodig?</div>
      <div style="display:flex;gap:10px;margin-top:22px;flex-wrap:wrap"><span class="chip">WhatsApp</span><span class="chip">Mail</span><span class="chip">Demo</span></div></div>`, { AB, bg: 'b00-basis', pos: 'center' }) });

  // 2 · Twee deuren
  L.push({ titel: 'Twee deuren', tekst: 'Elk gesprek begint bij een vraag. Wil je klant over laden praten, en vooral: kunnen we onze wagens laden? Dan start je bij Betere Lader. Is hij al bezig met zijn aansluiting, zijn energiebudget of de elektrificatie van zijn bedrijf? Dan ga je meteen naar Energie-Compas. En vanuit Betere Lader zie je vanzelf wanneer die stap zinvol wordt.',
    html: basis(`<div class="ey">Waar begin je?</div><h1 style="font-size:32px">Elk gesprek begint<br>bij een vraag</h1>
      <div style="position:relative;margin-top:22px;display:flex;flex-direction:column;gap:46px">
        <div class="kaart" style="border-color:rgba(107,174,65,.6)"><div style="display:flex;justify-content:space-between;align-items:center">${blLogo(AB, 30)}<span style="font-size:12px;color:${BL_GROEN};font-weight:700;letter-spacing:1.5px">DE KLANT WIL LADEN</span></div>
          <div style="font-size:19px;font-weight:600;margin-top:10px">«Kunnen we onze wagens laden?»</div></div>
        <div class="kaart" style="border-color:rgba(5,176,80,.6)"><div style="display:flex;justify-content:space-between;align-items:center">${ecLogo(AB, 24)}<span style="font-size:12px;color:${GROEN};font-weight:700;letter-spacing:1.5px">AL BEZIG MET ENERGIE</span></div>
          <div style="font-size:17px;font-weight:600;margin-top:10px">Aansluiting · energiebudget · elektrificatie</div></div>
        <div style="position:absolute;left:40px;top:94px;height:46px;display:flex;align-items:center;gap:10px">
          <svg width="20" height="46" viewBox="0 0 20 46"><path d="M10 2v38" stroke="${GROEN}" stroke-width="2.5" stroke-dasharray="5 5"/><path d="M3 34l7 9 7-9" fill="none" stroke="${GROEN}" stroke-width="2.5"/></svg>
          <span style="font-size:13px;color:#cfe9da;font-weight:600">groeipad, zodra het zinvol wordt</span></div>
      </div>`, { AB, bg: 'w01-di', pos: 'right center' }) });

  // 3 · WhatsApp
  const bub = (t, uit, ex = '') => `<div style="align-self:${uit ? 'flex-end' : 'flex-start'};max-width:84%;background:${uit ? '#d9fdd3' : '#fff'};color:#111b21;border-radius:9px;padding:5px 8px;font-size:11px;line-height:1.3;box-shadow:0 1px 1px rgba(0,0,0,.12)">${t}${ex}</div>`;
  const knop = t => `<div style="margin-top:4px;border-top:1px solid #e3e6e8;padding-top:3px;text-align:center;color:#027eb5;font-weight:600">${t}</div>`;
  const pag = `<div style="display:flex;gap:4px">${[1, 2, 3].map(i => `<div style="width:32px;height:42px;background:#fff;border-radius:3px;padding:4px;display:flex;flex-direction:column;gap:3px">${'<div style="height:2px;background:#c7ced6"></div>'.repeat(6)}</div>`).join('')}</div>`;
  const telefoon = `<div style="width:266px;height:556px;border-radius:36px;background:#0b0f14;padding:10px;box-shadow:0 20px 50px rgba(0,0,0,.55),0 0 0 2px #2c3644">
    <div style="width:100%;height:100%;border-radius:28px;overflow:hidden;background:#efeae2;display:flex;flex-direction:column">
      <div style="background:#075e54;color:#fff;padding:20px 12px 8px;display:flex;align-items:center;gap:8px"><div style="width:28px;height:28px;border-radius:50%;background:${GROEN};display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:800">EC</div><div><div style="font-size:12.5px;font-weight:600">Energie-Compas</div><div style="font-size:9.5px;opacity:.8">zakelijk account</div></div></div>
      <div style="flex:1;padding:8px 8px;display:flex;flex-direction:column;gap:5px">
        <div style="align-self:flex-end;background:#d9fdd3;border-radius:9px;padding:5px">${pag}</div>
        ${bub('Betere Lader of Energie-Compas?', false, knop('Betere Lader') + knop('Energie-Compas'))}
        ${bub('Betere Lader', true)}
        ${bub('Waar wordt er geladen?', false, knop('Thuis') + knop('KMO') + knop('Logistiek'))}
        ${bub('KMO', true)}
        ${bub('Hoeveel wagens? <b>10 3</b>', false)}
        ${bub(`<b>Laadcheck</b> · ± ${D.km} km/jaar per wagen · slim € ${D.slim} per 100 km<br><span style="color:#027eb5">Laadplan: app.energie-compas.eu/…</span>`, false)}
      </div></div></div>`;
  const stap = (n, t) => `<div style="display:flex;gap:11px;align-items:flex-start"><span style="flex:none;width:26px;height:26px;border-radius:50%;background:${GROEN};display:flex;align-items:center;justify-content:center;font-weight:700;font-size:13px">${n}</span><span style="font-size:15px;line-height:1.35;padding-top:2px">${t}</span></div>`;
  L.push({ titel: 'Instap via WhatsApp', tekst: 'De snelste instap is WhatsApp. Ligt de factuur op tafel? Neem een foto van elke pagina en stuur ze naar Energie-Compas. Je krijgt meteen de keuze: Betere Lader of Energie-Compas. Kies je Betere Lader, dan vraagt de bot waar er geladen wordt, en hoeveel wagens. Terwijl jij antwoordt, wordt de factuur al gelezen. Enkele seconden later heb je de laadcheck en de link naar het dossier.',
    html: basis(`<div style="display:flex;gap:30px;height:100%;align-items:center">${telefoon}
      <div style="width:300px"><div class="ey">Instap 1 · WhatsApp</div><h1 style="font-size:30px">Factuur op tafel?<br>Foto per pagina.</h1>
        <div style="display:flex;flex-direction:column;gap:12px;margin-top:20px">${stap(1, 'Foto van elke pagina')}${stap(2, 'Kies Betere Lader of Energie-Compas')}${stap(3, 'Waar laden? Hoeveel wagens?')}${stap(4, 'Laadcheck + link naar het dossier')}</div>
        <div class="kaart" style="margin-top:20px;padding:12px 16px"><div style="font-size:11px;letter-spacing:1.5px;color:#9fb0c8;font-weight:600">WHATSAPP ENERGIE-COMPAS</div><div class="n" style="font-size:24px;font-weight:700;margin-top:2px">+32 483 62 79 58</div></div>
      </div></div>`, { AB, bg: 'factuurcheck-maart', pos: 'right top', maat: 'auto 125%', breed: 720, css: '.pn{padding:34px 30px 34px 50px}' }) });

  // 4 · Mail
  const env = (adres, logo, wat, kl) => `<div class="kaart" style="display:flex;gap:16px;align-items:center;border-color:${kl}">
      <div style="flex:none;width:52px;height:52px;border-radius:12px;background:rgba(255,255,255,.08);display:flex;align-items:center;justify-content:center">${ic('mail', 28, kl)}</div>
      <div style="flex:1"><div class="n" style="font-size:20px;font-weight:700">${adres}</div><div style="display:flex;align-items:center;gap:10px;margin-top:6px">${ic('pijl', 16, kl, 2.2)}${logo}<span style="font-size:14px;color:#cdd6e3">${wat}</span></div></div></div>`;
  L.push({ titel: 'Instap via mail', tekst: 'Liever per mail? Stuur de factuur naar het laadadres van Betere Lader voor een laadplan, of naar het dossieradres van Energie-Compas voor een volledig dossier. Binnen enkele minuten krijg je de link. Een digitale factuur stuur je gewoon door.',
    html: basis(`<div class="ey">Instap 2 · mail</div><h1 style="font-size:32px">Factuur mailen<br>of doorsturen</h1>
      <div style="display:flex;flex-direction:column;gap:16px;margin-top:26px">
        ${env('laden@energie-compas.eu', blLogo(AB, 22), 'laadplan', 'rgba(107,174,65,.7)')}
        ${env('dossier@energie-compas.eu', ecLogo(AB, 16), 'volledig dossier', 'rgba(5,176,80,.7)')}</div>
      <div style="display:flex;gap:10px;margin-top:22px;flex-wrap:wrap"><span class="chip">${ic('check', 16, GROEN, 2.4)} Link binnen enkele minuten</span><span class="chip">${ic('check', 16, GROEN, 2.4)} Digitale factuur? Gewoon doorsturen</span></div>`, { AB, bg: 'w02-di-v2', pos: 'right center' }) });

  // 5a · Betere Lader — km per wagen
  const pctKm = Math.min(100, Math.round(D.nodigN / D.kmN * 100));
  L.push({ titel: 'Betere Lader: wat kan vandaag', tekst: 'Betere Lader laat zien wat er vandaag al kan. Hoeveel kilometer per wagen past er op je bestaande aansluiting?',
    html: basis(`<div class="lgr">${blLogo(AB, 34)}</div><div class="ey bl">Laadcheck · demofactuur KMO · ${D.toeg} kW</div>
      <h1 style="font-size:30px">Wat kan er vandaag al<br>op de huidige aansluiting?</h1>
      <div style="margin-top:26px"><div class="n" style="font-size:66px;font-weight:800;line-height:1;color:${BL_GROEN}">± ${D.km} <span style="font-size:30px">km</span></div>
        <div style="font-size:16px;color:#cdd6e3;margin-top:8px">per personeelswagen per jaar, slim geladen · 10 wagens</div></div>
      <div style="margin-top:26px;width:470px"><div style="height:16px;border-radius:8px;background:rgba(107,174,65,.25);position:relative;overflow:hidden">
          <div style="position:absolute;left:0;top:0;bottom:0;width:${pctKm}%;background:${BL_GROEN};border-radius:8px"></div></div>
        <div style="display:flex;justify-content:space-between;font-size:13px;color:#b9c4d6;margin-top:7px"><span>nodig: ${D.nodig} km</span><span>mogelijk: ± ${D.km} km</span></div></div>`, { AB, bg: 'laden-onder-5-euro-v2', pos: 'right center' }) });

  // 5b · slim sturen
  const bw = 340, wd = bw, ws = Math.max(40, Math.round(bw * D.slimN / D.domN));
  L.push({ titel: 'Betere Lader: slim sturen', tekst: 'Wat bespaar je door slim te sturen, in plaats van iedereen tegelijk te laten laden bij aankomst?',
    html: basis(`<div class="lgr">${blLogo(AB, 34)}</div><div class="ey bl">Slim sturen</div>
      <h1 style="font-size:30px">Iedereen tegelijk bij aankomst,<br>of <em style="color:${BL_GROEN}">slim gestuurd</em>?</h1>
      <div style="margin-top:30px;display:flex;flex-direction:column;gap:20px">
        <div><div style="font-size:14px;color:#cdd6e3;margin-bottom:6px">Ongestuurd · iedereen laadt bij aankomst</div><div style="display:flex;align-items:center;gap:14px"><div style="width:${wd}px;height:34px;border-radius:0 6px 6px 0;background:#8a98ad"></div><span class="n" style="font-size:22px;font-weight:700">€ ${D.dom}</span></div></div>
        <div><div style="font-size:14px;color:#cdd6e3;margin-bottom:6px">Slim gestuurd · goedkope uren, piek onder controle</div><div style="display:flex;align-items:center;gap:14px"><div style="width:${ws}px;height:34px;border-radius:0 6px 6px 0;background:${BL_GROEN}"></div><span class="n" style="font-size:22px;font-weight:700;color:${BL_GROEN}">€ ${D.slim}</span></div></div>
        <div style="font-size:13px;color:#9fb0c8">energiekost per 100 km · demofactuur KMO</div></div>
      <div style="display:flex;gap:12px;align-items:center;margin-top:18px"><span class="n" style="font-size:44px;font-weight:800;color:${BL_GROEN}">−${D.pct} %</span><span style="font-size:14px;color:#cdd6e3;line-height:1.35">en zonder sturing gaat de site<br>over haar toegangsvermogen</span></div>`, { AB, bg: 'thuisladen-eigen-parking', pos: 'right center' }) });

  // 5c · aansluiting harder laten werken
  const boog = (v, kl) => { const r = 62, c = 2 * Math.PI * r; return `<svg width="150" height="150" viewBox="0 0 150 150"><circle cx="75" cy="75" r="${r}" fill="none" stroke="rgba(255,255,255,.12)" stroke-width="14"/><circle cx="75" cy="75" r="${r}" fill="none" stroke="${kl}" stroke-width="14" stroke-linecap="round" stroke-dasharray="${(c * v / 100).toFixed(1)} ${c.toFixed(1)}" transform="rotate(-90 75 75)"/><text x="75" y="83" text-anchor="middle" font-family="Poppins" font-weight="700" font-size="26" fill="#fff">${String(v).replace('.', ',')} %</text></svg>`; };
  L.push({ titel: 'Betere Lader: aansluiting harder laten werken', tekst: 'En wanneer moet je aansluiting harder werken: meer uren benut, of een batterij die de kilometers nog goedkoper maakt?',
    html: basis(`<div class="lgr">${blLogo(AB, 34)}</div><div class="ey bl">Benutting van de aansluiting</div>
      <h1 style="font-size:30px">Wanneer moet de aansluiting<br>harder werken?</h1>
      <div style="display:flex;align-items:center;gap:18px;margin-top:22px">
        <div style="text-align:center">${boog(D.lf0n, '#8a98ad')}<div style="font-size:13px;color:#cdd6e3">vandaag · 13 wagens</div></div>
        ${ic('pijl', 34, BL_GROEN, 2.4)}
        <div style="text-align:center">${boog(D.lf1n, BL_GROEN)}<div style="font-size:13px;color:#cdd6e3">binnen een jaar · 26 wagens</div></div></div>
      <div style="font-size:15px;color:#dff7e8;margin-top:12px;font-weight:600">${D.toeg} kW volstaat, slim gestuurd</div>
      <div style="display:flex;gap:10px;margin-top:16px;flex-wrap:wrap"><span class="chip" style="background:rgba(107,174,65,.14);border-color:rgba(107,174,65,.6)">${ic('sturing', 16, '#dff7e8')} Meer uren benutten</span><span class="chip" style="background:rgba(107,174,65,.14);border-color:rgba(107,174,65,.6)">${ic('batterij', 16, '#dff7e8')} Batterij: nog goedkopere km</span></div>`, { AB, bg: 'kmo-transitie-2', pos: 'right center' }) });

  // 6 · Groeipad
  const jaar = (j, w) => `<div style="flex:1;text-align:center"><div style="width:44px;height:44px;border-radius:50%;margin:0 auto;background:${GROEN};display:flex;align-items:center;justify-content:center;font-weight:800;font-size:16px;box-shadow:0 0 0 6px rgba(5,176,80,.18)">Y${j}</div><div class="kaart" style="margin-top:14px;padding:10px 8px"><div style="font-size:12px;color:#9fb0c8;letter-spacing:1px;font-weight:600">DOSSIER</div><div style="font-size:14px;font-weight:600;margin-top:2px">${w}</div></div></div>`;
  L.push({ titel: 'Groeipad', tekst: 'Groeit je vloot, dan zie je het meteen. Met één knop wordt het laadplan een groeipad in Energie-Compas: vier jaar, één dossier per jaar, zonder iets opnieuw in te vullen.',
    html: basis(`<div class="ey">Van laadplan naar groeipad</div><h1 style="font-size:32px">Eén knop: <em>MEER</em></h1>
      <div style="display:flex;align-items:center;gap:14px;margin-top:22px">${blLogo(AB, 30)}<span style="padding:6px 16px;border-radius:8px;background:#fff;color:${NAVY};font-weight:800;letter-spacing:1px;font-size:15px">MEER</span>${ic('pijl', 26, GROEN, 2.4)}${ecLogo(AB, 22)}</div>
      <div style="position:relative;margin-top:30px"><div style="position:absolute;left:12%;right:12%;top:21px;height:3px;background:linear-gradient(90deg,${GROEN},rgba(5,176,80,.35))"></div>
        <div style="display:flex;gap:12px;position:relative">${jaar(1, 'vandaag')}${jaar(2, '+ wagens')}${jaar(3, '+ zon')}${jaar(4, '+ batterij')}</div></div>
      <div class="sub" style="margin-top:22px">Vier jaar · één dossier per jaar · niets opnieuw invullen</div>`, { AB, bg: 'w11-di', pos: 'left center', rechts: true }) });

  // 7a · Ecosysteem
  const nodes = [['Aansluiting', 0], ['Zelfconsumptie', 90], ['Piekvermogen', 180], ['Spotmarkt', 270]];
  const R = 150, CX = 250, CY = 200;
  const wiel = `<div style="position:relative;width:500px;height:400px;margin-top:8px">
    <svg width="500" height="400" style="position:absolute;inset:0"><circle cx="${CX}" cy="${CY}" r="${R}" fill="none" stroke="rgba(5,176,80,.45)" stroke-width="2" stroke-dasharray="6 8"/>${nodes.map(([, a]) => { const x = CX + R * Math.sin(a * Math.PI / 180), y = CY - R * Math.cos(a * Math.PI / 180); return `<line x1="${CX}" y1="${CY}" x2="${x}" y2="${y}" stroke="rgba(255,255,255,.14)" stroke-width="2"/>`; }).join('')}</svg>
    <div style="position:absolute;left:${CX - 80}px;top:${CY - 80}px;width:160px;height:160px;border-radius:50%;background:${NAVY2};border:2px solid ${GROEN};display:flex;align-items:center;justify-content:center;box-shadow:0 0 40px rgba(5,176,80,.35)"><img src="${AB}/apps/ec-logo-dark.png" style="height:26px"></div>
    ${nodes.map(([t, a]) => { const x = CX + R * Math.sin(a * Math.PI / 180), y = CY - R * Math.cos(a * Math.PI / 180); return `<div style="position:absolute;left:${x - 80}px;top:${y - 19}px;width:160px;height:38px;border-radius:19px;background:${GROEN};display:flex;align-items:center;justify-content:center;font-weight:700;font-size:15px;box-shadow:0 6px 18px rgba(0,0,0,.35)">${t}</div>`; }).join('')}</div>`;
  L.push({ titel: 'Energie-Compas: het ecosysteem', tekst: 'Energie-Compas brengt alles samen: je aansluiting, je zelfconsumptie, je piekvermogen en de spotmarkt.',
    html: basis(`<div class="lgr">${ecLogo(AB, 26)}</div><div class="ey">Eén ecosysteem</div><h1 style="font-size:30px">Alles komt samen</h1>${wiel}`, { AB, bg: 'kmo-transitie-3', pos: 'right center', css: '.pn{padding-top:34px}' }) });

  // 7b · als één systeem
  const tegel = (k, t) => `<div class="kaart" style="flex:1;text-align:center;padding:14px 6px"><div style="width:46px;height:46px;margin:0 auto;border-radius:12px;background:rgba(5,176,80,.16);display:flex;align-items:center;justify-content:center">${ic(k, 26, GROEN, 2)}</div><div style="font-size:13.5px;font-weight:600;margin-top:9px;line-height:1.25">${t}</div></div>`;
  const uitk = t => `<div style="display:flex;gap:10px;align-items:center;font-size:16px">${ic('check', 20, GROEN, 2.6)}<span>${t}</span></div>`;
  L.push({ titel: 'Energie-Compas: één systeem', tekst: 'Betere laadpalen, een batterij, zonnepanelen en slimmere sturing werken als één systeem. Zo laad je wanneer stroom goedkoop is, blijft je piek laag, en gebruik je je eigen zonnestroom zelf.',
    html: basis(`<div class="lgr">${ecLogo(AB, 26)}</div><div class="ey">Werkt als één systeem</div><h1 style="font-size:30px">Vier bouwstenen,<br>één sturing</h1>
      <div style="display:flex;gap:10px;margin-top:22px">${tegel('stekker', 'Betere<br>laadpalen')}${tegel('batterij', 'Batterij')}${tegel('zon', 'Zonne-<br>panelen')}${tegel('sturing', 'Slimmere<br>sturing')}</div>
      <div style="display:flex;flex-direction:column;gap:11px;margin-top:24px">${uitk('Laden wanneer stroom goedkoop is')}${uitk('Je piek blijft laag')}${uitk('Je eigen zonnestroom zelf gebruiken')}</div>`, { AB, bg: 'kmo-transitie-4', pos: 'right center' }) });

  // 8 · Boodschap
  L.push({ titel: 'Boodschap', tekst: 'Energie-Compas leert je wat je echt nodig hebt, zodat je je energie met meer vertrouwen en goedkoper inkoopt. En we helpen je de juiste keuzes te maken. Minder investeren, meer besparen, meer vertrouwen. Energie voor iedereen.',
    html: basis(`<div style="margin:auto 0"><div class="lgr">${ecLogo(AB, 28)}</div>
      <div class="sub" style="font-size:18px;color:#dfe6f0;margin-top:0;max-width:470px">Leer wat je echt nodig hebt. Koop je energie met meer vertrouwen en goedkoper in. Wij helpen je de juiste keuzes te maken.</div>
      <div style="margin-top:26px;font-size:40px;font-weight:800;line-height:1.18">Minder investeren.<br>Meer besparen.<br>Meer vertrouwen.</div>
      <div style="margin-top:20px;font-size:26px;font-weight:700;color:${GROEN}">Energie voor iedereen.</div></div>`, { AB, bg: 'w12-do', pos: 'left center', rechts: true }) });

  // 9 · Eindbeeld
  L.push({ titel: 'Slot', eind: true, tekst: 'Probeer het zelf. Start met een demo.',
    html: basis(`<div style="display:flex;height:100%;align-items:center;justify-content:space-between;gap:40px">
      <div><div class="lgr">${ecLogo(AB, 30)}<span style="width:1px;height:30px;background:rgba(255,255,255,.3)"></span>${blLogo(AB, 34)}</div>
        <div class="ey">Probeer het zelf</div><div style="font-size:56px;font-weight:800;line-height:1.05">Start met<br>een <span style="color:${GROEN}">demo</span></div>
        <div style="font-size:17px;color:#cdd6e3;margin-top:18px;line-height:1.5">Stuur <b style="color:#fff">demo</b> via WhatsApp naar<br><span class="n" style="font-size:24px;font-weight:700;color:#fff">+32 483 62 79 58</span></div>
        <div style="font-size:14px;color:#9fb0c8;margin-top:12px">of mail een factuur naar dossier@energie-compas.eu</div></div>
      <div style="text-align:center"><div style="background:#fff;border-radius:16px;padding:14px;width:236px;height:236px"><img src="${qr}" style="width:208px;height:208px;display:block"></div><div style="font-size:13px;color:#b9c4d6;margin-top:10px">Scan · WhatsApp demo</div></div></div>
      <div style="position:absolute;left:54px;bottom:22px;font-size:11px;color:#8193ad;letter-spacing:.5px">Stem gegenereerd met AI · Fluctus.net CVSO</div>`, { AB, breed: BW, merk: false, css: '.pn{padding:40px 90px 50px 90px}' }) });
  return L;
}
module.exports = { beelden, BW, BH };
