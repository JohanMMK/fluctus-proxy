'use strict';
// =====================================================================
//  DESTINATION-RAPPORT  (module, gemount vanuit server.js — v0.1, Johan 21-11)
//  Genereert per locatie (vanuit de Lead Generator-tegel, enkel oekene) een
//  destination-charging voorstel in de stijl van het KFC Wambeek-rapport,
//  GENERIEK voor elk locatietype (sportclub, school, zorg, OC-lid, parking).
//
//  Endpoint:  POST /api/destination-rapport   (oekene@gmail.com only)
//    body: { naam, type, lat, lng, gemeente, plaatsen? , params? }
//    → application/pdf  (inline)   of  { ok:false, error } bij fout
//
//  Twee operationele scenario's: (A) alleen AC, (B) AC + 1x DC 150 kVA + batterij.
//  Twee financieringen per scenario: concessie (uitvoering door partner) en
//  eigen investering door de locatie-eigenaar.  Bijlage per scenario.
//
//  Dependencies worden geïnjecteerd vanuit server.js (geen harde koppeling):
//    deps = { managerGuard, luchtfotoUrl, grbDak, fetch }
//  Alles is defensief: ontbreekt een dep, dan degradeert het nette (bv. geen
//  luchtfoto i.p.v. een crash). De mount in server.js staat in try/catch, dus
//  dit bestand kan de live diensten niet platleggen.
// =====================================================================

const EXE = process.env.PUPPETEER_EXECUTABLE_PATH || '/usr/bin/chromium';
const OEKENE = 'oekene@gmail.com';

// ─── Standaardparameters (spiegel van public/apps/data/rapport_parameters.json) ──
// Oekene calibreert deze; de body mag een { params } meesturen die hierop merget.
const DEFAULTS = {
  model_jaren: 15,
  inflatie: 0.02,                 // aankoopprijs volgt inflatie
  verkoop_spread: 0.01,           // verkoopprijs groeit inflatie + 1%
  discontovoet: 0.10,             // KFC
  // prijzen (€/kWh) — KFC-basis
  ac_verkoop_eur_kwh: 0.35,       // ~CREG-index (geen AC in KFC-basis; thuistarief-niveau)
  dc_verkoop_eur_kwh: 0.54,       // KFC-basis verkoopprijs
  spot_aankoop_eur_kwh: 0.17,     // KFC-basis blended energieaankoop (steady state)
  heffingen_eur_kwh: 0.0,         // in de blended aankoopprijs begrepen
  netkosten_eur_kwh: 0.0,         // hypothese: 0 (slimme config + batterijbuffer)
  // verbruik
  ac_kwh_sessie: 20,
  dc_kwh_sessie: 28,
  ref_ml: 800, ref_ac_sessies_dag: 10, ref_dc_sessies_dag: 4.5,
  // sizing
  ev_aandeel_parking: 0.20, ac_rotatiefactor: 3, ac_max_palen: 5,
  plaatsen_default: 60,           // als er geen parking gevonden is
  plaats_per_m2_dak: 25,          // fallback: gebouwoppervlak → ~plaatsen
  // capex (€) — KFC-basis
  capex_ac_paal: 3000,
  capex_dc_150: 48125,            // KFC-basis laadpaal-blok (incl. installatie, studie, survey)
  capex_batterij: 55091,          // KFC-basis batterij-blok (batterij + ontkoppelbord + implementatie + Bebat)
  capex_aansluiting_ems: 5000,    // basis-aansluiting (KFC-basis bundelt de DC-installatie al in capex_dc_150)
  contingency: 0.0,               // KFC-CAPEX = som van de componenten
  // opex — vaste bedragen per jaar, zoals het KFC-basismodel
  onderhoud_laadpalen_jaar: 2400, // KFC-basis onderhoud laadpaal
  onderhoud_batterij_jaar: 800,   // KFC-basis onderhoud batterij (enkel scenario B)
  sturing_batterij_jaar: 582.5,   // KFC-basis sturing batterij (enkel scenario B)
  verzekering_pct_capex: 0.0147,  // KFC-basis verzekeringsfactor
  // inkomsten
  parkeergeld_eur_paal_dag: 8,
  parkeergeld_dagen_jaar: 300,
  // omzetdeling naar de locatie (concessie) — staffel op jaaromzet
  omzetdeling: [ { tot: 50000, pct: 0.05 }, { tot: 75000, pct: 0.10 }, { tot: 1e12, pct: 0.20 } ],
};

const TYPE_LABEL = {
  sport:      { label: 'sportclub / sportaccommodatie', oms: 'Deze locatie is een sportclub of -accommodatie. Het laadprofiel wordt gedragen door bezoekers rond trainingen en wedstrijden, aangevuld met dubbelgebruik van de parking door de buurt.' },
  school:     { label: 'school', oms: 'Deze locatie is een onderwijsinstelling. Het laadprofiel komt van kiss-and-ride, personeel en buurtgebruik van de parking.' },
  zorg:       { label: 'zorginstelling', oms: 'Deze locatie is een zorginstelling (ziekenhuis, woonzorgcentrum of verzorgingsinstelling). Het laadprofiel komt van bezoekers, personeel en patiëntenvervoer.' },
  'oc-overig':{ label: 'publieke entiteit', oms: 'Deze locatie is een publieke entiteit, doorgaans een kantoor met personeelsparking en beperkt bezoek.' },
  geen:       { label: 'parking', oms: 'Deze locatie is een parking zonder verder gekend type (kantoor, winkel of gemengd gebruik).' },
};

// ─── helpers ──────────────────────────────────────────────────────────
function esc(s){ return String(s==null?'':s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
function eur(n){ return '€ ' + Math.round(n).toLocaleString('nl-BE'); }
function eur2(n){ return '€ ' + (Math.round(n*100)/100).toLocaleString('nl-BE', { minimumFractionDigits:2, maximumFractionDigits:2 }); }
function npv(rate, cfs){ return cfs.reduce((a,c,i)=> a + c/Math.pow(1+rate,i), 0); }
function irr(cfs){
  // bisectie; enkel zinvol als er een negatieve (investering) en positieve CF is
  if (!cfs.some(c=>c<0) || !cfs.some(c=>c>0)) return null;
  let lo=-0.9, hi=1.5, flo=npv(lo,cfs), fhi=npv(hi,cfs);
  if (flo*fhi>0) return null;
  for (let i=0;i<100;i++){ const mid=(lo+hi)/2, f=npv(mid,cfs); if (Math.abs(f)<1) return mid; if (flo*f<0){ hi=mid; fhi=f; } else { lo=mid; flo=f; } }
  return (lo+hi)/2;
}
function omzetdeelPct(omzet, staffel){ for (const s of staffel) if (omzet<=s.tot) return s.pct; return staffel[staffel.length-1].pct; }

// ─── sizing ───────────────────────────────────────────────────────────
function sizing(P, type, par){
  const plaatsen = (P && P>0) ? P : par.plaatsen_default;
  const evPl = plaatsen * par.ev_aandeel_parking;
  let nAC = Math.round(evPl / par.ac_rotatiefactor);
  nAC = Math.max(1, Math.min(par.ac_max_palen, nAC));
  // ML-proxy in V1: parkinggrootte t.o.v. de referentie (KFC ~ gebruikt ref_ml)
  // We schalen de DC-sessies met de parkinggrootte i.p.v. ledenaantal (V1).
  const mlProxy = plaatsen;                       // 1 plaats = 1 ML-eenheid (V1)
  const mlRefPlaatsen = par.ref_ml / 10;          // ruwe ijking: 800 ML ~ 80 plaatsen-equivalent
  const dcSessiesDag = Math.max(1, par.ref_dc_sessies_dag * (mlProxy / mlRefPlaatsen));
  const acSessiesDag = Math.max(2, nAC * (par.ref_ac_sessies_dag / Math.max(1, Math.round(par.ref_ml/ (par.ref_ml/5)))) ); // ~2 sessies/paal/dag
  return { plaatsen, nAC, acSessiesDag: Math.round(acSessiesDag*10)/10, dcSessiesDag: Math.round(dcSessiesDag*10)/10 };
}

// ─── energie + financieel model voor één scenario ─────────────────────
function scenarioModel(s, metDC, par){
  const acKwhJaar = s.acSessiesDag * 365 * par.ac_kwh_sessie;
  const dcKwhJaar = metDC ? s.dcSessiesDag * 365 * par.dc_kwh_sessie : 0;

  // marge per kWh (verkoop - aankoop - heffingen - netkosten); sturing/onderhoud zijn nu vaste jaarkosten
  const kostKwh = par.spot_aankoop_eur_kwh + par.heffingen_eur_kwh + par.netkosten_eur_kwh;
  const acMargeKwh = par.ac_verkoop_eur_kwh - kostKwh;
  const dcMargeKwh = par.dc_verkoop_eur_kwh - kostKwh;

  const acOmzet = acKwhJaar * par.ac_verkoop_eur_kwh;
  const dcOmzet = dcKwhJaar * par.dc_verkoop_eur_kwh;
  const omzet = acOmzet + dcOmzet;
  const margeEnergie = acKwhJaar*acMargeKwh + dcKwhJaar*dcMargeKwh;

  // capex
  let capex = s.nAC*par.capex_ac_paal + par.capex_aansluiting_ems;
  if (metDC) capex += par.capex_dc_150 + par.capex_batterij;
  capex = Math.round(capex * (1+par.contingency));

  // opex (jaar 1)
  const nPalen = s.nAC + (metDC?1:0);
  const onderhoud = par.onderhoud_laadpalen_jaar + (metDC ? (par.onderhoud_batterij_jaar + par.sturing_batterij_jaar) : 0);
  const verzekering = capex*par.verzekering_pct_capex;
  const parkeergeld = s.nAC * par.parkeergeld_eur_paal_dag * par.parkeergeld_dagen_jaar;
  const opex = onderhoud + verzekering;

  return { metDC, acKwhJaar, dcKwhJaar, omzet, margeEnergie, capex, opex, parkeergeld,
           netto_jaar1: margeEnergie + parkeergeld - opex, nPalen };
}

// 15-jaar DCF voor "eigen investering" (locatie draagt capex, houdt volledige netto)
function dcfEigen(m, par){
  const cfs=[-m.capex]; let cum=-m.capex, tvt=null;
  for (let j=1;j<=par.model_jaren;j++){
    const gV = Math.pow(1+par.inflatie+par.verkoop_spread, j-1);   // omzet/marge groeit sneller
    const gC = Math.pow(1+par.inflatie, j-1);                       // opex volgt inflatie
    // marge schaalt met verkoop; opex met inflatie; parkeergeld met inflatie
    const netto = (m.margeEnergie*gV) + (m.parkeergeld*gC) - (m.opex*gC);
    cfs.push(netto); cum+=netto; if (tvt===null && cum>=0) tvt=j;
  }
  return { cfs, totaal: cfs.reduce((a,c)=>a+c,0), irr: irr(cfs), ncw: npv(par.discontovoet,cfs), tvt };
}

// Concessie: locatie draagt €0, krijgt omzetdeling + parkeergeld
function concessieLocatie(m, par){
  let totaal=0; const rij=[];
  for (let j=1;j<=par.model_jaren;j++){
    const gV=Math.pow(1+par.inflatie+par.verkoop_spread,j-1), gC=Math.pow(1+par.inflatie,j-1);
    const omzet=m.omzet*gV, deel=omzet*omzetdeelPct(omzet,par.omzetdeling), pk=m.parkeergeld*gC;
    totaal+=deel+pk; if (j<=3||j===par.model_jaren) rij.push({j, deel, pk});
  }
  return { totaal, rij };
}

// Concessie — rendement van de concessiegever (investeerder/exploitant): draagt de CAPEX,
// houdt de energiemarge minus de omzetdeling aan de locatie en minus de opex. Parkeergeld
// gaat naar de locatie, niet naar de concessiegever. Zelfde parameters als "eigen investering".
function concessieOperator(m, par){
  const cfs=[-m.capex]; let cum=-m.capex, tvt=null;
  for (let j=1;j<=par.model_jaren;j++){
    const gV=Math.pow(1+par.inflatie+par.verkoop_spread,j-1), gC=Math.pow(1+par.inflatie,j-1);
    const omzet=m.omzet*gV, deel=omzet*omzetdeelPct(omzet,par.omzetdeling);
    const netto=(m.margeEnergie*gV) - deel - (m.opex*gC);
    cfs.push(netto); cum+=netto; if (tvt===null && cum>=0) tvt=j;
  }
  return { cfs, totaal: cfs.reduce((a,c)=>a+c,0), irr: irr(cfs), ncw: npv(par.discontovoet,cfs), tvt };
}

// ─── HTML-template (generiek, print A4) ───────────────────────────────
function rapportHtml(ctx){
  const { naam, type, gemeente, lat, lng, luchtfoto, sizeA, sizeB, mA, mB, par, ligging } = ctx;
  const tl = TYPE_LABEL[type] || TYPE_LABEL.geen;
  const lg = ligging || {};
  const eigenA=dcfEigen(mA,par), eigenB=dcfEigen(mB,par);
  const concA=concessieLocatie(mA,par), concB=concessieLocatie(mB,par);
  const concOpA=concessieOperator(mA,par), concOpB=concessieOperator(mB,par);

  function kvTable(rows){ return '<table class="kv">'+rows.map(r=>`<tr><td>${esc(r[0])}</td><td class="r">${r[1]}</td></tr>`).join('')+'</table>'; }

  function bijlage(nr, titel, s, m, eigen, conc, concOp){
    const staffelTxt = par.omzetdeling.map(function(x){ const pct=Math.round(x.pct*100)+'%'; return x.tot>=1e11 ? (pct+' daarboven') : (pct+' tot €'+Math.round(x.tot/1000)+'k'); }).join(', ');
    return `<h2>Bijlage ${nr} — ${esc(titel)}</h2>
    ${kvTable([
      ['Laadpunten', `${s.nAC}× AC${m.metDC?' + 1× DC 150 kVA + batterij (C&I 125/250)':''}`],
      ['Geschat laadverkeer AC', `${s.acSessiesDag} sessies/dag · ${Math.round(m.acKwhJaar).toLocaleString('nl-BE')} kWh/jaar`],
      m.metDC?['Geschat laadverkeer DC', `${s.dcSessiesDag} sessies/dag · ${Math.round(m.dcKwhJaar).toLocaleString('nl-BE')} kWh/jaar`]:['Laadverkeer DC','—'],
      ['Jaaromzet laadverkoop', eur(m.omzet)],
      ['Investering (CAPEX, incl. '+Math.round(par.contingency*100)+'% marge)', eur(m.capex)],
    ])}
    <div class="two">
      <div class="col"><h3>Concessie (uitvoering door partner)</h3>
        <div class="sm" style="font-weight:bold;color:#1F3864;margin:4px 0 2px">Wat de locatie krijgt</div>
        ${kvTable([
          ['Investering voor de locatie', eur(0)],
          ['Omzetdeling (jaar 1)', eur(conc.rij[0]?conc.rij[0].deel:0)],
          ['Parkeergeld (jaar 1)', eur(conc.rij[0]?conc.rij[0].pk:0)],
          ['Opbrengst locatie (jaar 1)', eur(conc.rij[0]?conc.rij[0].deel+conc.rij[0].pk:0)],
          ['Totale opbrengst locatie (15 jaar)', eur(conc.totaal)],
        ])}
        <div class="sm" style="font-weight:bold;color:#1F3864;margin:8px 0 2px">Rendement voor de concessiegever</div>
        ${kvTable([
          ['Investering (CAPEX)', eur(m.capex)],
          ['Netto jaar 1', eur(concOp.cfs[1]||0)],
          ['Terugverdientijd', concOp.tvt?concOp.tvt+' jaar':'> '+par.model_jaren+' jaar'],
          ['IRR (15 jaar)', concOp.irr!=null?(Math.round(concOp.irr*1000)/10)+' %':'—'],
          ['NCW @ '+Math.round(par.discontovoet*100)+'%', eur(concOp.ncw)],
          ['Netto over 15 jaar', eur(concOp.totaal)],
        ])}
        <p class="sm">De locatie investeert niets en ontvangt een omzetdeling (${staffelTxt}) plus het parkeergeld. De concessiegever draagt de CAPEX en de exploitatie en houdt de resterende netto-opbrengst — berekend met dezelfde parameters als de eigen investering ernaast.</p>
      </div>
      <div class="col"><h3>Eigen investering door de locatie-eigenaar</h3>
        ${kvTable([
          ['Investering (CAPEX)', eur(m.capex)],
          ['Netto-opbrengst jaar 1', eur(m.netto_jaar1)],
          ['Terugverdientijd', eigen.tvt?eigen.tvt+' jaar':'> '+par.model_jaren+' jaar'],
          ['IRR (15 jaar)', eigen.irr!=null?(Math.round(eigen.irr*1000)/10)+' %':'—'],
          ['NCW @ '+Math.round(par.discontovoet*100)+'%', eur(eigen.ncw)],
          ['Netto over 15 jaar', eur(eigen.totaal)],
        ])}
        <p class="sm">De locatie investeert zelf en houdt de volledige netto-opbrengst uit de laadverkoop.</p>
      </div>
    </div>`;
  }

  // ── ligging: twee kaarten + legende + lijst van nabije bestemmingen ──
  function legendeItem(kleur, label){ return `<span class="lg-it"><span class="lg-dot" style="background:#${kleur}"></span>${esc(label)}</span>`; }
  function entLink(d){
    const href = d.web ? (/^https?:/i.test(d.web)?d.web:'https://'+d.web)
                       : 'https://www.google.com/maps/search/?api=1&query='+encodeURIComponent((d.naam||'')+' '+(d.adres||lg.gemeente||''));
    return `<a href="${esc(href)}" target="_blank" rel="noopener">${esc(d.naam)}</a>`;
  }
  const KINDLBL={ sport:'Sportclub', school:'School', zorg:'Zorginstelling' };
  function destLijst(){
    const ds = lg.dests || [];
    if (!ds.length) return `<p class="sm">Geen sportclubs, scholen of zorginstellingen binnen ~1,2 km gevonden.</p>`;
    return `<table class="kv ent"><tr><th>Bestemming</th><th>Type</th><th>Adres</th><th class="r">Afstand</th></tr>`
      + ds.map(function(d){
          return `<tr><td>${entLink(d)}${d.web?'':' <span class="sm">↗ kaart</span>'}</td>`
               + `<td>${esc(KINDLBL[d.kind]||'')}${d.sub?(' · '+esc(d.sub)):''}</td>`
               + `<td>${esc(d.adres||'—')}</td>`
               + `<td class="r">${d.d} m</td></tr>`;
        }).join('')
      + `</table>`;
  }
  function liggingSectie(){
    if (!lg.belgie && !lg.omgeving) return '';
    const legende = [
      legendeItem(KLEUR.locatie,'deze locatie'),
      lg.telInfra&&lg.telInfra.parkings?legendeItem(KLEUR.parking,'andere parkings'):'',
      lg.telInfra&&lg.telInfra.cabines?legendeItem(KLEUR.cabine,'MS-cabines'):'',
      lg.telInfra&&lg.telInfra.laadpalen?legendeItem(KLEUR.laadpaal,'laadpalen'):'',
      (lg.dests||[]).some(d=>d.kind==='sport')?legendeItem(KLEUR.sport,'sportclub'):'',
      (lg.dests||[]).some(d=>d.kind==='school')?legendeItem(KLEUR.school,'school'):'',
      (lg.dests||[]).some(d=>d.kind==='zorg')?legendeItem(KLEUR.zorg,'zorg'):'',
    ].filter(Boolean).join('');
    return `<div class="kaarten">
      ${lg.belgie?`<div class="k-be"><img src="${lg.belgie}" alt="ligging in België"><div class="k-cap">Ligging in België</div></div>`:''}
      ${lg.omgeving?`<div class="k-omg"><img src="${lg.omgeving}" alt="omgeving"><div class="k-cap">Omgeving (~1 km) — parkings, MS-cabines, laadpalen en bestemmingen</div></div>`:''}
    </div>
    ${legende?`<div class="legende">${legende}</div>`:''}`;
  }

  return `<!doctype html><html lang="nl"><head><meta charset="utf-8"><style>
  @page { size:A4; margin:16mm 14mm; }
  *{box-sizing:border-box} body{font-family:Arial,Helvetica,sans-serif;color:#1b2430;font-size:11px;line-height:1.5}
  h1{color:#1F3864;font-size:20px;margin:0 0 2px} h2{color:#1F3864;font-size:15px;margin:18px 0 6px;border-bottom:2px solid #E2E8F2;padding-bottom:3px}
  h3{color:#1F3864;font-size:12px;margin:10px 0 4px} .sub{color:#5b6b85;font-size:11px;margin:0 0 10px}
  .hero{width:100%;height:320px;object-fit:cover;border-radius:8px;margin:8px 0}
  .kaarten{display:flex;gap:12px;margin:8px 0 4px;align-items:stretch}
  .k-be{flex:0 0 31%} .k-omg{flex:1}
  .kaarten img{width:100%;border-radius:8px;border:1px solid #E2E8F2;display:block}
  .k-be img{height:300px;object-fit:cover} .k-omg img{height:300px;object-fit:cover}
  .k-cap{color:#5b6b85;font-size:9.5px;margin-top:3px}
  .legende{display:flex;flex-wrap:wrap;gap:4px 14px;margin:6px 0 2px;font-size:10px;color:#3a475c}
  .lg-it{display:inline-flex;align-items:center;gap:5px} .lg-dot{width:9px;height:9px;border-radius:50%;border:1px solid #fff;box-shadow:0 0 0 1px #c9d3e4}
  table.ent th{background:#F2F6FC;color:#1F3864;text-align:left;font-size:10px;padding:4px 7px;border:1px solid #E2E8F2}
  table.ent td{font-size:10.5px} table.ent a{color:#05803B;text-decoration:none;font-weight:bold}
  table.kv{border-collapse:collapse;width:100%;margin:4px 0} table.kv td{border:1px solid #E2E8F2;padding:4px 7px;vertical-align:top}
  table.kv td.r{text-align:right;font-weight:bold;white-space:nowrap} .two{display:flex;gap:12px;margin-top:6px} .col{flex:1}
  .sm{color:#5b6b85;font-size:10px;margin:4px 0} .tag{display:inline-block;background:#1F3864;color:#fff;border-radius:999px;font-size:10px;padding:2px 9px}
  .foot{color:#9AA3B0;font-size:9px;margin-top:14px;border-top:1px solid #E2E8F2;padding-top:6px}
  .warn{background:#FFF7E6;border:1px solid #F0C36D;border-radius:6px;padding:8px;color:#8a5a00;font-size:10px;margin-top:10px}
  </style></head><body>
  <h1>Destination-laadvoorstel — ${esc(naam)}</h1>
  <div class="sub"><span class="tag">${esc(tl.label)}</span> &middot; ${esc(lg.adres||gemeente||'')} &middot; ${(+lat).toFixed(5)}, ${(+lng).toFixed(5)}</div>
  ${luchtfoto?`<img class="hero" src="${luchtfoto}" alt="luchtfoto">`:''}

  <h2>1. De locatie</h2>
  <p>${esc(tl.oms)}</p>
  ${kvTable([
    lg.adres?['Adres', esc(lg.adres)]:['Gemeente', esc(gemeente||'—')],
    ['Parking (geschat aantal plaatsen)', String(sizeA.plaatsen)],
    ['EV-aandeel parking (aanname)', Math.round(par.ev_aandeel_parking*100)+' %'],
    ['Voorgestelde AC-laadpunten', sizeA.nAC+'×'],
  ])}
  ${liggingSectie()}

  <h2>2. Waar parkeerders naartoe gaan</h2>
  <p class="sm">Bestemmingen in de buurt die de parking voeden — sportclubs, scholen en zorginstellingen binnen ~1,2 km. Klik een naam voor de website of de locatie op de kaart.</p>
  ${destLijst()}

  <h2>3. Twee operationele scenario's</h2>
  <div class="two">
    <div class="col"><h3>Scenario A — alleen AC</h3>
      ${kvTable([['AC-laadpunten', sizeA.nAC+'×'],['AC-laadverkeer', Math.round(mA.acKwhJaar).toLocaleString('nl-BE')+' kWh/jaar'],['Jaaromzet', eur(mA.omzet)],['Investering', eur(mA.capex)]])}
    </div>
    <div class="col"><h3>Scenario B — AC + 1× DC 150 kVA + batterij</h3>
      ${kvTable([['Laadpunten', sizeB.nAC+'× AC + 1× DC'],['Totaal laadverkeer', Math.round(mB.acKwhJaar+mB.dcKwhJaar).toLocaleString('nl-BE')+' kWh/jaar'],['Jaaromzet', eur(mB.omzet)],['Investering', eur(mB.capex)]])}
    </div>
  </div>

  <h2>4. Financiële samenvatting</h2>
  <table class="kv">
    <tr><td></td><td class="r">Concessie — opbrengst locatie (15 j)</td><td class="r">Eigen investering — IRR</td><td class="r">Eigen — netto (15 j)</td></tr>
    <tr><td>Scenario A (AC)</td><td class="r">${eur(concA.totaal)}</td><td class="r">${eigenA.irr!=null?(Math.round(eigenA.irr*1000)/10)+' %':'—'}</td><td class="r">${eur(eigenA.totaal)}</td></tr>
    <tr><td>Scenario B (AC+DC)</td><td class="r">${eur(concB.totaal)}</td><td class="r">${eigenB.irr!=null?(Math.round(eigenB.irr*1000)/10)+' %':'—'}</td><td class="r">${eur(eigenB.totaal)}</td></tr>
  </table>
  <p class="sm">Aannames: AC verkocht aan ${eur2(par.ac_verkoop_eur_kwh)}/kWh (CREG-niveau), DC aan ${eur2(par.dc_verkoop_eur_kwh)}/kWh. Energieaankoop ${eur2(par.spot_aankoop_eur_kwh)}/kWh (blended; laden op de spotmarkt, sturing van AC en batterij op de goedkoopste momenten). Geen netkosten verondersteld (bestaande aansluiting, batterij buffert de DC-piek). Model ${par.model_jaren} jaar; verkoopprijs groeit inflatie + ${Math.round(par.verkoop_spread*100)}%, aankoop volgt inflatie (${Math.round(par.inflatie*100)}%).</p>

  ${bijlage('A','Scenario alleen AC', sizeA, mA, eigenA, concA, concOpA)}
  ${bijlage('B','Scenario AC + 1× DC 150 kVA + batterij', sizeB, mB, eigenB, concB, concOpB)}

  <div class="warn">⚠ Voorbehoud — Dit is een automatisch gegenereerde, indicatieve raming op basis van beperkt beschikbare gegevens (parkinggrootte en locatietype). Alle bedragen en volumes zijn richtwaarden die na een plaatsbezoek en een netcheck verfijnd worden. De aanname van nul netkosten geldt enkel bij een slimme configuratie en sturing die de maandpiek onder het bestaande toegangsvermogen houdt; dat wordt in een tweede versie geverifieerd.</div>
  <div class="foot">Gegenereerd door de Energie-Compas Lead Generator · ${new Date().toLocaleDateString('nl-BE')} · versie 1 (concept)</div>
  </body></html>`;
}

// ─── render ───────────────────────────────────────────────────────────
async function renderPdf(html){
  let browser;
  const puppeteer = require('puppeteer-core');   // zoals de rest van de proxy (geen bundled chromium)
  try {
    browser = await puppeteer.launch({ executablePath: EXE, headless: true, args: ['--no-sandbox','--disable-setuid-sandbox','--disable-gpu','--disable-dev-shm-usage','--font-render-hinting=none'] });
    const page = await browser.newPage();
    await page.setContent(html, { waitUntil: 'networkidle0', timeout: 45000 });
    const buf = await page.pdf({ format:'A4', printBackground:true, preferCSSPageSize:true });
    return Buffer.from(buf);   // puppeteer-core 23 geeft Uint8Array terug; Express res.send wil een Buffer
  } finally { if (browser) try { await browser.close(); } catch(e){} }
}

// ─── luchtfoto zoals EK: Esri World Imagery export (token-vrij) → base64 data-URI ──
async function aerialDataUri(lat, lng, _fetch){
  try {
    // grond-span bepaalt de uitsnede (parking + clubgebouw + omliggende percelen);
    // de uitvoerresolutie blijft binnen wat de World_Imagery-export aankan (fijner dan ~z19 geeft HTTP 500).
    const grondBreedte = 460, w = 760, h = 360;           // meter grondafstand horizontaal × uitvoerpixels (ruim overzicht)
    const latK = 1/Math.cos(lat*Math.PI/180);             // Web Mercator rekt met de breedtegraad (BE ~51°)
    const mPx = grondBreedte/w;                            // grond-meter per pixel
    const R=6378137, x=R*lng*Math.PI/180, y=R*Math.log(Math.tan(Math.PI/4 + lat*Math.PI/360));
    const hw=w/2*mPx*latK, hh=h/2*mPx*latK;               // halve bbox in Mercator-meter
    const url=`https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/export?bbox=${(x-hw).toFixed(2)},${(y-hh).toFixed(2)},${(x+hw).toFixed(2)},${(y+hh).toFixed(2)}&bboxSR=3857&imageSR=3857&size=${w},${h}&format=jpg&f=image`;
    const r = await _fetch(url); if (!r.ok) return null;
    const ct = String(r.headers.get('content-type')||''); if (!ct.startsWith('image/')) return null;
    const buf = Buffer.from(await r.arrayBuffer());
    return 'data:'+ct+';base64,'+buf.toString('base64');
  } catch(e){ return null; }
}

// ─── ligging: adres + kaarten + nabije infrastructuur en bestemmingen ──
// Alles null-/leeg-veilig: ontbreekt de token of een bron, dan valt dat stuk
// gewoon weg en rendert de rest van het rapport normaal.
const MAPBOX   = process.env.MAPBOX_TOKEN || '';
const GEM_BASE = 'https://johanmmk.github.io/gemeenten/';
const ENT_BASE = 'https://www.energie-compas.eu/apps/data/';
const DAG = 864e5;
let _statsCache = null, _statsAt = 0;
const _parkCache = {};
let _entCache = null, _entAt = 0;

function haversine(aLat,aLng,bLat,bLng){
  const R=6371000, dLa=(bLat-aLat)*Math.PI/180, dLo=(bLng-aLng)*Math.PI/180;
  const s=Math.sin(dLa/2)**2 + Math.cos(aLat*Math.PI/180)*Math.cos(bLat*Math.PI/180)*Math.sin(dLo/2)**2;
  return 2*R*Math.asin(Math.sqrt(s));
}
function _norm(s){ return String(s||'').toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g,'').replace(/[^a-z0-9]+/g,''); }

// JS-literal ([...] of {...}) scannen vanaf een marker (zoals gemeentekaart.html)
function literalNa(src, marker){
  let i = src.indexOf(marker); if (i<0) return null; i += marker.length;
  while (i<src.length && /\s/.test(src[i])) i++;
  const open=src[i]; if (open!=='[' && open!=='{') return null;
  const close = open==='[' ? ']' : '}'; let depth=0, inStr=false, q='';
  for (let j=i;j<src.length;j++){ const c=src[j];
    if (inStr){ if (c==='\\'){ j++; continue; } if (c===q) inStr=false; }
    else { if (c==='"'||c==="'"){ inStr=true; q=c; } else if (c===open) depth++; else if (c===close){ depth--; if (depth===0) return src.slice(i,j+1); } }
  } return null;
}

async function reverseGeocode(lat,lng,_fetch){
  if (!MAPBOX) return null;
  try {
    const u=`https://api.mapbox.com/geocoding/v5/mapbox.places/${lng},${lat}.json?language=nl&limit=1&types=address,poi,place&access_token=${MAPBOX}`;
    const r=await _fetch(u); if (!r.ok) return null; const j=await r.json();
    const f=(j.features||[])[0]; if (!f) return null;
    let gemeente='', pc='';
    for (const c of (f.context||[])){ const id=String(c.id||''); if (id.startsWith('place')&&!gemeente) gemeente=c.text; if (id.startsWith('postcode')&&!pc) pc=c.text; }
    const straat=[f.text, f.address].filter(Boolean).join(' ');
    return { adres: f.place_name||'', straat, gemeente, pc };
  } catch(e){ return null; }
}

async function gemeenteSlug(gemeenteNaam,_fetch){
  if (!gemeenteNaam) return null;
  try {
    if (!_statsCache || Date.now()-_statsAt>DAG){
      const r=await _fetch(GEM_BASE+'gemeenten_stats.json',{cache:'no-cache'});
      if (r.ok){ _statsCache=await r.json(); _statsAt=Date.now(); }
    }
    if (!_statsCache) return null;
    const want=_norm(gemeenteNaam);
    for (const k in _statsCache){ const v=_statsCache[k]; if (v && _norm(v.naam)===want) return v.slug||v.naam; }
    return null;
  } catch(e){ return null; }
}

async function parkingInfra(slug,_fetch){
  if (!slug) return { sites:[], cabines:[], chargers:[] };
  try {
    const c=_parkCache[slug]; if (c && Date.now()-c.at<DAG) return c.data;
    const u=GEM_BASE+'Kaart_bedrijfsparkings_'+encodeURIComponent(slug)+'.html';
    const r=await _fetch(u,{cache:'no-cache'}); if (!r.ok) throw 0;
    const h=await r.text();
    const parse=m=>{ const lit=literalNa(h,m); if (!lit) return []; try{ const a=JSON.parse(lit); return Array.isArray(a)?a:[]; }catch(e){ return []; } };
    const data={ sites:parse('sites='), cabines:parse('cabines='), chargers:parse('chargers=') };
    _parkCache[slug]={ at:Date.now(), data }; return data;
  } catch(e){ return { sites:[], cabines:[], chargers:[] }; }
}

async function entityData(_fetch){
  if (_entCache && Date.now()-_entAt<DAG) return _entCache;
  const grab=async f=>{ try{ const r=await _fetch(ENT_BASE+f,{cache:'no-cache'}); if (!r.ok) return []; const j=await r.json(); return Array.isArray(j)?j:[]; }catch(e){ return []; } };
  try {
    const [sport,zorg,scholen]=await Promise.all([grab('sportclub_leads.json'),grab('zorg_leads.json'),grab('scholen.json')]);
    _entCache={ sport,zorg,scholen }; _entAt=Date.now(); return _entCache;
  } catch(e){ return { sport:[], zorg:[], scholen:[] }; }
}

// nabije bestemmingen (sportclub/school/zorg) binnen radiusM, de locatie zelf uitgesloten
function nearbyDest(ent, lat, lng, radiusM, capPer){
  const NETLBL={ go:'GO!', gemeente:'gemeente', katholiek:'katholiek', overig:'onderwijs' };
  const out=[];
  const add=(arr, kind, sf)=>{
    const near=[];
    for (const e of (arr||[])){ const la=+e.lat, lo=+e.lng; if (!isFinite(la)||!isFinite(lo)) continue;
      const d=haversine(lat,lng,la,lo); if (d>radiusM || d<10) continue;   // d<10 m = de locatie zelf
      near.push({ kind, naam:String(e.n||'—'), adres:[e.g,e.pc].filter(Boolean).join(' '), web:String(e.web||''), sub:sf(e), lat:la, lng:lo, d:Math.round(d) });
    }
    near.sort((a,b)=>a.d-b.d); out.push(...near.slice(0,capPer));
  };
  add(ent.sport,   'sport',  e=>(e.s||[]).join(', ')||'sportclub');
  add(ent.scholen, 'school', e=>NETLBL[e.net]||'onderwijs');
  add(ent.zorg,    'zorg',   e=>e.t||'zorg');
  return out;
}

// Mapbox Static Images → base64 data-URI
function _pin(size, icon, color, lng, lat){ return `pin-${size}${icon?'-'+icon:''}+${color}(${(+lng).toFixed(5)},${(+lat).toFixed(5)})`; }
async function mapboxStatic(style, overlays, lng, lat, zoom, w, h, _fetch){
  if (!MAPBOX) return null;
  try {
    const ov = overlays ? overlays+'/' : '';
    const u=`https://api.mapbox.com/styles/v1/mapbox/${style}/static/${ov}${lng},${lat},${zoom}/${w}x${h}@2x?access_token=${MAPBOX}`;
    const r=await _fetch(u); if (!r.ok) return null;
    const ct=String(r.headers.get('content-type')||''); if (!ct.startsWith('image/')) return null;
    const buf=Buffer.from(await r.arrayBuffer());
    return 'data:'+ct+';base64,'+buf.toString('base64');
  } catch(e){ return null; }
}

// kleuren van de omgevingskaart (ook in de legende)
const KLEUR = { locatie:'C0392B', parking:'1F3864', cabine:'E0A800', laadpaal:'05803B', sport:'8E44AD', school:'2980B9', zorg:'16A085' };

async function liggingData(lat, lng, gemeenteHint, _fetch){
  const geo = await reverseGeocode(lat,lng,_fetch);
  const gemeente = (geo && geo.gemeente) || gemeenteHint || '';
  const slug = await gemeenteSlug(gemeente,_fetch);
  const infraAll = await parkingInfra(slug,_fetch);
  const ent = await entityData(_fetch);

  // nabije infra (voor de kaart) — binnen ~1 km
  const near = (arr, laf, lof, r)=> (arr||[]).map(x=>({ x, d:haversine(lat,lng,laf(x),lof(x)) })).filter(o=>o.d>=0 && o.d<=r).sort((a,b)=>a.d-b.d).map(o=>o.x);
  const sitesN   = near(infraAll.sites,   s=>+s.lat, s=>+s.lng, 1000).slice(0,10);
  const cabinesN = near(infraAll.cabines, c=>+c.la,  c=>+c.lo,  1000).slice(0,8);
  const chargersN= near(infraAll.chargers,c=>+c.la,  c=>+c.lo,  1000).slice(0,8);
  const dests    = nearbyDest(ent, lat, lng, 1200, 7);

  // overlays voor de omgevingskaart (locatie bovenaan laatst)
  const ov=[];
  sitesN.forEach(s=>ov.push(_pin('s','parking',KLEUR.parking,s.lng,s.lat)));
  cabinesN.forEach(c=>ov.push(_pin('s','',KLEUR.cabine,c.lo,c.la)));
  chargersN.forEach(c=>ov.push(_pin('s','charging-station',KLEUR.laadpaal,c.lo,c.la)));
  dests.forEach(d=>{ const ic=d.kind==='sport'?'pitch':d.kind==='school'?'school':'hospital'; ov.push(_pin('s',ic,KLEUR[d.kind],d.lng,d.lat)); });
  ov.push(_pin('l','star',KLEUR.locatie,lng,lat));

  const [belgie, omgeving] = await Promise.all([
    mapboxStatic('light-v11', _pin('l','star',KLEUR.locatie,lng,lat), lng, lat, 6.4, 300, 380, _fetch),
    mapboxStatic('streets-v12', ov.join(','), lng, lat, 14, 660, 460, _fetch),
  ]);

  return {
    adres: geo ? (geo.adres || [geo.straat, geo.pc, geo.gemeente].filter(Boolean).join(', ')) : '',
    gemeente, belgie, omgeving, dests,
    telInfra: { parkings:sitesN.length, cabines:cabinesN.length, laadpalen:chargersN.length },
  };
}

// ─── parameter-opslag (oekene calibreert vanuit de kaart) ──────────────
const PARAM_PAD = 'config/destination-params.json';
async function loadStored(deps){
  try { const t = await deps.factuurDownload(PARAM_PAD, true); const j = JSON.parse(t); return (j && typeof j==='object' && !Array.isArray(j)) ? j : null; }
  catch(e){ return null; }
}

// ─── mount ────────────────────────────────────────────────────────────
module.exports = function mountDestination(app, deps){
  deps = deps || {};
  const managerGuard = deps.managerGuard;
  const _fetch = deps.fetch || global.fetch;

  // lees de effectieve parameters (DEFAULTS + opgeslagen overrides)
  app.get('/api/destination-params', async (req, res) => {
    try {
      let u = null;
      if (typeof managerGuard === 'function') { u = await managerGuard(req, res); if (!u) return; }
      const stored = (typeof deps.factuurDownload==='function') ? await loadStored(deps) : null;
      res.json({ ok:true, params: Object.assign({}, DEFAULTS, stored||{}), is_oekene: String((u&&u.email)||'').toLowerCase()===OEKENE });
    } catch (e) { res.status(500).json({ ok:false, error:e.message }); }
  });
  // schrijf de parameters (enkel oekene)
  app.post('/api/destination-params', async (req, res) => {
    try {
      let u = null;
      if (typeof managerGuard === 'function') { u = await managerGuard(req, res); if (!u) return; }
      if (String((u&&u.email)||'').toLowerCase() !== OEKENE) return res.status(403).json({ ok:false, error:'Enkel oekene@gmail.com kan de rapport-parameters wijzigen.' });
      const p = (req.body && req.body.params && typeof req.body.params==='object') ? req.body.params : (req.body||{});
      if (typeof p!=='object' || Array.isArray(p)) return res.status(400).json({ ok:false, error:'params moet een object zijn' });
      if (typeof deps.factuurUpload === 'function') await deps.factuurUpload(Buffer.from(JSON.stringify(p),'utf8').toString('base64'), 'application/json', PARAM_PAD);
      console.log(`[destination] parameters bijgewerkt door ${(u&&u.email)||'oekene'}`);
      res.json({ ok:true, params: Object.assign({}, DEFAULTS, p) });
    } catch (e) { res.status(500).json({ ok:false, error:e.message }); }
  });

  app.post('/api/destination-rapport', async (req, res) => {
    try {
      // auth: manager + oekene-only
      let u = null;
      if (typeof managerGuard === 'function') { u = await managerGuard(req, res); if (!u) return; }
      if (!u || String(u.email||'').toLowerCase() !== OEKENE)
        return res.status(403).json({ ok:false, error:'Enkel oekene@gmail.com kan destination-rapporten genereren.' });

      const b = req.body || {};
      const naam = String(b.naam||'Locatie').slice(0,120);
      const type = TYPE_LABEL[b.type] ? b.type : 'geen';
      const lat = +b.lat, lng = +b.lng;
      if (!isFinite(lat) || !isFinite(lng)) return res.status(400).json({ ok:false, error:'lat/lng ontbreken' });
      const gemeente = String(b.gemeente||'').slice(0,80);
      const stored = (typeof deps.factuurDownload==='function') ? await loadStored(deps) : null;
      const par = Object.assign({}, DEFAULTS, stored||{}, (b.params && typeof b.params==='object') ? b.params : {});

      // parkinggrootte: uit de body (client kent de nabije bedrijfsparking) of GRB-dak als proxy
      let plaatsen = (+b.plaatsen>0) ? +b.plaatsen : 0;
      if (!plaatsen && typeof deps.grbDak === 'function') {
        try { const g = await deps.grbDak(lat,lng); if (g && g.opp_m2) plaatsen = Math.round(g.opp_m2 / par.plaats_per_m2_dak); } catch(e){}
      }

      const sizeA = sizing(plaatsen, type, par);
      const sizeB = Object.assign({}, sizeA); // zelfde parking; B voegt DC toe
      const mA = scenarioModel(sizeA, false, par);
      const mB = scenarioModel(sizeB, true,  par);

      // luchtfoto (Esri) + ligging (adres, kaarten, nabije infra/bestemmingen) parallel
      const [luchtfoto, ligging] = await Promise.all([
        aerialDataUri(lat, lng, _fetch).catch(()=>null),
        liggingData(lat, lng, gemeente, _fetch).catch(()=>({})),
      ]);

      const html = rapportHtml({ naam, type, gemeente: (ligging&&ligging.gemeente)||gemeente, lat, lng, luchtfoto, sizeA, sizeB, mA, mB, par, ligging });
      const pdf = await renderPdf(html);

      res.set('Content-Type','application/pdf');
      res.set('Content-Disposition', `inline; filename="Destination_${naam.replace(/[^A-Za-z0-9]+/g,'_')}.pdf"`);
      return res.send(pdf);
    } catch (e) {
      console.error('[destination-rapport]', e && e.message);
      return res.status(500).json({ ok:false, error: (e && e.message) || 'render mislukt' });
    }
  });

  console.log('[destination] /api/destination-rapport gemount');
};
