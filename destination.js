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

// Betere Lader-logo (inline SVG, merk-kleuren #003658 / #6BAE41) — zelfde als /apps/bl/betere-lader-logo.svg
const BL_LOGO = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 464 120" role="img" aria-label="Betere Lader"><path d="M21.01 81.88 A46 46 0 0 1 52.44 16.01" fill="none" stroke="#003658" stroke-width="3" stroke-linecap="round"/><path d="M77.73 17.77 A46 46 0 0 1 77.73 104.23" fill="none" stroke="#6BAE41" stroke-width="3" stroke-linecap="round"/><path d="M62 1.5 L67.5 13 L56.5 13Z" fill="#003658"/><path d="M62 118.5 L67.5 107 L56.5 107Z" fill="#003658"/><path d="M1.5 61 L11 55.5 L11 66.5Z" fill="#003658"/><path d="M122.5 61 L113 55.5 L113 66.5Z" fill="#003658"/><path d="M84 31 L52.5 58.5 L66.5 67.5Z" fill="#6BAE41"/><path d="M35 95 L52.5 58.5 L66.5 67.5Z" fill="#003658"/><circle cx="59.5" cy="63" r="4.6" fill="#FFFFFF"/><path d="M141.01661631419938 80.0V39.0H159.41087613293053Q164.2416918429003 39.0 167.059667673716 40.67220543806647Q169.8776435045317 42.34441087613293 171.11631419939576 44.85271903323263Q172.35498489425981 47.36102719033233 172.35498489425981 49.838368580060425Q172.35498489425981 53.80211480362538 170.68277945619334 55.938821752265866Q169.01057401812687 58.07552870090635 166.8429003021148 59.066465256797585Q168.57703927492446 59.62386706948641 170.34214501510573 60.831570996978854Q172.107250755287 62.0392749244713 173.28398791540786 64.08308157099698Q174.4607250755287 66.12688821752266 174.4607250755287 69.22356495468279Q174.4607250755287 73.80664652567975 170.96148036253777 76.90332326283988Q167.4622356495468 80.0 160.46374622356495 80.0ZM145.66163141993957 57.33232628398792H159.41087613293053Q161.5785498489426 57.33232628398792 163.46752265861028 56.58912386706949Q165.35649546827796 55.84592145015105 166.5332326283988 54.266616314199396Q167.70996978851963 52.68731117824774 167.70996978851963 50.20996978851964Q167.70996978851963 48.47583081570997 166.9358006042296 46.92749244712991Q166.16163141993957 45.37915407854985 164.4584592145015 44.388217522658614Q162.75528700906344 43.397280966767376 159.90634441087613 43.397280966767376H145.66163141993957ZM145.66163141993957 75.60271903323263H160.9592145015106Q165.17069486404833 75.60271903323263 167.49320241691842 73.83761329305136Q169.81570996978851 72.07250755287009 169.81570996978851 68.85196374622356Q169.81570996978851 65.94108761329305 167.58610271903325 63.742447129909365Q165.35649546827796 61.54380664652568 160.46374622356495 61.54380664652568H145.66163141993957ZM191.55438066465257 80.49546827794562Q186.84743202416917 80.49546827794562 183.31722054380663 78.35876132930514Q179.7870090634441 76.22205438066466 177.83610271903322 72.56797583081571Q175.88519637462235 68.91389728096676 175.88519637462235 64.5166163141994Q175.88519637462235 60.05740181268882 177.74320241691842 56.434290030211486Q179.6012084592145 52.81117824773414 183.0694864048338 50.67447129909365Q186.53776435045316 48.53776435045317 191.24471299093653 48.53776435045317Q196.01359516616313 48.53776435045317 199.45090634441087 50.67447129909365Q202.8882175226586 52.81117824773414 204.74622356495468 56.434290030211486Q206.60422960725074 60.05740181268882 206.60422960725074 64.5166163141994V66.37462235649546H180.65407854984895Q181.02567975830814 69.09969788519638 182.41918429003022 71.36027190332327Q183.81268882175226 73.62084592145015 186.16616314199393 74.95241691842901Q188.51963746223564 76.28398791540785 191.61631419939576 76.28398791540785Q194.89879154078548 76.28398791540785 197.12839879154077 74.8285498489426Q199.35800604229607 73.37311178247734 200.59667673716012 71.08157099697885H205.67522658610272Q204.06495468277944 75.23111782477342 200.56570996978851 77.86329305135952Q197.06646525679758 80.49546827794562 191.55438066465257 80.49546827794562ZM180.71601208459214 62.0392749244713H201.77341389728096Q201.15407854984895 58.07552870090635 198.42900302114805 55.41238670694864Q195.70392749244712 52.74924471299094 191.24471299093653 52.74924471299094Q186.78549848942598 52.74924471299094 184.09138972809666 55.41238670694864Q181.39728096676737 58.07552870090635 180.71601208459214 62.0392749244713ZM212.42598187311177 80.0V53.120845921450154H206.1087613293051V49.03323262839879H212.42598187311177V37.94712990936556H217.07099697885195V49.03323262839879H224.19335347432022V53.120845921450154H217.07099697885195V80.0ZM239.80060422960725 80.49546827794562Q235.09365558912384 80.49546827794562 231.5634441087613 78.35876132930514Q228.03323262839876 76.22205438066466 226.0823262839879 72.56797583081571Q224.13141993957703 68.91389728096676 224.13141993957703 64.5166163141994Q224.13141993957703 60.05740181268882 225.9894259818731 56.434290030211486Q227.84743202416917 52.81117824773414 231.3157099697885 50.67447129909365Q234.78398791540783 48.53776435045317 239.4909365558912 48.53776435045317Q244.2598187311178 48.53776435045317 247.69712990936554 50.67447129909365Q251.13444108761328 52.81117824773414 252.99244712990935 56.434290030211486Q254.85045317220542 60.05740181268882 254.85045317220542 64.5166163141994V66.37462235649546H228.90030211480362Q229.27190332326282 69.09969788519638 230.6654078549849 71.36027190332327Q232.05891238670694 73.62084592145015 234.4123867069486 74.95241691842901Q236.7658610271903 76.28398791540785 239.86253776435044 76.28398791540785Q243.14501510574016 76.28398791540785 245.37462235649545 74.8285498489426Q247.60422960725074 73.37311178247734 248.8429003021148 71.08157099697885H253.9214501510574Q252.31117824773412 75.23111782477342 248.8119335347432 77.86329305135952Q245.31268882175226 80.49546827794562 239.80060422960725 80.49546827794562ZM228.9622356495468 62.0392749244713H250.01963746223564Q249.40030211480362 58.07552870090635 246.67522658610272 55.41238670694864Q243.9501510574018 52.74924471299094 239.4909365558912 52.74924471299094Q235.03172205438065 52.74924471299094 232.33761329305133 55.41238670694864Q229.64350453172204 58.07552870090635 228.9622356495468 62.0392749244713ZM257.3277945619335 80.0V49.03323262839879H261.9728096676737V56.65105740181269Q262.2205438066465 55.84592145015105 262.9327794561933 54.514350453172206Q263.6450151057402 53.18277945619336 265.00755287009065 51.8202416918429Q266.37009063444106 50.457703927492446 268.44486404833833 49.49773413897281Q270.5196374622356 48.53776435045317 273.3685800604229 48.53776435045317H273.67824773413895V53.18277945619336H273.12084592145015Q269.6525679758308 53.18277945619336 267.1442598187311 55.07175226586103Q264.6359516616314 56.9607250755287 263.3043806646525 59.96450151057402Q261.9728096676737 62.968277945619334 261.9728096676737 66.37462235649546V80.0ZM289.22356495468273 80.49546827794562Q284.5166163141994 80.49546827794562 280.98640483383684 78.35876132930514Q277.4561933534743 76.22205438066466 275.5052870090634 72.56797583081571Q273.5543806646525 68.91389728096676 273.5543806646525 64.5166163141994Q273.5543806646525 60.05740181268882 275.4123867069486 56.434290030211486Q277.2703927492447 52.81117824773414 280.738670694864 50.67447129909365Q284.20694864048335 48.53776435045317 288.91389728096675 48.53776435045317Q293.6827794561933 48.53776435045317 297.12009063444106 50.67447129909365Q300.5574018126888 52.81117824773414 302.4154078549849 56.434290030211486Q304.27341389728093 60.05740181268882 304.27341389728093 64.5166163141994V66.37462235649546H278.32326283987913Q278.69486404833833 69.09969788519638 280.0883685800604 71.36027190332327Q281.4818731117824 73.62084592145015 283.83534743202415 74.95241691842901Q286.1888217522658 76.28398791540785 289.28549848942595 76.28398791540785Q292.56797583081567 76.28398791540785 294.79758308157096 74.8285498489426Q297.02719033232626 73.37311178247734 298.2658610271903 71.08157099697885H303.3444108761329Q301.73413897280966 75.23111782477342 298.2348942598187 77.86329305135952Q294.73564954682774 80.49546827794562 289.22356495468273 80.49546827794562ZM278.38519637462235 62.0392749244713H299.44259818731115Q298.82326283987913 58.07552870090635 296.0981873111782 55.41238670694864Q293.3731117824773 52.74924471299094 288.91389728096675 52.74924471299094Q284.45468277945616 52.74924471299094 281.7605740181268 55.41238670694864Q279.0664652567975 58.07552870090635 278.38519637462235 62.0392749244713Z" fill="#003658"/><path d="M319.61435045317216 80.0V39.0H324.25936555891235V75.54078549848943H346.61737160120845V80.0ZM361.481419939577 80.49546827794562Q356.96027190332325 80.49546827794562 353.5848942598187 78.35876132930514Q350.2095166163142 76.22205438066466 348.3515105740181 72.56797583081571Q346.493504531722 68.91389728096676 346.493504531722 64.5166163141994Q346.493504531722 60.05740181268882 348.3515105740181 56.434290030211486Q350.2095166163142 52.81117824773414 353.5848942598187 50.67447129909365Q356.96027190332325 48.53776435045317 361.481419939577 48.53776435045317Q365.32129909365557 48.53776435045317 368.0773413897281 50.02416918429003Q370.8333836858006 51.51057401812689 372.56752265861024 54.04984894259819V49.03323262839879H377.21253776435043V80.0H372.56752265861024V75.04531722054381Q370.8333836858006 77.5226586102719 368.0773413897281 79.00906344410876Q365.32129909365557 80.49546827794562 361.481419939577 80.49546827794562ZM362.10075528700906 76.28398791540785Q365.5690332326284 76.28398791540785 367.95347432024164 74.67371601208458Q370.33791540785495 73.06344410876133 371.57658610271903 70.36933534743203Q372.81525679758306 67.67522658610272 372.81525679758306 64.5166163141994Q372.81525679758306 61.29607250755287 371.57658610271903 58.63293051359517Q370.33791540785495 55.96978851963746 367.95347432024164 54.359516616314195Q365.5690332326284 52.74924471299094 362.10075528700906 52.74924471299094Q358.6944108761329 52.74924471299094 356.21706948640485 54.359516616314195Q353.73972809667674 55.96978851963746 352.4391238670695 58.63293051359517Q351.1385196374622 61.29607250755287 351.1385196374622 64.5166163141994Q351.1385196374622 67.67522658610272 352.4391238670695 70.36933534743203Q353.73972809667674 73.06344410876133 356.21706948640485 74.67371601208458Q358.6944108761329 76.28398791540785 362.10075528700906 76.28398791540785ZM395.1113293051359 80.49546827794562Q390.59018126888213 80.49546827794562 387.2148036253776 78.35876132930514Q383.8394259818731 76.22205438066466 381.981419939577 72.56797583081571Q380.1234138972809 68.91389728096676 380.1234138972809 64.5166163141994Q380.1234138972809 60.05740181268882 381.981419939577 56.434290030211486Q383.8394259818731 52.81117824773414 387.2148036253776 50.67447129909365Q390.59018126888213 48.53776435045317 395.1113293051359 48.53776435045317Q398.95120845921446 48.53776435045317 401.70725075528696 50.02416918429003Q404.46329305135947 51.51057401812689 406.19743202416913 54.04984894259819V36.64652567975831H410.8424471299093V80.0H406.19743202416913V75.04531722054381Q404.46329305135947 77.5226586102719 401.70725075528696 79.00906344410876Q398.95120845921446 80.49546827794562 395.1113293051359 80.49546827794562ZM395.73066465256795 76.28398791540785Q399.1989425981873 76.28398791540785 401.5833836858005 74.67371601208458Q403.96782477341384 73.06344410876133 405.2064954682779 70.36933534743203Q406.44516616314195 67.67522658610272 406.44516616314195 64.5166163141994Q406.44516616314195 61.29607250755287 405.2064954682779 58.63293051359517Q403.96782477341384 55.96978851963746 401.5833836858005 54.359516616314195Q399.1989425981873 52.74924471299094 395.73066465256795 52.74924471299094Q392.3243202416918 52.74924471299094 389.84697885196374 54.359516616314195Q387.36963746223563 55.96978851963746 386.0690332326284 58.63293051359517Q384.7684290030211 61.29607250755287 384.7684290030211 64.5166163141994Q384.7684290030211 67.67522658610272 386.0690332326284 70.36933534743203Q387.36963746223563 73.06344410876133 389.84697885196374 74.67371601208458Q392.3243202416918 76.28398791540785 395.73066465256795 76.28398791540785ZM429.42250755287 80.49546827794562Q424.71555891238665 80.49546827794562 421.1853474320241 78.35876132930514Q417.6551359516616 76.22205438066466 415.7042296072507 72.56797583081571Q413.7533232628398 68.91389728096676 413.7533232628398 64.5166163141994Q413.7533232628398 60.05740181268882 415.6113293051359 56.434290030211486Q417.469335347432 52.81117824773414 420.9376132930513 50.67447129909365Q424.4058912386706 48.53776435045317 429.112839879154 48.53776435045317Q433.8817220543806 48.53776435045317 437.3190332326283 50.67447129909365Q440.75634441087607 52.81117824773414 442.61435045317216 56.434290030211486Q444.4723564954682 60.05740181268882 444.4723564954682 64.5166163141994V66.37462235649546H418.5222054380664Q418.8938066465256 69.09969788519638 420.28731117824765 71.36027190332327Q421.6808157099697 73.62084592145015 424.0342900302114 74.95241691842901Q426.3877643504531 76.28398791540785 429.4844410876132 76.28398791540785Q432.76691842900294 76.28398791540785 434.99652567975824 74.8285498489426Q437.22613293051353 73.37311178247734 438.46480362537756 71.08157099697885H443.54335347432016Q441.93308157099693 75.23111782477342 438.433836858006 77.86329305135952Q434.934592145015 80.49546827794562 429.42250755287 80.49546827794562ZM418.5841389728096 62.0392749244713H439.6415407854984Q439.0222054380664 58.07552870090635 436.2971299093655 55.41238670694864Q433.57205438066455 52.74924471299094 429.112839879154 52.74924471299094Q424.65362537764344 52.74924471299094 421.9595166163141 55.41238670694864Q419.2654078549848 58.07552870090635 418.5841389728096 62.0392749244713ZM446.9496978851963 80.0V49.03323262839879H451.5947129909365V56.65105740181269Q451.8424471299093 55.84592145015105 452.55468277945613 54.514350453172206Q453.26691842900294 53.18277945619336 454.6294561933534 51.8202416918429Q455.99199395770387 50.457703927492446 458.06676737160114 49.49773413897281Q460.1415407854984 48.53776435045317 462.9904833836857 48.53776435045317H463.30015105740176V53.18277945619336H462.7427492447129Q459.2744712990936 53.18277945619336 456.7661631419939 55.07175226586103Q454.2578549848942 56.9607250755287 452.9262839879153 59.96450151057402Q451.5947129909365 62.968277945619334 451.5947129909365 66.37462235649546V80.0Z" fill="#6BAE41"/></svg>`;

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
  let totaal=0; const rij=[]; const cfs=[0]; let jaar1=0;
  for (let j=1;j<=par.model_jaren;j++){
    const gV=Math.pow(1+par.inflatie+par.verkoop_spread,j-1), gC=Math.pow(1+par.inflatie,j-1);
    const omzet=m.omzet*gV, deel=omzet*omzetdeelPct(omzet,par.omzetdeling), pk=m.parkeergeld*gC;
    const y=deel+pk; cfs.push(y); totaal+=y; if (j===1) jaar1=y;
    if (j<=3||j===par.model_jaren) rij.push({j, deel, pk});
  }
  return { totaal, rij, jaar1, ncw: npv(par.discontovoet, cfs) };
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
      <div class="col" style="flex:1.55">
        <h3>Rendement voor de locatie-eigenaar</h3>
        <table class="kv cmp">
          <tr><td class="lbl"></td><th>Eigen financiering</th><th>Concessie (concessiegever)</th></tr>
          <tr><td class="lbl">Investering (CAPEX)</td><td class="r">${eur(m.capex)}</td><td class="r">${eur(0)}</td></tr>
          <tr><td class="lbl">Opbrengst jaar 1</td><td class="r">${eur(m.netto_jaar1)}</td><td class="r">${eur(conc.jaar1)}</td></tr>
          <tr><td class="lbl">Totaal over ${par.model_jaren} jaar</td><td class="r">${eur(eigen.totaal)}</td><td class="r">${eur(conc.totaal)}</td></tr>
          <tr><td class="lbl">IRR</td><td class="r">${eigen.irr!=null?(Math.round(eigen.irr*1000)/10)+' %':'—'}</td><td class="r">n.v.t.</td></tr>
          <tr><td class="lbl">NCW @ ${Math.round(par.discontovoet*100)}%</td><td class="r">${eur(eigen.ncw)}</td><td class="r">${eur(conc.ncw)}</td></tr>
          <tr><td class="lbl">Terugverdientijd</td><td class="r">${eigen.tvt?eigen.tvt+' jaar':'> '+par.model_jaren+' jaar'}</td><td class="r">—</td></tr>
        </table>
        <p class="sm">Twee routes voor de eigenaar: <b>zelf financieren</b> (draagt de CAPEX, houdt de volledige netto-opbrengst) of een <b>concessie verlenen</b> (geen investering; ontvangt de omzetdeling ${staffelTxt} plus het parkeergeld). Zonder eigen investering zijn IRR en terugverdientijd niet van toepassing.</p>
      </div>
      <div class="col">
        <h3>Rendement voor de concessienemer</h3>
        ${kvTable([
          ['Investering (CAPEX)', eur(m.capex)],
          ['Netto jaar 1', eur(concOp.cfs[1]||0)],
          ['Totaal over '+par.model_jaren+' jaar', eur(concOp.totaal)],
          ['IRR', concOp.irr!=null?(Math.round(concOp.irr*1000)/10)+' %':'—'],
          ['NCW @ '+Math.round(par.discontovoet*100)+'%', eur(concOp.ncw)],
          ['Terugverdientijd', concOp.tvt?concOp.tvt+' jaar':'> '+par.model_jaren+' jaar'],
        ])}
        <p class="sm">De concessienemer (operator) draagt de CAPEX en de exploitatie en houdt de energiemarge minus de omzetdeling aan de locatie. Zelfde parameters als de eigen financiering ernaast; het parkeergeld gaat naar de locatie, niet naar de concessienemer.</p>
      </div>
    </div>`;
  }

  // ── Bijlage C: de gebruikte parameters (uit de OEKENE-kaart) ──
  function bijlageParams(){
    const pct=x=>Math.round(x*1000)/10+' %';
    const staffelRows = par.omzetdeling.map(function(x,i){
      const tot = x.tot>=1e11 ? 'daarboven' : ('tot € '+Math.round(x.tot/1000)+'k');
      return ['Omzetdeling '+(i+1)+' ('+tot+')', Math.round(x.pct*100)+' %'];
    });
    return `<h2><span class="nr">C</span>Gebruikte parameters</h2>
    <p class="sm">Alle berekeningen hierboven gebruiken deze rekenstanden. Ze worden beheerd via de OEKENE-kaart (knop «Rapport-parameters» in de Lead Generator) en op de server bewaard.</p>
    <div class="two">
      <div class="col">
        <h3>Model &amp; prijzen</h3>
        ${kvTable([
          ['Looptijd model', par.model_jaren+' jaar'],
          ['Disconteringsvoet', pct(par.discontovoet)],
          ['Inflatie (aankoop/opex)', pct(par.inflatie)],
          ['Groei verkoopprijs', 'inflatie + '+Math.round(par.verkoop_spread*100)+' %'],
          ['Verkoop AC', eur2(par.ac_verkoop_eur_kwh)+'/kWh'],
          ['Verkoop DC', eur2(par.dc_verkoop_eur_kwh)+'/kWh'],
          ['Energieaankoop (blended)', eur2(par.spot_aankoop_eur_kwh)+'/kWh'],
          ['Netkosten', eur2(par.netkosten_eur_kwh)+'/kWh'],
        ])}
        <h3>Sizing (V1 — enkel parking)</h3>
        ${kvTable([
          ['EV-aandeel parking', pct(par.ev_aandeel_parking)],
          ['Rotatiefactor AC', '÷ '+par.ac_rotatiefactor],
          ['Max. AC-palen', par.ac_max_palen+'×'],
          ['Parking indien onbekend', par.plaatsen_default+' plaatsen'],
        ])}
      </div>
      <div class="col">
        <h3>Investering (CAPEX)</h3>
        ${kvTable([
          ['AC-laadpaal', eur(par.capex_ac_paal)],
          ['DC 150 kVA (blok)', eur(par.capex_dc_150)],
          ['Batterij (blok)', eur(par.capex_batterij)],
          ['Aansluiting / EMS', eur(par.capex_aansluiting_ems)],
          ['Marge (contingency)', pct(par.contingency)],
        ])}
        <h3>Vaste jaarkosten (OPEX) &amp; inkomsten</h3>
        ${kvTable([
          ['Onderhoud laadpalen', eur(par.onderhoud_laadpalen_jaar)+'/jaar'],
          ['Onderhoud batterij (B)', eur(par.onderhoud_batterij_jaar)+'/jaar'],
          ['Sturing batterij (B)', eur2(par.sturing_batterij_jaar)+'/jaar'],
          ['Verzekering', pct(par.verzekering_pct_capex)+' van CAPEX'],
          ['Parkeergeld', eur(par.parkeergeld_eur_paal_dag)+'/paal/dag · '+par.parkeergeld_dagen_jaar+' dagen'],
        ].concat(staffelRows))}
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

  return `<!doctype html><html lang="nl"><head><meta charset="utf-8">
  <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@600;700;800&display=swap" rel="stylesheet"><style>
  :root{ --navy:#1F3864; --ink:#14203a; --acc:#05B050; --acc-d:#05803C; --acc-l:#EAF7EF; --muted:#5A6577; --lijn:#E3E8EF; }
  @page { size:A4; margin:15mm 13mm; }
  *{box-sizing:border-box} body{font-family:'Helvetica Neue',Helvetica,Arial,sans-serif;color:var(--ink);font-size:11px;line-height:1.45}
  h1,h2,h3{font-family:'Poppins','Helvetica Neue',Helvetica,Arial,sans-serif;letter-spacing:-.005em}
  h1{color:var(--navy);font-size:20px;margin:0 0 2px}
  h2{color:var(--navy);font-size:14px;margin:16px 0 6px;display:flex;align-items:center;gap:7px}
  h2 .nr{display:inline-flex;align-items:center;justify-content:center;width:19px;height:19px;border-radius:50%;background:var(--navy);color:#fff;font-size:10.5px;font-weight:700;flex:0 0 19px}
  h3{color:var(--navy);font-size:12px;margin:10px 0 4px} .sub{color:var(--muted);font-size:11px;margin:0 0 8px}
  .kop{display:flex;align-items:flex-end;justify-content:space-between;gap:12px;border-bottom:2px solid var(--acc);padding-bottom:7px;margin-bottom:10px}
  .kop .logo svg{height:30px;width:auto;display:block} .kop .ey{font-size:9.5px;font-weight:800;letter-spacing:.13em;text-transform:uppercase;color:var(--muted);text-align:right;white-space:nowrap}
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
  table.cmp th{background:#F2F6FC;color:#1F3864;font-size:10px;text-align:right;padding:4px 7px;border:1px solid #E2E8F2;white-space:nowrap}
  table.cmp td.lbl{color:#3a475c;font-weight:normal}
  table.kv td.r{text-align:right;font-weight:bold;white-space:nowrap} .two{display:flex;gap:12px;margin-top:6px} .col{flex:1}
  .sm{color:#5b6b85;font-size:10px;margin:4px 0} .tag{display:inline-block;background:var(--navy);color:#fff;border:1.5px solid var(--acc);border-radius:999px;font-size:10px;padding:2px 9px}
  .foot{color:var(--muted);font-size:9px;margin-top:16px;border-top:1px solid var(--lijn);padding-top:7px;display:flex;justify-content:space-between;gap:12px}
  .foot b{color:var(--navy)}
  .warn{background:#FFF7E6;border:1px solid #F0C36D;border-radius:6px;padding:8px;color:#8a5a00;font-size:10px;margin-top:10px}
  </style></head><body>
  <div class="kop">
    <div class="logo">${BL_LOGO}</div>
    <div class="ey">Destination-laadvoorstel · ${esc(new Date().toLocaleDateString('nl-BE'))}</div>
  </div>
  <h1>${esc(naam)}</h1>
  <div class="sub"><span class="tag">${esc(tl.label)}</span> &middot; ${esc(lg.adres||gemeente||'')} &middot; ${(+lat).toFixed(5)}, ${(+lng).toFixed(5)}</div>
  ${luchtfoto?`<img class="hero" src="${luchtfoto}" alt="luchtfoto">`:''}

  <h2><span class="nr">1</span>De locatie</h2>
  <p>${esc(tl.oms)}</p>
  ${kvTable([
    lg.adres?['Adres', esc(lg.adres)]:['Gemeente', esc(gemeente||'—')],
    ['Parking (geschat aantal plaatsen)', String(sizeA.plaatsen)],
    ['EV-aandeel parking (aanname)', Math.round(par.ev_aandeel_parking*100)+' %'],
    ['Voorgestelde AC-laadpunten', sizeA.nAC+'×'],
  ])}
  ${liggingSectie()}

  <h2><span class="nr">2</span>Waar parkeerders naartoe gaan</h2>
  <p class="sm">Bestemmingen in de buurt die de parking voeden — sportclubs, scholen en zorginstellingen binnen ~1,2 km. Klik een naam voor de website of de locatie op de kaart.</p>
  ${destLijst()}

  <h2><span class="nr">3</span>Twee operationele scenario's</h2>
  <div class="two">
    <div class="col"><h3>Scenario A — alleen AC</h3>
      ${kvTable([['AC-laadpunten', sizeA.nAC+'×'],['AC-laadverkeer', Math.round(mA.acKwhJaar).toLocaleString('nl-BE')+' kWh/jaar'],['Jaaromzet', eur(mA.omzet)],['Investering', eur(mA.capex)]])}
    </div>
    <div class="col"><h3>Scenario B — AC + 1× DC 150 kVA + batterij</h3>
      ${kvTable([['Laadpunten', sizeB.nAC+'× AC + 1× DC'],['Totaal laadverkeer', Math.round(mB.acKwhJaar+mB.dcKwhJaar).toLocaleString('nl-BE')+' kWh/jaar'],['Jaaromzet', eur(mB.omzet)],['Investering', eur(mB.capex)]])}
    </div>
  </div>

  <h2><span class="nr">4</span>Financiële samenvatting</h2>
  <table class="kv">
    <tr><td></td><td class="r">Concessie — opbrengst locatie (15 j)</td><td class="r">Eigen investering — IRR</td><td class="r">Eigen — netto (15 j)</td></tr>
    <tr><td>Scenario A (AC)</td><td class="r">${eur(concA.totaal)}</td><td class="r">${eigenA.irr!=null?(Math.round(eigenA.irr*1000)/10)+' %':'—'}</td><td class="r">${eur(eigenA.totaal)}</td></tr>
    <tr><td>Scenario B (AC+DC)</td><td class="r">${eur(concB.totaal)}</td><td class="r">${eigenB.irr!=null?(Math.round(eigenB.irr*1000)/10)+' %':'—'}</td><td class="r">${eur(eigenB.totaal)}</td></tr>
  </table>
  <p class="sm">Aannames: AC verkocht aan ${eur2(par.ac_verkoop_eur_kwh)}/kWh (CREG-niveau), DC aan ${eur2(par.dc_verkoop_eur_kwh)}/kWh. Energieaankoop ${eur2(par.spot_aankoop_eur_kwh)}/kWh (blended; laden op de spotmarkt, sturing van AC en batterij op de goedkoopste momenten). Geen netkosten verondersteld (bestaande aansluiting, batterij buffert de DC-piek). Model ${par.model_jaren} jaar; verkoopprijs groeit inflatie + ${Math.round(par.verkoop_spread*100)}%, aankoop volgt inflatie (${Math.round(par.inflatie*100)}%).</p>

  ${bijlage('A','Scenario alleen AC', sizeA, mA, eigenA, concA, concOpA)}
  ${bijlage('B','Scenario AC + 1× DC 150 kVA + batterij', sizeB, mB, eigenB, concB, concOpB)}
  ${bijlageParams()}

  <div class="warn">⚠ Voorbehoud — Dit is een automatisch gegenereerde, indicatieve raming op basis van beperkt beschikbare gegevens (parkinggrootte en locatietype). Alle bedragen en volumes zijn richtwaarden die na een plaatsbezoek en een netcheck verfijnd worden. De aanname van nul netkosten geldt enkel bij een slimme configuratie en sturing die de maandpiek onder het bestaande toegangsvermogen houdt; dat wordt in een tweede versie geverifieerd.</div>
  <div class="foot"><span><b>Betere Lader</b> · Energie-Compas Lead Generator</span><span>${esc(new Date().toLocaleDateString('nl-BE'))} · versie 1 (concept)</span></div>
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
