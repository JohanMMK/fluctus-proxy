// ═══════════════════════════════════════════════════════════════════════════════════════════════
//  vena-video.js v1.0.0 (16-11-2026, Johan) — V&A-VIDEO per dossier: «Je vragen, onze antwoorden»
//
//  Eén template, vier varianten (doelgroep: thuisladen · kmo · logistiek · zonder_ev), NL/FR/EN.
//  Input = window.__KR_VENA_DATA uit klantrapport.html (≥ 0.114.0, ?mode=vena) → exact dezelfde cijfers als de V&A-PDF.
//  Opbouw volgens «EK-videostijl — regieregels» (16-11):
//    1. bewegend kompas + begintune (ewb.js renderSting, liggend 1920×1080)
//    2. aankondiging: openingsvraag in beeld + de vier vragen; stem: wat de video toont
//    3. vier vragen, één per blok: vraag als titel, één groot getal + vergelijking, icoon; stem = spreekzin z
//    4. slotzin + «bespreek de volgende stap met je adviseur»
//    5. eindbeeld (± 5 s, niet uitgesproken): adviseur + e-mail, eindtune
//  Ondertitelvak = de gesproken zin. «Stem gegenereerd met AI» in beeld zodra er een stem is. Je-vorm. Batterij: enkel energy arbitrage en passieve respons op onbalans.
//  Stem: OpenAI gpt-4o-mini-tts (TTS_PROVIDER=openai), stem VENA_STEM || UITLEG_STEM || cedar, tempo VENA_TEMPO (1,12).
//  Zonder stem → ondertitels op leestempo (video blijft bruikbaar).
// ═══════════════════════════════════════════════════════════════════════════════════════════════
'use strict';
const fs = require('fs');
const os = require('os');
const path = require('path');
const { execFile } = require('child_process');

const VENA_VERSIE = '1.0.0';
const W = 1920, H = 1080, FPS = 30;
const LOGO = (process.env.EWB_ASSET_BASE || 'https://app.energie-compas.eu') + '/apps/ec-logo-dark.png';
const esc = s => String(s == null ? '' : s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
const run = (cmd, args) => new Promise((ok, ko) => execFile(cmd, args, { maxBuffer: 64 * 1024 * 1024 }, (e, so, se) => e ? ko(new Error(cmd + ': ' + (se || e.message).slice(-600))) : ok(so)));
async function duur(p) { const o = await run('ffprobe', ['-v', 'error', '-show_entries', 'format=duration', '-of', 'csv=p=0', p]); return parseFloat(o) || 0; }

const TXT = {
  nl: { eb: 'Vraag en antwoord', kop: 'Je vragen, onze antwoorden', vraag: (i) => `Vraag ${i} van 4`, ai: 'Stem gegenereerd met AI', bron: 'Cijfers uit je energierapport · Energie-Compas',
        eind: 'Bespreek de volgende stap met je adviseur', slot: 'Kort samengevat', advFallback: 'Je adviseur' },
  fr: { eb: 'Questions et réponses', kop: 'Tes questions, nos réponses', vraag: (i) => `Question ${i} sur 4`, ai: 'Voix générée par IA', bron: 'Chiffres de ton rapport énergétique · Energie-Compas',
        eind: 'Discute de l’étape suivante avec ton conseiller', slot: 'En bref', advFallback: 'Ton conseiller' },
  en: { eb: 'Questions and answers', kop: 'Your questions, our answers', vraag: (i) => `Question ${i} of 4`, ai: 'Voice generated with AI', bron: 'Figures from your energy report · Energie-Compas',
        eind: 'Discuss the next step with your adviser', slot: 'In short', advFallback: 'Your adviser' }
};
const INSTR = {
  nl: 'Je bent een Vlaamse energie-adviseur die een klant persoonlijk de antwoorden uit zijn dossier toelicht. Warm, zelfzeker en helder, rustig tempo met korte natuurlijke pauzes, nadruk op het belangrijkste getal. Nederlands met een Vlaamse tongval. Getallen voluit.',
  fr: 'Tu es un conseiller en énergie belge qui explique personnellement à un client les réponses de son dossier. Chaleureux, sûr de toi et clair, rythme posé avec de courtes pauses naturelles, accent sur le chiffre principal. Français de Belgique. Nombres en toutes lettres.',
  en: 'You are a friendly Belgian energy adviser personally explaining the answers from a client’s file. Warm, confident and clear, calm pace with short natural pauses, emphasis on the key number. Clear neutral English. Numbers spoken in full.'
};

// ── spreekvorm: wat in beeld staat → wat de stem zegt ─────────────────────────────────────────
function spreek(t, lang) {
  const L = lang === 'fr' ? 'fr' : (lang === 'en' ? 'en' : 'nl');
  const komma = { nl: ' komma ', fr: ' virgule ', en: ' point ' }[L];
  let s = String(t || '').replace(/±\s*/g, '').replace(/\s+/g, ' ');
  // bedragen: «− € 1.234,56» / «€ 117.200» / «€ 2,82»
  s = s.replace(/(?:(−|-)\s?)?€\s?(\d{1,3}(?:\.\d{3})*|\d+)(?:,(\d{1,2}))?/g, (m, neg, ent, dec) => {
    const e = String(ent).replace(/\./g, ''), min = neg ? { nl: 'min ', fr: 'moins ', en: 'minus ' }[L] : '';
    if (dec && +dec > 0 && +e === 0) return min + String(+(dec.length === 1 ? dec + '0' : dec)) + (L === 'en' ? ' cents' : ' cent');
    if (dec && +dec > 0) return min + e + (L === 'nl' ? ' euro ' : ' euros ') + String(+(dec.length === 1 ? dec + '0' : dec));
    return min + e + (L === 'nl' ? ' euro' : ' euros');
  });
  s = s.replace(/(\d)\.(\d{3})\b/g, '$1$2').replace(/(\d)\.(\d{3})\b/g, '$1$2');      // duizendtallen
  s = s.replace(/(\d),(\d)/g, '$1' + komma + '$2');                                      // decimalen
  s = s.replace(/\s?%/g, { nl: ' procent', fr: ' pour cent', en: ' percent' }[L]);
  s = s.replace(/\bkVA\b/g, { nl: 'kaa vee aa', fr: 'kilovoltampères', en: 'kay vee ay' }[L]);
  s = s.replace(/\bkWp\b/g, { nl: 'kilowattpiek', fr: 'kilowatts-crête', en: 'kilowatt peak' }[L]);
  s = s.replace(/\bkWh\b/g, { nl: 'kilowattuur', fr: 'kilowattheures', en: 'kilowatt hours' }[L]);
  s = s.replace(/\bkW\b/g, { nl: 'kilowatt', fr: 'kilowatts', en: 'kilowatts' }[L]);
  s = s.replace(/’s nachts/g, 's nachts');
  return s.trim();
}
function zinnen(t) { return String(t || '').match(/[^.!?;]+[.!?;]?/g)?.map(x => x.trim()).filter(Boolean) || []; }

// ── iconen (eigen tekeningen, lijnstijl) ───────────────────────────────────────────────────────
function icoon(k) {
  const g = '#05B050', w = '#e6edf3', sw = 14;
  const st = `fill="none" stroke="${w}" stroke-width="${sw}" stroke-linecap="round" stroke-linejoin="round"`;
  const sg = `fill="none" stroke="${g}" stroke-width="${sw}" stroke-linecap="round" stroke-linejoin="round"`;
  const m = {
    piek: `<path d="M150 40 L90 165 H150 L120 270 L215 125 H150 L185 40 Z" ${sg}/><path d="M40 300 H260" ${st}/>`,
    piek0: `<path d="M130 40 L80 150 H130 L105 240 L185 115 H130 L160 40 Z" ${sg}/><path d="M225 120 V270 M190 235 L225 270 L260 235" ${st}/>`,
    km: `<path d="M40 230 A110 110 0 0 1 260 230" ${st}/><path d="M150 230 L215 150" ${sg}/><circle cx="150" cy="230" r="16" fill="${g}"/><path d="M60 270 H240" ${st}/>`,
    inv: `<circle cx="150" cy="150" r="110" ${st}/><path d="M195 95 A65 65 0 1 0 195 205 M80 135 H170 M80 170 H160" ${sg}/>`,
    dek: `<rect x="40" y="90" width="200" height="120" rx="18" ${st}/><path d="M250 125 V175" ${st}/><path d="M65 115 H145 V185 H65 Z" fill="${g}" stroke="none"/><path d="M160 115 V185" ${sg}/>`,
    bank: `<path d="M30 110 L150 40 L270 110 Z" ${st}/><path d="M60 130 V230 M110 130 V230 M190 130 V230 M240 130 V230" ${st}/><path d="M30 260 H270" ${sg}/>`,
    fact: `<path d="M70 30 H190 L240 80 V270 H70 Z" ${st}/><path d="M105 120 H200 M105 160 H200 M105 200 H160" ${sg}/>`,
    ruimte: `<rect x="70" y="40" width="110" height="230" rx="14" ${st}/><path d="M100 80 H150 V130 H100 Z" ${sg}/><path d="M180 120 H215 Q240 120 240 145 V215 Q240 240 260 240" ${st}/><path d="M118 165 L105 200 H130 L117 235" ${sg}/>`
  };
  return `<svg viewBox="0 0 300 300" width="300" height="300" xmlns="http://www.w3.org/2000/svg">${m[k] || m.fact}</svg>`;
}

// ── scènes (HTML) ──────────────────────────────────────────────────────────────────────────────
const CSS = `*{margin:0;padding:0;box-sizing:border-box}body{width:${W}px;height:${H}px;overflow:hidden;position:relative;font-family:Poppins,'DejaVu Sans',Arial,sans-serif;color:#fff;background:radial-gradient(ellipse at 70% 30%,#22345c 0%,#14203a 62%)}
.eb{font-size:26px;font-weight:600;letter-spacing:6px;text-transform:uppercase;color:#05B050;display:flex;align-items:center;gap:16px}.eb:before{content:'';width:46px;height:5px;background:#05B050;border-radius:3px}
.band{position:absolute;left:0;right:0;bottom:0;height:12px;background:#05B050}
.chip{position:absolute;right:56px;top:48px;background:#fff;color:#14203a;border-radius:40px;padding:10px 22px 10px 12px;display:flex;align-items:center;gap:12px;font-weight:800;font-size:22px}
.chip img{height:40px}
.voet{position:absolute;left:110px;bottom:34px;font-size:18px;color:#8fa0bb}.voet b{color:#b9c4d6;font-weight:600}
.cap{position:absolute;left:110px;right:110px;bottom:78px;background:rgba(8,14,30,.78);border-left:6px solid #05B050;border-radius:10px;padding:18px 26px;font-size:32px;line-height:1.4;font-weight:600}`;
const FONT = '<link href="https://fonts.googleapis.com/css2?family=Poppins:wght@400;600;800&display=swap" rel="stylesheet">';
function kader(inhoud, D, T, cap) {
  return `<!doctype html><html><head><meta charset="utf-8">${FONT}<style>${CSS}</style></head><body>
<div class="chip"><img src="${LOGO}" style="height:34px">${esc(T.eb)}</div>${inhoud}
${cap ? `<div class="cap">${esc(cap)}</div>` : ''}
<div class="voet"><b>${esc(D.klant || '')}</b>${D.dossier ? ' · ' + esc(D.dossier) : ''} · ${esc(T.bron)}${D.__ai ? ' · ' + esc(T.ai) : ''}</div><div class="band"></div></body></html>`;
}
function introHtml(D, T, cap) {
  const lijst = (D.kern || []).map((k, i) => `<div style="display:flex;gap:22px;align-items:flex-start;margin-top:22px"><div style="min-width:54px;height:54px;border-radius:50%;background:#05B050;color:#14203a;font-weight:800;font-size:28px;display:flex;align-items:center;justify-content:center">${i + 1}</div><div style="font-size:34px;font-weight:600;line-height:1.3;padding-top:4px">${esc(k.q)}</div></div>`).join('');
  return kader(`<div style="position:absolute;left:110px;top:120px;right:110px"><div class="eb">${esc(T.kop)}</div>
<div style="font-size:50px;font-weight:800;line-height:1.22;margin-top:24px;max-width:1500px">${esc(D.open)}</div>
<div style="margin-top:34px;max-width:1500px">${lijst}</div></div>`, D, T, cap);
}
function vraagHtml(D, T, i, k, cap) {
  const v = k.v ? `<div style="font-size:150px;font-weight:800;color:#4be08a;line-height:1;margin-top:40px;letter-spacing:-2px">${esc(k.v)}</div>${k.s ? `<div style="font-size:38px;color:#b9c4d6;margin-top:18px">${esc(k.s)}</div>` : ''}` : `<div style="font-size:40px;line-height:1.4;margin-top:40px;max-width:1150px;color:#dbe4f0">${esc(k.a)}</div>`;
  return kader(`<div style="position:absolute;left:110px;top:120px;right:600px"><div class="eb">${esc(T.vraag(i + 1))}</div>
<div style="font-size:58px;font-weight:800;line-height:1.18;margin-top:24px">${esc(k.q)}</div>${v}</div>
<div style="position:absolute;right:150px;top:250px;width:380px;height:380px;border-radius:50%;background:rgba(5,176,80,.10);border:3px solid rgba(5,176,80,.45);display:flex;align-items:center;justify-content:center">${icoon(k.k)}</div>`, D, T, cap);
}
function slotHtml(D, T, cap) {
  return kader(`<div style="position:absolute;left:110px;top:170px;right:110px"><div class="eb">${esc(T.slot)}</div>
<div style="font-size:66px;font-weight:800;line-height:1.22;margin-top:30px;max-width:1600px">${esc(D.slot)}</div></div>`, D, T, cap);
}
function eindHtml(D, T, adv) {
  return `<!doctype html><html><head><meta charset="utf-8">${FONT}<style>${CSS}</style></head><body style="display:flex;flex-direction:column;align-items:center;justify-content:center;text-align:center">
<img src="${LOGO}" style="height:120px;margin-bottom:46px"><div class="eb" style="justify-content:center">${esc(T.kop)}</div>
<div style="font-size:60px;font-weight:800;margin-top:22px">${esc(T.eind)}</div>
<div style="font-size:44px;font-weight:600;margin-top:40px;color:#fff">${esc((adv && adv.naam) || T.advFallback)}</div>
<div style="font-size:38px;font-weight:800;margin-top:12px;color:#4be08a">${esc((adv && adv.email) || 'dossier@energie-compas.eu')}</div>
<div class="voet" style="left:0;right:0;text-align:center">${D.__ai ? esc(T.ai) : ''}</div><div class="band"></div></body></html>`;
}

// ── stem ───────────────────────────────────────────────────────────────────────────────────────
async function tts(tekst, uitPad, lang) {
  if (String(process.env.TTS_PROVIDER || '').toLowerCase() !== 'openai' || !process.env.OPENAI_API_KEY) return false;
  const stem = process.env.VENA_STEM || process.env.UITLEG_STEM || 'cedar';
  const r = await fetch('https://api.openai.com/v1/audio/speech', { method: 'POST', headers: { Authorization: `Bearer ${process.env.OPENAI_API_KEY}`, 'content-type': 'application/json' },
    body: JSON.stringify({ model: 'gpt-4o-mini-tts', voice: stem, input: tekst, response_format: 'mp3', instructions: INSTR[lang] || INSTR.nl }) });
  if (!r.ok) throw new Error('openai tts ' + r.status + ' ' + (await r.text()).slice(0, 200));
  fs.writeFileSync(uitPad, Buffer.from(await r.arrayBuffer())); return true;
}

// ── render ─────────────────────────────────────────────────────────────────────────────────────
//   D = window.__KR_VENA_DATA · o = { adviseur:{naam,email}, ewb:{ renderSting, STING_S, JINGLE_BEGIN, JINGLE_EINDE }, log }
let _bezig = Promise.resolve();
function maakVenaVideo(D, o = {}) { const p = _bezig.then(() => _maak(D, o)); _bezig = p.catch(() => {}); return p; }   // één render tegelijk
async function _maak(D, o) {
  if (!D || !Array.isArray(D.kern) || D.kern.length !== 4) throw new Error('geen V&A-data (klantrapport ≥ 0.114.0)');
  const lang = (D.lang === 'fr' || D.lang === 'en') ? D.lang : 'nl', T = TXT[lang], log = o.log || (() => {});
  const E = o.ewb || {}, dir = fs.mkdtempSync(path.join(os.tmpdir(), 'vena-'));
  const t0 = Date.now();
  // blokken: { html(cap), spreek, min }
  const blokken = [
    { id: 'intro', html: c => introHtml(D, T, c), tekst: D.intro },
    ...D.kern.map((k, i) => ({ id: 'v' + (i + 1), html: c => vraagHtml(D, T, i, k, c), tekst: (k.q + ' ' + (k.z || k.a || '')).trim() })),
    { id: 'slot', html: c => slotHtml(D, T, c), tekst: D.slot_z || D.slot },
    { id: 'eind', html: () => eindHtml(D, T, o.adviseur), tekst: null, stil: 5.0 }
  ];
  const segs = [], audios = []; let metStem = null, ttsFout = null, sting = null, poster = null;
  try {
    const puppeteer = require('puppeteer-core');
    const browser = await puppeteer.launch({ executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || '/usr/bin/chromium', args: ['--no-sandbox', '--disable-dev-shm-usage', '--font-render-hinting=none'] });
    try {
      const page = await browser.newPage(); await page.setViewport({ width: W, height: H });
      if (E.renderSting) { try { sting = await E.renderSting(page, {}, dir, { a: T.eb, b: D.klant || T.kop, bw: W, bh: H }); } catch (e) { log('vena sting:', e.message); sting = null; } }
      const tempo = Math.min(1.3, Math.max(1, +process.env.VENA_TEMPO || 1.12));
      let n = 0;
      for (const b of blokken) {
        const zs = b.tekst ? zinnen(b.tekst) : [''];
        let blokDur = b.stil || zs.reduce((a, z) => a + Math.max(2.4, z.split(/\s+/).length / 2.6 + 0.4), 0), ap = null;
        if (b.tekst && metStem !== false) {
          try {
            const p = path.join(dir, `a_${b.id}.mp3`);
            if (await tts(spreek(b.tekst, lang), p, lang)) {
              metStem = true; const p2 = p.replace('.mp3', '.t.mp3');
              await run('ffmpeg', ['-y', '-i', p, '-filter:a', `atempo=${tempo}`, p2]); ap = p2; blokDur = (await duur(p2)) + 0.6;
            } else metStem = false;
          } catch (e) { ttsFout = e.message; metStem = false; log('vena tts → zonder stem:', e.message); }
        }
        if (metStem === false) audios.length = 0;
        if (ap) audios.push({ ap, dur: blokDur });
        D.__ai = (metStem === true);   // «Stem gegenereerd met AI» enkel als er een stem is
        const lens = zs.map(z => Math.max(8, z.length)), som = lens.reduce((a, x) => a + x, 0);
        for (let z = 0; z < zs.length; z++) {
          const dur = blokDur * lens[z] / som, png = path.join(dir, `s${String(n).padStart(3, '0')}.png`);
          await page.setContent(b.html(b.tekst ? zs[z] : ''), { waitUntil: 'networkidle0', timeout: 15000 }).catch(() => {});
          await page.screenshot({ path: png, type: 'png' });
          if (!poster && b.id === 'v1') { poster = path.join(dir, 'poster.jpg'); await page.screenshot({ path: poster, type: 'jpeg', quality: 86 }); }
          segs.push({ png, dur, fadeIn: z === 0 }); n++;
        }
        if (!b.tekst) audios.push({ ap: null, dur: blokDur });
      }
    } finally { await browser.close().catch(() => {}); }
    // beeld
    const S = sting ? (E.STING_S || 2.8) : 0, lijst = [];
    if (sting) lijst.push(`file '${sting}'`);
    for (let i = 0; i < segs.length; i++) {
      const s = segs[i], out = path.join(dir, `c${String(i).padStart(3, '0')}.mp4`);
      await run('ffmpeg', ['-y', '-framerate', String(FPS), '-i', s.png, '-vf', `format=yuv420p,loop=-1:1,trim=duration=${s.dur.toFixed(3)}${s.fadeIn ? ',fade=t=in:st=0:d=0.3' : ''},setpts=PTS-STARTPTS`, '-r', String(FPS), '-c:v', 'libx264', '-crf', '20', '-preset', 'veryfast', '-pix_fmt', 'yuv420p', out]);
      lijst.push(`file '${out}'`);
    }
    const lf = path.join(dir, 'l.txt'); fs.writeFileSync(lf, lijst.join('\n'));
    const stil = path.join(dir, 'stil.mp4'); await run('ffmpeg', ['-y', '-f', 'concat', '-safe', '0', '-i', lf, '-c', 'copy', stil]);
    const T2 = S + segs.reduce((a, s) => a + s.dur, 0), eind = path.join(dir, 'vena.mp4');
    const stemOk = metStem === true && audios.filter(a => a.ap).length === blokken.filter(b => b.tekst).length;
    // geluid: stil spoor tijdens de sting, dan per blok de stem (opgevuld tot de blokduur), loudnorm -16 LUFS
    let stemWav = null;
    if (stemOk) {
      const al = [];
      if (S) { const w0 = path.join(dir, 'w_sting.wav'); await run('ffmpeg', ['-y', '-f', 'lavfi', '-i', 'anullsrc=r=44100:cl=stereo', '-t', S.toFixed(3), w0]); al.push(`file '${w0}'`); }
      for (let i = 0; i < audios.length; i++) {
        const a = audios[i], w = path.join(dir, `w${i}.wav`);
        if (a.ap) await run('ffmpeg', ['-y', '-i', a.ap, '-af', `apad,atrim=0:${a.dur.toFixed(3)}`, '-ar', '44100', '-ac', '2', w]);
        else await run('ffmpeg', ['-y', '-f', 'lavfi', '-i', 'anullsrc=r=44100:cl=stereo', '-t', a.dur.toFixed(3), w]);
        al.push(`file '${w}'`);
      }
      const af = path.join(dir, 'a.txt'); fs.writeFileSync(af, al.join('\n'));
      const ruw = path.join(dir, 'ruw.wav'); await run('ffmpeg', ['-y', '-f', 'concat', '-safe', '0', '-i', af, '-c', 'copy', ruw]);
      stemWav = path.join(dir, 'stem.wav'); await run('ffmpeg', ['-y', '-i', ruw, '-af', 'loudnorm=I=-16:TP=-1.5:LRA=11', '-ar', '44100', '-ac', '2', stemWav]);
    }
    const muziek = E.JINGLE_BEGIN && E.JINGLE_EINDE && fs.existsSync(E.JINGLE_BEGIN) && fs.existsSync(E.JINGLE_EINDE) && process.env.EWB_JINGLE !== 'uit';
    const JV = Math.min(1, Math.max(0.05, +process.env.EWB_JINGLE_VOLUME || 0.45));
    const inp = ['-y', '-i', stil, ...(stemWav ? ['-i', stemWav] : ['-f', 'lavfi', '-t', T2.toFixed(3), '-i', 'anullsrc=r=44100:cl=stereo'])];
    let fc = `[1:a]aformat=sample_rates=44100:channel_layouts=stereo,apad,atrim=0:${T2.toFixed(3)}[v]`, mix = '[v]', k = 1;
    if (muziek) {
      const s0 = Math.max(0.5, S - 0.3), eStart = Math.max(0, T2 - 5.5), stemEind = T2 - 5.0;
      inp.push('-i', E.JINGLE_BEGIN, '-i', E.JINGLE_EINDE);
      fc += `;[2:a]aformat=sample_rates=44100:channel_layouts=stereo,volume=eval=frame:volume='${JV}*if(lt(t,${s0.toFixed(2)}),1,max(0.10,1-(t-${s0.toFixed(2)})/0.6*0.90))'[b]`;
      fc += `;[3:a]aformat=sample_rates=44100:channel_layouts=stereo,adelay=${Math.round(eStart * 1000)}|${Math.round(eStart * 1000)},volume=eval=frame:volume='${JV}*if(lt(t,${stemEind.toFixed(2)}),0.3,min(1,0.3+(t-${stemEind.toFixed(2)})/0.4*0.7))'[e]`;
      mix += '[b][e]'; k = 3;
    }
    fc += k > 1 ? `;${mix}amix=inputs=${k}:duration=first:normalize=0,alimiter=limit=0.95[a]` : `;[v]anull[a]`;
    await run('ffmpeg', [...inp, '-filter_complex', fc, '-map', '0:v', '-map', '[a]', '-c:v', 'copy', '-c:a', 'aac', '-b:a', '160k', '-t', T2.toFixed(3), '-movflags', '+faststart', eind]);
    const uit = { mp4: fs.readFileSync(eind), poster: poster && fs.existsSync(poster) ? fs.readFileSync(poster) : null, duur: Math.round(T2 * 10) / 10, metStem: !!stemOk, ttsFout, sting: !!sting, jingle: !!muziek, versie: VENA_VERSIE, sec: Math.round((Date.now() - t0) / 1000) };
    log(`vena-video ${D.dg}/${lang} klaar: ${uit.duur} s video in ${uit.sec} s (stem ${uit.metStem ? 'ja' : 'nee'})`);
    return uit;
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
}
module.exports = { maakVenaVideo, spreek, VENA_VERSIE, _html: { introHtml, vraagHtml, slotHtml, eindHtml, TXT } };
