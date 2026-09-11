'use strict';
// ─────────────────────────────────────────────────────────────────────────────
// Microsoft Graph inbound-intake voor het gratis energie-abonnement (Slice 1).
// Leest ongelezen mails MET bijlagen uit de M365-mailbox (GRAPH_MAILBOX) via de
// client-credentials-flow (app-only). Volledig GEGUARD: zonder de vier ENV-vars is
// graphEnabled() false en doet de module niets. Geen enkele bestaande flow raakt gewijzigd.
//
// Vereiste ENV (door Johan te zetten na Azure-app-registratie + M365-mailbox):
//   GRAPH_TENANT_ID, GRAPH_CLIENT_ID, GRAPH_CLIENT_SECRET, GRAPH_MAILBOX (bv. energiekompas@fluctus.net)
// Azure app-permissions (Application): Mail.Read + Mail.ReadWrite (markeer gelezen) [+ Mail.Send indien we via Graph mailen].
// ─────────────────────────────────────────────────────────────────────────────

const _env = (k) => String(process.env[k] || '').trim();   // trim: mobiel plakt soms een spatie/enter mee → brak URL/credentials (HTTP 400)
function graphEnabled() {
  return !!(_env('GRAPH_TENANT_ID') && _env('GRAPH_CLIENT_ID') && _env('GRAPH_CLIENT_SECRET') && _env('GRAPH_MAILBOX'));
}

let _tok = { value: null, exp: 0 };
async function getToken() {
  if (_tok.value && Date.now() < _tok.exp - 60000) return _tok.value;
  const tenant = _env('GRAPH_TENANT_ID'), clientId = _env('GRAPH_CLIENT_ID'), secret = _env('GRAPH_CLIENT_SECRET');
  const ontbreekt = [];
  if (!tenant) ontbreekt.push('GRAPH_TENANT_ID');
  if (!clientId) ontbreekt.push('GRAPH_CLIENT_ID');
  if (!secret) ontbreekt.push('GRAPH_CLIENT_SECRET');
  if (ontbreekt.length) throw new Error('Graph token faalde: ontbrekende/lege ENV: ' + ontbreekt.join(', '));
  const url = `https://login.microsoftonline.com/${encodeURIComponent(tenant)}/oauth2/v2.0/token`;
  const body = new URLSearchParams({
    client_id: clientId, client_secret: secret,
    scope: 'https://graph.microsoft.com/.default', grant_type: 'client_credentials',
  });
  const r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body });
  if (!r.ok) {
    const raw = await r.text().catch(() => '');
    let detail = raw.slice(0, 400);
    try { const j = JSON.parse(raw); detail = (j.error || '') + (j.error_description ? (': ' + String(j.error_description).split(/\r?\n/)[0]) : ''); } catch (e) {}
    throw new Error('Graph token faalde: HTTP ' + r.status + (detail ? ' — ' + detail.slice(0, 300) : ' (leeg antwoord — controleer of GRAPH_TENANT_ID/CLIENT_ID geen spatie of extra teken bevat)'));
  }
  const j = await r.json();
  _tok = { value: j.access_token, exp: Date.now() + (Number(j.expires_in) || 3600) * 1000 };
  return _tok.value;
}

async function _g(path, opts) {
  const tok = await getToken();
  const headers = Object.assign({ Authorization: 'Bearer ' + tok, 'Content-Type': 'application/json' }, (opts && opts.headers) || {});
  return fetch('https://graph.microsoft.com/v1.0' + path, Object.assign({}, opts || {}, { headers }));
}

// Ongelezen mails MET bijlagen (max 25), oudste eerst.
// LET OP: Graph mail-messages weigert $filter (isRead/hasAttachments) SAMEN met $orderby=receivedDateTime
// ("InefficientFilter — restriction or sort order too complex"). Daarom halen we hier GEEN $orderby op en
// sorteren we oudste-eerst in code. (v15.142.1)
async function fetchUnread() {
  const mb = encodeURIComponent(_env('GRAPH_MAILBOX'));
  const q = `/users/${mb}/messages?$filter=isRead eq false and hasAttachments eq true`
    + `&$select=id,subject,from,receivedDateTime&$top=25`;
  const r = await _g(q);
  if (!r.ok) throw new Error('Graph fetchUnread: HTTP ' + r.status + ' ' + (await r.text()).slice(0, 200));
  const j = await r.json();
  return (j.value || []).map(m => ({
    id: m.id, subject: m.subject || '',
    from: (m.from && m.from.emailAddress && m.from.emailAddress.address) || '',
    ontvangen: m.receivedDateTime || '',
  })).sort((a, b) => (a.ontvangen < b.ontvangen ? -1 : a.ontvangen > b.ontvangen ? 1 : 0));   // oudste eerst, client-side
}

// PDF-bijlagen van één mail als [{ base64, mediaType, fileName }].
async function getPdfAttachments(msgId) {
  const mb = encodeURIComponent(_env('GRAPH_MAILBOX'));
  // GEEN $select: contentBytes bestaat niet op het polymorfe base-type 'attachment' → $select met contentBytes gaf HTTP 400.
  // Zonder $select geeft Graph de volledige bijlage terug, inclusief contentBytes voor fileAttachments. (v15.142.3)
  const r = await _g(`/users/${mb}/messages/${encodeURIComponent(msgId)}/attachments`);
  if (!r.ok) throw new Error('Graph attachments: HTTP ' + r.status + ' ' + (await r.text().catch(() => '')).slice(0, 200));
  const j = await r.json();
  return (j.value || [])
    .filter(a => String(a['@odata.type'] || '').indexOf('fileAttachment') >= 0 && a.contentBytes)
    .filter(a => /pdf/i.test(a.contentType || '') || /\.pdf$/i.test(a.name || ''))
    .map(a => ({ base64: a.contentBytes, mediaType: 'application/pdf', fileName: a.name || 'factuur.pdf' }));
}

// v15.142.4: recente mails MET bijlage, ONAFHANKELIJK van gelezen/ongelezen (robuuste intake met durabel watermerk
// in server.js). GEEN $filter/$orderby → vermijdt de Graph "InefficientFilter"-valkuil volledig: we halen de 50
// nieuwste berichten op (Graph geeft ze standaard op receivedDateTime aflopend), filteren hasAttachments client-side
// en sorteren oudste-eerst. Het datum-venster + de verwerkt-lijst worden server-side toegepast.
async function fetchRecent() {
  const mb = encodeURIComponent(_env('GRAPH_MAILBOX'));
  const q = `/users/${mb}/messages?$select=id,subject,from,receivedDateTime,hasAttachments&$top=50`;
  const r = await _g(q);
  if (!r.ok) throw new Error('Graph fetchRecent: HTTP ' + r.status + ' ' + (await r.text().catch(() => '')).slice(0, 200));
  const j = await r.json();
  return (j.value || [])
    .filter(m => m.hasAttachments)
    .map(m => ({
      id: m.id, subject: m.subject || '',
      from: (m.from && m.from.emailAddress && m.from.emailAddress.address) || '',
      ontvangen: m.receivedDateTime || '',
    }))
    .sort((a, b) => (a.ontvangen < b.ontvangen ? -1 : a.ontvangen > b.ontvangen ? 1 : 0));
}

async function markRead(msgId) {
  const mb = encodeURIComponent(_env('GRAPH_MAILBOX'));
  const r = await _g(`/users/${mb}/messages/${encodeURIComponent(msgId)}`, { method: 'PATCH', body: JSON.stringify({ isRead: true }) });
  return r.ok;
}

module.exports = { graphEnabled, getToken, fetchUnread, fetchRecent, getPdfAttachments, markRead };
