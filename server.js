'use strict';
// Servidor de Fogón Dorado: sirve el juego (public/) y reenvía las posiciones entre jugadores por WebSocket.
const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');
const MAX_PER_ROOM = 40;

const INDEX = path.join(__dirname, 'index.html');
const server = http.createServer((req, res) => {
  const f = (req.url || '/').split('?')[0];
  if (f === '/healthz') { res.writeHead(200); return res.end('ok'); }
  if (f !== '/' && f !== '/index.html') { res.writeHead(404); return res.end('No encontrado'); }
  fs.readFile(INDEX, (e, d) => {
    if (e) { res.writeHead(500); return res.end('Falta index.html'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(d);
  });
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 4096 });
const rooms = new Map(); // clave de sala -> { peers: Map(id -> {ws, pres}), dirty }
let nextId = 1;
const keyOf = n => String(n || '').toLowerCase().replace(/[^a-z0-9_.-]/g, '').replace(/^[^a-z0-9]+/, '').slice(0, 24) || '~';
const num = v => typeof v === 'number' && isFinite(v);
const crypto = require('crypto');
const OWNER_KEY = process.env.OWNER_KEY || '';
const sha = s => crypto.createHash('sha256').update(String(s)).digest();
const NUM_KEYS = ['col', 'x', 'z', 'a', 'y', 'm', 'sv', 'emT', 'chT', 'slot', 'jt', 'o0', 'o1', 'o2', 'ti', 'blk', 'pv', 'gn', 'fn', 'fa', 'fp', 'fx', 'fz', 'fy', 'fw'];
function applyPatch(pres, patch) {
  if (!patch || typeof patch !== 'object') return;
  for (const k of NUM_KEYS) if (k in patch) { if (patch[k] === null) delete pres[k]; else if (num(patch[k])) pres[k] = patch[k]; }
  if ('chat' in patch && typeof patch.chat === 'string') pres.chat = patch.chat.replace(/[\u0000-\u001f\u007f<>]/g, '').slice(0, 80);
  if ('mv' in patch) { if (patch.mv === null) delete pres.mv; else pres.mv = !!patch.mv; }
  if ('nick' in patch && typeof patch.nick === 'string') pres.nick = patch.nick.replace(/[\u0000-\u001f<>]/g, '').slice(0, 16);
  if ('em' in patch) { if (patch.em === null) delete pres.em; else if (['👋', '😂', '🔥', '❤️'].includes(patch.em)) pres.em = patch.em; }
}
function leaveRoom(ws, key) {
  const r = rooms.get(key); if (!r) return;
  if (r.peers.delete(ws.id)) r.dirty = true;
  ws.rooms.delete(key);
  if (!r.peers.size) rooms.delete(key);
}
wss.on('connection', ws => {
  ws.id = 'p' + (nextId++).toString(36);
  ws.rooms = new Set(); ws.alive = true; ws.cnt = 0; ws.owner = false; ws.authN = 0;
  ws.on('pong', () => { ws.alive = true; });
  ws.send(JSON.stringify({ t: 'hello', id: ws.id }));
  ws.on('message', raw => {
    if (++ws.cnt > 60) return; // límite de mensajes por segundo
    let m; try { m = JSON.parse(raw.toString()); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;
    const key = keyOf(m.room);
    if (m.t === 'join') {
      if (ws.rooms.has(key)) return;
      if (ws.rooms.size >= 4) return;
      let r = rooms.get(key); if (!r) { r = { peers: new Map(), dirty: false }; rooms.set(key, r); }
      if (r.peers.size >= MAX_PER_ROOM) { ws.send(JSON.stringify({ t: 'full', room: key })); return; }
      r.peers.set(ws.id, { ws, pres: ws.owner ? { ow: 1 } : {} }); ws.rooms.add(key); r.dirty = true;
    } else if (m.t === 'pres') {
      const r = rooms.get(key), p = r && r.peers.get(ws.id); if (!p) return;
      applyPatch(p.pres, m.patch); r.dirty = true;
    } else if (m.t === 'auth') {
      if (!OWNER_KEY || ws.owner || ++ws.authN > 5 || typeof m.key !== 'string') return;
      if (!crypto.timingSafeEqual(sha(m.key), sha(OWNER_KEY))) return;
      ws.owner = true;
      for (const k of ws.rooms) { const rr = rooms.get(k), pp = rr && rr.peers.get(ws.id); if (pp) { pp.pres.ow = 1; rr.dirty = true; } }
    } else if (m.t === 'leave') leaveRoom(ws, key);
  });
  ws.on('close', () => { for (const k of [...ws.rooms]) leaveRoom(ws, k); });
  ws.on('error', () => {});
});
setInterval(() => { for (const c of wss.clients) c.cnt = 0; }, 1000);
setInterval(() => { // difusión a 10 Hz
  for (const [key, r] of rooms) {
    if (!r.dirty) continue; r.dirty = false;
    const list = [...r.peers].map(([id, p]) => ({ id, presence: p.pres }));
    const msg = JSON.stringify({ t: 'peers', room: key, list });
    for (const p of r.peers.values()) if (p.ws.readyState === 1) p.ws.send(msg);
  }
}, 100);
setInterval(() => { // latido: cierra conexiones muertas
  for (const c of wss.clients) { if (!c.alive) { c.terminate(); continue; } c.alive = false; try { c.ping(); } catch (e) {} }
}, 30000);
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Fogón Dorado en puerto ' + PORT));
