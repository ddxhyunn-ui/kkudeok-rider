// 꾸덕꾸덕라이더 멀티플레이 서버 (정적 파일 + WebSocket 파티 서버)
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 10000;
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.json': 'application/json', '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.svg': 'image/svg+xml', '.ico': 'image/x-icon', '.mp3': 'audio/mpeg', '.ogg': 'audio/ogg', '.wav': 'audio/wav', '.webp': 'image/webp' };
const HIDDEN = new Set(['server.js', 'package.json', 'yarn.lock', 'package-lock.json']);

const server = http.createServer((req, res) => {
  let u = '/';
  try { u = decodeURIComponent((req.url || '/').split('?')[0]); } catch (_) {}
  if (u === '/') u = '/index.html';
  const fp = path.join(__dirname, path.normalize(u));
  const rel = path.relative(__dirname, fp);
  if (rel.startsWith('..') || path.isAbsolute(rel) || HIDDEN.has(path.basename(fp)) || rel.split(path.sep).some((p) => p.startsWith('.') || p === 'node_modules')) { res.writeHead(404); return res.end('Not found'); }
  fs.readFile(fp, (err, buf) => {
    if (err) { res.writeHead(404); return res.end('Not found'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(fp).toLowerCase()] || 'application/octet-stream', 'Cache-Control': 'no-cache' });
    res.end(buf);
  });
});

const wss = new WebSocketServer({ server });
const rooms = new Map(); // code -> room
let nextId = 1;
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const RACE_TIMEOUT = 15 * 60 * 1000;
const COUNTDOWN_MS = +process.env.CD_MS || 10000; // 1등 완주 후 나머지 라이더 제한 시간

const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
const clean = (t, n, d) => { const v = String(t == null ? '' : t).replace(/[<>&"'\u0000-\u001f]/g, '').trim().slice(0, n); return v || d; };
const num = (v) => (Number.isFinite(+v) ? +v : 0);
const makeCode = () => { for (let k = 0; k < 200; k++) { let c = ''; for (let i = 0; i < 4; i++) c += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)]; if (!rooms.has(c)) return c; } return null; };
const isRacing = (r) => r.racing && Date.now() - r.startedAt < RACE_TIMEOUT;

function roomMsg(r) {
  return { t: 'room', code: r.code, hostId: r.hostId, title: r.title, public: r.public, max: r.max,
    players: [...r.players.values()].map((p) => ({ id: p.id, name: p.name, v: p.v, host: p.id === r.hostId })) };
}
// 완주했거나 경기 도중 나간(quit) 라이더만 남았는지
const allDone = (r) => [...r.players.values()].every((p) => p.done || p.quit) && [...r.players.values()].some((p) => p.done || p.quit);
const broadcast = (r, o, exceptId) => { r.players.forEach((p) => { if (p.id !== exceptId) send(p.ws, o); }); };

function listRooms() {
  const out = [];
  rooms.forEach((r) => {
    if (!r.public) return;
    const host = r.players.get(r.hostId);
    out.push({ code: r.code, title: r.title, host: host ? host.name : '-', count: r.players.size, max: r.max, public: true, racing: isRacing(r) });
  });
  return out;
}

function endRace(r) {
  if (!r.racing) return;
  clearTimeout(r.endTimer); r.endTimer = null; r.racing = false;
  const done = r.results.slice().sort((a, b) => a.ms - b.ms).map((x) => ({ id: x.id, name: x.name, v: x.v, ms: x.ms, retire: false }));
  const rest = [...r.players.values()].filter((p) => !p.done).sort((a, b) => b.prog - a.prog).map((p) => ({ id: p.id, name: p.name, v: p.v, ms: null, retire: true }));
  broadcast(r, { t: 'end', list: done.concat(rest) });
  r.results = [];
}

function leave(ws) {
  const r = ws.room; if (!r) return;
  ws.room = null;
  const who = r.players.get(ws.pid);
  r.players.delete(ws.pid);
  if (r.players.size === 0) { clearTimeout(r.endTimer); rooms.delete(r.code); return; }
  if (r.hostId === ws.pid) r.hostId = r.players.keys().next().value;
  broadcast(r, { t: 'left', id: ws.pid, name: who ? who.name : '' });
  broadcast(r, roomMsg(r));
  if (r.racing && allDone(r)) endRace(r);
}

function addPlayer(ws, r, m) {
  const p = { id: ws.pid, ws, name: clean(m.name, 10, 'USER'), v: clean(m.v, 20, 'kart'), done: false, prog: 0 };
  r.players.set(p.id, p); ws.room = r;
  broadcast(r, roomMsg(r));
}

wss.on('connection', (ws) => {
  ws.pid = nextId++; ws.room = null; ws.alive = true;
  ws.on('pong', () => { ws.alive = true; });
  send(ws, { t: 'hello', id: ws.pid });

  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw); } catch (_) { return; }
    if (!m || typeof m.t !== 'string') return;
    const r = ws.room;

    if (m.t === 'list') return send(ws, { t: 'list', rooms: listRooms() });

    if (m.t === 'create') {
      if (r) leave(ws);
      const code = makeCode(); if (!code) return send(ws, { t: 'error', msg: '방을 만들 수 없어요. 잠시 후 다시 시도해 주세요.' });
      const room = { code, title: clean(m.title, 20, '신나게 달려볼까!'), public: m.public !== false, max: Math.min(6, Math.max(2, parseInt(m.max, 10) || 4)),
        hostId: ws.pid, players: new Map(), racing: false, startedAt: 0, results: [] };
      rooms.set(code, room); addPlayer(ws, room, m); return;
    }

    if (m.t === 'join') {
      if (r) leave(ws);
      const room = rooms.get(String(m.code || '').toUpperCase());
      if (!room) return send(ws, { t: 'error', msg: '파티를 찾을 수 없어요.' });
      if (room.public && !m.list) return send(ws, { t: 'error', msg: '공개 파티는 파티 찾기 목록에서 입장해 주세요. (코드 입장은 비공개 파티 전용이에요)' });
      if (room.players.size >= room.max) return send(ws, { t: 'error', msg: '파티가 가득 찼어요.' });
      if (isRacing(room)) return send(ws, { t: 'error', msg: '이미 경기가 진행 중이에요.' });
      addPlayer(ws, room, m); return;
    }

    if (!r) return;
    const me = r.players.get(ws.pid); if (!me) return;

    if (m.t === 'leave') return leave(ws);
    if (m.t === 'veh') { me.v = clean(m.v, 20, 'kart'); return broadcast(r, roomMsg(r)); }
    if (m.t === 'state') {
      me.prog = num(m.p);
      return broadcast(r, { t: 'state', id: ws.pid, v: me.v, x: num(m.x), y: num(m.y), z: num(m.z), h: num(m.h), s: num(m.s), b: m.b ? 1 : 0, p: num(m.p) }, ws.pid);
    }
    if (m.t === 'start') {
      if (ws.pid !== r.hostId) return;
      clearTimeout(r.endTimer); r.endTimer = null;
      r.racing = true; r.startedAt = Date.now(); r.results = []; r.players.forEach((p) => { p.done = false; p.quit = false; p.prog = 0; });
      return broadcast(r, { t: 'start', map: clean(m.map, 40, '') });
    }
    if (m.t === 'finish') {
      if (!r.racing || me.done || me.quit) return;
      me.done = true; r.results.push({ id: ws.pid, name: me.name, v: me.v, ms: Math.max(0, Math.round(num(m.ms))) });
      broadcast(r, { t: 'results', list: r.results });
      if (allDone(r)) return endRace(r);
      if (!r.endTimer) { // 1등 완주 → 제한 시간 카운트다운 시작
        r.endTimer = setTimeout(() => endRace(r), COUNTDOWN_MS);
        broadcast(r, { t: 'countdown', ms: COUNTDOWN_MS, id: ws.pid, name: me.name });
      }
      return;
    }
    // 경기 도중 '메인 메뉴로' → 그 라이더만 리타이어 처리하고 다른 라이더에게 알림 (경기는 계속)
    if (m.t === 'quit') {
      if (!r.racing || me.done || me.quit) return;
      me.quit = true;
      broadcast(r, { t: 'quit', id: ws.pid, name: me.name }, ws.pid);
      if (allDone(r)) endRace(r);
      return;
    }
    if (m.t === 'abort') { if (ws.pid === r.hostId) { clearTimeout(r.endTimer); r.endTimer = null; r.racing = false; r.results = []; r.players.forEach((p) => { p.done = false; }); } }
  });

  ws.on('close', () => leave(ws));
  ws.on('error', () => {});
});

setInterval(() => { wss.clients.forEach((ws) => { if (!ws.alive) return ws.terminate(); ws.alive = false; try { ws.ping(); } catch (_) {} }); }, 30000);

server.listen(PORT, () => console.log('꾸덕꾸덕라이더 멀티플레이 서버 실행: http://localhost:' + PORT));
