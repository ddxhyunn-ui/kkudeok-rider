// 꾸덕꾸덕라이더 멀티플레이 서버: 정적 파일(public/index.html) + WebSocket 방(룸) 관리
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const MAX_PLAYERS = 8;
// index.html은 server.js와 같은 폴더(루트) 또는 public/ 폴더 어디에 있어도 됨
const INDEX = [path.join(__dirname, 'index.html'), path.join(__dirname, 'public', 'index.html')].find((p) => fs.existsSync(p)) || path.join(__dirname, 'index.html');

const server = http.createServer((req, res) => {
  if (req.url === '/healthz') { res.writeHead(200); return res.end('ok'); }
  fs.readFile(INDEX, (err, buf) => {
    if (err) { res.writeHead(404); return res.end('index.html not found'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(buf);
  });
});

const wss = new WebSocketServer({ server, maxPayload: 4096 });
const rooms = new Map(); // code -> { code, hostId, players: Map(id -> player), phase, map, startAt, results: [] }
let nextId = 1;

const CODE_CHARS = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
function makeCode() {
  for (;;) {
    let c = '';
    for (let i = 0; i < 4; i++) c += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)];
    if (!rooms.has(c)) return c;
  }
}
const send = (ws, msg) => { if (ws.readyState === 1) ws.send(JSON.stringify(msg)); };
const cleanName = (n) => String(n || '').replace(/[<>&"']/g, '').trim().slice(0, 10) || '라이더';

function roster(room) {
  return [...room.players.values()].map((p) => ({ id: p.id, name: p.name, v: p.v, host: p.id === room.hostId }));
}
function broadcast(room, msg, exceptId) {
  const s = JSON.stringify(msg);
  room.players.forEach((p) => { if (p.id !== exceptId && p.ws.readyState === 1) p.ws.send(s); });
}
function sendRoster(room) { broadcast(room, { t: 'room', code: room.code, hostId: room.hostId, phase: room.phase, players: roster(room) }); }

function leave(ws) {
  const room = ws.room; if (!room) return;
  room.players.delete(ws.pid); ws.room = null;
  if (!room.players.size) { rooms.delete(room.code); return; }
  if (room.hostId === ws.pid) room.hostId = room.players.keys().next().value; // 방장 위임
  broadcast(room, { t: 'left', id: ws.pid });
  sendRoster(room);
}

wss.on('connection', (ws) => {
  ws.pid = nextId++; ws.room = null; ws.alive = true; ws.lastState = 0;
  ws.on('pong', () => { ws.alive = true; });
  send(ws, { t: 'hello', id: ws.pid });

  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw); } catch (_) { return; }
    if (!m || typeof m.t !== 'string') return;

    if (m.t === 'create' || m.t === 'join') {
      if (ws.room) leave(ws);
      let room;
      if (m.t === 'create') {
        room = { code: makeCode(), hostId: ws.pid, players: new Map(), phase: 'lobby', map: 'village', results: [] };
        rooms.set(room.code, room);
      } else {
        room = rooms.get(String(m.code || '').toUpperCase().trim());
        if (!room) return send(ws, { t: 'error', msg: '방을 찾을 수 없어요. 코드를 확인해 주세요.' });
        if (room.players.size >= MAX_PLAYERS) return send(ws, { t: 'error', msg: '방이 가득 찼어요.' });
        if (room.phase === 'racing') return send(ws, { t: 'error', msg: '이미 경기가 진행 중이에요. 끝난 뒤 입장해 주세요.' });
      }
      ws.room = room;
      room.players.set(ws.pid, { id: ws.pid, ws, name: cleanName(m.name), v: m.v || 'kart' });
      send(ws, { t: 'joined', id: ws.pid });
      sendRoster(room);
      return;
    }

    const room = ws.room; if (!room) return;
    const me = room.players.get(ws.pid);

    if (m.t === 'veh') { me.v = String(m.v || 'kart').slice(0, 12); sendRoster(room); }
    else if (m.t === 'state') {
      const now = Date.now(); if (now - ws.lastState < 30) return; ws.lastState = now; // 과도한 전송 제한
      broadcast(room, { t: 'state', id: ws.pid, x: m.x, y: m.y, z: m.z, h: m.h, s: m.s, b: m.b, p: m.p, v: me.v }, ws.pid);
    }
    else if (m.t === 'start' && room.hostId === ws.pid) {
      room.phase = 'racing'; room.map = String(m.map || 'village').slice(0, 20); room.results = [];
      broadcast(room, { t: 'start', map: room.map });
      sendRoster(room);
    }
    else if (m.t === 'finish' && room.phase === 'racing' && !room.results.some((r) => r.id === ws.pid)) {
      const ms = Number(m.ms); if (!Number.isFinite(ms) || ms <= 0) return;
      room.results.push({ id: ws.pid, name: me.name, ms });
      room.results.sort((a, b) => a.ms - b.ms);
      broadcast(room, { t: 'results', list: room.results });
      if (room.results.length >= room.players.size) { room.phase = 'lobby'; sendRoster(room); }
    }
    else if (m.t === 'abort' && room.phase === 'racing') { // 방장이 경기 중단 (메뉴로 나감)
      if (room.hostId === ws.pid) { room.phase = 'lobby'; sendRoster(room); }
    }
    else if (m.t === 'leave') leave(ws);
  });
  ws.on('close', () => leave(ws));
});

// 연결 유지(핑) - 무료 호스팅에서 유휴 연결이 끊기는 것 방지
setInterval(() => {
  wss.clients.forEach((ws) => { if (!ws.alive) return ws.terminate(); ws.alive = false; ws.ping(); });
}, 25000);

server.listen(PORT, () => console.log('꾸덕꾸덕라이더 멀티플레이 서버 실행: http://localhost:' + PORT));
