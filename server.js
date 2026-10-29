/* WatchParty v1.2 — server.js (Node.js + ws)
 * À pousser à la racine du dépôt GitHub relié à Render (avec package.json).
 * wss://watchparty-sn9a.onrender.com — écoute sur process.env.PORT || 8080
 */
'use strict';

const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const MAX_PER_ROOM = 10;
const GRACE_MS = 90 * 1000; // un client déconnecté garde sa place 90 s

const rooms = new Map();

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8', 'Access-Control-Allow-Origin': '*' });
  res.end('WatchParty OK - salons actifs : ' + rooms.size);
});
const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });

// ---------------------------------------------------------------- utilitaires
const now = () => Date.now();
const uid = () => crypto.randomBytes(6).toString('hex');
const ID_RE = /^[0-9a-f]{12}$/;

function send(ws, obj) {
  if (ws && ws.readyState === 1) {
    try { ws.send(JSON.stringify(obj)); } catch (e) { /* ignore */ }
  }
}

function genCode() {
  for (let i = 0; i < 300; i++) {
    const c = String(Math.floor(1000 + Math.random() * 9000));
    if (!rooms.has(c)) return c;
  }
  return null;
}

function cleanName(n) {
  n = String(n || '').replace(/\s+/g, ' ').trim().slice(0, 24);
  return n || 'Invité' + Math.floor(10 + Math.random() * 90);
}

function fmt(sec) {
  sec = Math.max(0, Math.floor(Number(sec) || 0));
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = String(sec % 60).padStart(2, '0');
  return h > 0 ? h + ':' + String(m).padStart(2, '0') + ':' + s : m + ':' + s;
}

function participantsOf(room) {
  return Array.from(room.clients.values()).map((c) => ({
    id: c.id, name: c.name, connected: !!c.ws, isHost: c.id === room.hostId
  }));
}

// Film actif avec la position ESTIMÉE à l'instant présent
function movieNow(room) {
  const m = room.movie;
  if (!m) return null;
  const t = m.playing ? m.time + (now() - m.updatedAt) / 1000 : m.time;
  const hostClient = room.clients.get(room.hostId);
  return {
    videoId: m.videoId,
    url: m.url,
    title: m.title || '',
    time: Math.max(0, t),
    playing: m.playing,
    hostName: hostClient ? hostClient.name : ''
  };
}

function sendRoom(room, client, reason, withHistory) {
  send(client.ws, {
    type: 'room',
    reason: reason,
    code: room.code,
    you: { id: client.id, name: client.name },
    hostId: room.hostId,
    participants: participantsOf(room),
    movie: movieNow(room),
    history: withHistory ? room.history : undefined
  });
}

function broadcastRoom(room, reason, exceptId) {
  room.clients.forEach((c) => {
    if (c.ws && c.id !== exceptId) sendRoom(room, c, reason || 'update', false);
  });
}

function broadcast(room, obj, exceptId) {
  room.clients.forEach((c) => {
    if (c.ws && c.id !== exceptId) send(c.ws, obj);
  });
}

function addChat(room, opts) {
  const msg = {
    id: uid(),
    ts: now(),
    system: !!opts.system,
    from: opts.from ? { id: opts.from.id, name: opts.from.name } : null,
    text: opts.text
  };
  room.history.push(msg);
  if (room.history.length > 100) room.history.shift();
  broadcast(room, { type: 'chat', message: msg });
}

function newClient(ws, name, id) {
  return { id: id || uid(), name: cleanName(name), ws: ws, graceTimer: null, lastSeekChat: 0 };
}

function getCtx(ws) {
  if (!ws.ctx) return null;
  const room = rooms.get(ws.ctx.code);
  if (!room) return null;
  const client = room.clients.get(ws.ctx.id);
  if (!client || client.ws !== ws) return null;
  return { room: room, client: client };
}

function removeClient(room, client, why) {
  clearTimeout(client.graceTimer);
  room.clients.delete(client.id);
  if (room.clients.size === 0) {
    rooms.delete(room.code);
    return;
  }
  addChat(room, {
    system: true,
    text: why === 'timeout' ? client.name + ' a été déconnecté' : client.name + ' a quitté le salon'
  });
  if (room.hostId === client.id) {
    const all = Array.from(room.clients.values());
    const next = all.find((c) => c.ws) || all[0];
    room.hostId = next.id;
    addChat(room, { system: true, text: next.name + ' est maintenant l’hôte' });
  }
  broadcastRoom(room, 'update');
}

function detach(ws) {
  const ctx = getCtx(ws);
  ws.ctx = null;
  if (ctx) removeClient(ctx.room, ctx.client, 'left');
}

// ---------------------------------------------------------------- handlers
function onCreate(ws, msg) {
  detach(ws);
  const code = genCode();
  if (!code) return send(ws, { type: 'error', code: 'server_busy', message: 'Serveur saturé, réessaie dans un instant.' });
  const client = newClient(ws, msg.name);
  const room = { code: code, hostId: client.id, clients: new Map(), movie: null, history: [], restored: false, hostClaimed: true };
  room.clients.set(client.id, client);
  rooms.set(code, room);
  ws.ctx = { code: code, id: client.id };
  sendRoom(room, client, 'created', true);
  addChat(room, { system: true, text: client.name + ' a créé le salon' });
}

function onJoin(ws, msg) {
  detach(ws);
  const code = String(msg.code || '').trim();
  const room = rooms.get(code);
  if (!room) return send(ws, { type: 'error', code: 'room_not_found', message: 'Le salon ' + code + ' n’existe pas.' });
  if (room.clients.size >= MAX_PER_ROOM) return send(ws, { type: 'error', code: 'room_full', message: 'Ce salon est plein.' });
  const client = newClient(ws, msg.name);
  room.clients.set(client.id, client);
  ws.ctx = { code: code, id: client.id };
  sendRoom(room, client, 'joined', true);
  broadcastRoom(room, 'update', client.id);
  addChat(room, { system: true, text: client.name + ' a rejoint le salon' });
}

// Reconnexion (changement de page, coupure réseau, redémarrage de Render).
// Si le salon n'existe plus (serveur redémarré), on le RECRÉE avec le même code.
function onRejoin(ws, msg) {
  const code = String(msg.code || '').trim();
  if (!/^\d{4}$/.test(code)) {
    return send(ws, { type: 'error', code: 'room_not_found', rejoin: true, message: 'Code de salon invalide.' });
  }
  detach(ws);

  let room = rooms.get(code);
  let restored = false;
  if (!room) {
    room = { code: code, hostId: null, clients: new Map(), movie: null, history: [], restored: true, hostClaimed: false };
    rooms.set(code, room);
    restored = true;
  }

  const wantedId = ID_RE.test(String(msg.clientId || '')) ? String(msg.clientId) : null;
  let client = wantedId ? room.clients.get(wantedId) : null;
  let isNew = false;
  if (client) {
    clearTimeout(client.graceTimer);
    client.graceTimer = null;
    if (client.ws && client.ws !== ws) {
      const old = client.ws;
      old.ctx = null;
      try { old.close(); } catch (e) { /* ignore */ }
    }
    client.ws = ws;
    if (msg.name) client.name = cleanName(msg.name);
  } else {
    if (room.clients.size >= MAX_PER_ROOM) {
      return send(ws, { type: 'error', code: 'room_full', rejoin: true, message: 'Ce salon est plein.' });
    }
    client = newClient(ws, msg.name, wantedId);
    room.clients.set(client.id, client);
    isNew = true;
  }
  ws.ctx = { code: room.code, id: client.id };

  if (restored) {
    room.hostId = client.id;
    room.hostClaimed = !!msg.wasHost;
    sendRoom(room, client, 'rejoined', true);
    addChat(room, { system: true, text: 'Le salon ' + code + ' a été restauré après un redémarrage du serveur' });
    return;
  }

  // Salon restauré : l'ancien hôte, quand il revient, reprend son rôle
  let hostChanged = false;
  if (room.restored && !room.hostClaimed && msg.wasHost && client.id !== room.hostId) {
    room.hostId = client.id;
    room.hostClaimed = true;
    hostChanged = true;
  }
  sendRoom(room, client, 'rejoined', true);
  broadcastRoom(room, 'update', client.id);
  if (isNew) addChat(room, { system: true, text: client.name + ' a rejoint le salon' });
  if (hostChanged) addChat(room, { system: true, text: client.name + ' reprend le rôle d’hôte' });
}

function onLeave(ws) {
  detach(ws);
}

function onClose(ws) {
  const ctx = getCtx(ws);
  if (!ctx) return;
  const room = ctx.room;
  const client = ctx.client;
  client.ws = null;
  broadcastRoom(room, 'update');
  client.graceTimer = setTimeout(() => {
    if (room.clients.get(client.id) === client && !client.ws) removeClient(room, client, 'timeout');
  }, GRACE_MS);
}

function handle(ws, msg) {
  switch (msg.type) {
    case 'ping': return send(ws, { type: 'pong' });
    case 'create': return onCreate(ws, msg);
    case 'join': return onJoin(ws, msg);
    case 'rejoin': return onRejoin(ws, msg);
    case 'leave': return onLeave(ws);
    default: break;
  }

  const ctx = getCtx(ws);
  if (!ctx) return send(ws, { type: 'error', code: 'not_in_room', message: 'Tu n’es dans aucun salon.' });
  const room = ctx.room;
  const client = ctx.client;

  switch (msg.type) {
    case 'chat': {
      const text = String(msg.text || '').trim().slice(0, 500);
      if (text) addChat(room, { from: client, text: text });
      break;
    }

    case 'rename': {
      const old = client.name;
      client.name = cleanName(msg.name);
      if (client.name !== old) {
        broadcastRoom(room, 'update');
        addChat(room, { system: true, text: old + ' s’appelle maintenant ' + client.name });
      }
      break;
    }

    case 'movie': { // hôte uniquement
      if (room.hostId !== client.id) break;
      const videoId = String(msg.videoId || '');
      if (!/^\d{4,12}$/.test(videoId)) break;
      const title = String(msg.title || '').trim().slice(0, 120);
      if (room.movie && room.movie.videoId === videoId) {
        if (title && title !== room.movie.title) {
          room.movie.title = title;
          broadcast(room, { type: 'movie', changed: false, movie: movieNow(room) });
        }
        break;
      }
      room.movie = {
        videoId: videoId,
        url: 'https://www.netflix.com/watch/' + videoId, // URL reconstruite côté serveur
        title: title,
        time: Math.max(0, Number(msg.time) || 0),
        playing: typeof msg.playing === 'boolean' ? msg.playing : true,
        updatedAt: now()
      };
      broadcast(room, { type: 'movie', changed: true, movie: movieNow(room) });
      addChat(room, {
        system: true,
        text: title ? client.name + ' a lancé « ' + title + ' »' : client.name + ' a lancé un film'
      });
      break;
    }

    case 'sync': {
      const m = room.movie;
      const action = msg.action;
      if (!m || String(msg.videoId) !== m.videoId) break;
      if (action !== 'play' && action !== 'pause' && action !== 'seek') break;
      const time = Math.max(0, Number(msg.time) || 0);
      m.time = time;
      if (action === 'play') m.playing = true;
      if (action === 'pause') m.playing = false;
      m.updatedAt = now();
      broadcast(room, { type: 'sync', action: action, time: time, videoId: m.videoId, by: { id: client.id, name: client.name } }, client.id);
      if (action === 'pause') {
        addChat(room, { system: true, text: client.name + ' a mis en pause à ' + fmt(time) });
      } else if (action === 'play') {
        addChat(room, { system: true, text: client.name + ' a relancé la lecture à ' + fmt(time) });
      } else if (now() - client.lastSeekChat > 2500) {
        addChat(room, { system: true, text: client.name + ' a déplacé la lecture à ' + fmt(time) });
      }
      if (action === 'seek') client.lastSeekChat = now();
      break;
    }

    case 'heartbeat': { // hôte uniquement : sa position sert de référence
      const m = room.movie;
      if (room.hostId !== client.id || !m || String(msg.videoId) !== m.videoId) break;
      m.time = Math.max(0, Number(msg.time) || 0);
      m.playing = !!msg.playing;
      m.updatedAt = now();
      broadcast(room, { type: 'heartbeat', time: m.time, playing: m.playing, videoId: m.videoId }, client.id);
      break;
    }

    case 'resync': { // un invité demande la position actuelle de l'hôte
      const mv = movieNow(room);
      if (!mv) break;
      send(ws, { type: 'resync', movie: { videoId: mv.videoId, time: mv.time, playing: mv.playing }, by: { name: mv.hostName } });
      break;
    }

    case 'force_sync': { // l'hôte recale tout le monde sur lui
      const m = room.movie;
      if (room.hostId !== client.id || !m || String(msg.videoId) !== m.videoId) break;
      m.time = Math.max(0, Number(msg.time) || 0);
      m.playing = !!msg.playing;
      m.updatedAt = now();
      broadcast(room, {
        type: 'resync',
        movie: { videoId: m.videoId, time: m.time, playing: m.playing },
        by: { id: client.id, name: client.name }
      }, client.id);
      addChat(room, { system: true, text: client.name + ' a synchronisé tout le monde à ' + fmt(m.time) });
      break;
    }

    default:
      break;
  }
}

// ---------------------------------------------------------------- connexions
wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.ctx = null;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch (e) { return; }
    if (!msg || typeof msg.type !== 'string') return;
    try { handle(ws, msg); } catch (e) { console.error('handler error', e); }
  });
  ws.on('close', () => onClose(ws));
  ws.on('error', () => {});
});

// Détection des connexions mortes
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    try { ws.ping(); } catch (e) { /* ignore */ }
  });
}, 30000);

server.listen(PORT, () => console.log('WatchParty server en écoute sur le port ' + PORT));
