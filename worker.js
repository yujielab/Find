/**
 * Find — Real-time location sharing
 * Cloudflare Worker + Durable Objects backend
 *
 * Routes:
 *   GET  /                       → serves index.html (via [assets] binding)
 *   GET  /ws/:roomId             → WebSocket upgrade into the room's Durable Object
 *   GET  /api/health             → JSON health check
 *
 * Each room is a single Durable Object instance addressed by the room ID,
 * which gives us a globally consistent in-memory state for all participants
 * connected to that room.
 */

/* ──────────────────────────────────────────────────────────────────────
 * Durable Object: one instance per room.
 * ────────────────────────────────────────────────────────────────────── */
export class LocationRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    /** @type {Map<string, {ws: WebSocket, userId: string, userName: string, location: any}>} */
    this.sessions = new Map();
    this.lastActivity = Date.now();
  }

  async fetch(request) {
    const upgrade = request.headers.get('Upgrade');
    if (!upgrade || upgrade.toLowerCase() !== 'websocket') {
      return new Response('Expected WebSocket upgrade', { status: 426 });
    }

    const url = new URL(request.url);
    const userId = (url.searchParams.get('userId') || '').slice(0, 64) || crypto.randomUUID();
    const userName = (url.searchParams.get('name') || 'Anonymous').slice(0, 32);

    const pair = new WebSocketPair();
    const [client, server] = [pair[0], pair[1]];
    this.handleSession(server, userId, userName);
    return new Response(null, { status: 101, webSocket: client });
  }

  handleSession(ws, userId, userName) {
    ws.accept();
    this.lastActivity = Date.now();

    // If a session with this userId already exists (e.g. reconnect), drop the old one.
    const existing = this.sessions.get(userId);
    if (existing) {
      try { existing.ws.close(1000, 'replaced'); } catch {}
      this.sessions.delete(userId);
    }

    const session = { ws, userId, userName, location: null };
    this.sessions.set(userId, session);

    // Send the newcomer the current roster.
    const members = [];
    for (const [id, s] of this.sessions) {
      if (id === userId) continue;
      members.push({
        userId: s.userId,
        userName: s.userName,
        location: s.location,
      });
    }
    safeSend(ws, { type: 'welcome', userId, members });

    // Tell everyone else someone joined.
    this.broadcast({ type: 'join', userId, userName }, userId);

    ws.addEventListener('message', evt => {
      this.lastActivity = Date.now();
      let msg;
      try {
        msg = JSON.parse(typeof evt.data === 'string' ? evt.data : new TextDecoder().decode(evt.data));
      } catch {
        return;
      }
      this.onMessage(session, msg);
    });

    const cleanup = () => {
      if (this.sessions.get(userId) === session) {
        this.sessions.delete(userId);
        this.broadcast({ type: 'leave', userId }, null);
      }
    };
    ws.addEventListener('close', cleanup);
    ws.addEventListener('error', cleanup);
  }

  onMessage(session, msg) {
    switch (msg.type) {
      case 'location': {
        // Validate basic shape
        if (typeof msg.lat !== 'number' || typeof msg.lng !== 'number') return;
        if (Math.abs(msg.lat) > 90 || Math.abs(msg.lng) > 180) return;

        session.location = {
          lat: msg.lat,
          lng: msg.lng,
          heading: typeof msg.heading === 'number' ? msg.heading : null,
          accuracy: typeof msg.accuracy === 'number' ? msg.accuracy : null,
          speed: typeof msg.speed === 'number' ? msg.speed : null,
          timestamp: Date.now(),
        };

        this.broadcast({
          type: 'location',
          userId: session.userId,
          userName: session.userName,
          location: session.location,
        }, session.userId);
        break;
      }
      case 'rename': {
        if (typeof msg.name === 'string') {
          session.userName = msg.name.slice(0, 32);
          this.broadcast({ type: 'rename', userId: session.userId, userName: session.userName }, session.userId);
        }
        break;
      }
      case 'ping': {
        safeSend(session.ws, { type: 'pong', t: Date.now() });
        break;
      }
    }
  }

  broadcast(msg, excludeUserId) {
    const data = JSON.stringify(msg);
    for (const [id, s] of this.sessions) {
      if (id === excludeUserId) continue;
      try { s.ws.send(data); }
      catch {
        this.sessions.delete(id);
      }
    }
  }
}

function safeSend(ws, obj) {
  try { ws.send(JSON.stringify(obj)); } catch {}
}

/* ──────────────────────────────────────────────────────────────────────
 * Worker entrypoint.
 * ────────────────────────────────────────────────────────────────────── */
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // WebSocket → forward to the room's Durable Object.
    if (url.pathname.startsWith('/ws/')) {
      const roomId = sanitizeRoomId(url.pathname.slice(4));
      if (!roomId) return new Response('Bad room id', { status: 400 });

      const id = env.ROOMS.idFromName(roomId);
      const room = env.ROOMS.get(id);
      return room.fetch(request);
    }

    // Health check.
    if (url.pathname === '/api/health') {
      return Response.json({ ok: true, time: new Date().toISOString() });
    }

    // Everything else → static asset (index.html, etc.) via the Assets binding.
    if (env.ASSETS) {
      return env.ASSETS.fetch(request);
    }

    // Fallback: serve a minimal placeholder if ASSETS isn't configured.
    return new Response(
      'Static assets not configured. Add an [assets] binding in wrangler.toml.',
      { status: 500, headers: { 'Content-Type': 'text/plain' } }
    );
  },
};

function sanitizeRoomId(s) {
  s = (s || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 32);
  return s.length >= 4 ? s : null;
}
