/**
 * ms.tools — Draw & Guess · SQLite-backed Signalling Server
 * Optimized for Cloudflare Workers Free Plan + PeerJS Compatibility
 */

export interface Env {
  MY_DURABLE_OBJECT: DurableObjectNamespace;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    // 1. Handle CORS Preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: corsHeaders() });
    }

    // 2. Route all signalling traffic to the global Durable Object
    const id = env.MY_DURABLE_OBJECT.idFromName('global');
    const stub = env.MY_DURABLE_OBJECT.get(id);
    return stub.fetch(request);
  },
};

export class MyDurableObject implements DurableObject {
  private sessions = new Map<string, WebSocket>();

  constructor(public state: DurableObjectState, public env: Env) {
    // Initialize SQLite to track "Registered" Peer IDs
    this.state.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS peers (
        id TEXT PRIMARY KEY,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    `);
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    // PATH: /id — PeerJS calls this to get a unique ID
    if (request.method === 'GET' && /\/id\/?$/.test(url.pathname)) {
      const newId = randomId();
      this.state.storage.sql.exec("INSERT INTO peers (id) VALUES (?)", newId);
      return new Response(JSON.stringify(newId), { headers: corsHeaders('application/json') });
    }

    // PATH: WebSocket — The main PeerJS signalling channel
    if (request.headers.get('Upgrade') === 'websocket') {
      const peerId = url.searchParams.get('id');
      if (!peerId) return new Response('Missing id', { status: 400 });

      const [client, server] = Object.values(new WebSocketPair());

      // Use Hibernation: Tag the socket so we know its identity in handlers
      this.state.acceptWebSocket(server, [peerId]);
      
      // Keep in-memory for fast routing
      this.sessions.set(peerId, server);

      // PeerJS expects an 'OPEN' message to confirm connection
      this._send(server, { type: 'OPEN' });

      return new Response(null, { status: 101, webSocket: client });
    }

    return new Response('Not found', { status: 404, headers: corsHeaders() });
  }

  // --- WebSocket Hibernation Handlers ---

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
    const [senderId] = this.state.getTags(ws);
    
    let msg: any;
    try {
      msg = JSON.parse(message as string);
    } catch {
      return;
    }

    // 1. PeerJS Heartbeat
    if (msg.type === 'HEARTBEAT') {
      this._send(ws, { type: 'HEARTBEAT' });
      return;
    }

    // 2. Explicit Leave (cleanup SQLite)
    if (msg.type === 'LEAVE') {
      this.state.storage.sql.exec("DELETE FROM peers WHERE id = ?", senderId);
      this.sessions.delete(senderId);
      return;
    }

    // 3. Routing (Offer/Answer/Candidate)
    if (msg.dst) {
      msg.src = senderId;
      const targetSocket = this.sessions.get(msg.dst);

      if (targetSocket && targetSocket.readyState === WebSocket.OPEN) {
        this._send(targetSocket, msg);
      } else {
        // If they aren't in memory, check if they exist in SQLite
        const exists = this.state.storage.sql.exec(
          "SELECT 1 FROM peers WHERE id = ?", msg.dst
        ).toArray().length > 0;

        this._send(ws, { 
          type: 'EXPIRE', 
          src: msg.dst, 
          msg: exists ? 'Peer exists but is not connected' : 'Peer does not exist' 
        });
      }
    }
  }

  async webSocketClose(ws: WebSocket) {
    const [peerId] = this.state.getTags(ws);
    this.sessions.delete(peerId);
    // Note: We don't delete from SQLite here so IDs can "reconnect" 
    // unless you want IDs to be strictly one-time use.
  }

  async webSocketError(ws: WebSocket) {
    const [peerId] = this.state.getTags(ws);
    this.sessions.delete(peerId);
  }

  private _send(ws: WebSocket, obj: object) {
    try {
      ws.send(JSON.stringify(obj));
    } catch (e) {
      // Socket likely closed
    }
  }
}

// --- Utilities ---

function randomId(): string {
  // Generates 6-character room codes (common for Draw & Guess)
  const CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // No confusing 0, O, 1, I
  const buf = new Uint8Array(6);
  crypto.getRandomValues(buf);
  return Array.from(buf, b => CHARS[b % CHARS.length]).join('');
}

function corsHeaders(contentType?: string): Record<string, string> {
  const h: Record<string, string> = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Upgrade, Connection',
  };
  if (contentType) h['Content-Type'] = contentType;
  return h;
}
