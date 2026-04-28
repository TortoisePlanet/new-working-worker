export interface Env {
  MY_DURABLE_OBJECT: DurableObjectNamespace;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: corsHeaders() });
    
    // Route everything to the single Durable Object instance
    const id = env.MY_DURABLE_OBJECT.idFromName('global');
    const stub = env.MY_DURABLE_OBJECT.get(id);
    return stub.fetch(request);
  },
};

export class MyDurableObject implements DurableObject {
  private sessions = new Map<string, WebSocket>();

  constructor(public state: DurableObjectState) {
    this.state.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS peers (
        id TEXT PRIMARY KEY,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    `);
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    // 1. Handle /id (Path-aware check)
    if (request.method === 'GET' && url.pathname.endsWith('/id')) {
      const newId = randomId();
      this.state.storage.sql.exec("INSERT INTO peers (id) VALUES (?)", newId);
      return new Response(JSON.stringify(newId), { headers: corsHeaders('application/json') });
    }

    // 2. Handle WebSocket Signalling
    if (request.headers.get('Upgrade') === 'websocket') {
      const peerId = url.searchParams.get('id');
      if (!peerId) return new Response('Missing id', { status: 400 });

      const [client, server] = Object.values(new WebSocketPair());
      
      // Use Hibernation API for Free Plan support
      this.state.acceptWebSocket(server, [peerId]);
      this.sessions.set(peerId, server);

      this._send(server, { type: 'OPEN' }); 
      return new Response(null, { status: 101, webSocket: client });
    }

    return new Response(`Worker Active. Path: ${url.pathname}`, { status: 404, headers: corsHeaders() });
  }

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
    const [senderId] = this.state.getTags(ws);
    let msg;
    try { msg = JSON.parse(message as string); } catch { return; }

    if (msg.type === 'HEARTBEAT') return this._send(ws, { type: 'HEARTBEAT' });

    if (msg.type === 'LEAVE') {
      this.state.storage.sql.exec("DELETE FROM peers WHERE id = ?", senderId);
      this.sessions.delete(senderId);
      return;
    }

    if (msg.dst) {
      msg.src = senderId;
      const target = this.sessions.get(msg.dst);
      if (target && target.readyState === WebSocket.OPEN) {
        this._send(target, msg);
      } else {
        this._send(ws, { type: 'EXPIRE', src: msg.dst });
      }
    }
  }

  async webSocketClose(ws: WebSocket) {
    const [peerId] = this.state.getTags(ws);
    this.sessions.delete(peerId);
  }

  private _send(ws: WebSocket, obj: object) {
    try { ws.send(JSON.stringify(obj)); } catch {}
  }
}

function randomId() {
  const CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  const buf = new Uint8Array(6);
  crypto.getRandomValues(buf);
  return Array.from(buf, b => CHARS[b % CHARS.length]).join('');
}

function corsHeaders(contentType?: string) {
  const h: Record<string, string> = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Upgrade, Connection',
  };
  if (contentType) h['Content-Type'] = contentType;
  return h;
}
