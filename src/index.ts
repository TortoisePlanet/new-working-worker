/**
 * ms.tools — Draw & Guess · SQLite-backed Durable Object Signalling
 */

export interface Env {
  MY_DURABLE_OBJECT: DurableObjectNamespace;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: corsHeaders() });
    }

    // All peers share the 'global' DO instance to find each other
    const id = env.MY_DURABLE_OBJECT.idFromName('global');
    const stub = env.MY_DURABLE_OBJECT.get(id);
    return stub.fetch(request);
  },
};

export class MyDurableObject implements DurableObject {
  // In-memory sessions map (wiped on DO restart, but sockets reconnect)
  private sessions = new Map<string, WebSocket>();

  constructor(public state: DurableObjectState, public env: Env) {
    // 1. Initialize the SQLite table
    this.state.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS peers (
        id TEXT PRIMARY KEY,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    `);
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    // WebSocket Signalling
    if (request.headers.get('Upgrade') === 'websocket') {
      const peerId = url.searchParams.get('id');
      if (!peerId) return new Response('Missing id', { status: 400 });

      const [client, server] = Object.values(new WebSocketPair());

      // Use Hibernation API: tags the socket for the 'webSocketMessage' handler
      this.state.acceptWebSocket(server, [peerId]);
      this.sessions.set(peerId, server);

      this._send(server, { type: 'OPEN' });
      return new Response(null, { status: 101, webSocket: client });
    }

    // REST: Get new ID
    if (request.method === 'GET' && /\/id\/?$/.test(url.pathname)) {
      const newId = randomId();
      // 2. Persist the generated ID to SQLite
      this.state.storage.sql.exec("INSERT INTO peers (id) VALUES (?)", newId);
      
      return new Response(JSON.stringify(newId), {
        headers: corsHeaders('application/json'),
      });
    }

    return new Response('Not found', { status: 404, headers: corsHeaders() });
  }

  /**
   * HIBERNATION HANDLERS
   * Required for Durable Objects on the Workers Free Plan.
   */
  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
    const [peerId] = this.state.getTags(ws);
    let msg: any;
    try {
      msg = JSON.parse(message as string);
    } catch {
      return;
    }

    if (msg.type === 'HEARTBEAT') {
      this._send(ws, { type: 'HEARTBEAT' });
      return;
    }

    if (msg.dst) {
      msg.src = peerId;
      const dstSocket = this.sessions.get(msg.dst);
      if (dstSocket && dstSocket.readyState === WebSocket.OPEN) {
        this._send(dstSocket, msg);
      } else {
        this._send(ws, { type: 'EXPIRE', src: msg.dst });
      }
    }
  }

  async webSocketClose(ws: WebSocket) {
    const [peerId] = this.state.getTags(ws);
    this.sessions.delete(peerId);
  }

  async webSocketError(ws: WebSocket) {
    const [peerId] = this.state.getTags(ws);
    this.sessions.delete(peerId);
  }

  private _send(ws: WebSocket, obj: object) {
    try {
      ws.send(JSON.stringify(obj));
    } catch (e) {}
  }
}

// --- Helpers ---

function randomId(): string {
  const CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  const buf = new Uint8Array(16);
  crypto.getRandomValues(buf);
  return Array.from(buf, b => CHARS[b % CHARS.length]).join('');
}

function corsHeaders(contentType?: string): Record<string, string> {
  const h: Record<string, string> = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Upgrade, Connection',
  };
  if (contentType) h['Content-Type'] = contentType;
  return h;
}
