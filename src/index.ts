type AiBinding = {
  run(model: string, input: unknown): Promise<unknown>;
};

type DemoEvent = {
  id: string;
  checkinId: string;
  eventType: "checkin.created";
  createdAt: string;
  payload: {
    name: string;
    messageLength: number;
    receiptKey: string;
    aiRequested: boolean;
  };
};

interface Env {
  ROOM: DurableObjectNamespace;
  DB: D1Database;
  BUCKET: R2Bucket;
  EVENTS_QUEUE: Queue<DemoEvent>;
  ASSETS: Fetcher;
  AI?: AiBinding;
}

type CheckinRequest = {
  name?: string;
  message?: string;
  useAi?: boolean;
};

type RoomState = {
  totalCheckins: number;
  totalVisits: number;
  lastCheckinAt: string | null;
};

const JSON_HEADERS = {
  "content-type": "application/json; charset=utf-8",
  "cache-control": "no-store"
};

export class DemoRoom {
  constructor(private readonly state: DurableObjectState) {}

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/visit" && request.method === "POST") {
      const room = await this.readState();
      room.totalVisits += 1;
      await this.writeState(room);
      return json(room);
    }

    if (url.pathname === "/checkin" && request.method === "POST") {
      const room = await this.readState();
      room.totalCheckins += 1;
      room.lastCheckinAt = new Date().toISOString();
      await this.writeState(room);
      return json(room);
    }

    if (url.pathname === "/state" && request.method === "GET") {
      return json(await this.readState());
    }

    return new Response("Not found", { status: 404 });
  }

  private async readState(): Promise<RoomState> {
    const [totalCheckins, totalVisits, lastCheckinAt] = await Promise.all([
      this.state.storage.get<number>("totalCheckins"),
      this.state.storage.get<number>("totalVisits"),
      this.state.storage.get<string>("lastCheckinAt")
    ]);

    return {
      totalCheckins: totalCheckins ?? 0,
      totalVisits: totalVisits ?? 0,
      lastCheckinAt: lastCheckinAt ?? null
    };
  }

  private async writeState(room: RoomState): Promise<void> {
    await Promise.all([
      this.state.storage.put("totalCheckins", room.totalCheckins),
      this.state.storage.put("totalVisits", room.totalVisits),
      this.state.storage.put("lastCheckinAt", room.lastCheckinAt)
    ]);
  }
}

const worker: ExportedHandler<Env, DemoEvent> = {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/" && request.method === "GET") {
      ctx.waitUntil(touchRoom(env, "visit"));
      return html(homeHtml());
    }

    if (url.pathname === "/favicon.ico" && request.method === "GET") {
      return favicon();
    }

    if (url.pathname.startsWith("/assets/") && request.method === "GET") {
      return env.ASSETS.fetch(request);
    }

    if (url.pathname === "/api/status" && request.method === "GET") {
      return handleStatus(env);
    }

    if (url.pathname === "/api/checkins" && request.method === "GET") {
      return handleListCheckins(env);
    }

    if (url.pathname === "/api/checkins" && request.method === "POST") {
      return handleCreateCheckin(request, env);
    }

    if (url.pathname.startsWith("/api/receipt/") && request.method === "GET") {
      const id = url.pathname.split("/").pop();
      return id ? handleReceipt(id, env) : json({ error: "Missing receipt id" }, 400);
    }

    return new Response("Not found", { status: 404 });
  },

  async queue(batch, env) {
    for (const message of batch.messages) {
      const event = message.body;
      await env.DB.prepare(
        "INSERT INTO queue_events (id, checkin_id, event_type, payload, created_at) VALUES (?, ?, ?, ?, ?)"
      )
        .bind(event.id, event.checkinId, event.eventType, JSON.stringify(event.payload), event.createdAt)
        .run();

      await env.BUCKET.put(`queue/${event.id}.json`, JSON.stringify(event, null, 2), {
        httpMetadata: { contentType: "application/json" }
      });
    }
  }
};

export default worker;

async function handleStatus(env: Env): Promise<Response> {
  const [room, checkinsCount, queueCount] = await Promise.all([
    touchRoom(env, "state"),
    env.DB.prepare("SELECT COUNT(*) AS count FROM checkins").first<{ count: number }>(),
    env.DB.prepare("SELECT COUNT(*) AS count FROM queue_events").first<{ count: number }>()
  ]);

  return json({
    room,
    d1: {
      checkins: checkinsCount?.count ?? 0,
      queueEvents: queueCount?.count ?? 0
    },
    r2: {
      receiptsBucket: "web-agent-cf-demo-receipts"
    },
    ai: {
      configured: Boolean(env.AI)
    }
  });
}

async function handleListCheckins(env: Env): Promise<Response> {
  const { results } = await env.DB.prepare(
    "SELECT id, name, message, ai_note AS aiNote, receipt_key AS receiptKey, created_at AS createdAt FROM checkins ORDER BY created_at DESC LIMIT 20"
  ).all();

  return json({ checkins: results });
}

async function handleCreateCheckin(request: Request, env: Env): Promise<Response> {
  let input: CheckinRequest;

  try {
    input = (await request.json()) as CheckinRequest;
  } catch {
    return json({ error: "Expected a JSON request body" }, 400);
  }

  const name = cleanText(input.name, 48);
  const message = cleanText(input.message, 280);

  if (!name || !message) {
    return json({ error: "Name and message are required" }, 400);
  }

  const id = crypto.randomUUID();
  const eventId = crypto.randomUUID();
  const createdAt = new Date().toISOString();
  const receiptKey = `receipts/${id}.json`;
  const aiNote = input.useAi ? await createAiNote(env, message) : null;
  const room = await touchRoom(env, "checkin");

  const receipt = {
    id,
    name,
    message,
    aiNote,
    createdAt,
    room
  };

  await env.DB.prepare(
    "INSERT INTO checkins (id, name, message, ai_note, receipt_key, created_at) VALUES (?, ?, ?, ?, ?, ?)"
  )
    .bind(id, name, message, aiNote, receiptKey, createdAt)
    .run();

  await env.BUCKET.put(receiptKey, JSON.stringify(receipt, null, 2), {
    httpMetadata: { contentType: "application/json" }
  });

  await env.EVENTS_QUEUE.send({
    id: eventId,
    checkinId: id,
    eventType: "checkin.created",
    createdAt,
    payload: {
      name,
      messageLength: message.length,
      receiptKey,
      aiRequested: Boolean(input.useAi)
    }
  });

  return json({ checkin: receipt, receiptUrl: `/api/receipt/${id}` }, 201);
}

async function handleReceipt(id: string, env: Env): Promise<Response> {
  const object = await env.BUCKET.get(`receipts/${id}.json`);

  if (!object) {
    return json({ error: "Receipt not found" }, 404);
  }

  return new Response(object.body, {
    headers: {
      ...JSON_HEADERS,
      etag: object.httpEtag
    }
  });
}

async function touchRoom(env: Env, action: "visit" | "checkin" | "state"): Promise<RoomState> {
  const id = env.ROOM.idFromName("public-demo-room");
  const stub = env.ROOM.get(id);
  const method = action === "state" ? "GET" : "POST";
  const response = await stub.fetch(`https://room.local/${action === "state" ? "state" : action}`, { method });

  if (!response.ok) {
    throw new Error(`Durable Object request failed with ${response.status}`);
  }

  return response.json();
}

async function createAiNote(env: Env, message: string): Promise<string> {
  if (!env.AI) {
    return "Workers AI binding is not configured.";
  }

  try {
    const result = await env.AI.run("@cf/meta/llama-3.1-8b-instruct-fast", {
      messages: [
        {
          role: "system",
          content: "Write one concise, neutral dashboard note about the user's check-in. Return plain text only."
        },
        {
          role: "user",
          content: message
        }
      ]
    });

    return extractAiText(result).slice(0, 240);
  } catch (error) {
    return `Workers AI unavailable: ${error instanceof Error ? error.message : "unknown error"}`;
  }
}

function extractAiText(result: unknown): string {
  if (typeof result === "string") {
    return result;
  }

  if (result && typeof result === "object" && "response" in result) {
    const response = (result as { response?: unknown }).response;
    if (typeof response === "string") {
      return response;
    }
  }

  return JSON.stringify(result);
}

function cleanText(value: unknown, maxLength: number): string {
  if (typeof value !== "string") {
    return "";
  }

  return value.trim().replace(/\s+/g, " ").slice(0, maxLength);
}

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: JSON_HEADERS
  });
}

function html(markup: string): Response {
  return new Response(markup, {
    headers: {
      "content-type": "text/html; charset=utf-8",
      "cache-control": "no-store",
      "content-security-policy":
        "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src 'self' data:; connect-src 'self';"
    }
  });
}

function favicon(): Response {
  return new Response(
    `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><rect width="64" height="64" rx="12" fill="#d9480f"/><path d="M18 36c4-11 11-17 21-17 6 0 10 2 13 5-2-1-5-2-8-2-11 0-19 6-24 18l-2-4Zm5 10c5-11 12-16 21-16 4 0 7 .7 10 2-4 8-12 14-22 14h-9Z" fill="#fff"/></svg>`,
    {
      headers: {
        "content-type": "image/svg+xml",
        "cache-control": "public, max-age=86400"
      }
    }
  );
}

function homeHtml(): string {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Oslo Edge Registry</title>
  <style>
    :root {
      color-scheme: light;
      --ink: #172126;
      --muted: #64717a;
      --line: #d9e1e5;
      --panel: #ffffff;
      --soft: #eef3f4;
      --ice: #f7faf9;
      --fjord: #0d5963;
      --forest: #124536;
      --copper: #b9562d;
      --sun: #dca447;
      --blue: #285c8f;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        linear-gradient(180deg, #f8fbfb 0, var(--soft) 460px, #e8eeef 100%);
    }
    .hero {
      min-height: 42vh;
      display: grid;
      align-items: end;
      padding: 42px 20px 30px;
      background:
        linear-gradient(90deg, rgba(12, 31, 37, .84), rgba(18, 69, 54, .52) 48%, rgba(12, 31, 37, .18)),
        url("/assets/oslo-opera-house.jpg");
      background-size: cover;
      background-position: center;
    }
    .wrap {
      width: min(1120px, calc(100% - 32px));
      margin: 0 auto;
    }
    .hero-inner {
      display: grid;
      gap: 18px;
      align-content: end;
      min-height: 230px;
    }
    .kicker {
      width: fit-content;
      padding: 7px 10px;
      border: 1px solid rgba(255,255,255,.44);
      border-radius: 999px;
      color: rgba(255,255,255,.92);
      font-size: .76rem;
      font-weight: 800;
      letter-spacing: .08em;
      text-transform: uppercase;
    }
    h1 {
      margin: 0;
      color: white;
      font-size: clamp(2.4rem, 7vw, 5.6rem);
      line-height: .94;
      letter-spacing: 0;
      max-width: 850px;
    }
    .hero p {
      margin: 0;
      color: rgba(255,255,255,.9);
      font-size: clamp(1rem, 2vw, 1.18rem);
      max-width: 650px;
    }
    main { padding: 18px 0 46px; }
    .deck {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      margin-bottom: 16px;
    }
    .tile {
      min-height: 82px;
      padding: 13px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: rgba(255,255,255,.84);
    }
    .tile span {
      display: block;
      color: var(--muted);
      font-size: .76rem;
      font-weight: 760;
      text-transform: uppercase;
      letter-spacing: .05em;
    }
    .tile strong {
      display: block;
      margin-top: 8px;
      font-size: 1.55rem;
      line-height: 1;
    }
    .grid {
      display: grid;
      grid-template-columns: minmax(0, 1fr) 360px;
      gap: 16px;
      align-items: start;
    }
    section, aside, .item {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 6px;
    }
    section, aside { padding: 18px; box-shadow: 0 16px 38px rgba(31, 45, 50, .07); }
    h2 {
      margin: 0 0 14px;
      font-size: 1.02rem;
      letter-spacing: .01em;
    }
    label { display: block; margin: 0 0 6px; color: var(--muted); font-size: .84rem; font-weight: 650; }
    input, textarea {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 12px;
      font: inherit;
      background: var(--ice);
      color: var(--ink);
    }
    input:focus, textarea:focus {
      outline: 2px solid rgba(40, 92, 143, .2);
      border-color: var(--blue);
      background: white;
    }
    textarea { min-height: 104px; resize: vertical; }
    .fields { display: grid; gap: 12px; }
    .row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
      margin-top: 14px;
    }
    .toggle {
      display: inline-flex;
      align-items: center;
      gap: 9px;
      color: var(--ink);
      font-size: .92rem;
    }
    .toggle input { width: 18px; height: 18px; }
    button {
      border: 0;
      border-radius: 6px;
      min-height: 42px;
      padding: 0 16px;
      font: inherit;
      font-weight: 700;
      background: var(--fjord);
      color: white;
      cursor: pointer;
    }
    button:hover { background: var(--forest); }
    button:disabled { opacity: .6; cursor: wait; }
    .meta {
      display: grid;
      gap: 12px;
    }
    .route {
      display: grid;
      grid-template-columns: 12px minmax(0, 1fr);
      gap: 11px;
      align-items: start;
      padding: 11px 0;
      border-top: 1px solid var(--line);
    }
    .route:first-of-type { border-top: 0; }
    .dot {
      width: 10px;
      height: 10px;
      margin-top: 4px;
      border-radius: 999px;
      background: var(--copper);
    }
    .dot.green { background: var(--forest); }
    .dot.blue { background: var(--blue); }
    .route strong { display: block; font-size: .9rem; }
    .route span { display: block; margin-top: 3px; color: var(--muted); font-size: .84rem; line-height: 1.45; }
    .source {
      margin-top: 16px;
      color: var(--muted);
      font-size: .75rem;
      line-height: 1.45;
    }
    .source a { color: var(--fjord); font-weight: 700; text-decoration: none; }
    .feed { display: grid; gap: 10px; margin-top: 16px; }
    .item { padding: 13px; }
    .item header {
      min-height: auto;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 0;
      background: transparent;
    }
    .item strong { overflow-wrap: anywhere; }
    .item time { color: var(--muted); font-size: .78rem; white-space: nowrap; }
    .item p { margin: 9px 0 0; color: var(--muted); overflow-wrap: anywhere; }
    .ai-note {
      margin-top: 10px;
      padding-left: 10px;
      border-left: 3px solid var(--blue);
      color: var(--ink);
      font-size: .92rem;
    }
    .receipt {
      display: inline-block;
      margin-top: 10px;
      color: var(--forest);
      font-weight: 700;
      text-decoration: none;
      font-size: .88rem;
    }
    .status { min-height: 22px; color: var(--muted); font-size: .9rem; }
    @media (max-width: 820px) {
      .hero { min-height: 34vh; padding-top: 34px; }
      .grid { grid-template-columns: 1fr; }
      .deck { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
    @media (max-width: 520px) {
      .deck { grid-template-columns: 1fr; }
      .row { align-items: stretch; }
      button { width: 100%; }
      .item header { align-items: flex-start; flex-direction: column; gap: 4px; }
    }
  </style>
</head>
<body>
  <header class="hero">
    <div class="wrap hero-inner">
      <div class="kicker">Bjorvika / Oslo</div>
      <h1>Oslo Edge Registry</h1>
      <p>A quiet Nordic check-in template shaped around the Opera House, fjord light, and an edge-native data trail.</p>
    </div>
  </header>
  <main class="wrap">
    <div class="deck" aria-label="Live edge state">
      <div class="tile"><span>Visits</span><strong id="visits">0</strong></div>
      <div class="tile"><span>Check-ins</span><strong id="roomCheckins">0</strong></div>
      <div class="tile"><span>D1 rows</span><strong id="d1Checkins">0</strong></div>
      <div class="tile"><span>Queue rows</span><strong id="queueRows">0</strong></div>
    </div>
    <div class="grid">
      <section>
        <h2>Fjord note</h2>
        <form id="form" class="fields">
          <div>
            <label for="name">Name</label>
            <input id="name" name="name" maxlength="48" required autocomplete="name" placeholder="Ingrid">
          </div>
          <div>
            <label for="message">Note</label>
            <textarea id="message" name="message" maxlength="280" required placeholder="Morning light over Bjorvika, clean lines, quiet data."></textarea>
          </div>
          <div class="row">
            <label class="toggle"><input id="useAi" type="checkbox"> Use Workers AI</label>
            <button id="submit" type="submit">Add note</button>
          </div>
          <div id="status" class="status" role="status"></div>
        </form>
        <div id="feed" class="feed"></div>
      </section>
      <aside>
        <h2>Edge route</h2>
        <div class="meta">
          <div class="route"><i class="dot"></i><div><strong>Durable Object</strong><span>Shared room state for Oslo visitor counts.</span></div></div>
          <div class="route"><i class="dot green"></i><div><strong>D1 + R2</strong><span>Structured notes in D1, receipts stored as R2 objects.</span></div></div>
          <div class="route"><i class="dot blue"></i><div><strong>Queue worker</strong><span>Background audit events after each submitted note.</span></div></div>
        </div>
        <p class="source">Photo: Oslo Opera House by Matic Kozinc, CC0 via <a href="https://commons.wikimedia.org/wiki/File:Oslo_Opera_House,_Oslo,_Norway_(Unsplash_njYp4KqjqF8).jpg" target="_blank" rel="noreferrer">Wikimedia Commons</a>.</p>
      </aside>
    </div>
  </main>
  <script>
    const form = document.querySelector("#form");
    const statusEl = document.querySelector("#status");
    const feed = document.querySelector("#feed");
    const submit = document.querySelector("#submit");

    async function api(path, options) {
      const res = await fetch(path, options);
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "Request failed");
      return data;
    }

    async function refresh() {
      const [status, list] = await Promise.all([
        api("/api/status"),
        api("/api/checkins")
      ]);
      document.querySelector("#visits").textContent = status.room.totalVisits;
      document.querySelector("#roomCheckins").textContent = status.room.totalCheckins;
      document.querySelector("#d1Checkins").textContent = status.d1.checkins;
      document.querySelector("#queueRows").textContent = status.d1.queueEvents;
      feed.innerHTML = list.checkins.map(renderCheckin).join("") || '<div class="item"><p>No Oslo notes yet.</p></div>';
    }

    function renderCheckin(item) {
      const date = new Date(item.createdAt).toLocaleString();
      const note = item.aiNote ? '<div class="ai-note">' + escapeHtml(item.aiNote) + '</div>' : "";
      return '<article class="item"><header><strong>' + escapeHtml(item.name) + '</strong><time>' + date + '</time></header><p>' + escapeHtml(item.message) + '</p>' + note + '<a class="receipt" href="/api/receipt/' + item.id + '" target="_blank" rel="noreferrer">View R2 receipt</a></article>';
    }

    function escapeHtml(value) {
      return String(value).replace(/[&<>"']/g, char => ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#039;"
      })[char]);
    }

    form.addEventListener("submit", async event => {
      event.preventDefault();
      submit.disabled = true;
      statusEl.textContent = "Writing the Oslo note...";
      try {
        await api("/api/checkins", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            name: form.name.value,
            message: form.message.value,
            useAi: form.useAi.checked
          })
        });
        form.reset();
        statusEl.textContent = "Stored at the edge and queued for the audit trail.";
        await refresh();
      } catch (error) {
        statusEl.textContent = error.message;
      } finally {
        submit.disabled = false;
      }
    });

    refresh().catch(error => {
      statusEl.textContent = error.message;
    });
  </script>
</body>
</html>`;
}
