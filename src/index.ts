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

function homeHtml(): string {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Cloudflare Edge Guestbook</title>
  <style>
    :root {
      color-scheme: light;
      --ink: #1b1f23;
      --muted: #5f6670;
      --line: #d8dde3;
      --panel: #ffffff;
      --soft: #f3f7f8;
      --accent: #d9480f;
      --green: #0b7a53;
      --blue: #1455d9;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: var(--ink);
      background: var(--soft);
    }
    header {
      min-height: 34vh;
      display: grid;
      align-items: end;
      padding: 48px 20px 28px;
      background:
        linear-gradient(120deg, rgba(6, 41, 61, 0.78), rgba(10, 89, 74, 0.56)),
        url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 1200 520'%3E%3Crect width='1200' height='520' fill='%23e8f2f1'/%3E%3Cg fill='none' stroke='%23ffffff' stroke-width='22' opacity='.52'%3E%3Cpath d='M80 390c180-150 310-210 470-160s250 150 570-20'/%3E%3Cpath d='M20 255c210-130 405-145 560-45s270 118 590-80'/%3E%3C/g%3E%3Cg fill='%23ffffff' opacity='.9'%3E%3Ccircle cx='223' cy='185' r='18'/%3E%3Ccircle cx='573' cy='260' r='24'/%3E%3Ccircle cx='932' cy='171' r='20'/%3E%3C/g%3E%3C/svg%3E");
      background-size: cover;
      background-position: center;
    }
    .hero, main { width: min(1080px, calc(100% - 32px)); margin: 0 auto; }
    .hero h1 {
      margin: 0 0 10px;
      color: white;
      font-size: clamp(2.2rem, 8vw, 5.8rem);
      line-height: .96;
      letter-spacing: 0;
      max-width: 920px;
    }
    .hero p {
      margin: 0;
      color: rgba(255,255,255,.88);
      font-size: 1.08rem;
      max-width: 680px;
    }
    main { padding: 22px 0 44px; }
    .grid {
      display: grid;
      grid-template-columns: minmax(0, 1fr) 340px;
      gap: 16px;
      align-items: start;
    }
    section, aside, .item {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
    }
    section, aside { padding: 18px; }
    h2 { margin: 0 0 14px; font-size: 1.05rem; }
    label { display: block; margin: 0 0 6px; color: var(--muted); font-size: .84rem; }
    input, textarea {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 11px 12px;
      font: inherit;
      background: white;
      color: var(--ink);
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
      background: var(--accent);
      color: white;
      cursor: pointer;
    }
    button:disabled { opacity: .6; cursor: wait; }
    .stats {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
    }
    .stat {
      min-height: 86px;
      padding: 12px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fbfcfc;
    }
    .stat span { display: block; color: var(--muted); font-size: .78rem; }
    .stat strong { display: block; margin-top: 5px; font-size: 1.7rem; }
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
      color: var(--green);
      font-weight: 700;
      text-decoration: none;
      font-size: .88rem;
    }
    .status { min-height: 22px; color: var(--muted); font-size: .9rem; }
    @media (max-width: 820px) {
      header { min-height: 30vh; padding-top: 36px; }
      .grid { grid-template-columns: 1fr; }
      .stats { grid-template-columns: repeat(3, minmax(0, 1fr)); }
    }
    @media (max-width: 520px) {
      .stats { grid-template-columns: 1fr; }
      .row { align-items: stretch; }
      button { width: 100%; }
    }
  </style>
</head>
<body>
  <header>
    <div class="hero">
      <h1>Cloudflare Edge Guestbook</h1>
      <p>A compact public demo wired through Workers, Durable Objects, D1, R2, Queues, and optional Workers AI.</p>
    </div>
  </header>
  <main>
    <div class="grid">
      <section>
        <h2>Check in</h2>
        <form id="form" class="fields">
          <div>
            <label for="name">Name</label>
            <input id="name" name="name" maxlength="48" required autocomplete="name" placeholder="Ada">
          </div>
          <div>
            <label for="message">Message</label>
            <textarea id="message" name="message" maxlength="280" required placeholder="This Worker touched every storage primitive in one request."></textarea>
          </div>
          <div class="row">
            <label class="toggle"><input id="useAi" type="checkbox"> Use Workers AI</label>
            <button id="submit" type="submit">Submit</button>
          </div>
          <div id="status" class="status" role="status"></div>
        </form>
        <div id="feed" class="feed"></div>
      </section>
      <aside>
        <h2>Live edge state</h2>
        <div class="stats">
          <div class="stat"><span>DO visits</span><strong id="visits">0</strong></div>
          <div class="stat"><span>DO check-ins</span><strong id="roomCheckins">0</strong></div>
          <div class="stat"><span>D1 rows</span><strong id="d1Checkins">0</strong></div>
          <div class="stat"><span>Queue rows</span><strong id="queueRows">0</strong></div>
        </div>
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
      feed.innerHTML = list.checkins.map(renderCheckin).join("") || '<div class="item"><p>No check-ins yet.</p></div>';
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
      statusEl.textContent = "Writing to Worker bindings...";
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
        statusEl.textContent = "Stored in D1 and R2; queued for background processing.";
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
