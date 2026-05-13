# web-agent-cf

A small public Cloudflare Workers demo that uses:

- Workers for the app, HTML UI, and API
- Durable Objects for live shared state
- D1 for check-in records and queue audit rows
- R2 for JSON receipts and processed queue payloads
- Queues for background processing
- Workers AI as an optional enrichment step

The app is intentionally compact: visitors submit an Oslo-styled note, the Durable Object increments shared counters, the Worker writes to D1 and R2, a Queue message records background audit data, and Workers AI can add a short note when enabled in the UI.

## Run locally

Requires Node.js 22 or newer.

```sh
npm install
npm run db:migrate:local
npm run dev
```

Open the local Wrangler URL and submit a check-in. Leave "Use Workers AI" off unless you want local development to call Workers AI through your Cloudflare account.

## Provision Cloudflare resources

```sh
npx wrangler d1 create web-agent-cf-demo-db-2
npx wrangler r2 bucket create web-agent-cf-demo-receipts
npx wrangler queues create web-agent-cf-events
```

Copy the D1 `database_id` into `wrangler.jsonc`, then apply the migration remotely:

```sh
npm run db:migrate:remote
npm run deploy
```

The Worker deploys to `workers.dev` because `workers_dev` is enabled.

## API

- `GET /api/status` returns Durable Object state and recent D1/Queue counts.
- `GET /api/checkins` returns the latest check-ins from D1.
- `POST /api/checkins` accepts `{ "name": "...", "message": "...", "useAi": false }`.
- `GET /api/receipt/:id` returns the JSON receipt stored in R2.

## Notes

`wrangler.jsonc` is currently wired to `web-agent-cf-demo-db-2`. If you create a different D1 database, replace the `database_id` and keep the binding name as `DB`.

The Oslo Opera House image is by Matic Kozinc and is available as CC0 via Wikimedia Commons.
