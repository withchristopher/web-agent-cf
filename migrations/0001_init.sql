CREATE TABLE IF NOT EXISTS checkins (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  message TEXT NOT NULL,
  ai_note TEXT,
  receipt_key TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS queue_events (
  id TEXT PRIMARY KEY,
  checkin_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_checkins_created_at ON checkins (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_queue_events_created_at ON queue_events (created_at DESC);
