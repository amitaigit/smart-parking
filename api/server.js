'use strict';

const express = require('express');
const { createClient } = require('redis');

// ---------------------------------------------------------------------------
// Configuration
// Everything environment-specific comes from env vars (12-factor style), so the
// SAME image runs on your laptop, in docker compose, and in CI without edits.
// Inside docker compose the hostname "redis" is the Redis service's name; the
// compose network's built-in DNS resolves it to the container's IP.
// ---------------------------------------------------------------------------
const PORT = Number(process.env.PORT) || 3000;
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const DEFAULT_CAPACITY = Number(process.env.BAY_CAPACITY) || 5;

// ---------------------------------------------------------------------------
// Redis data model
//   bay:ships    HASH    field = license plate, value = ISO docking timestamp
//   bay:capacity STRING  number of docking spots in the bay
//
// Why a HASH? It gives O(1) "is this plate already docked?" (HEXISTS), O(1)
// removal (HDEL), O(1) count (HLEN), and we can store the arrival time as the
// value. A plain LIST would force us to scan it for every duplicate check.
// ---------------------------------------------------------------------------
const KEY_SHIPS = 'bay:ships';
const KEY_CAPACITY = 'bay:capacity';

const redis = createClient({
  url: REDIS_URL,
  // docker compose starts containers in parallel, so the API can boot before
  // Redis accepts connections. Instead of crashing, retry with a short backoff
  // (200ms, 400ms, ... capped at 3s).
  socket: { reconnectStrategy: (retries) => Math.min(retries * 200, 3000) },
});
redis.on('error', (err) => console.error('[redis] error:', err.message));
redis.on('ready', () => console.log('[redis] connection ready'));

// ---------------------------------------------------------------------------
// Atomic "dock a ship" operation (Lua script, executed inside Redis).
//
// The naive version is: check HLEN, then HSET. That is a race condition: two
// cameras posting at the same instant can both see "4 of 5 spots used" and both
// succeed, leaving 6 ships in a 5-spot bay. Redis runs a Lua script as a single
// uninterruptible unit, so check-and-insert becomes one atomic step.
//
// KEYS[1] = ships hash, KEYS[2] = capacity key
// ARGV[1] = plate, ARGV[2] = timestamp, ARGV[3] = fallback capacity
// ---------------------------------------------------------------------------
const DOCK_SCRIPT = `
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then
  return 'DUPLICATE'
end
local capacity = tonumber(redis.call('GET', KEYS[2])) or tonumber(ARGV[3])
if redis.call('HLEN', KEYS[1]) >= capacity then
  return 'FULL'
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
return 'OK'
`;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Never trust input, even from "our own" camera. Normalise first so that
// "abc 123" and "ABC123" are the same ship, then validate with a whitelist.
function normalizePlate(raw) {
  if (typeof raw !== 'string') return null;
  const plate = raw.trim().toUpperCase().replace(/\s+/g, '');
  return /^[A-Z0-9-]{2,10}$/.test(plate) ? plate : null;
}

async function readBay() {
  const [capacityRaw, shipsRaw] = await Promise.all([
    redis.get(KEY_CAPACITY),
    redis.hGetAll(KEY_SHIPS),
  ]);
  const capacity = Number(capacityRaw) || DEFAULT_CAPACITY;
  // ISO-8601 timestamps sort correctly as plain strings: oldest ship first.
  const ships = Object.entries(shipsRaw)
    .map(([plate, dockedAt]) => ({ plate, dockedAt }))
    .sort((a, b) => a.dockedAt.localeCompare(b.dockedAt));

  return {
    capacity,
    occupied: ships.length,
    available: Math.max(capacity - ships.length, 0),
    full: ships.length >= capacity,
    ships,
  };
}

// Express 4 does NOT catch rejected promises from async handlers; an unhandled
// rejection would leave the request hanging. This wrapper forwards any error to
// Express's error middleware instead.
const wrap = (fn) => (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);

// ---------------------------------------------------------------------------
// App + routes
// ---------------------------------------------------------------------------
const app = express();
app.use(express.json({ limit: '1kb' })); // a plate payload is tiny; reject anything bigger

// Liveness/readiness probe: proves the API AND its Redis link work. Docker
// healthchecks and orchestrators hit endpoints like this.
app.get('/health', wrap(async (req, res) => {
  await redis.ping();
  res.json({ status: 'ok' });
}));

// IoT camera -> API: a ship (license plate) arrives at the bay.
app.post('/api/dock', wrap(async (req, res) => {
  const plate = normalizePlate(req.body && req.body.plate);
  if (!plate) {
    return res.status(400).json({
      error: 'INVALID_PLATE',
      message: 'Body must be JSON like {"plate":"ABC-123"} (2-10 letters, digits or dashes).',
    });
  }

  const dockedAt = new Date().toISOString();
  const result = await redis.eval(DOCK_SCRIPT, {
    keys: [KEY_SHIPS, KEY_CAPACITY],
    arguments: [plate, dockedAt, String(DEFAULT_CAPACITY)],
  });

  if (result === 'DUPLICATE') {
    return res.status(409).json({ error: 'ALREADY_DOCKED', message: `${plate} is already in the bay.` });
  }
  if (result === 'FULL') {
    return res.status(409).json({ error: 'BAY_FULL', message: 'Docking bay is at full capacity.' });
  }
  console.log(`[dock] ${plate} docked at ${dockedAt}`);
  res.status(201).json({ plate, dockedAt });
}));

// Dashboard -> API: current state of the bay (polled every few seconds).
app.get('/api/bay', wrap(async (req, res) => {
  res.json(await readBay());
}));

// Dashboard -> API: the "Release Ship" button.
app.delete('/api/dock/:plate', wrap(async (req, res) => {
  const plate = normalizePlate(req.params.plate);
  if (!plate) {
    return res.status(400).json({ error: 'INVALID_PLATE', message: 'Malformed license plate.' });
  }
  const removed = await redis.hDel(KEY_SHIPS, plate);
  if (removed === 0) {
    return res.status(404).json({ error: 'NOT_DOCKED', message: `${plate} is not in the bay.` });
  }
  console.log(`[release] ${plate} released`);
  res.json({ released: plate });
}));

// Bad JSON bodies arrive here as a SyntaxError from express.json().
// Everything else is an unexpected failure (e.g. Redis down) -> 500, no stack leak.
app.use((err, req, res, next) => {
  if (err.type === 'entity.parse.failed') {
    return res.status(400).json({ error: 'BAD_JSON', message: 'Request body is not valid JSON.' });
  }
  console.error('[error]', err);
  res.status(500).json({ error: 'INTERNAL_ERROR', message: 'Something went wrong in the bay.' });
});

// ---------------------------------------------------------------------------
// Startup + graceful shutdown
// ---------------------------------------------------------------------------
async function start() {
  await redis.connect();

  // NX = "only set if the key does not exist". On the very first boot we seed
  // the capacity; on later boots (data restored from the Docker volume) we do
  // NOT overwrite whatever is already stored.
  await redis.set(KEY_CAPACITY, String(DEFAULT_CAPACITY), { NX: true });

  // Bind 0.0.0.0 so the port is reachable from OTHER containers. Binding to
  // 127.0.0.1 would only accept connections from inside this same container.
  const server = app.listen(PORT, '0.0.0.0', () => {
    console.log(`[api] listening on :${PORT}, redis at ${REDIS_URL}`);
  });

  // `docker compose down` sends SIGTERM. If we ignore it, Docker waits 10s and
  // then SIGKILLs us. Handling it lets us finish in-flight requests and close
  // the Redis connection cleanly.
  const shutdown = (signal) => {
    console.log(`[api] ${signal} received, shutting down`);
    server.close(async () => {
      await redis.quit();
      process.exit(0);
    });
    setTimeout(() => process.exit(1), 8000).unref(); // safety net
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

start().catch((err) => {
  console.error('[api] failed to start:', err);
  process.exit(1);
});
