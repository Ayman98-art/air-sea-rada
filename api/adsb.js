// Proxy for ADS-B data with caching so many visitors share one upstream request.
// Layers:
//   1. Vercel CDN cache (s-maxage) — identical URLs are served from the edge.
//   2. In-memory cache per function instance — fresh for FRESH_MS.
//   3. Request coalescing — concurrent misses for the same key share one fetch.
//   4. Stale fallback — if every source fails, serve the last good data (up to STALE_MS old).

const SOURCES = [
  "https://api.airplanes.live/v2/",
  "https://api.adsb.lol/v2/",
];

const FRESH_MS = 8000;          // reuse data for 8s
const STALE_MS = 5 * 60 * 1000; // keep last good copy for 5 min as fallback
const TIMEOUT_MS = 8000;
const MAX_KEYS = 200;

const cache = new Map();    // key -> { t, body }
const pending = new Map();  // key -> Promise<body>

async function fetchUpstream(p) {
  const details = [];
  for (const base of SOURCES) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), TIMEOUT_MS);
    try {
      const r = await fetch(base + p, {
        signal: ctrl.signal,
        headers: { "User-Agent": "air-sea-rada/1.0", "Accept": "application/json" },
      });
      const body = await r.text();
      if (r.ok && body.startsWith("{")) return body;
      details.push(base + " -> " + r.status);
    } catch (e) {
      details.push(base + " -> " + (e && e.name === "AbortError" ? "timeout" : String(e)));
    } finally {
      clearTimeout(timer);
    }
  }
  const err = new Error("all sources failed");
  err.details = details;
  throw err;
}

function remember(key, body) {
  cache.set(key, { t: Date.now(), body });
  if (cache.size > MAX_KEYS) {
    const oldest = [...cache.entries()].sort((a, b) => a[1].t - b[1].t).slice(0, cache.size - MAX_KEYS);
    for (const [k] of oldest) cache.delete(k);
  }
}

function send(res, body, state) {
  res.setHeader("Content-Type", "application/json");
  res.setHeader("Cache-Control", state === "stale"
    ? "s-maxage=5, stale-while-revalidate=30"
    : "s-maxage=8, stale-while-revalidate=30");
  res.setHeader("X-Cache", state);
  res.status(200).send(body);
}

module.exports = async (req, res) => {
  const p = String(req.query.p || "");
  const ok = /^(mil|point\/-?\d+(\.\d+)?\/-?\d+(\.\d+)?\/\d+)$/.test(p);
  if (!ok) {
    res.status(400).json({ error: "bad request" });
    return;
  }

  const hit = cache.get(p);
  if (hit && Date.now() - hit.t < FRESH_MS) {
    send(res, hit.body, "hit");
    return;
  }

  try {
    let job = pending.get(p);
    if (!job) {
      job = fetchUpstream(p).then(body => { remember(p, body); return body; });
      pending.set(p, job);
      job.finally(() => pending.delete(p)).catch(() => {});
    }
    const body = await job;
    send(res, body, "miss");
  } catch (e) {
    if (hit && Date.now() - hit.t < STALE_MS) {
      send(res, hit.body, "stale");
      return;
    }
    res.setHeader("Cache-Control", "no-store");
    res.status(502).json({ error: "all sources failed", details: e.details || [String(e)] });
  }
};
