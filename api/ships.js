// Ship positions from aisstream.io (AIS). The key stays on the server (AISSTREAM_KEY env var).
//
// aisstream is a live WebSocket stream. Instead of listening separately for every area a
// visitor opens (slow: ~6s each), the server listens to the WHOLE WORLD in one shared
// session, keeps every ship it hears in memory, and answers any area instantly from that
// memory. A fresh listen runs at most every 30s and happens in the background whenever
// there is already data to show, so visitors never wait for it.

const STREAM_URL = "wss://stream.aisstream.io/v0/stream";
const WORLD = [-90, -180, 90, 180];
const LISTEN_MS = 7000;           // length of one world listen
const FRESH_MS = 30000;           // start a new listen when the last is older than this
const KEEP_MS = 30 * 60 * 1000;   // show ships heard in the last 30 min
const MAX_FLEET = 60000;
const MAX_RETURN = 1500;          // cap per response so phones stay smooth
const TRACK_POINTS = 20;

const fleet = new Map();          // mmsi -> ship (shared across all requests on this instance)
let lastListen = 0;
let listening = null;             // Promise while a listen is running
let lastError = "";

function getWS() {
  if (typeof WebSocket !== "undefined") return WebSocket;
  return require("ws");
}

function toText(data) {
  if (typeof data === "string") return data;
  if (data instanceof ArrayBuffer) return new TextDecoder().decode(data);
  if (ArrayBuffer.isView(data)) return new TextDecoder().decode(data);
  return String(data);
}

function num(v) { return Number.isFinite(v) ? v : null; }

function listen(box) {
  return new Promise((resolve, reject) => {
    const WS = getWS();
    const ws = new WS(STREAM_URL);
    try { ws.binaryType = "arraybuffer"; } catch (e) {}
    let got = 0, opened = false, done = false;
    const finish = (err) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      try { ws.close(); } catch (e) {}
      if (err && !got) reject(err); else resolve(got);
    };
    const timer = setTimeout(() => finish(opened ? null : new Error("connect timeout")), LISTEN_MS);

    ws.onopen = () => {
      opened = true;
      ws.send(JSON.stringify({
        APIKey: process.env.AISSTREAM_KEY,
        BoundingBoxes: [[[box[0], box[1]], [box[2], box[3]]]],
        FilterMessageTypes: ["PositionReport", "StandardClassBPositionReport", "ShipStaticData"],
      }));
    };
    ws.onerror = (e) => finish(new Error("stream error" + (e && e.message ? ": " + e.message : "")));
    ws.onclose = () => finish(opened ? null : new Error("stream closed"));
    ws.onmessage = (ev) => {
      let m;
      try { m = JSON.parse(toText(ev.data)); } catch (e) { return; }
      if (m.error) { finish(new Error(String(m.error))); return; }
      const meta = m.MetaData || {};
      const mmsi = meta.MMSI;
      if (!mmsi) return;
      const s = fleet.get(mmsi) || { mmsi };
      s.seen = Date.now();
      const name = String(meta.ShipName || "").trim();
      if (name) s.name = name;
      const msg = m.Message || {};
      const pos = msg.PositionReport || msg.StandardClassBPositionReport;
      if (pos) {
        const lat = num(pos.Latitude), lon = num(pos.Longitude);
        if (lat == null || lon == null || Math.abs(lat) > 90 || Math.abs(lon) > 180) return;
        s.lat = lat; s.lon = lon;
        const tr = s.tr || (s.tr = []);
        const p = [Math.round(lat * 1e5) / 1e5, Math.round(lon * 1e5) / 1e5];
        const last = tr[tr.length - 1];
        if (!last || Math.abs(last[0] - p[0]) + Math.abs(last[1] - p[1]) > 0.0006) {
          tr.push(p);
          if (tr.length > TRACK_POINTS) tr.shift();
        }
        s.sog = pos.Sog != null && pos.Sog < 102.3 ? pos.Sog : null;      // knots
        s.cog = pos.Cog != null && pos.Cog < 360 ? pos.Cog : null;
        s.hdg = pos.TrueHeading != null && pos.TrueHeading < 360 ? pos.TrueHeading : null;
        if (pos.NavigationalStatus != null) s.nav = pos.NavigationalStatus;
        s.t = Date.now();
        got++;
      }
      const st = msg.ShipStaticData;
      if (st) {
        if (st.Type != null) s.type = st.Type;
        const dest = String(st.Destination || "").trim();
        if (dest) s.dest = dest;
        const cs = String(st.CallSign || "").trim();
        if (cs) s.cs = cs;
        if (st.Dimension) {
          const d = st.Dimension;
          const len = (d.A || 0) + (d.B || 0);
          if (len > 0) s.len = len;
        }
      }
      fleet.set(mmsi, s);
    };
  });
}

function pruneFleet() {
  const now = Date.now();
  for (const [k, s] of fleet) {
    if (now - (s.t || s.seen || 0) > KEEP_MS * 2) fleet.delete(k);
  }
  if (fleet.size > MAX_FLEET) {
    const old = [...fleet.entries()].sort((a, b) => (a[1].t || 0) - (b[1].t || 0));
    for (let i = 0; i < fleet.size - MAX_FLEET; i++) fleet.delete(old[i][0]);
  }
}

// Start a world listen unless one is already running. Returns its promise.
function startListen() {
  if (!listening) {
    listening = listen(WORLD)
      .then(n => { lastListen = Date.now(); lastError = ""; pruneFleet(); return n; })
      .catch(e => { lastError = String(e && e.message || e); throw e; })
      .finally(() => { listening = null; });
    listening.catch(() => {});
  }
  return listening;
}

// Keep the function alive for background work after the response when the platform allows it.
function inBackground(promise) {
  try {
    const ctx = globalThis[Symbol.for("@vercel/request-context")];
    const wu = ctx && ctx.get && ctx.get().waitUntil;
    if (typeof wu === "function") wu(promise.catch(() => {}));
  } catch (e) {}
}

function parseBox(q) {
  const p = String(q || "").split(",").map(Number);
  if (p.length !== 4 || p.some(v => !Number.isFinite(v))) return null;
  let [s, w, n, e] = p;
  if (s > n) [s, n] = [n, s];
  if (w > e) [w, e] = [e, w];
  if (s < -90 || n > 90 || w < -180 || e > 180) return null;
  return [s, w, n, e];
}

function snapshot(box) {
  const now = Date.now();
  let list = [];
  for (const s of fleet.values()) {
    if (!s.t || now - s.t > KEEP_MS) continue;
    if (s.lat < box[0] || s.lat > box[2] || s.lon < box[1] || s.lon > box[3]) continue;
    list.push(s);
  }
  const total = list.length;
  if (list.length > MAX_RETURN) {
    // Too many for one screen: prefer ships we know most about, then the most recent.
    const score = s => (s.type != null ? 2 : 0) + (s.name ? 1 : 0);
    list.sort((a, b) => score(b) - score(a) || b.t - a.t);
    list = list.slice(0, MAX_RETURN).map(s => { const o = Object.assign({}, s); delete o.tr; return o; });
  }
  return { now, total, ships: list };
}

module.exports = async (req, res) => {
  if (!process.env.AISSTREAM_KEY) {
    res.status(500).json({ error: "AISSTREAM_KEY is not set" });
    return;
  }
  const box = parseBox(req.query.bbox);
  if (!box) {
    res.status(400).json({ error: "bad bbox" });
    return;
  }

  let state = "hit";
  const stale = Date.now() - lastListen > FRESH_MS;
  if (!lastListen) {
    // Cold start: nothing in memory yet, so wait for the first listen.
    state = "miss";
    try { await startListen(); }
    catch (e) {
      res.setHeader("Cache-Control", "no-store");
      res.status(502).json({ error: "ship stream failed", detail: lastError });
      return;
    }
  } else if (stale) {
    state = "refreshing";
    inBackground(startListen());   // answer now from memory, refresh behind the scenes
  }

  // The CDN also serves the last copy instantly while it revalidates in the background.
  res.setHeader("Content-Type", "application/json");
  res.setHeader("Cache-Control", "s-maxage=20, stale-while-revalidate=600");
  res.setHeader("X-Cache", state);
  res.status(200).send(JSON.stringify(snapshot(box)));
};
