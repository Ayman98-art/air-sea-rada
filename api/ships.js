// Ship positions from aisstream.io (AIS). The key stays on the server (AISSTREAM_KEY env var).
// aisstream is a live WebSocket stream, so each refresh listens for a few seconds inside the
// requested box and merges what it hears into a per-box memory of recently seen ships.

const STREAM_URL = "wss://stream.aisstream.io/v0/stream";
const LISTEN_MS = 6000;           // how long to listen per refresh
const FRESH_MS = 45000;           // reuse a box's result for 45s
const KEEP_MS = 20 * 60 * 1000;   // forget ships not heard from in 20 min
const MAX_BOXES = 60;
const MAX_FLEET = 30000;
const TRACK_POINTS = 20;

// One shared registry of every ship heard (across all boxes), so names, types and
// tracks learned in one area are kept when a visitor moves to the next.
const fleet = new Map();    // mmsi -> ship
const boxes = new Map();    // key -> { t } (last time this box was refreshed)
const pending = new Map();  // key -> Promise

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

function listen(box, ships) {
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
      const s = ships.get(mmsi) || { mmsi };
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
      ships.set(mmsi, s);
    };
  });
}

function parseBox(q) {
  const p = String(q || "").split(",").map(Number);
  if (p.length !== 4 || p.some(v => !Number.isFinite(v))) return null;
  let [s, w, n, e] = p;
  if (s > n) [s, n] = [n, s];
  if (w > e) [w, e] = [e, w];
  if (s < -90 || n > 90 || w < -180 || e > 180) return null;
  if ((n - s) * (e - w) > 900) return null;   // too large (≈ 30°×30°)
  return [s, w, n, e];
}

function pruneFleet() {
  const now = Date.now();
  for (const [k, s] of fleet) {
    if (now - (s.t || s.seen || 0) > KEEP_MS * 3) fleet.delete(k);
  }
  if (fleet.size > MAX_FLEET) {
    const old = [...fleet.entries()].sort((a, b) => (a[1].t || 0) - (b[1].t || 0));
    for (let i = 0; i < fleet.size - MAX_FLEET; i++) fleet.delete(old[i][0]);
  }
}

function snapshot(box) {
  const now = Date.now();
  const list = [];
  for (const s of fleet.values()) {
    if (!s.t || now - s.t > KEEP_MS) continue;            // no recent position
    if (s.lat < box[0] || s.lat > box[2] || s.lon < box[1] || s.lon > box[3]) continue;
    list.push(s);
  }
  return { now, ships: list };
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
  const key = box.join(",");
  let entry = boxes.get(key);
  if (!entry) {
    entry = { t: 0 };
    boxes.set(key, entry);
    if (boxes.size > MAX_BOXES) {
      const oldest = [...boxes.entries()].sort((a, b) => a[1].t - b[1].t)[0];
      if (oldest) boxes.delete(oldest[0]);
    }
  }

  let state = "hit";
  if (Date.now() - entry.t > FRESH_MS) {
    state = "miss";
    try {
      let job = pending.get(key);
      if (!job) {
        job = listen(box, fleet).then(n => { entry.t = Date.now(); pruneFleet(); return n; });
        pending.set(key, job);
        job.finally(() => pending.delete(key)).catch(() => {});
      }
      await job;
    } catch (e) {
      if (!snapshot(box).ships.length) {
        res.setHeader("Cache-Control", "no-store");
        res.status(502).json({ error: "ship stream failed", detail: String(e && e.message || e) });
        return;
      }
      state = "stale";
    }
  }

  res.setHeader("Content-Type", "application/json");
  // The CDN serves the last copy instantly for up to 10 minutes while it fetches a
  // fresh one in the background, so visitors almost never wait for the 6s listen.
  res.setHeader("Cache-Control", "s-maxage=30, stale-while-revalidate=600");
  res.setHeader("X-Cache", state);
  res.status(200).send(JSON.stringify(snapshot(box)));
};
