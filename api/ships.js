// Ship positions from aisstream.io (AIS). The key stays on the server (AISSTREAM_KEY env var).
// aisstream is a live WebSocket stream, so each refresh listens for a few seconds inside the
// requested box and merges what it hears into a per-box memory of recently seen ships.

const STREAM_URL = "wss://stream.aisstream.io/v0/stream";
const LISTEN_MS = 6000;           // how long to listen per refresh
const FRESH_MS = 45000;           // reuse a box's result for 45s
const KEEP_MS = 20 * 60 * 1000;   // forget ships not heard from in 20 min
const MAX_BOXES = 60;

const boxes = new Map();    // key -> { t, ships: Map<mmsi, ship> }
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
      const name = String(meta.ShipName || "").trim();
      if (name) s.name = name;
      const msg = m.Message || {};
      const pos = msg.PositionReport || msg.StandardClassBPositionReport;
      if (pos) {
        const lat = num(pos.Latitude), lon = num(pos.Longitude);
        if (lat == null || lon == null || Math.abs(lat) > 90 || Math.abs(lon) > 180) return;
        s.lat = lat; s.lon = lon;
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

function snapshot(entry, box) {
  const now = Date.now();
  const list = [];
  for (const [k, s] of entry.ships) {
    if (!s.t) continue;                                   // static data only, no position yet
    if (now - s.t > KEEP_MS) { entry.ships.delete(k); continue; }
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
    entry = { t: 0, ships: new Map() };
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
        job = listen(box, entry.ships).then(n => { entry.t = Date.now(); return n; });
        pending.set(key, job);
        job.finally(() => pending.delete(key)).catch(() => {});
      }
      await job;
    } catch (e) {
      if (!entry.ships.size) {
        res.setHeader("Cache-Control", "no-store");
        res.status(502).json({ error: "ship stream failed", detail: String(e && e.message || e) });
        return;
      }
      state = "stale";
    }
  }

  res.setHeader("Content-Type", "application/json");
  res.setHeader("Cache-Control", "s-maxage=30, stale-while-revalidate=60");
  res.setHeader("X-Cache", state);
  res.status(200).send(JSON.stringify(snapshot(entry, box)));
};
