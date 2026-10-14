const SOURCES = [
  "https://api.airplanes.live/v2/",
  "https://api.adsb.lol/v2/",
];

module.exports = async (req, res) => {
  const p = String(req.query.p || "");
  const ok = /^(mil|point\/-?\d+(\.\d+)?\/-?\d+(\.\d+)?\/\d+)$/.test(p);
  if (!ok) {
    res.status(400).json({ error: "bad request" });
    return;
  }
  const details = [];
  for (const base of SOURCES) {
    try {
      const r = await fetch(base + p, {
        headers: { "User-Agent": "air-sea-rada/1.0", "Accept": "application/json" },
      });
      const body = await r.text();
      if (r.ok) {
        res.setHeader("Content-Type", "application/json");
        res.setHeader("Cache-Control", "s-maxage=5, stale-while-revalidate=10");
        res.status(200).send(body);
        return;
      }
      details.push(base + " -> " + r.status + " " + body.slice(0, 120));
    } catch (e) {
      details.push(base + " -> " + String(e));
    }
  }
  res.status(502).json({ error: "all sources failed", details });
};
