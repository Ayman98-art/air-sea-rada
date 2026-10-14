module.exports = async (req, res) => {
  const p = String(req.query.p || "");
  const ok = /^(mil|point\/-?\d+(\.\d+)?\/-?\d+(\.\d+)?\/\d+)$/.test(p);
  if (!ok) {
    res.status(400).json({ error: "bad request" });
    return;
  }
  try {
    const r = await fetch("https://api.adsb.lol/v2/" + p);
    const body = await r.text();
    res.setHeader("Content-Type", "application/json");
    res.setHeader("Cache-Control", "s-maxage=5, stale-while-revalidate=10");
    res.status(r.status).send(body);
  } catch (e) {
    res.status(502).json({ error: String(e) });
  }
};
