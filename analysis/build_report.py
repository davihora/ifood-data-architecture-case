"""Generate a self-contained HTML report (Chart.js) with the case's Q1 & Q2 charts —
meant for screenshots into a slide deck.

Data source, in order of preference:
1. The live Gold marts via DuckDB (when run with the stack up, e.g. `make report`).
2. The validated pipeline output embedded below — the same numbers as `docs/RESULTS.md`
   and `make analyze`, so `python3 -m analysis.build_report` works anywhere with no stack.

Pure standard library + a Chart.js CDN (no matplotlib/plotly/pandas), so it runs offline-ish
(the CDN is only needed to *view* the page) and adds no project dependencies.
"""

from __future__ import annotations

import json
import os

# --- Validated pipeline output (Jan–May 2023) — matches docs/RESULTS.md / `make analyze`. ---
Q1_FALLBACK = [
    {"month": "2023-01", "trips": 3_040_951, "avg_total_amount": 27.45},
    {"month": "2023-02", "trips": 2_888_258, "avg_total_amount": 27.34},
    {"month": "2023-03", "trips": 3_372_941, "avg_total_amount": 28.27},
    {"month": "2023-04", "trips": 3_257_885, "avg_total_amount": 28.76},
    {"month": "2023-05", "trips": 3_481_304, "avg_total_amount": 29.46},
]
Q2_FALLBACK = [
    {"hour": 0, "trips": 88_573, "avg_passenger_count": 1.427},
    {"hour": 1, "trips": 57_517, "avg_passenger_count": 1.438},
    {"hour": 2, "trips": 37_012, "avg_passenger_count": 1.455},
    {"hour": 3, "trips": 24_078, "avg_passenger_count": 1.452},
    {"hour": 4, "trips": 15_727, "avg_passenger_count": 1.405},
    {"hour": 5, "trips": 18_188, "avg_passenger_count": 1.284},
    {"hour": 6, "trips": 45_434, "avg_passenger_count": 1.261},
    {"hour": 7, "trips": 91_706, "avg_passenger_count": 1.282},
    {"hour": 8, "trips": 125_386, "avg_passenger_count": 1.296},
    {"hour": 9, "trips": 140_803, "avg_passenger_count": 1.312},
    {"hour": 10, "trips": 153_495, "avg_passenger_count": 1.348},
    {"hour": 11, "trips": 167_242, "avg_passenger_count": 1.362},
    {"hour": 12, "trips": 180_349, "avg_passenger_count": 1.376},
    {"hour": 13, "trips": 184_485, "avg_passenger_count": 1.385},
    {"hour": 14, "trips": 200_597, "avg_passenger_count": 1.390},
    {"hour": 15, "trips": 204_904, "avg_passenger_count": 1.402},
    {"hour": 16, "trips": 205_028, "avg_passenger_count": 1.399},
    {"hour": 17, "trips": 223_995, "avg_passenger_count": 1.390},
    {"hour": 18, "trips": 238_019, "avg_passenger_count": 1.384},
    {"hour": 19, "trips": 213_717, "avg_passenger_count": 1.392},
    {"hour": 20, "trips": 189_943, "avg_passenger_count": 1.401},
    {"hour": 21, "trips": 194_147, "avg_passenger_count": 1.420},
    {"hour": 22, "trips": 179_509, "avg_passenger_count": 1.428},
    {"hour": 23, "trips": 140_030, "avg_passenger_count": 1.423},
]


def _from_gold() -> tuple[list, list] | None:
    """Best-effort: pull live numbers from the Gold marts via DuckDB. None on any failure."""
    try:
        from analysis.run_questions import _table, connect
    except Exception:
        return None
    try:
        con = connect()
        q1 = [
            {"month": m, "trips": int(t), "avg_total_amount": round(float(a), 2)}
            for (m, t, a) in con.sql(
                f"SELECT pickup_year_month, trips, avg_total_amount "
                f"FROM delta_scan('{_table('agg_monthly_total_amount')}') ORDER BY 1"
            ).fetchall()
        ]
        q2 = [
            {"hour": int(h), "trips": int(t), "avg_passenger_count": round(float(a), 3)}
            for (h, t, a) in con.sql(
                f"SELECT pickup_hour, trips, avg_passenger_count "
                f"FROM delta_scan('{_table('agg_may_passengers_by_hour')}') ORDER BY 1"
            ).fetchall()
        ]
        return (q1, q2) if q1 and q2 else None
    except Exception:
        return None


_TEMPLATE = """<!doctype html>
<html lang="pt-br">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>iFood — NYC Taxi · Resultados das Análises</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js"></script>
<style>
  :root { --ifood:#EA1D2C; --ink:#2b2b2b; --muted:#6b7280; --card:#ffffff; --bg:#f6f7f9; }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--bg); color:var(--ink);
         font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif; }
  header { background:var(--ifood); color:#fff; padding:28px 40px; }
  header h1 { margin:0; font-size:26px; letter-spacing:.2px; }
  header p { margin:6px 0 0; opacity:.92; font-size:14px; }
  main { max-width:1080px; margin:0 auto; padding:32px 24px 56px; }
  .card { background:var(--card); border-radius:16px; padding:26px 28px; margin:0 0 28px;
          box-shadow:0 1px 3px rgba(0,0,0,.08),0 8px 24px rgba(0,0,0,.04); }
  .qtag { display:inline-block; background:var(--ifood); color:#fff; font-weight:700;
          font-size:12px; padding:4px 10px; border-radius:999px; letter-spacing:.4px; }
  .card h2 { margin:12px 0 4px; font-size:20px; }
  .q { color:var(--muted); font-size:15px; margin:0 0 18px; line-height:1.5; }
  .chart-wrap { position:relative; height:340px; }
  .kpis { display:flex; gap:16px; flex-wrap:wrap; margin-top:18px; }
  .kpi { flex:1 1 150px; background:#fafafa; border:1px solid #eee; border-radius:12px; padding:14px 16px; }
  .kpi .v { font-size:22px; font-weight:700; color:var(--ifood); }
  .kpi .l { font-size:12px; color:var(--muted); margin-top:2px; }
  footer { color:var(--muted); font-size:12px; text-align:center; padding:0 24px 40px; }
  code { background:#f0f0f0; padding:1px 6px; border-radius:6px; }
</style>
</head>
<body>
<header>
  <h1>NYC Taxi Lakehouse — Resultados das Análises</h1>
  <p>iFood · Data Architecture Case · yellow taxi · Jan–Mai 2023 · fonte: camada Gold (Delta)</p>
</header>
<main>
  <section class="card">
    <span class="qtag">PERGUNTA 1</span>
    <h2>Média de <code>total_amount</code> por mês (yellow taxis)</h2>
    <p class="q">"Qual a média de valor total (total_amount) recebido em um mês considerando
       todos os yellow táxis da frota?"</p>
    <div class="chart-wrap"><canvas id="q1"></canvas></div>
    <div class="kpis" id="q1kpis"></div>
  </section>

  <section class="card">
    <span class="qtag">PERGUNTA 2</span>
    <h2>Média de <code>passenger_count</code> por hora do dia (maio)</h2>
    <p class="q">"Qual a média de passageiros (passenger_count) por cada hora do dia que pegaram
       táxi no mês de maio?" — filtrando <code>passenger_count &gt; 0</code>.</p>
    <div class="chart-wrap"><canvas id="q2"></canvas></div>
    <div class="kpis" id="q2kpis"></div>
  </section>
</main>
<footer>
  Números reproduzíveis via <code>make analyze</code> / <code>make eda</code> — idênticos a docs/RESULTS.md.
  Limpeza: corridas fora da janela, <code>total_amount &le; 0</code> e dropoff &lt; pickup são
  quarentenadas antes do Gold.
</footer>
<script>
const Q1 = __Q1__;
const Q2 = __Q2__;
const ifood = "#EA1D2C", gray = "rgba(120,120,130,.35)";
const brl = v => "$" + v.toFixed(2);
const fmt = n => n.toLocaleString("en-US");

// --- Q1: avg_total_amount (bars) + trips (line, secondary axis) ---
new Chart(document.getElementById("q1"), {
  data: {
    labels: Q1.map(d => d.month),
    datasets: [
      { type:"bar", label:"Média total_amount ($)", data:Q1.map(d=>d.avg_total_amount),
        backgroundColor:ifood, borderRadius:6, yAxisID:"y", order:2 },
      { type:"line", label:"Corridas (milhões)", data:Q1.map(d=>d.trips/1e6),
        borderColor:"#444", backgroundColor:"#444", tension:.3, yAxisID:"y1", order:1 }
    ]
  },
  options: { responsive:true, maintainAspectRatio:false,
    plugins:{ legend:{ position:"bottom" },
      tooltip:{ callbacks:{ label:c => c.datasetIndex===0
        ? " Média: "+brl(c.parsed.y)
        : " Corridas: "+fmt(Math.round(c.parsed.y*1e6)) } } },
    scales:{
      y:{ title:{display:true,text:"Média total_amount ($)"}, suggestedMin:26, suggestedMax:30 },
      y1:{ position:"right", title:{display:true,text:"Corridas (milhões)"},
           grid:{drawOnChartArea:false}, suggestedMin:0 } } }
});

// --- Q2: avg_passenger_count (line) + trips (faint bars, secondary axis) ---
new Chart(document.getElementById("q2"), {
  data: {
    labels: Q2.map(d => String(d.hour).padStart(2,"0")+"h"),
    datasets: [
      { type:"bar", label:"Corridas (volume)", data:Q2.map(d=>d.trips),
        backgroundColor:gray, yAxisID:"y1", order:2 },
      { type:"line", label:"Média passenger_count", data:Q2.map(d=>d.avg_passenger_count),
        borderColor:ifood, backgroundColor:ifood, tension:.35, pointRadius:3, yAxisID:"y", order:1 }
    ]
  },
  options: { responsive:true, maintainAspectRatio:false,
    plugins:{ legend:{ position:"bottom" } },
    scales:{
      y:{ title:{display:true,text:"Média passenger_count"}, suggestedMin:1.2, suggestedMax:1.5 },
      y1:{ position:"right", title:{display:true,text:"Corridas"}, grid:{drawOnChartArea:false} } } }
});

// --- KPI cards ---
const q1avg = (Q1.reduce((s,d)=>s+d.avg_total_amount,0)/Q1.length);
const q1hi = Q1.reduce((a,b)=>b.avg_total_amount>a.avg_total_amount?b:a);
document.getElementById("q1kpis").innerHTML = `
  <div class="kpi"><div class="v">${brl(q1avg)}</div><div class="l">média do período (Jan–Mai)</div></div>
  <div class="kpi"><div class="v">${brl(q1hi.avg_total_amount)}</div><div class="l">pico — ${q1hi.month}</div></div>
  <div class="kpi"><div class="v">${fmt(Q1.reduce((s,d)=>s+d.trips,0))}</div><div class="l">corridas totais</div></div>`;

const q2hi = Q2.reduce((a,b)=>b.avg_passenger_count>a.avg_passenger_count?b:a);
const q2lo = Q2.reduce((a,b)=>b.avg_passenger_count<a.avg_passenger_count?b:a);
const q2peak = Q2.reduce((a,b)=>b.trips>a.trips?b:a);
document.getElementById("q2kpis").innerHTML = `
  <div class="kpi"><div class="v">${q2hi.avg_passenger_count.toFixed(3)}</div><div class="l">máx — ${String(q2hi.hour).padStart(2,"0")}h (madrugada)</div></div>
  <div class="kpi"><div class="v">${q2lo.avg_passenger_count.toFixed(3)}</div><div class="l">mín — ${String(q2lo.hour).padStart(2,"0")}h (rush manhã)</div></div>
  <div class="kpi"><div class="v">${String(q2peak.hour).padStart(2,"0")}h</div><div class="l">hora de maior volume</div></div>`;
</script>
</body>
</html>
"""


def render_html(q1: list, q2: list) -> str:
    return _TEMPLATE.replace("__Q1__", json.dumps(q1)).replace("__Q2__", json.dumps(q2))


def main() -> int:
    live = _from_gold()
    q1, q2 = live if live else (Q1_FALLBACK, Q2_FALLBACK)
    source = "live Gold marts" if live else "embedded validated output"
    out = os.path.join(os.path.dirname(__file__), "report.html")
    with open(out, "w", encoding="utf-8") as f:
        f.write(render_html(q1, q2))
    print(f"[report] wrote {out}  (data source: {source})")
    print("[report] open it in a browser and screenshot the two cards into your slides.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
