/* CPU and RAM line charts for the Gossip monitoring page (Chart.js 4). */
(() => {
  const PALETTE = ["#0e9f9a", "#2563eb", "#b87503", "#7c3aed", "#c93a3a", "#0891b2", "#65a30d", "#db2777", "#475569", "#ea580c"];
  const charts = {};
  const MAX_AGE = 600;               // seconds shown on the x axis

  function ensure(id, title) {
    if (charts[id]) return charts[id];
    const canvas = document.getElementById(id);
    if (!canvas || !window.Chart) return null;
    charts[id] = new Chart(canvas, {
      type: "line",
      data: { datasets: [] },
      options: {
        animation: false, responsive: true, maintainAspectRatio: false, parsing: false,
        interaction: { mode: "nearest", intersect: false },
        scales: {
          x: { type: "linear", min: -MAX_AGE, max: 0, ticks: { callback: (v) => (v === 0 ? "now" : `${Math.round(v / 60 * 10) / 10}m`), maxTicksLimit: 7 }, title: { display: true, text: "time" } },
          y: { min: 0, max: 100, ticks: { callback: (v) => `${v}%` }, title: { display: true, text: title } },
        },
        plugins: { legend: { position: "bottom", labels: { boxWidth: 12 } }, tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${c.parsed.y.toFixed(1)}%` } } },
      },
    });
    return charts[id];
  }

  function update(series, nodes, serverTime) {
    if (!window.Chart) return;            // Chart.js still loading: try again on the next refresh
    const order = nodes.map((n) => n.node_id);
    [["cpuChart", "cpu", "CPU"], ["ramChart", "ram", "RAM"]].forEach(([id, key, title]) => {
      const chart = ensure(id, title);
      if (!chart) return;
      chart.data.datasets = order.filter((nid) => series[nid]).map((nid, i) => {
        const n = nodes.find((x) => x.node_id === nid);
        const color = PALETTE[i % PALETTE.length];
        return {
          label: n.name + (n.source === "simulated" ? " (sim)" : ""), borderColor: color, backgroundColor: color,
          borderWidth: n.is_self ? 3 : 1.6, pointRadius: 0, tension: 0.25, borderDash: n.source === "simulated" ? [5, 4] : [],
          data: series[nid].map((p) => ({ x: p.t - serverTime, y: p[key] })).filter((p) => p.x >= -MAX_AGE),
        };
      });
      chart.update("none");
    });
  }

  function notice() {
    setTimeout(() => {
      if (window.Chart) return;
      document.querySelectorAll(".chart-box").forEach((b) => { b.innerHTML = '<div class="chart-note">Chart.js did not load. Check your internet connection, or place chart.umd.min.js in frontend/static/js/vendor/.</div>'; });
    }, 6000);
  }
  notice();
  window.MWCharts = { update };
})();
