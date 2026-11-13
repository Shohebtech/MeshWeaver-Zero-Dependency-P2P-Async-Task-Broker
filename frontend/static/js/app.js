/* MeshWeaver dashboard: polling, tables, per-page rendering. Charts live in charts.js. */
(() => {
  const PAGE = document.body.dataset.page;
  const REFRESH = Number(document.body.dataset.refresh) || 5000;
  const INTERVAL = Number(document.body.dataset.interval) || 5;
  const $ = (s, r = document) => r.querySelector(s);
  let clockOffset = 0;                       // server time minus browser time, seconds

  const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const pct = (v) => (v == null ? "–" : `${v.toFixed(1)}%`);
  const clock = (ts) => (ts ? new Date(ts * 1000).toLocaleTimeString([], { hour12: false }) : "–");
  const age = (s) => (s == null ? "–" : s < 1 ? "just now" : s < 60 ? `${Math.round(s)}s ago` : `${Math.round(s / 60)}m ago`);
  const getJSON = async (url, opt) => { const r = await fetch(url, opt); if (!r.ok && r.status !== 503) throw new Error(`${url}: ${r.status}`); return r.json(); };

  const label = (r) => (r.status === "offline" ? "offline" : r.stale ? "stale" : "online");
  const chip = (s) => `<span class="chip ${esc(s)}">${esc(s)}</span>`;
  const badge = (src) => `<span class="badge ${esc(src)}">${esc(src === "real" ? "real p2p" : src)}</span>`;
  const meter = (v, cls) => `<div class="meter ${cls}"><div class="track"><div class="fill" style="width:${v == null ? 0 : v}%"></div></div><span class="num">${pct(v)}</span></div>`;
  const bigMeter = (name, v, cls) => `<div class="big-meter ${cls}"><div class="row"><span>${name}</span><b>${pct(v)}</b></div><div class="track"><div class="fill" style="width:${v || 0}%"></div></div></div>`;
  const events = (list, empty) => list.length
    ? list.map((e) => `<li><time>${clock(e.ts)}</time><span class="k">${esc(e.kind.replace("_", " "))}</span><span>${esc(e.detail)}</span></li>`).join("")
    : `<li class="empty">${esc(empty)}</li>`;
  const tile = (v, l, cls = "") => `<div class="tile ${cls}"><div class="v">${v}</div><div class="l">${l}</div></div>`;

  /* ---------- sortable, filterable tables ---------- */
  const tstate = {};
  function table(id, cols, rows, opts = {}) {
    const el = document.getElementById(id);
    if (!el) return;
    const st = (tstate[id] ||= { key: opts.sort || null, dir: 1, filter: "" });
    let data = rows.slice();
    if (st.filter) data = data.filter((r) => JSON.stringify(r).toLowerCase().includes(st.filter));
    const col = cols.find((c) => c.key === st.key);
    if (col) data.sort((a, b) => { const x = col.val ? col.val(a) : a[col.key], y = col.val ? col.val(b) : b[col.key]; return ((x ?? -1) > (y ?? -1) ? 1 : (x ?? -1) < (y ?? -1) ? -1 : 0) * st.dir; });
    if (!rows.length) { el.innerHTML = `<div class="empty">${esc(opts.empty || "Nothing to show yet.")}</div>`; return; }
    el.innerHTML = `<table><thead><tr>${cols.map((c) => `<th data-table="${id}" data-sort="${c.key}" aria-sort="${st.key === c.key ? (st.dir > 0 ? "ascending" : "descending") : "none"}">${c.label}${st.key === c.key ? `<span class="arrow"> ${st.dir > 0 ? "▲" : "▼"}</span>` : ""}</th>`).join("")}</tr></thead>
      <tbody>${data.map((r) => `<tr class="${(r.is_self ? "self " : "") + (r.status === "offline" ? "offline" : "")}">${cols.map((c) => `<td>${c.html(r)}</td>`).join("")}</tr>`).join("") || `<tr><td colspan="${cols.length}" class="empty">No match.</td></tr>`}</tbody></table>`;
  }
  let lastData = null, renderFn = null;
  document.addEventListener("click", (e) => {
    const th = e.target.closest("th[data-sort]");
    if (!th) return;
    const st = (tstate[th.dataset.table] ||= { key: null, dir: 1, filter: "" });
    st.dir = st.key === th.dataset.sort ? -st.dir : 1; st.key = th.dataset.sort;
    if (lastData && renderFn) renderFn(lastData);
  });
  document.addEventListener("input", (e) => {
    const f = e.target.closest("input[data-filter]");
    if (!f) return;
    (tstate[f.dataset.filter] ||= { key: null, dir: 1, filter: "" }).filter = f.value.trim().toLowerCase();
    if (lastData && renderFn) renderFn(lastData);
  });

  /* ---------- shared column sets ---------- */
  const nameCell = (r) => `<b>${esc(r.name)}</b>${r.is_self ? ' <span class="badge real">this node</span>' : ""}<div style="color:var(--muted);font-size:.78rem">${esc(r.node_id.slice(0, 16))}</div>`;
  const addr = (r) => (r.host && r.port ? `${esc(r.host)}:${r.port}` : "–");

  /* ---------- pages ---------- */
  const pages = {
    async overview() {
      const d = await getJSON("/api/overview"); sync(d.server_time);
      const g = d.gossip, t = d.tasks;
      $("#tiles").innerHTML = [
        tile(g.total_nodes, "Total nodes"), tile(g.online_nodes, "Online nodes"), tile(g.offline_nodes, "Offline nodes", g.offline_nodes ? "bad" : ""),
        tile(pct(g.avg_cpu), "Average CPU", "blue"), tile(pct(g.avg_ram), "Average RAM", "blue"),
        tile(t.active, 'Active tasks <span class="badge mock">mock</span>', "mock"), tile(t.completed, 'Completed tasks <span class="badge mock">mock</span>', "mock"),
        tile(t.failed, 'Failed tasks <span class="badge mock">mock</span>', "mock")].join("");
      const ll = g.least_loaded;
      $("#least").innerHTML = ll ? `<dl class="kv"><dt>Node</dt><dd><b>${esc(ll.name)}</b> ${badge(ll.source)}</dd><dt>Address</dt><dd>${addr(ll)}</dd></dl>${bigMeter("CPU", ll.cpu, "cpu")}${bigMeter("RAM", ll.ram, "ram")}`
        : '<div class="empty">No online node with fresh load data yet.</div>';
      $("#recent").innerHTML = events(d.recent_events, "No joins or failures yet.");
      const nodes = (await getJSON("/api/gossip/nodes")).nodes;
      $("#mini-load").innerHTML = nodes.map((n) => `<div class="mini-row"><span>${esc(n.name)}${n.status === "offline" ? ' <span class="chip offline">offline</span>' : ""}</span>${meter(n.cpu, "cpu")}</div>`).join("") || '<div class="empty">No nodes yet.</div>';
      $("#sources").innerHTML = `<p style="margin:0 0 8px">Nodes, CPU, RAM, heartbeats and offline detection come from the <b>Module 3 gossip engine</b>${g.simulated_nodes ? ` (${g.simulated_nodes} simulated demo node${g.simulated_nodes > 1 ? "s are" : " is"} included and labelled)` : ""}.
        ${g.real_nodes} node${g.real_nodes === 1 ? "" : "s"} reported over real UDP.</p><p style="margin:0">Task counts come from the <span class="badge mock">mock</span> Module 4 adapter and are simulated.</p>`;
    },

    async mesh() {
      const d = await getJSON("/api/mesh"); sync(d.server_time); lastData = d; renderFn = renderMesh; renderMesh(d);
    },

    async gossip() {
      const [sum, nodes, health, hist, ll] = await Promise.all([getJSON("/api/gossip/summary"), getJSON("/api/gossip/nodes"),
        getJSON("/api/gossip/health"), getJSON("/api/gossip/history?limit=60&kinds=gossip_sent,gossip_recv,peer_discovered,status_change"), getJSON("/api/gossip/least-loaded")]);
      sync(health.server_time);
      const d = { sum, nodes: nodes.nodes, health, hist, ll: ll.node }; lastData = d; renderFn = renderGossip; renderGossip(d);
      if (window.MWCharts) window.MWCharts.update(hist.series, nodes.nodes, health.server_time);
    },

    async tasks() {
      const d = await getJSON("/api/tasks"); sync(d.server_time); lastData = d; renderFn = renderTasks; renderTasks(d);
    },

    async network() {
      const d = await getJSON("/api/network"); sync(d.server_time); lastData = d; renderFn = renderNetwork; renderNetwork(d);
    },
  };

  function renderMesh(d) {
    const sCount = d.nodes.filter((n) => n.status === "online" && !n.stale).length;
    $("#topo-sub").textContent = `${d.nodes.length} nodes, ${d.edges.length} links`;
    drawTopology($("#topo"), d.nodes, d.edges);
    const di = d.discovery;
    $("#discovery").innerHTML = `<dl class="kv"><dt>Implementation</dt><dd>${esc(di.implementation)}</dd><dt>Seed peers</dt><dd>${(di.seeds || []).map(esc).join(", ") || "none"}</dd>
      <dt>Learned via gossip</dt><dd>${(di.learned || []).map(esc).join(", ") || "none"}</dd><dt>Transport</dt><dd>${esc(d.transport.implementation)}</dd></dl>
      <p style="color:var(--muted);margin:12px 0 0">${esc(di.note || "")} Peers listed above are real addresses learned from real UDP messages; the directory logic itself is a stand-in for the Kademlia DHT. ${sCount} of ${d.nodes.length} nodes currently report fresh data.</p>`;
    table("mesh-table", [
      { key: "name", label: "Node", html: nameCell },
      { key: "host", label: "IP : port", html: addr, val: (r) => `${r.host}:${r.port}` },
      { key: "status", label: "Status", html: (r) => chip(label(r)), val: label },
      { key: "source", label: "Data", html: (r) => badge(r.source) },
      { key: "links", label: "Peer connections", html: (r) => (r.links.length ? esc(r.links.map((l) => (d.nodes.find((n) => n.node_id === l) || { name: l.slice(0, 8) }).name).join(", ")) : "–"), val: (r) => r.links.length },
      { key: "heartbeat_age", label: "Last heard", html: (r) => age(r.heartbeat_age) },
    ], d.nodes, { sort: "name" });
  }

  function drawTopology(svg, nodes, edges) {
    const W = 560, H = 360, cx = W / 2, cy = H / 2, R = Math.min(W, H) / 2 - 56;
    const pos = {}; nodes.forEach((n, i) => { const a = (2 * Math.PI * i) / Math.max(nodes.length, 1) - Math.PI / 2; pos[n.node_id] = nodes.length === 1 ? [cx, cy] : [cx + R * 1.35 * Math.cos(a), cy + R * Math.sin(a)]; });
    const color = { online: "#0e9f7a", stale: "#b87503", offline: "#c93a3a" };
    svg.innerHTML = edges.map(([a, b]) => pos[a] && pos[b] ? `<line x1="${pos[a][0]}" y1="${pos[a][1]}" x2="${pos[b][0]}" y2="${pos[b][1]}" stroke="#b9c7d9" stroke-width="1.2"/>` : "").join("")
      + nodes.map((n) => { const [x, y] = pos[n.node_id], c = color[label(n)];
        return `<g><title>${esc(n.name)} · ${esc(label(n))} · CPU ${pct(n.cpu)} RAM ${pct(n.ram)}</title><circle cx="${x}" cy="${y}" r="${n.is_self ? 15 : 12}" fill="${c}" ${n.source === "simulated" ? 'stroke="#14213d" stroke-dasharray="3 2"' : n.is_self ? 'stroke="#14213d"' : 'stroke="#fff"'} stroke-width="2.5"/><text x="${x}" y="${y + 29}">${esc(n.name)}</text></g>`; }).join("");
    if (!nodes.length) svg.innerHTML = `<text x="${cx}" y="${cy}">No nodes yet</text>`;
  }

  function renderGossip(d) {
    const me = d.nodes.find((n) => n.is_self), s = d.sum, h = d.health;
    $("#self-sub").textContent = me ? `${me.name} · sampled ${age(me.update_age)}` : "";
    $("#self-panel").innerHTML = me ? `${bigMeter("CPU utilisation", me.cpu, "cpu")}${bigMeter("RAM utilisation", me.ram, "ram")}
      <dl class="kv"><dt>Monitor</dt><dd>${esc(h.monitor_backend)}</dd><dt>Last update</dt><dd>${clock(me.last_update)}</dd><dt>Heartbeat</dt><dd>${h.status === "running" ? "sending every " + h.gossip_interval + "s" : "stopped"}</dd></dl>` : '<div class="empty">Engine not ready.</div>';
    $("#cycle-kv").innerHTML = `<dt>Update interval</dt><dd>${h.gossip_interval}s</dd><dt>Last broadcast</dt><dd>${clock(h.last_tick)}</dd><dt>Neighbours / round</dt><dd>${h.fanout}</dd><dt>Stale after</dt><dd>${h.stale_after}s</dd><dt>Offline after</dt><dd>${h.offline_after}s</dd><dt>Signed gossip</dt><dd>${h.signed_messages ? "yes (HMAC)" : "no"}</dd>`;
    $("#least-panel").innerHTML = d.ll ? `<dl class="kv"><dt>Node</dt><dd><b>${esc(d.ll.name)}</b> ${badge(d.ll.source)}</dd><dt>Address</dt><dd>${addr(d.ll)}</dd><dt>Updated</dt><dd>${age(d.ll.update_age)}</dd></dl>${bigMeter("CPU", d.ll.cpu, "cpu")}`
      : '<div class="empty">No online node with fresh data.</div>';

    const off = d.nodes.filter((n) => n.status === "offline"), stale = d.nodes.filter((n) => n.status === "online" && n.stale);
    let w = "";
    if (off.length) w += `<div class="banner bad"><b>${off.length} node${off.length > 1 ? "s" : ""} offline:</b> ${off.map((n) => esc(n.name)).join(", ")}. No heartbeat for more than ${h.offline_after}s; they are excluded from task routing.</div>`;
    if (stale.length) w += `<div class="banner warn"><b>Stale load information:</b> ${stale.map((n) => esc(n.name)).join(", ")}. The node is alive but its CPU/RAM reading is older than ${h.stale_after}s, so it is not offered as least-loaded.</div>`;
    $("#warnings").innerHTML = w || `<div class="banner ok">All ${s.online_nodes} known node${s.online_nodes === 1 ? " is" : "s are"} online with fresh load data.</div>`;

    table("load-table", [
      { key: "name", label: "Node", html: nameCell },
      { key: "host", label: "Address", html: addr, val: (r) => `${r.host}:${r.port}` },
      { key: "cpu", label: "CPU", html: (r) => meter(r.cpu, "cpu") },
      { key: "ram", label: "RAM", html: (r) => meter(r.ram, "ram") },
      { key: "last_heartbeat", label: "Last heartbeat", html: (r) => `${clock(r.last_heartbeat)}<div style="color:var(--muted);font-size:.78rem">${age(r.heartbeat_age)}</div>` },
      { key: "last_update", label: "Last update", html: (r) => `${clock(r.last_update)}<div style="color:var(--muted);font-size:.78rem">${age(r.update_age)}</div>` },
      { key: "status", label: "Status", html: (r) => chip(label(r)) + (r.stale && r.status === "online" ? "" : ""), val: label },
      { key: "source", label: "Data", html: (r) => badge(r.source) },
    ], d.nodes, { sort: "cpu" });

    $("#hb-sub").textContent = `offline after ${h.offline_after}s`;
    table("hb-table", [
      { key: "name", label: "Node", html: (r) => esc(r.name) },
      { key: "heartbeat_age", label: "Since last heartbeat", html: (r) => meter(Math.min(100, ((r.heartbeat_age ?? h.offline_after) / h.offline_after) * 100), "cpu").replace(/\d+\.\d%/, age(r.heartbeat_age)), val: (r) => r.heartbeat_age },
      { key: "status", label: "State", html: (r) => chip(label(r)), val: label },
    ], d.nodes.filter((n) => !n.is_self), { sort: "heartbeat_age", empty: "No peers yet. Start another node with --peers." });
    $("#history").innerHTML = events(d.hist.events, "No gossip activity yet.");
  }

  function renderTasks(d) {
    const s = d.summary;
    $("#task-tiles").innerHTML = [tile(s.active, "Active", "mock"), tile(s.completed, "Completed", "mock"), tile(s.failed, "Failed", s.failed ? "bad" : "mock"), tile(s.rerouted, "Re-routed", "mock")].join("");
    table("task-table", [
      { key: "task_id", label: "Task ID", html: (t) => esc(t.task_id) },
      { key: "name", label: "Task name", html: (t) => esc(t.name) },
      { key: "node_name", label: "Assigned node", html: (t) => esc(t.node_name || "–") },
      { key: "status", label: "Status", html: (t) => chip(t.status) },
      { key: "result", label: "Result", html: (t) => esc(t.result || t.error || "–") },
      { key: "reroutes", label: "Re-routing", html: (t) => (t.reroutes.length ? t.reroutes.map((r) => `from ${esc(r.from)} at ${clock(r.at)} (${esc(r.reason)})`).join("<br>") : "–"), val: (t) => t.reroutes.length },
    ], d.tasks, { sort: "task_id", empty: "No tasks yet. Submit a demo task to see routing by gossip load data." });
  }

  function renderNetwork(d) {
    const h = d.health, on = d.nodes.filter((n) => n.status === "online"), peers = d.nodes.filter((n) => !n.is_self);
    const liveIds = new Set(on.map((n) => n.node_id));
    const links = new Set(on.flatMap((n) => n.links.filter((l) => liveIds.has(l)).map((l) => [n.node_id, l].sort().join("|"))));
    $("#net-tiles").innerHTML = [tile(on.length - 1 < 0 ? 0 : on.length - 1, "Active peers"), tile(links.size, "Peer connections", "blue"),
      tile(h.stats.heartbeat_recv || 0, "Heartbeats received"), tile(d.failures.length, "Failures in view", d.failures.length ? "bad" : "")].join("");
    table("health-table", [
      { key: "name", label: "Node", html: nameCell },
      { key: "status", label: "Health", html: (r) => chip(label(r)), val: label },
      { key: "heartbeat_age", label: "Last heartbeat", html: (r) => age(r.heartbeat_age), val: (r) => r.heartbeat_age },
      { key: "source", label: "Data", html: (r) => badge(r.source) },
    ], peers, { sort: "heartbeat_age", empty: "No peers yet." });
    $("#failures").innerHTML = events(d.failures, "No failures detected.");
    $("#hb-events").innerHTML = events(d.events.filter((e) => e.kind.startsWith("heartbeat")).slice(0, 60), "No heartbeats yet.");
    $("#comm-events").innerHTML = events(d.events.filter((e) => e.kind.startsWith("gossip") || e.kind === "peer_discovered").slice(0, 60), "No messages yet.");
    const t = h.transport, st = t.stats || {};
    $("#transport").innerHTML = `<dl class="kv"><dt>Implementation</dt><dd>${esc(t.implementation)}</dd><dt>Bound to</dt><dd>${esc(t.bind || "–")}</dd><dt>Datagrams sent / received</dt><dd>${st.sent ?? 0} / ${st.received ?? 0}</dd>
      <dt>Malformed / send failures</dt><dd>${st.malformed ?? 0} / ${st.send_failed ?? 0}</dd><dt>Rejected (bad signature)</dt><dd>${h.stats.rejected || 0}</dd></dl>`;
  }

  /* ---------- shared plumbing ---------- */
  function sync(serverTime) { if (serverTime) clockOffset = serverTime - Date.now() / 1000; }

  function ringLoop() {
    const ring = $("#ring"); if (!ring) return;
    const fg = $(".fg", ring), num = $("#ring-num"), C = 2 * Math.PI * 52;
    setInterval(() => {
      const next = lastData && lastData.health && lastData.health.next_tick_at;
      if (!next) return;
      const total = lastData.health.gossip_interval, now = Date.now() / 1000 + clockOffset;
      // the page only refetches every few seconds, so project the last known tick forward
      const upcoming = next < now ? next + Math.ceil((now - next) / total) * total : next;
      const left = upcoming - now, frac = Math.max(0, Math.min(1, left / total));
      fg.style.strokeDashoffset = String(C * (1 - frac));
      num.textContent = `${Math.max(1, Math.ceil(left))}s`;
      ring.classList.toggle("fire", total - left < 0.4);
    }, 200);
  }

  let busy = false;
  async function refresh() {
    if (busy) return; busy = true;
    const live = $("#live"), txt = $("#live-text");
    try { await pages[PAGE](); live.className = "live ok"; txt.textContent = `Live · updated ${new Date().toLocaleTimeString([], { hour12: false })} · refreshes every ${REFRESH / 1000}s`; }
    catch (e) { live.className = "live err"; txt.textContent = `Connection problem: ${e.message}`; }
    finally { busy = false; }
  }

  if (PAGE === "tasks") $("#submit-task").addEventListener("click", async () => { await getJSON("/api/tasks/demo", { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" }); refresh(); });
  ringLoop(); refresh(); setInterval(refresh, REFRESH);
})();
