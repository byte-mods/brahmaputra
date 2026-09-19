//! The dashboard, compiled into the binary (DESIGN.md §9.3).
//!
//! One file, no build step, no Node toolchain, no CDN: vanilla JS with
//! interactive SVG charts. That is a deliberate trade — a framework would
//! buy nicer code and cost an entire toolchain in the release path, and an
//! operations page that ships inside the broker has to work in an air-gapped
//! network with no package registry in reach.

pub const INDEX_HTML: &str = r##"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Brahmaputra</title>
<style>
  :root {
    color-scheme: light dark;
    --bg: #0f1115; --panel: #171a21; --line: #262b36; --text: #e6e9ef;
    --muted: #8b93a7; --accent: #4da3ff; --good: #3fb950; --warn: #d29922; --bad: #f85149;
  }
  @media (prefers-color-scheme: light) {
    :root { --bg:#f6f7f9; --panel:#fff; --line:#e3e6ec; --text:#1b1f27; --muted:#5b6478; }
  }
  * { box-sizing: border-box; }
  body { margin:0; background:var(--bg); color:var(--text);
         font:14px/1.5 ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif; }
  header { display:flex; align-items:center; gap:16px; padding:14px 20px;
           border-bottom:1px solid var(--line); background:var(--panel); }
  header h1 { font-size:16px; margin:0; font-weight:650; letter-spacing:.2px; }
  header .sp { flex:1; }
  .muted { color:var(--muted); }
  main { padding:20px; max-width:1200px; margin:0 auto; }
  .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(190px,1fr)); gap:12px; }
  .card { background:var(--panel); border:1px solid var(--line); border-radius:10px; padding:14px 16px; }
  .card h3 { margin:0 0 6px; font-size:12px; font-weight:600; color:var(--muted);
             text-transform:uppercase; letter-spacing:.6px; }
  .card .v { font-size:24px; font-weight:600; font-variant-numeric:tabular-nums; }
  section { margin-top:24px; }
  section h2 { font-size:13px; text-transform:uppercase; letter-spacing:.6px;
               color:var(--muted); margin:0 0 10px; }
  table { width:100%; border-collapse:collapse; background:var(--panel);
          border:1px solid var(--line); border-radius:10px; overflow:hidden; }
  th, td { text-align:left; padding:9px 12px; border-bottom:1px solid var(--line);
           font-variant-numeric:tabular-nums; }
  th { font-size:12px; color:var(--muted); font-weight:600; }
  tr:last-child td { border-bottom:none; }
  .pill { display:inline-block; padding:1px 8px; border-radius:999px; font-size:12px; }
  .ok { background:rgba(63,185,80,.15); color:var(--good); }
  .no { background:rgba(248,81,73,.15); color:var(--bad); }
  .warn { background:rgba(210,153,34,.15); color:var(--warn); }
  button, select, input { font:inherit; color:inherit; background:var(--panel);
           border:1px solid var(--line); border-radius:8px; padding:7px 11px; }
  button { cursor:pointer; }
  button.primary { background:var(--accent); border-color:var(--accent); color:#001; font-weight:600; }
  button.danger { border-color:var(--bad); color:var(--bad); }
  .toolbar { display:flex; flex-wrap:wrap; gap:8px; align-items:center; margin-bottom:10px; }
  .toolbar input { min-width:160px; }
  .bad { color:var(--bad); }
  tr.pick { cursor:pointer; }
  tr.pick:hover td { background:rgba(77,163,255,.07); }
  tr.on td { background:rgba(77,163,255,.12); }
  .detail { margin-top:10px; }
  .detail h4 { margin:0 0 8px; font-size:12px; font-weight:600; color:var(--muted);
               text-transform:uppercase; letter-spacing:.6px; }
  .detail table { margin-bottom:12px; }
  .detail table:last-child { margin-bottom:0; }
  .kv td:first-child { color:var(--muted); width:280px; }
  #messages td { vertical-align:top; word-break:break-word; }
  #messages td:nth-child(5) { max-width:640px; font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:12px; }
  #login { max-width:340px; margin:14vh auto; }
  #login .card { display:grid; gap:10px; }
  #err { color:var(--bad); min-height:1.2em; }
  svg { width:100%; height:120px; display:block; }
  .analytics { display:grid; grid-template-columns:repeat(auto-fit,minmax(min(100%,320px),1fr)); gap:12px; }
  progress { width:100%; height:14px; accent-color:var(--accent); }
  .chart-summary { display:flex; justify-content:space-between; gap:12px; font-variant-numeric:tabular-nums; }
  .chart-summary strong { font-size:22px; }
  #feedstatus { margin:12px 0; }
  @media (max-width:640px) { main { padding:10px; } header { flex-wrap:wrap; } section { overflow-x:auto; } }
</style>
</head>
<body>

<div id="login">
  <div class="card">
    <h3>Brahmaputra</h3>
    <input id="u" placeholder="username" autocomplete="username">
    <input id="p" type="password" placeholder="password" autocomplete="current-password">
    <button class="primary" onclick="login()">Sign in</button>
    <div id="err"></div>
  </div>
</div>

<div id="app" hidden>
  <header>
    <h1>Brahmaputra</h1>
    <span class="muted" id="who"></span>
    <span class="sp"></span>
    <span class="muted" id="clock"></span>
    <button onclick="logout()">Sign out</button>
  </header>
  <main>
    <div class="grid" id="tiles"></div>
    <div id="feedstatus" role="status" aria-live="polite" class="muted">Connecting to live metrics...</div>
    <section>
      <div class="toolbar">
        <h2 style="margin:0;flex:1">Live analytics</h2>
        <label>Window <select id="window" onchange="drawChart()"><option value="300">5 minutes</option><option value="1800">30 minutes</option><option value="21600">6 hours</option></select></label>
        <button id="pausebtn" onclick="toggleAnalytics()">Pause updates</button>
        <button onclick="exportAnalytics()">Export samples</button>
      </div>
      <p class="muted">Rates are sampled every 5 seconds on this broker. Health and consumer lag cover the cluster.</p>
      <div class="analytics" id="analytics"></div>
      <div class="analytics" id="health" style="margin-top:12px"></div>
    </section>

    <section>
      <h2>Throughput</h2>
      <div class="card">
        <select id="metric" onchange="drawChart()"></select>
        <svg id="chart" viewBox="0 0 600 120" preserveAspectRatio="none"></svg>
        <div class="muted" id="chartinfo"></div>
      </div>
    </section>

    <section>
      <h2>Brokers</h2><table id="brokers"></table>
      <div id="brokerdetail"></div>
    </section>
    <section>
      <h2>Topics</h2><table id="topics"></table>
      <div id="topicdetail"></div>
    </section>
    <section>
      <h2>Consumer groups</h2><table id="groups"></table>
      <div id="groupdetail"></div>
    </section>
    <section>
      <h2>Messages</h2>
      <div class="card">
        <div class="toolbar">
          <select id="mtopic" onchange="topicChanged()"></select>
          <select id="mpart"><option value="">all partitions</option></select>
          <input id="msearch" placeholder="filter key or value" oninput="debouncedMessages()">
          <select id="morder">
            <option value="desc">newest first</option>
            <option value="asc">oldest first</option>
          </select>
          <select id="mlimit">
            <option>50</option><option selected>100</option><option>250</option><option>500</option>
          </select>
          <button onclick="loadMessages()">Refresh</button>
          <button id="livebtn" onclick="toggleLive()">Go live</button>
          <span class="muted" id="mcount"></span>
        </div>
        <table id="messages"></table>
      </div>
    </section>

    <section id="adminpanel">
      <h2>Topic administration</h2>
      <div class="card">
        <div class="toolbar">
          <span class="muted">Partitions</span>
          <input id="partcount" type="number" min="1" style="width:90px" placeholder="total">
          <button onclick="addPartitions()">Increase</button>
          <span class="muted">·</span>
          <input id="cfgkey" placeholder="config key e.g. retention.ms" style="width:220px">
          <input id="cfgval" placeholder="value" style="width:140px">
          <button onclick="setConfig()">Apply</button>
          <span class="muted">·</span>
          <button class="danger" onclick="removeTopic()">Delete topic</button>
        </div>
        <div id="adminmsg" class="muted"></div>
      </div>
    </section>
  </main>
</div>

<script>
let token = sessionStorage.getItem("token") || "";
let role = sessionStorage.getItem("role") || "";
let user = sessionStorage.getItem("user") || "";
let refreshTimer = null, chartTimer = null, refreshing = false, charting = false;
let paused = false, lastRefresh = null, chartSamples = {};
let sessionGeneration = 0;

async function api(path, options) {
  const response = await fetch(path, Object.assign({
    signal: AbortSignal.timeout(10000),
    headers: { "authorization": "Bearer " + token, "content-type": "application/json" }
  }, options || {}));
  if (response.status === 401) { logout(); throw new Error("session expired"); }
  if (!response.ok) throw new Error((await response.json()).error || response.statusText);
  return response.json();
}

async function login() {
  const err = document.getElementById("err");
  err.textContent = "";
  try {
    const response = await fetch("/api/v1/auth/login", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        username: document.getElementById("u").value,
        password: document.getElementById("p").value
      })
    });
    const body = await response.json();
    if (!response.ok) throw new Error(body.error || "login failed");
    token = body.token; role = body.role; user = body.username;
    sessionStorage.setItem("token", token);
    sessionStorage.setItem("role", role);
    sessionStorage.setItem("user", user);
    start();
  } catch (e) { err.textContent = e.message; }
}

function logout() {
  sessionGeneration++;
  clearTimeout(refreshTimer); clearTimeout(chartTimer);
  if (liveSource) { liveSource.close(); liveSource = null; }
  token = ""; sessionStorage.clear();
  document.getElementById("app").hidden = true;
  document.getElementById("login").hidden = false;
}

const fmt = n => n === undefined || n === null ? "—"
  : n >= 1e9 ? (n/1e9).toFixed(1)+"G" : n >= 1e6 ? (n/1e6).toFixed(1)+"M"
  : n >= 1e3 ? (n/1e3).toFixed(1)+"k" : String(Math.round(n));

// Disk figures arrive as bytes and as -1 where the platform could not be
// asked. That is not zero and must not render as "0 B": a full disk and an
// unanswerable question are different operational situations.
const fmtBytes = n => {
  if (n === undefined || n === null || n < 0) return "\u2014";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let value = n, unit = 0;
  while (value >= 1024 && unit < units.length - 1) { value /= 1024; unit++; }
  return (unit === 0 ? value : value.toFixed(1)) + " " + units[unit];
};

function tile(label, value, cls) {
  return `<div class="card"><h3>${label}</h3><div class="v ${cls||""}">${value}</div></div>`;
}

// What is expanded right now. Kept out of the tables themselves so the
// three-second refresh can redraw them without collapsing a panel the
// operator is reading.
let picked = { broker: null, topic: null, group: null };

// A string argument inside an onclick attribute has to survive two
// parsers: JSON.stringify quotes it for JavaScript, escapeHtml then makes
// those quotes safe inside the attribute.
const arg = value => escapeHtml(JSON.stringify(value));

function pick(kind, id) {
  picked[kind] = picked[kind] === id ? null : id;
  renderDetail(kind);
  refresh();
}

function renderDetail(kind) {
  if (kind === "broker") return renderBrokerDetail();
  if (kind === "topic") return renderTopicDetail();
  if (kind === "group") return renderGroupDetail();
}

function detailBox(id, html) {
  document.getElementById(id).innerHTML = html ? `<div class="card detail">${html}</div>` : "";
}

// ---- broker detail: configuration and disk ----------------------------

async function renderBrokerDetail() {
  const id = picked.broker;
  if (id === null) { detailBox("brokerdetail", ""); return; }
  try {
    const [cfg, usage] = await Promise.all([
      api("/api/v1/brokers/config"),
      api("/api/v1/logdirs")
    ]);
    const entry = (cfg.brokers || []).find(b => b.broker_id === id);
    const dirs = (usage.dirs || []).filter(d => d.broker_id === id);

    let html = `<h4>Broker ${id} \u00b7 configuration</h4>`;
    if (!entry) {
      html += '<div class="muted">not in the cluster metadata</div>';
    } else if (entry.error) {
      html += `<div class="bad">${escapeHtml(entry.error)}</div>`;
    } else {
      html += '<table class="kv"><tr><th>Setting</th><th>Value</th><th>Source</th></tr>' +
        entry.configs.map(c => `<tr><td>${escapeHtml(c.name)}</td>
          <td>${escapeHtml(c.value)}</td>
          <td><span class="pill ${c.is_default ? "" : "ok"}">${c.is_default ? "default" : "set"}</span></td></tr>`).join("") +
        "</table>";
      // Stated in the UI because the absence of an edit control is a
      // design fact, not a missing feature: AlterConfigs takes topic
      // resources only, and these values come from the broker's flags.
      html += '<div class="muted">Read-only: broker settings come from the flags the node was ' +
        'started with. Only topic configuration can be altered at runtime.</div>';
    }

    html += "<h4>Data directories</h4>";
    html += dirs.length
      ? '<table><tr><th>Directory</th><th>Status</th><th>Logs</th><th>Free</th><th>Capacity</th></tr>' +
        dirs.map(d => `<tr><td>${escapeHtml(d.log_dir)}</td>
          <td><span class="pill ${d.online ? "ok" : "no"}">${d.online ? "online" : escapeHtml(d.offline_reason || "offline")}</span></td>
          <td>${fmtBytes(d.size_bytes)}</td>
          <td>${fmtBytes(d.usable_bytes)}</td>
          <td>${fmtBytes(d.total_bytes)}${d.total_bytes > 0 && d.usable_bytes >= 0 ? `<progress max="100" value="${Math.max(0, Math.min(100, (1-d.usable_bytes/d.total_bytes)*100))}" aria-label="Filesystem space used"></progress>` : ""}</td></tr>`).join("") +
        "</table>"
      : '<div class="muted">no directories reported</div>';
    detailBox("brokerdetail", html);
  } catch (error) {
    detailBox("brokerdetail", `<div class="bad">${escapeHtml(String(error.message || error))}</div>`);
  }
}

// ---- topic detail: partitions and configuration -----------------------

async function renderTopicDetail() {
  const name = picked.topic;
  if (!name) { detailBox("topicdetail", ""); return; }
  try {
    const [detail, usage] = await Promise.all([
      api(`/api/v1/topics/${encodeURIComponent(name)}`),
      api(`/api/v1/logdirs?topic=${encodeURIComponent(name)}`)
    ]);
    // Size is per broker per partition; the leader's copy is the one an
    // operator means by "how big is this partition".
    const size = new Map();
    for (const dir of usage.dirs || []) {
      for (const p of dir.partitions || []) {
        if (p.topic === name && p.is_leader) size.set(p.partition, p.size_bytes);
      }
    }

    let html = `<h4>${escapeHtml(name)} \u00b7 partitions</h4>`;
    html += '<table><tr><th>Partition</th><th>Leader</th><th>Replicas</th><th>ISR</th>' +
      '<th>Epoch</th><th>Start</th><th>End</th><th>High watermark</th><th>Size</th></tr>' +
      detail.partitions.map(p => `<tr>
        <td>${p.partition}</td><td>${p.leader}</td>
        <td class="muted">${(p.replicas || []).join(", ")}</td>
        <td><span class="pill ${p.under_replicated ? "no" : "ok"}">${(p.isr || []).join(", ")}</span></td>
        <td class="muted">${p.leader_epoch}</td>
        <td>${offset(p.log_start_offset)}</td>
        <td>${offset(p.log_end_offset)}</td>
        <td>${offset(p.high_watermark)}</td>
        <td>${fmtBytes(size.has(p.partition) ? size.get(p.partition) : -1)}</td>
      </tr>`).join("") + "</table>";

    const configs = Object.entries(detail.configs || {});
    html += "<h4>Configuration in force</h4>";
    html += configs.length
      ? '<table class="kv"><tr><th>Setting</th><th>Value</th></tr>' +
        configs.map(([k, v]) => `<tr><td>${escapeHtml(k)}</td><td>${escapeHtml(String(v))}</td></tr>`).join("") +
        "</table>"
      : '<div class="muted">nothing set on the topic; every value is the broker default</div>';
    detailBox("topicdetail", html);
  } catch (error) {
    detailBox("topicdetail", `<div class="bad">${escapeHtml(String(error.message || error))}</div>`);
  }
}

// A gauge that was never sampled reads -1, which is not offset -1.
const offset = n => (n === undefined || n === null || n < 0) ? "\u2014" : fmt(n);

// ---- consumer group detail: per-partition lag -------------------------

async function renderGroupDetail() {
  const group = picked.group;
  if (!group) { detailBox("groupdetail", ""); return; }
  try {
    const lag = await api(`/api/v1/groups/${encodeURIComponent(group)}/lag`);
    const rows = lag.partitions || [];
    const total = rows.reduce((sum, p) => sum + (p.lag > 0 ? p.lag : 0), 0);
    let html = `<h4>${escapeHtml(group)} \u00b7 lag (${fmt(total)} records behind)</h4>`;
    html += rows.length
      ? '<table><tr><th>Topic</th><th>Partition</th><th>Committed</th><th>Log end</th><th>Lag</th></tr>' +
        rows.map(p => `<tr><td>${escapeHtml(p.topic)}</td><td>${p.partition}</td>
          <td>${offset(p.committed_offset)}</td>
          <td>${offset(p.log_end_offset)}</td>
          <td><span class="pill ${p.lag < 0 ? "" : p.lag > 0 ? "warn" : "ok"}">${p.lag < 0 ? "\u2014" : fmt(p.lag)}</span></td>
        </tr>`).join("") + "</table>"
      : '<div class="muted">the group has committed no offsets yet</div>';
    detailBox("groupdetail", html);
  } catch (error) {
    detailBox("groupdetail", `<div class="bad">${escapeHtml(String(error.message || error))}</div>`);
  }
}

async function refresh() {
  if (refreshing || !token || paused) return;
  refreshing = true;
  try {
    const o = await api("/api/v1/overview");
    document.getElementById("tiles").innerHTML =
      tile("Brokers", `${o.brokers_alive}/${o.brokers}`, o.brokers_alive < o.brokers ? "no" : "") +
      tile("Topics", o.topics) +
      tile("Partitions", o.partitions) +
      tile("Under-replicated", o.under_replicated_partitions,
           o.under_replicated_partitions > 0 ? "no" : "ok") +
      tile("Offline", o.offline_partitions, o.offline_partitions > 0 ? "no" : "ok") +
      tile("Records in", fmt(o.produce_records_total)) +
      tile("Bytes in", fmt(o.produce_bytes_total)) +
      tile("Bytes out", fmt(o.fetch_bytes_total));

    const b = await api("/api/v1/brokers");
    // Disk is a per-broker fact and needs its own sweep; a failure here
    // must not blank the broker list, so it degrades to an empty map.
    const usage = await api("/api/v1/logdirs").catch(() => ({ dirs: [] }));
    const byBroker = new Map();
    for (const dir of usage.dirs || []) {
      const acc = byBroker.get(dir.broker_id) || { logs: 0, usable: -1 };
      acc.logs += Math.max(dir.size_bytes, 0);
      // Two directories on one disk would double-count free space, so the
      // smallest is reported: that is the one that fills first.
      if (dir.usable_bytes >= 0) {
        acc.usable = acc.usable < 0 ? dir.usable_bytes : Math.min(acc.usable, dir.usable_bytes);
      }
      byBroker.set(dir.broker_id, acc);
    }
    document.getElementById("brokers").innerHTML =
      "<tr><th>ID</th><th>Address</th><th>Roles</th><th>Logs</th><th>Free</th><th>Status</th></tr>" +
      b.brokers.map(x => {
        const disk = byBroker.get(x.broker_id) || { logs: -1, usable: -1 };
        return `<tr class="pick ${picked.broker === x.broker_id ? "on" : ""}" onclick="pick('broker', ${x.broker_id})">
        <td>${x.broker_id}${x.is_controller ? ' <span class="pill warn">controller</span>' : ""}</td>
        <td>${x.host}:${x.data_port}</td><td class="muted">${(x.roles||[]).join(", ")}</td>
        <td>${fmtBytes(disk.logs)}</td><td>${fmtBytes(disk.usable)}</td>
        <td><span class="pill ${x.alive ? "ok" : "no"}">${x.alive ? "alive" : "down"}</span></td></tr>`;
      }).join("");

    const t = await api("/api/v1/topics");
    fillTopicPicker(t.topics || []);
    document.getElementById("topics").innerHTML =
      "<tr><th>Topic</th><th>Partitions</th><th>RF</th><th>Under-replicated</th></tr>" +
      (t.topics.length ? t.topics.map(x => `<tr class="pick ${picked.topic === x.name ? "on" : ""}"
        onclick="pick('topic', ${arg(x.name)})">
        <td>${escapeHtml(x.name)}</td><td>${x.partitions}</td>
        <td>${x.replication_factor}</td>
        <td><span class="pill ${x.under_replicated ? "no" : "ok"}">${x.under_replicated}</span></td></tr>`).join("")
        : '<tr><td class="muted" colspan="4">no topics</td></tr>');

    const g = await api("/api/v1/groups");
    // Total lag per group, fetched alongside the list so the headline
    // number an operator looks for is on the row rather than a click away.
    const lags = await Promise.all((g.groups || []).map(x =>
      api(`/api/v1/groups/${encodeURIComponent(x.group_id)}/lag`)
        .then(l => (l.partitions || []).reduce((sum, p) => sum + (p.lag > 0 ? p.lag : 0), 0))
        .catch(() => -1)));
    document.getElementById("health").innerHTML =
      healthBar("Brokers available", o.brokers_alive, o.brokers) +
      healthBar("Partitions fully replicated", o.partitions - o.under_replicated_partitions, o.partitions) +
      healthBar("Partitions online", o.partitions - o.offline_partitions, o.partitions) +
      tile("Consumer group lag", lags.some(l => l < 0) ? "Unavailable" : fmt(lags.reduce((a, b) => a + b, 0)) + " records");
    document.getElementById("groups").innerHTML =
      "<tr><th>Group</th><th>State</th><th>Members</th><th>Generation</th><th>Lag</th></tr>" +
      (g.groups.length ? g.groups.map((x, i) => `<tr class="pick ${picked.group === x.group_id ? "on" : ""}"
        onclick="pick('group', ${arg(x.group_id)})">
        <td>${escapeHtml(x.group_id)}</td>
        <td><span class="pill ${x.state === "Stable" ? "ok" : "warn"}">${x.state}</span></td>
        <td>${x.member_count}</td><td>${x.generation}</td>
        <td><span class="pill ${lags[i] < 0 ? "" : lags[i] > 0 ? "warn" : "ok"}">${lags[i] < 0 ? "\u2014" : fmt(lags[i])}</span></td></tr>`).join("")
        : '<tr><td class="muted" colspan="5">no groups</td></tr>');

    document.getElementById("clock").textContent = new Date().toLocaleTimeString();
    lastRefresh = Date.now();
    document.getElementById("feedstatus").textContent = "Live · updated " + new Date(lastRefresh).toLocaleTimeString();
    // Whatever is expanded follows the same three-second cadence as the
    // tables above it; lag that only moved when clicked would be worse
    // than no lag column at all.
    if (picked.broker !== null) renderBrokerDetail();
    if (picked.topic) renderTopicDetail();
    if (picked.group) renderGroupDetail();
  } catch (e) {
    document.getElementById("feedstatus").textContent = "Updates unavailable · " + e.message +
      (lastRefresh ? " · last success " + new Date(lastRefresh).toLocaleTimeString() : "");
  } finally { refreshing = false; }
}

function healthBar(label, value, total) {
  const valid = Number.isFinite(value) && Number.isFinite(total) && total > 0;
  const percent = valid ? Math.max(0, Math.min(100, value / total * 100)) : 0;
  return `<div class="card"><h3>${escapeHtml(label)}</h3><div class="chart-summary"><strong>${valid ? percent.toFixed(1) + "%" : "N/A"}</strong><span>${fmt(value)} / ${fmt(total)}</span></div>` +
    (valid ? `<progress max="100" value="${percent}" aria-label="${escapeHtml(label)}"></progress>` : '<div class="muted">No resources reported</div>') + '</div>';
}

// Counter resets and duplicate timestamps are gaps, never artificial spikes.
function seriesPoints(samples, counter, since) {
  const points = [];
  for (let i = counter ? 1 : 0; i < samples.length; i++) {
    const s = samples[i], previous = samples[i - 1];
    if (!Number.isFinite(s.timestamp_ms) || !Number.isFinite(s.value) || s.timestamp_ms < since) continue;
    if (counter && (!Number.isFinite(previous.value) || s.timestamp_ms <= previous.timestamp_ms || s.value < previous.value)) continue;
    const value = counter ? (s.value - previous.value) * 1000 / (s.timestamp_ms - previous.timestamp_ms) : s.value;
    if (Number.isFinite(value)) points.push({ time: s.timestamp_ms, value });
  }
  return points;
}

function chartValue(value) {
  return value !== 0 && Math.abs(value) < 10 ? value.toFixed(2) : fmt(value);
}

function chartMarkup(points, width = 600) {
  if (!points.length) return '<text x="12" y="55" fill="currentColor" font-size="12">Waiting for samples...</text>';
  // Preserve each bucket's extrema while bounding SVG nodes for six-hour views.
  if (points.length > 240) {
    const reduced = [points[0]], bucketSize = Math.ceil(points.length / 119);
    for (let start = 0; start < points.length; start += bucketSize) {
      const bucket = points.slice(start, start + bucketSize);
      const low = bucket.reduce((a, b) => a.value < b.value ? a : b);
      const high = bucket.reduce((a, b) => a.value > b.value ? a : b);
      reduced.push(...(low === high ? [low] : [low, high].sort((a, b) => a.time - b.time)));
    }
    reduced.push(points[points.length - 1]);
    points = reduced;
  }
  const values = points.map(p => p.value), max = Math.max(...values, 1), min = Math.min(...values, 0);
  const first = points[0].time, duration = Math.max(1, points[points.length - 1].time - first);
  const right = width - 10;
  const coords = points.map(p => ({ x: 44 + (p.time - first) / duration * (right - 44), y: 96 - (p.value - min) / (max - min) * 84 }));
  const path = coords.map((p, i) => `${i ? "L" : "M"}${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(" ");
  return [0, .5, 1].map(f => `<line x1="44" y1="${96-f*84}" x2="${right}" y2="${96-f*84}" stroke="var(--line)"/><text x="1" y="${99-f*84}" fill="currentColor" font-size="10">${chartValue(min + f*(max-min))}</text>`).join("") +
    `<path d="${path}" fill="none" stroke="var(--accent)" stroke-width="2"/>` +
    coords.map((p, i) => `<circle cx="${p.x}" cy="${p.y}" r="2" fill="var(--accent)"><title>${new Date(points[i].time).toLocaleTimeString()}: ${points[i].value.toFixed(2)}</title></circle>`).join("") +
    `<text x="44" y="116" fill="currentColor" font-size="10">${new Date(first).toLocaleTimeString()}</text><text x="${right}" y="116" text-anchor="end" fill="currentColor" font-size="10">${new Date(points[points.length-1].time).toLocaleTimeString()}</text>`;
}

const analyticsMetrics = [
  ["Records in / sec", "brahmaputra_produce_records_total"],
  ["Bytes in / sec", "brahmaputra_produce_bytes_total"],
  ["Bytes out / sec", "brahmaputra_fetch_bytes_total"],
  ["Produce errors / sec", "brahmaputra_produce_errors_total"],
  ["Throttled requests / sec", "brahmaputra_throttled_requests_total"],
  ["Open connections", "brahmaputra_connections_open"]
];

async function drawChart() {
  if (charting || !token || paused) return;
  charting = true;
  try {
    const metric = document.getElementById("metric").value;
    const since = Date.now() - Number(document.getElementById("window").value) * 1000;
    const names = [...new Set([...analyticsMetrics.map(x => x[1]), metric].filter(Boolean))];
    const fetched = await Promise.all(names.map(async name => [name, await api("/api/v1/metrics/timeseries?metric=" + encodeURIComponent(name) + "&from=" + (since - 5000))]));
    chartSamples = Object.fromEntries(fetched.map(([name, data]) => [name, data.samples || []]));
    const pointsFor = name => seriesPoints(chartSamples[name] || [], name.split("{")[0].endsWith("_total"), since);
    document.getElementById("analytics").innerHTML = analyticsMetrics.map(([label, name]) => {
      const points = pointsFor(name), values = points.map(p => p.value);
      return `<div class="card"><h3>${label}</h3><div class="chart-summary"><strong>${points.length ? chartValue(values[values.length-1]) : "—"}</strong><span class="muted">peak ${points.length ? chartValue(Math.max(...values)) : "—"}</span></div><svg viewBox="0 0 320 120" role="img" aria-label="${label}">${chartMarkup(points, 320)}</svg></div>`;
    }).join("");
    const points = pointsFor(metric);
    document.getElementById("chart").innerHTML = chartMarkup(points);
    document.getElementById("chartinfo").textContent = `${metric.split("{")[0].endsWith("_total") ? "rate/sec" : "value"} · ${points.length} samples · hover for details`;
  } catch (error) {
    document.getElementById("chartinfo").textContent = "Chart updates unavailable · " + error.message;
  } finally { charting = false; }
}

function toggleAnalytics() {
  paused = !paused;
  document.getElementById("pausebtn").textContent = paused ? "Resume updates" : "Pause updates";
  document.getElementById("feedstatus").textContent = paused ? "Updates paused" : "Resuming updates...";
  if (!paused) { refresh(); drawChart(); }
}

function exportAnalytics() {
  const blob = new Blob([JSON.stringify({ exported_at: new Date().toISOString(), scope: "local broker", metrics: chartSamples }, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob), link = document.createElement("a");
  link.href = url; link.download = "brahmaputra-metrics.json"; link.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

async function loadMetricList() {
  const data = await api("/api/v1/metrics/timeseries?metric=brahmaputra_produce_records_total");
  const select = document.getElementById("metric");
  const preferred = "brahmaputra_produce_records_total";
  select.innerHTML = (data.available || [preferred])
    .map(m => `<option ${m === preferred ? "selected" : ""}>${escapeHtml(m)}</option>`).join("");
}


// ----------------------------------------------------------- messages

let liveSource = null;
let searchTimer = null;
let knownTopics = [];

function currentTopic() {
  return document.getElementById("mtopic").value || "";
}

function escapeHtml(text) {
  return String(text).replace(/[&<>"']/g, c => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
  })[c]);
}

/// Long payloads are truncated in the row: an operations page has to stay
/// readable when someone posts a megabyte of JSON.
function preview(text, binary) {
  const limit = 300;
  const shown = text.length > limit ? text.slice(0, limit) + "…" : text;
  return escapeHtml(shown) + (binary ? ' <span class="muted">(binary)</span>' : "");
}

function renderMessages(rows) {
  const table = document.getElementById("messages");
  if (!rows.length) {
    table.innerHTML = "<tr><td class='muted'>no messages</td></tr>";
    document.getElementById("mcount").textContent = "";
    return;
  }
  table.innerHTML =
    "<tr><th>partition</th><th>offset</th><th>time</th><th>key</th><th>value</th><th>bytes</th></tr>" +
    rows.map(m => `<tr>
      <td>${m.partition}</td>
      <td>${m.offset}</td>
      <td class="muted">${m.timestamp ? new Date(m.timestamp).toLocaleTimeString() : ""}</td>
      <td>${m.key === null ? '<span class="muted">—</span>' : escapeHtml(m.key)}</td>
      <td>${preview(m.value, m.binary)}</td>
      <td class="muted">${m.size_bytes}</td>
    </tr>`).join("");
  document.getElementById("mcount").textContent = `${rows.length} shown`;
}

async function loadMessages() {
  const topic = currentTopic();
  if (!topic) { renderMessages([]); return; }
  const params = new URLSearchParams();
  const partition = document.getElementById("mpart").value;
  if (partition !== "") params.set("partition", partition);
  const search = document.getElementById("msearch").value.trim();
  if (search) params.set("search", search);
  params.set("order", document.getElementById("morder").value);
  params.set("limit", document.getElementById("mlimit").value);
  try {
    const data = await api(`/api/v1/topics/${encodeURIComponent(topic)}/messages?${params}`);
    renderMessages(data.messages || []);
  } catch (error) {
    renderMessages([]);
  }
}

function debouncedMessages() {
  clearTimeout(searchTimer);
  searchTimer = setTimeout(loadMessages, 250);
}

/// Live tail. EventSource cannot send an Authorization header, so the
/// token rides in the query string — the same session token, on the same
/// origin, over whatever transport the page was already served on.
function toggleLive() {
  const button = document.getElementById("livebtn");
  if (liveSource) {
    liveSource.close();
    liveSource = null;
    button.textContent = "Go live";
    return;
  }
  const topic = currentTopic();
  if (!topic) return;
  const params = new URLSearchParams({ access_token: token });
  const partition = document.getElementById("mpart").value;
  if (partition !== "") params.set("partition", partition);
  liveSource = new EventSource(`/api/v1/topics/${encodeURIComponent(topic)}/stream?${params}`);
  button.textContent = "Stop";
  const table = document.getElementById("messages");
  liveSource.onmessage = event => {
    const m = JSON.parse(event.data);
    const search = document.getElementById("msearch").value.trim().toLowerCase();
    if (search) {
      const hay = (m.value + " " + (m.key || "")).toLowerCase();
      if (!hay.includes(search)) return;
    }
    if (!table.rows.length || table.rows[0].cells.length !== 6) {
      table.innerHTML =
        "<tr><th>partition</th><th>offset</th><th>time</th><th>key</th><th>value</th><th>bytes</th></tr>";
    }
    const row = table.insertRow(1);
    row.innerHTML = `<td>${m.partition}</td><td>${m.offset}</td>
      <td class="muted">${m.timestamp ? new Date(m.timestamp).toLocaleTimeString() : ""}</td>
      <td>${m.key === null ? '<span class="muted">—</span>' : escapeHtml(m.key)}</td>
      <td>${preview(m.value, m.binary)}</td>
      <td class="muted">${m.size_bytes}</td>`;
    // Keep the tail bounded or a busy topic grows the DOM without limit.
    while (table.rows.length > 400) table.deleteRow(table.rows.length - 1);
  };
  liveSource.onerror = () => { toggleLive(); };
}

function topicChanged() {
  if (liveSource) toggleLive();
  const topic = knownTopics.find(t => t.name === currentTopic());
  const select = document.getElementById("mpart");
  const count = topic ? topic.partitions : 0;
  select.innerHTML = '<option value="">all partitions</option>' +
    Array.from({ length: count }, (_, i) => `<option value="${i}">partition ${i}</option>`).join("");
  loadMessages();
}

function fillTopicPicker(topics) {
  knownTopics = topics.map(t => ({
    name: t.name,
    partitions: typeof t.partitions === "number"
      ? t.partitions
      : (t.partitions && t.partitions.length) || t.partition_count || 0
  }));
  const select = document.getElementById("mtopic");
  const previous = select.value;
  select.innerHTML = knownTopics.map(t => `<option>${escapeHtml(t.name)}</option>`).join("");
  if (previous && knownTopics.some(t => t.name === previous)) select.value = previous;
  if (!select.dataset.ready) { select.dataset.ready = "1"; topicChanged(); }
}

// ------------------------------------------------- topic administration

function adminSay(message, bad) {
  const element = document.getElementById("adminmsg");
  element.textContent = message;
  element.className = bad ? "bad" : "muted";
}

async function addPartitions() {
  const topic = currentTopic();
  const count = parseInt(document.getElementById("partcount").value, 10);
  if (!topic || !count) { adminSay("choose a topic and a partition count", true); return; }
  try {
    await api(`/api/v1/topics/${encodeURIComponent(topic)}/partitions`, {
      method: "POST", body: JSON.stringify({ count })
    });
    adminSay(`${topic} now has ${count} partitions`);
    await refresh();
  } catch (error) {
    adminSay(String(error.message || error), true);
  }
}

async function setConfig() {
  const topic = currentTopic();
  const key = document.getElementById("cfgkey").value.trim();
  const value = document.getElementById("cfgval").value.trim();
  if (!topic || !key) { adminSay("choose a topic and a config key", true); return; }
  try {
    await api(`/api/v1/topics/${encodeURIComponent(topic)}/config`, {
      method: "POST", body: JSON.stringify({ configs: { [key]: value } })
    });
    adminSay(`${topic}: ${key} = ${value}`);
    await refresh();
  } catch (error) {
    adminSay(String(error.message || error), true);
  }
}

async function removeTopic() {
  const topic = currentTopic();
  if (!topic) return;
  if (!confirm(`Delete topic "${topic}" and everything in it?`)) return;
  try {
    await api(`/api/v1/topics/${encodeURIComponent(topic)}`, { method: "DELETE" });
    adminSay(`${topic} deleted`);
    await refresh();
  } catch (error) {
    adminSay(String(error.message || error), true);
  }
}

async function start() {
  const generation = ++sessionGeneration;
  clearTimeout(refreshTimer); clearTimeout(chartTimer);
  paused = false;
  document.getElementById("pausebtn").textContent = "Pause updates";
  document.getElementById("login").hidden = true;
  document.getElementById("app").hidden = false;
  document.getElementById("who").textContent = `${user} · ${role}`;
  await loadMetricList();
  await refresh();
  await loadMessages();
  await drawChart();
  const active = () => token && generation === sessionGeneration;
  const poll = async () => { if (!active()) return; await refresh(); if (active()) refreshTimer = setTimeout(poll, 3000); };
  const charts = async () => { if (!active()) return; await drawChart(); if (active()) chartTimer = setTimeout(charts, 5000); };
  if (active()) { refreshTimer = setTimeout(poll, 3000); chartTimer = setTimeout(charts, 5000); }
}

if (token) start().catch(error => { logout(); document.getElementById("err").textContent = error.message; }); else logout();
</script>
</body>
</html>
"##;
