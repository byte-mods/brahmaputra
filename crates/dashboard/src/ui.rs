//! The dashboard, compiled into the binary (DESIGN.md §9.3).
//!
//! One file, no build step, no Node toolchain, no CDN: vanilla JS with a
//! hand-drawn SVG sparkline. That is a deliberate trade — a framework would
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
  #messages td { vertical-align:top; word-break:break-word; }
  #messages td:nth-child(5) { max-width:640px; font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:12px; }
  #login { max-width:340px; margin:14vh auto; }
  #login .card { display:grid; gap:10px; }
  #err { color:var(--bad); min-height:1.2em; }
  svg { width:100%; height:120px; display:block; }
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

    <section>
      <h2>Throughput</h2>
      <div class="card">
        <select id="metric" onchange="drawChart()"></select>
        <svg id="chart" viewBox="0 0 600 120" preserveAspectRatio="none"></svg>
        <div class="muted" id="chartinfo"></div>
      </div>
    </section>

    <section><h2>Brokers</h2><table id="brokers"></table></section>
    <section><h2>Topics</h2><table id="topics"></table></section>
    <section><h2>Consumer groups</h2><table id="groups"></table></section>
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

async function api(path, options) {
  const response = await fetch(path, Object.assign({
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
  token = ""; sessionStorage.clear();
  document.getElementById("app").hidden = true;
  document.getElementById("login").hidden = false;
}

const fmt = n => n === undefined || n === null ? "—"
  : n >= 1e9 ? (n/1e9).toFixed(1)+"G" : n >= 1e6 ? (n/1e6).toFixed(1)+"M"
  : n >= 1e3 ? (n/1e3).toFixed(1)+"k" : String(Math.round(n));

function tile(label, value, cls) {
  return `<div class="card"><h3>${label}</h3><div class="v ${cls||""}">${value}</div></div>`;
}

async function refresh() {
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
    document.getElementById("brokers").innerHTML =
      "<tr><th>ID</th><th>Address</th><th>Roles</th><th>Status</th></tr>" +
      b.brokers.map(x => `<tr><td>${x.broker_id}${x.is_controller ? ' <span class="pill warn">controller</span>' : ""}</td>
        <td>${x.host}:${x.data_port}</td><td class="muted">${(x.roles||[]).join(", ")}</td>
        <td><span class="pill ${x.alive ? "ok" : "no"}">${x.alive ? "alive" : "down"}</span></td></tr>`).join("");

    const t = await api("/api/v1/topics");
    fillTopicPicker(t.topics || []);
    document.getElementById("topics").innerHTML =
      "<tr><th>Topic</th><th>Partitions</th><th>RF</th><th>Under-replicated</th></tr>" +
      (t.topics.length ? t.topics.map(x => `<tr><td>${x.name}</td><td>${x.partitions}</td>
        <td>${x.replication_factor}</td>
        <td><span class="pill ${x.under_replicated ? "no" : "ok"}">${x.under_replicated}</span></td></tr>`).join("")
        : '<tr><td class="muted" colspan="4">no topics</td></tr>');

    const g = await api("/api/v1/groups");
    document.getElementById("groups").innerHTML =
      "<tr><th>Group</th><th>State</th><th>Members</th><th>Generation</th></tr>" +
      (g.groups.length ? g.groups.map(x => `<tr><td>${x.group_id}</td>
        <td><span class="pill ${x.state === "Stable" ? "ok" : "warn"}">${x.state}</span></td>
        <td>${x.member_count}</td><td>${x.generation}</td></tr>`).join("")
        : '<tr><td class="muted" colspan="4">no groups</td></tr>');

    document.getElementById("clock").textContent = new Date().toLocaleTimeString();
  } catch (e) { /* a transient failure should not blank the page */ }
}

async function drawChart() {
  const select = document.getElementById("metric");
  const metric = select.value;
  if (!metric) return;
  const data = await api("/api/v1/metrics/timeseries?metric=" + encodeURIComponent(metric));
  const samples = data.samples || [];
  const svg = document.getElementById("chart");
  if (samples.length < 2) {
    svg.innerHTML = '<text x="8" y="20" fill="#8b93a7" font-size="11">collecting…</text>';
    return;
  }
  // Counters only make sense as a rate; gauges are plotted as-is.
  const isCounter = metric.includes("_total");
  const points = [];
  for (let i = 1; i < samples.length; i++) {
    const dt = (samples[i].timestamp_ms - samples[i-1].timestamp_ms) / 1000 || 1;
    points.push(isCounter ? Math.max(0, (samples[i].value - samples[i-1].value) / dt)
                          : samples[i].value);
  }
  const max = Math.max(...points, 1), min = Math.min(...points, 0);
  const span = (max - min) || 1;
  const path = points.map((v, i) => {
    const x = (i / (points.length - 1)) * 600;
    const y = 115 - ((v - min) / span) * 110;
    return `${i ? "L" : "M"}${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
  svg.innerHTML = `<path d="${path}" fill="none" stroke="#4da3ff" stroke-width="2"/>`;
  document.getElementById("chartinfo").textContent =
    `${isCounter ? "rate/sec" : "value"} · now ${fmt(points[points.length-1])} · peak ${fmt(max)} · ${points.length} samples`;
}

async function loadMetricList() {
  const data = await api("/api/v1/metrics/timeseries?metric=brahmaputra_produce_records_total");
  const select = document.getElementById("metric");
  const preferred = "brahmaputra_produce_records_total";
  select.innerHTML = (data.available || [preferred])
    .map(m => `<option ${m === preferred ? "selected" : ""}>${m}</option>`).join("");
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
    partitions: (t.partitions && t.partitions.length) || t.partition_count || 0
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
  document.getElementById("login").hidden = true;
  document.getElementById("app").hidden = false;
  document.getElementById("who").textContent = `${user} · ${role}`;
  await loadMetricList();
  await refresh();
  await loadMessages();
  await drawChart();
  setInterval(refresh, 3000);
  setInterval(drawChart, 5000);
}

if (token) start(); else logout();
</script>
</body>
</html>
"##;
