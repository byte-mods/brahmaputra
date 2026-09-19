// Exercise the exact JavaScript embedded in the shipping Rust dashboard.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('crates/dashboard/src/ui.rs', 'utf8').split('<script>')[1].split('</script>')[0];
function dashboard() {
  const elements = new Map();
  const timers = new Map();
  let id = 0;
  const context = vm.createContext({
    sessionStorage: { getItem: () => '', clear() {} },
    document: { getElementById(name) {
      if (!elements.has(name)) elements.set(name, { value: name === 'window' ? '300' : '', textContent: '', innerHTML: '', hidden: false });
      return elements.get(name);
    } },
    setTimeout(fn) { timers.set(++id, fn); return id; },
    clearTimeout(id) { timers.delete(id); },
    AbortSignal, Date, Blob, URL, URLSearchParams,
  });
  vm.runInContext(source, context);
  return { context, elements, timers, run: code => vm.runInContext(code, context) };
}
test('counter rates use elapsed time and exclude resets and repeated timestamps', () => {
  const d = dashboard();
  const result = d.run(`seriesPoints([
    {timestamp_ms:1000,value:10},{timestamp_ms:3000,value:30},
    {timestamp_ms:3000,value:99},{timestamp_ms:4000,value:2},
    {timestamp_ms:6000,value:12}],true,0)`);
  assert.deepEqual(JSON.parse(JSON.stringify(result)), [{ time: 3000, value: 10 }, { time: 6000, value: 5 }]);
});
test('window filtering preserves the preceding counter sample; gauges need one sample', () => {
  const d = dashboard();
  assert.equal(d.run('seriesPoints([{timestamp_ms:1000,value:10},{timestamp_ms:6000,value:30}],true,5000)[0].value'), 4);
  assert.equal(d.run('seriesPoints([{timestamp_ms:6000,value:7}],false,5000)[0].value'), 7);
});
test('empty and single-point charts never emit invalid coordinates', () => {
  const d = dashboard();
  assert.match(d.run('chartMarkup([])'), /Waiting/);
  const chart = d.run('chartMarkup([{time:1000,value:0}])');
  assert.doesNotMatch(chart, /NaN|Infinity/);
  assert.match(chart, /circle/);
  assert.match(d.run('healthBar("Empty",0,0)'), /N\/A/);
  assert.match(d.run('healthBar("Replicated",8,10)'), /value="80"/);
});
test('logout closes streams and clears both polling timers', () => {
  const d = dashboard();
  d.run('refreshTimer = setTimeout(() => {}, 3); chartTimer = setTimeout(() => {}, 5); liveSource = {close() { globalThis.closed = true; }}; logout()');
  assert.equal(d.timers.size, 0);
  assert.equal(d.context.closed, true);
});
test('six-hour charts retain spikes with a bounded SVG node count', () => {
  const d = dashboard();
  const chart = d.run('chartMarkup(Array.from({length:4320}, (_,i) => ({time:i*5000,value:i===2000 ? 99999 : 1})))');
  assert.ok((chart.match(/<circle/g) || []).length <= 240);
  assert.match(chart, /99999\.00/);
});
test('failed refresh reports stale data and releases the overlap guard', async () => {
  const d = dashboard();
  await d.run('token="test"; api=async () => {throw new Error("offline")}; refresh()');
  assert.match(d.elements.get('feedstatus').textContent, /Updates unavailable.*offline/);
  assert.equal(d.run('refreshing'), false);
});
test('pause suppresses network requests', async () => {
  const d = dashboard();
  await d.run('token="test"; paused=true; api=async () => {throw new Error("must not fetch")}; Promise.all([refresh(),drawChart()])');
  assert.equal(d.run('charting || refreshing'), false);
});
test('analytics renders all six panels from sampled API values', async () => {
  const d = dashboard();
  await d.run(`token="test"; api=async () => ({samples:[
    {timestamp_ms:Date.now()-5000,value:10},{timestamp_ms:Date.now(),value:30}
  ]}); drawChart()`);
  const html = d.elements.get('analytics').innerHTML;
  assert.equal((html.match(/role="img"/g) || []).length, 6);
  assert.doesNotMatch(html, /NaN|Infinity/);
  assert.match(html, /Records in \/ sec/);
});
test('metric labels cannot inject HTML into the selector', async () => {
  const d = dashboard();
  await d.run('api=async () => ({available:[\'<img src=x onerror=alert(1)>\']}); loadMetricList()');
  assert.doesNotMatch(d.elements.get('metric').innerHTML, /<img/);
  assert.match(d.elements.get('metric').innerHTML, /&lt;img/);
});
