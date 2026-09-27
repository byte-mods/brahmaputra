// End to end against a real broker and real gateway processes:
// UI client <-> brahmaputra-ws-gateway <-> Brahmaputra.
import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";

import {
  BrahmaputraClient,
  BrahmaputraError,
  connectionState,
  latestByKey,
  recentRecords,
} from "../dist/index.js";
import { readable } from "../dist/svelte.js";
import {
  eventually,
  mintToken,
  priceProducer,
  readTopic,
  sleep,
  startGateway,
  uniqueTopic,
} from "../../testkit/index.mjs";

let gw;
let prices;
const clients = [];

function client(options) {
  const c = new BrahmaputraClient({
    url: gw.url,
    token: mintToken("viewer", { topics: [], subscribe: ["prices.*"] }),
    reconnect: { minDelayMs: 50, maxDelayMs: 500 },
    ...options,
  });
  clients.push(c);
  return c;
}

before(async () => {
  gw = await startGateway();
  prices = await priceProducer();
});

after(async () => {
  for (const c of clients) c.close();
  await prices?.close();
  await gw?.stop();
});

describe("publish and subscribe", () => {
  test("a UI publish is acknowledged, lands in Brahmaputra and reaches subscribers", async () => {
    const topic = uniqueTopic("chat");
    const token = mintToken("alice", { topics: ["chat.*"], subscribe: ["chat.*"] });
    const alice = client({ token, topic });
    const bob = client({ token: mintToken("bob", { topics: [], subscribe: ["chat.*"] }) });
    await Promise.all([alice.connect(), bob.connect()]);
    assert.equal(alice.state, "open");
    assert.equal(alice.welcome.user, "alice");
    assert.equal(alice.welcome.subscribe, true);

    const received = [];
    const sub = bob.subscribe(topic, (r) => received.push(r));
    await sub.ready;

    const ack = await alice.publish({ key: "room-1", value: "hello" });
    assert.equal(ack.topic, topic);
    assert.ok(ack.offset >= 0);
    const bin = await alice.publish({ key: "room-1", value: new Uint8Array([0, 255, 7]) });
    const json = await alice.publishJson({ text: "hi" }, { key: "room-1", headers: { lang: "en" } });

    await eventually(() => received.length === 3, { what: "three records" });
    const [a, b, c] = received;
    assert.equal(a.value, "hello");
    assert.equal(a.key, "room-1");
    assert.equal(a.offset, ack.offset);
    assert.equal(a.partition, ack.partition);
    assert.equal(a.headers["x-gw-user"], "alice", "the gateway stamps the publisher");
    assert.equal(a.snapshot, false);
    assert.equal(b.value, null);
    assert.deepEqual([...b.valueBytes], [0, 255, 7]);
    assert.equal(b.offset, bin.offset);
    assert.deepEqual(c.json(), { text: "hi" });
    assert.equal(c.headers.lang, "en");
    assert.equal(c.offset, json.offset);

    // And it is really in Brahmaputra, where a back-end service reads it.
    const stored = await readTopic(topic);
    assert.equal(stored.length, 3);
    assert.ok(stored.some((r) => String(r.value) === "hello"));
    sub.unsubscribe();
  });

  test("tombstones travel as deletes", async () => {
    const topic = uniqueTopic("chat");
    const alice = client({ token: mintToken("alice", { topics: ["chat.*"], subscribe: ["chat.*"] }), topic });
    await alice.connect();
    const got = [];
    await alice.subscribe(topic, (r) => got.push(r)).ready;
    await alice.publish({ key: "k", value: null });
    await eventually(() => got.length === 1);
    assert.equal(got[0].tombstone, true);
  });
});

describe("price feed", () => {
  test("latestByKey starts from the snapshot, follows the feed and honours key filters", async () => {
    const topic = uniqueTopic("prices");
    // The market was open before the UI loaded.
    await prices.tick(topic, "AAPL", 100);
    await prices.tick(topic, "MSFT", 200);
    await prices.tick(topic, "AAPL", 101);

    const ui = client();
    await ui.connect();
    const board = latestByKey(ui, topic);
    const watch = latestByKey(ui, topic, { keys: ["MSFT"], snapshot: true });
    const tape = recentRecords(ui, topic, { limit: 3 });
    let notifications = 0;
    board.subscribe(() => notifications++);
    await Promise.all([board.ready, watch.ready, tape.ready]);
    await eventually(() => board.get().size === 2, { what: "snapshot" });
    assert.equal(board.get().get("AAPL").json().price, 101);
    assert.equal(board.get().get("AAPL").snapshot, true);
    await eventually(() => watch.get().size === 1, { what: "filtered snapshot" });
    assert.deepEqual([...watch.get().keys()], ["MSFT"]);
    assert.equal(tape.get().length, 0, "a tape has no snapshot by default");

    const before = board.get();
    for (let i = 0; i < 5; i++) await prices.tick(topic, "GOOG", 300 + i);
    await prices.tick(topic, "MSFT", 201);
    await eventually(() => board.get().get("MSFT")?.json().price === 201, { what: "live tick" });
    assert.notEqual(board.get(), before, "a new immutable map per change");
    assert.equal(board.get().get("GOOG").json().price, 304);
    assert.equal(board.get().get("GOOG").snapshot, false);
    assert.equal(watch.get().get("MSFT").json().price, 201);
    assert.equal(watch.get().size, 1);
    await eventually(() => tape.get().length === 3);
    assert.deepEqual(tape.get().map((r) => r.json().price), [303, 304, 201]);
    assert.ok(notifications < 9, `bursts coalesce (${notifications} notifications)`);

    // Delisting a symbol (tombstone) removes it.
    await prices.tombstone(topic, "GOOG");
    await eventually(() => !board.get().has("GOOG"), { what: "tombstone" });

    // The Svelte adapter follows the same store.
    const seen = [];
    const stop = readable(board).subscribe((m) => seen.push(m.size));
    await prices.tick(topic, "NVDA", 900);
    await eventually(() => seen.at(-1) === 3);
    stop();
    for (const s of [board, watch, tape]) s.close();
  });

  test("only listeners that asked for the snapshot get it", async () => {
    const topic = uniqueTopic("prices");
    await prices.tick(topic, "AAPL", 1);
    const ui = client();
    await ui.connect();
    const live = [];
    await ui.subscribe(topic, (r) => live.push(r)).ready;
    const snap = [];
    await ui.subscribe(topic, (r) => snap.push(r), { snapshot: true }).ready;
    await eventually(() => snap.length === 1);
    await sleep(200);
    assert.equal(live.length, 0, "the live-only listener did not get the snapshot");
    await prices.tick(topic, "AAPL", 2);
    await eventually(() => live.length === 1 && snap.length === 2);
  });
});

describe("authentication and authorization", () => {
  test("a forged or expired token never connects", async () => {
    for (const token of [
      mintToken("mallory", { secret: "not-the-gateway-secret-at-all" }),
      mintToken("late", { exp: Math.floor(Date.now() / 1000) - 3600 }),
      "not-a-jwt",
    ]) {
      const c = client({ token, reconnect: false });
      await assert.rejects(c.connect(), (e) => e instanceof BrahmaputraError && e.code === "CONNECTION");
      assert.equal(c.state, "closed");
    }
  });

  test("a read-only token cannot publish, and subscriptions follow the claim", async () => {
    const viewer = client(); // topics: [], subscribe: ["prices.*"]
    await viewer.connect();
    await assert.rejects(
      viewer.publish({ topic: uniqueTopic("orders"), value: "buy" }),
      (e) => e.code === "TOPIC_NOT_ALLOWED" && e.retryable === false,
    );
    const denied = latestByKey(viewer, uniqueTopic("orders"));
    await eventually(() => denied.error?.code === "TOPIC_NOT_ALLOWED", { what: "refusal" });
    const allowed = latestByKey(viewer, uniqueTopic("prices"));
    await allowed.ready;
    assert.equal(allowed.error, null);
  });

  test("the token function is called on every connection, so tokens can rotate", async () => {
    let calls = 0;
    const c = client({
      token: () => {
        calls++;
        return mintToken(`rotating-${calls}`, { subscribe: ["prices.*"] });
      },
    });
    await c.connect();
    assert.equal(c.welcome.user, "rotating-1");
    const restarted = await gw.restart();
    gw = restarted;
    await eventually(() => c.state === "open" && c.welcome.user !== "rotating-1", {
      what: "reconnect with a fresh token",
    });
    assert.ok(calls >= 2);
  });
});

describe("resilience", () => {
  test("a gateway restart is invisible: subscriptions resume, unacked publishes are resent", async () => {
    const topic = uniqueTopic("orders");
    const trader = client({
      token: mintToken("trader", { topics: ["orders.*"], subscribe: ["orders.*"] }),
      topic,
    });
    await trader.connect();
    const states = [];
    connectionState(trader).subscribe(() => states.push(trader.state));
    const got = [];
    await trader.subscribe(topic, (r) => got.push(r.value)).ready;

    await trader.publish({ key: "o", value: "before" });
    await eventually(() => got.includes("before"));

    // Stop the gateway; publish while it is down; start it again.
    await gw.stop();
    const inFlight = trader.publish({ key: "o", value: "during" });
    await eventually(() => trader.state === "reconnecting");
    gw = await startGateway([], { port: gw.port, httpPort: gw.httpPort });
    const ack = await inFlight;
    assert.ok(ack.offset >= 1);
    await eventually(() => got.includes("during"), { what: "resubscribed after restart" });
    assert.ok(states.includes("reconnecting") && states.at(-1) === "open", states.join(","));

    const stored = (await readTopic(topic)).map((r) => String(r.value));
    assert.ok(stored.includes("before") && stored.includes("during"), stored.join(","));
  });

  test("publishes made before the first connection are queued and sent", async () => {
    const topic = uniqueTopic("orders");
    const c = client({ token: mintToken("early", { topics: ["orders.*"] }), topic });
    const early = c.publish({ key: "x", value: "queued" });
    await c.connect();
    const ack = await early;
    assert.equal(ack.topic, topic);
  });

  test("close() rejects what is pending and stops reconnecting", async () => {
    const c = client({ token: mintToken("closer", { topics: ["orders.*"] }), topic: uniqueTopic("orders") });
    const pending = c.publish({ value: "never" });
    c.close();
    await assert.rejects(pending, (e) => e.code === "CLOSED");
    assert.equal(c.state, "closed");
  });
});
