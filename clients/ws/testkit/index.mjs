// Fixtures for the WebSocket SDK suites: real gateway processes against a
// real broker, HS256 tokens, and a "trading back end" that writes prices
// straight into Brahmaputra with the Node.js driver.
//
// Environment (clients/ws/test.sh sets it):
//   BRP_HOST, BRP_PORT   the broker
//   GW_BIN               the brahmaputra-ws-gateway binary

import { spawn } from "node:child_process";
import { createHmac } from "node:crypto";
import { createRequire } from "node:module";
import net from "node:net";
import path from "node:path";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const here = path.dirname(fileURLToPath(import.meta.url));
const driver = require(path.join(here, "../../nodejs/src/index.js"));

export const SECRET = "ws-sdk-e2e-secret-0123456789";
export const BRP_HOST = process.env.BRP_HOST ?? "127.0.0.1";
export const BRP_PORT = Number(process.env.BRP_PORT ?? 9092);
const GW_BIN =
  process.env.GW_BIN ?? path.join(here, "../../../target/release/brahmaputra-ws-gateway");

const b64url = (data) => Buffer.from(data).toString("base64url");

/** Sign an HS256 JWT like an identity provider would. */
export function mintToken(sub, { topics, subscribe, ttlSecs = 600, exp, secret = SECRET } = {}) {
  const now = Math.floor(Date.now() / 1000);
  const claims = { sub, iat: now, exp: exp ?? now + ttlSecs };
  if (topics !== undefined) claims.topics = topics;
  if (subscribe !== undefined) claims.subscribe = subscribe;
  const head = b64url(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const body = b64url(JSON.stringify(claims));
  const sig = createHmac("sha256", secret).update(`${head}.${body}`).digest("base64url");
  return `${head}.${body}.${sig}`;
}

async function freePort() {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.listen(0, "127.0.0.1", () => {
      const { port } = srv.address();
      srv.close(() => resolve(port));
    });
    srv.on("error", reject);
  });
}

async function waitForHttp(url, timeoutMs = 15000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try {
      const r = await fetch(url);
      if (r.ok) return;
    } catch {
      // not up yet
    }
    if (Date.now() > deadline) throw new Error(`${url} not ready`);
    await new Promise((r) => setTimeout(r, 100));
  }
}

/**
 * Start a gateway process. Defaults suit the price-feed demo: publishes to
 * orders.* and chat.*, subscriptions to prices.*, orders.* and chat.*.
 */
export async function startGateway(extraArgs = [], { port, httpPort } = {}) {
  port ??= await freePort();
  httpPort ??= await freePort();
  const args = [
    "--listen", `127.0.0.1:${port}`,
    "--http-listen", `127.0.0.1:${httpPort}`,
    "--broker", `${BRP_HOST}:${BRP_PORT}`,
    "--jwt-secret", SECRET,
    "--allow-topic", "orders.*,chat.*,prices.*",
    "--allow-subscribe", "prices.*,orders.*,chat.*",
    "--linger-ms", "2",
    "--feed-idle-secs", "2",
    // The Node.js driver used to read topics back has no LZ4 codec.
    "--compression", "gzip",
    ...extraArgs,
  ];
  const child = spawn(GW_BIN, args, {
    stdio: ["ignore", "ignore", "pipe"],
    env: { ...process.env, RUST_LOG: process.env.GW_LOG ?? "warn" },
  });
  let log = "";
  child.stderr.on("data", (d) => {
    log += d;
    if (log.length > 64 * 1024) log = log.slice(-32 * 1024);
  });
  const exited = new Promise((resolve) => child.on("exit", resolve));
  try {
    await waitForHttp(`http://127.0.0.1:${httpPort}/readyz`);
  } catch (e) {
    child.kill("SIGKILL");
    throw new Error(`gateway did not become ready: ${e.message}\n${log}`);
  }
  return {
    url: `ws://127.0.0.1:${port}/ws`,
    port,
    httpPort,
    get log() {
      return log;
    },
    async metrics() {
      const text = await (await fetch(`http://127.0.0.1:${httpPort}/metrics`)).text();
      const out = {};
      for (const line of text.split("\n")) {
        if (!line || line.startsWith("#")) continue;
        const i = line.lastIndexOf(" ");
        out[line.slice(0, i)] = Number(line.slice(i + 1));
      }
      return out;
    },
    /** SIGTERM: graceful drain (clients see 1001). */
    async stop() {
      if (child.exitCode !== null) return;
      child.kill("SIGTERM");
      await exited;
    },
    async kill() {
      if (child.exitCode !== null) return;
      child.kill("SIGKILL");
      await exited;
    },
    /** Start again on the same ports (clients reconnect to the same URL). */
    async restart() {
      await this.stop();
      return startGateway(extraArgs, { port, httpPort });
    },
  };
}

/** Writes prices straight into Brahmaputra, as a market-data service would. */
export async function priceProducer() {
  const producer = await driver.Producer.connect(BRP_HOST, BRP_PORT, { lingerMs: 0 });
  return {
    async tick(topic, symbol, price, extra = {}) {
      const value = JSON.stringify({ symbol, price, ts: Date.now(), ...extra });
      const offset = await producer.sendSync(topic, value, { key: symbol });
      return offset;
    },
    async tombstone(topic, symbol) {
      return producer.sendSync(topic, null, { key: symbol });
    },
    close: () => producer.close(),
  };
}

/** Read a topic straight from the broker: what a back-end order service sees. */
export async function readTopic(topic, partitions = 4) {
  const consumer = await driver.Consumer.connect(BRP_HOST, BRP_PORT);
  const out = [];
  try {
    for (let p = 0; p < partitions; p++) {
      let offset = 0n;
      for (;;) {
        const records = await consumer.fetch(topic, p, offset);
        if (!records.length) break;
        for (const r of records) {
          out.push({ partition: p, ...r });
          offset = BigInt(r.offset) + 1n;
        }
      }
    }
  } finally {
    await consumer.close?.();
  }
  return out;
}

export function uniqueTopic(prefix) {
  return `${prefix}.${Date.now().toString(36)}${Math.random().toString(36).slice(2, 7)}`;
}

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Poll until `check()` is truthy. */
export async function eventually(check, { timeoutMs = 10000, what = "condition" } = {}) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const v = await check();
    if (v) return v;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await sleep(25);
  }
}
