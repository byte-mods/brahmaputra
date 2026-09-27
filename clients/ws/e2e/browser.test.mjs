// The whole architecture, in real browsers:
//
//   React / Vue / Angular trading screen (Chromium)
//        <-> brahmaputra-ws-gateway (x2, separate processes)
//        <-> Brahmaputra
//
// A market-data service writes prices straight into Brahmaputra; each UI
// shows them live (starting from the gateway's snapshot) and places orders
// that land in Brahmaputra stamped with the authenticated user. Also:
// authentication and authorization failures as a user sees them, a gateway
// restart under a live page, and many screens across two gateway instances
// sharing one broker feed per instance.
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import http from "node:http";
import path from "node:path";
import { after, before, describe, test } from "node:test";
import { fileURLToPath } from "node:url";

import { chromium } from "playwright-core";

import {
  eventually,
  mintToken,
  priceProducer,
  readTopic,
  sleep,
  startGateway,
  uniqueTopic,
} from "../testkit/index.mjs";

const here = path.dirname(fileURLToPath(import.meta.url));
const FRAMEWORKS = ["react", "vue", "angular"];

let server;
let base;
let browser;
let gw;
let gw2;
let prices;

function serve() {
  return new Promise((resolve) => {
    const s = http.createServer(async (req, res) => {
      const file = path.join(here, "dist", new URL(req.url, "http://x").pathname);
      if (!file.startsWith(path.join(here, "dist"))) return res.writeHead(403).end();
      try {
        const body = await readFile(file);
        const type = file.endsWith(".html") ? "text/html" : "text/javascript";
        res.writeHead(200, { "content-type": type }).end(body);
      } catch {
        res.writeHead(404).end();
      }
    });
    s.listen(0, "127.0.0.1", () => resolve(s));
  });
}

before(async () => {
  server = await serve();
  base = `http://127.0.0.1:${server.address().port}`;
  browser = await chromium.launch({
    executablePath: process.env.CHROMIUM ?? "/opt/pw-browsers/chromium",
    args: ["--no-sandbox"],
  });
  gw = await startGateway();
  gw2 = await startGateway();
  prices = await priceProducer();
});

after(async () => {
  await browser?.close();
  server?.close();
  await prices?.close();
  await gw?.stop();
  await gw2?.stop();
});

async function open(framework, { gateway = gw, token, pricesTopic, ordersTopic, symbols = "AAPL,MSFT" }) {
  const page = await browser.newPage();
  const errors = [];
  page.on("pageerror", (e) => errors.push(String(e)));
  const q = new URLSearchParams({
    gw: gateway.url,
    token,
    prices: pricesTopic,
    orders: ordersTopic ?? uniqueTopic("orders"),
    symbols,
  });
  await page.goto(`${base}/${framework}/index.html?${q}`);
  page.errors = errors;
  return page;
}

const text = (page, selector) => page.locator(selector).first().textContent({ timeout: 10000 });

async function expectText(page, selector, expected, timeoutMs = 10000) {
  await eventually(
    async () => {
      const t = (await page.locator(selector).first().textContent({ timeout: 1000 }).catch(() => null))?.trim();
      return expected instanceof RegExp ? expected.test(t ?? "") : t === expected;
    },
    { timeoutMs, what: `${selector} = ${expected}` },
  ).catch(async (e) => {
    throw new Error(`${e.message}; was ${JSON.stringify(await text(page, selector).catch(() => null))}`);
  });
}

const priceCell = (symbol) => `tr[data-symbol="${symbol}"] .price`;

for (const framework of FRAMEWORKS) {
  describe(`${framework} trading screen`, () => {
    test("shows the snapshot and live prices, and places orders into Brahmaputra", async () => {
      const pricesTopic = uniqueTopic("prices");
      const ordersTopic = uniqueTopic("orders");
      await prices.tick(pricesTopic, "AAPL", 100);
      await prices.tick(pricesTopic, "MSFT", 200);
      await prices.tick(pricesTopic, "AAPL", 100.25);

      const user = `trader-${framework}`;
      const token = mintToken(user, { topics: ["orders.*"], subscribe: ["prices.*"] });
      const page = await open(framework, { token, pricesTopic, ordersTopic });
      await expectText(page, "#state", "open");
      // The snapshot: prices from before the page existed.
      await expectText(page, priceCell("AAPL"), "100.25");
      await expectText(page, priceCell("MSFT"), "200.00");

      // Live ticks, including a symbol the page did not list.
      await prices.tick(pricesTopic, "AAPL", 101.5);
      await prices.tick(pricesTopic, "NVDA", 950);
      await expectText(page, priceCell("AAPL"), "101.50");
      await expectText(page, priceCell("NVDA"), "950.00");
      await expectText(page, "#tape li:last-child", "NVDA 950.00");

      // An order from the UI goes through the gateway into Brahmaputra.
      await page.click('button[data-buy="MSFT"]');
      await expectText(page, "#last-order", /^\d+:\d+$/);
      const [partition, offset] = (await text(page, "#last-order")).split(":").map(Number);
      const orders = await readTopic(ordersTopic);
      assert.equal(orders.length, 1);
      const order = orders[0];
      assert.equal(order.partition, partition);
      assert.equal(Number(order.offset), offset);
      assert.deepEqual(JSON.parse(String(order.value)), { symbol: "MSFT", qty: 1 });
      const userHeader = order.headers.find((h) => h.key === "x-gw-user");
      assert.equal(String(userHeader.value), user, "the order carries the authenticated user");
      assert.equal(await text(page, "#order-error"), "");
      assert.deepEqual(page.errors, []);
      await page.close();
    });

    test("authorization failures are visible to the user", async () => {
      const pricesTopic = uniqueTopic("prices");
      // May subscribe to orders only: the price feed is refused, and so is
      // placing an order (no publish rights).
      const token = mintToken(`limited-${framework}`, { topics: [], subscribe: ["orders.*"] });
      const page = await open(framework, { token, pricesTopic });
      await expectText(page, "#state", "open");
      await expectText(page, "#feed-error", "TOPIC_NOT_ALLOWED");
      await page.click('button[data-buy="AAPL"]');
      await expectText(page, "#order-error", "TOPIC_NOT_ALLOWED");
      await page.close();

      // A forged token never gets a connection.
      const forged = mintToken("mallory", { secret: "an-entirely-different-secret-value" });
      const denied = await open(framework, { token: forged, pricesTopic });
      await sleep(1500);
      assert.notEqual((await text(denied, "#state")).trim(), "open");
      await denied.close();
    });

    test("a gateway restart under a live page: it reconnects and catches up", async () => {
      const pricesTopic = uniqueTopic("prices");
      await prices.tick(pricesTopic, "AAPL", 10);
      const token = mintToken(`resilient-${framework}`, { topics: ["orders.*"], subscribe: ["prices.*"] });
      const page = await open(framework, { token, pricesTopic });
      await expectText(page, priceCell("AAPL"), "10.00");

      await gw.stop();
      await expectText(page, "#state", "reconnecting");
      // The market keeps moving while the page is offline...
      await prices.tick(pricesTopic, "AAPL", 11);
      gw = await startGateway([], { port: gw.port, httpPort: gw.httpPort });
      await expectText(page, "#state", "open", 15000);
      // ...and the resubscription's snapshot brings the page up to date.
      await expectText(page, priceCell("AAPL"), "11.00");
      await prices.tick(pricesTopic, "AAPL", 12);
      await expectText(page, priceCell("AAPL"), "12.00");
      assert.deepEqual(page.errors, []);
      await page.close();
    });
  });
}

describe("scaling out", () => {
  test("many screens on two gateway instances: every one updates, each gateway reads the feed once", async () => {
    const pricesTopic = uniqueTopic("prices");
    await prices.tick(pricesTopic, "AAPL", 1);
    const token = mintToken("viewer", { topics: [], subscribe: ["prices.*"] });
    const PAGES = 12;
    const pages = [];
    for (let i = 0; i < PAGES; i++) {
      const framework = FRAMEWORKS[i % FRAMEWORKS.length];
      pages.push(await open(framework, { gateway: i % 2 ? gw2 : gw, token, pricesTopic }));
    }
    for (const page of pages) await expectText(page, priceCell("AAPL"), "1.00");
    const started = Date.now();
    await prices.tick(pricesTopic, "AAPL", 2);
    await Promise.all(pages.map((page) => expectText(page, priceCell("AAPL"), "2.00")));
    const fanout = Date.now() - started;
    for (const g of [gw, gw2]) {
      const m = await g.metrics();
      assert.ok(m.ws_feeds_active >= 1, "a feed per instance");
      assert.ok(m.ws_subscriptions_active >= PAGES / 2, `subscriptions: ${m.ws_subscriptions_active}`);
      assert.ok(m.ws_feed_records_total >= 1);
    }
    console.log(`# ${PAGES} browser screens on 2 gateways updated within ${fanout} ms of the tick`);
    for (const page of pages) await page.close();
  });
});
