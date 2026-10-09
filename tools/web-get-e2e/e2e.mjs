// GET-carrier browser fixture driver: probes the GET-only edge, then runs a
// real bridge page through MessageChannel init, HELLO, OPEN/DATA and a 1 MiB
// bytewise roundtrip, measures one held 30 s downlink poll, and audits that
// every carrier request is a GET without a body within the URL budget.
//
// usage: node e2e.mjs <capability-b64> [carrier] [outDir]
// Credentials are never logged: audit entries keep method/path/length only.

import { chromium } from "playwright";
import fs from "node:fs";
import https from "node:https";
import path from "node:path";

const [cap, carrier = "https", outDir = "artifacts"] = process.argv.slice(2);
if (!cap) {
  console.error("usage: node e2e.mjs <capability-b64> [carrier] [outDir]");
  process.exit(2);
}
fs.mkdirSync(outDir, { recursive: true });

const ORIGIN = "https://proxy.example.com";
const PARENT = "http://127.0.0.1:18000";
const URL_BUDGET = 7168;
const TOTAL = 1024 * 1024;
const CHUNK = 256 * 1024;
const LONG_POLL_MIN_MS = 29000;
const LONG_POLL_MAX_MS = 45000;

const results = {
  carrier,
  probes: {},
  audit: { requests: 0, apiRequests: 0, violations: [], failed: [] },
  roundtrip: null,
  longPollMs: null,
  ok: false,
};
const failures = [];

function edgeProbe(method, reqPath, headers = {}) {
  return new Promise((resolve) => {
    const req = https.request(
      {
        host: "127.0.0.1",
        port: 443,
        method,
        path: reqPath,
        headers: { Host: "proxy.example.com", Connection: "close", ...headers },
        rejectUnauthorized: false,
        timeout: 15000,
      },
      (res) => {
        res.resume();
        res.on("end", () => resolve(res.statusCode));
      },
    );
    req.on("timeout", () => {
      req.destroy();
      resolve("timeout");
    });
    req.on("error", (error) => resolve(`error:${error.code || error.message}`));
    req.end();
  });
}

async function main() {
  // Edge probes run before any carrier traffic.
  results.probes.get_root = await edgeProbe("GET", "/");
  results.probes.post = await edgeProbe("POST", "/api/v1/session");
  results.probes.put = await edgeProbe("PUT", "/api/v1/up");
  results.probes.delete = await edgeProbe("DELETE", "/api/v1/session");
  results.probes.head = await edgeProbe("HEAD", "/");
  results.probes.long_uri = await edgeProbe("GET", "/?" + "x".repeat(9000));
  results.probes.big_header = await edgeProbe("GET", "/", {
    "X-Big": "a".repeat(9000),
  });

  const browser = await chromium.launch({
    args: ["--host-resolver-rules=MAP proxy.example.com 127.0.0.1"],
  });
  try {
    const context = await browser.newContext({ ignoreHTTPSErrors: true });
    const page = await context.newPage();
    const consoleLog = fs.createWriteStream(
      path.join(outDir, `console-${carrier}.log`),
    );
    page.on("console", (msg) =>
      consoleLog.write(`[${msg.type()}] ${msg.text()}\n`),
    );

    const pending = new Map();
    const downPolls = [];
    const audit = [];
    const settlements = [];
    let closingAt = Infinity;
    // The bridge aborts each GET fetch once response headers arrive (204
    // replies carry no body), which Chromium reports as ERR_ABORTED even
    // though the request reached the server and was answered. A failure only
    // counts when no response was received at all.
    const settle = (req, finished) =>
      settlements.push(
        (async () => {
          const url = new URL(req.url());
          if (url.hostname !== "proxy.example.com") return;
          const started = pending.get(req);
          const response = await req.response().catch(() => null);
          if (url.pathname.endsWith("/api/v1/down") && started && response) {
            downPolls.push({
              started,
              ms: Date.now() - started,
              status: response.status(),
            });
          }
          if (!finished && !response && Date.now() < closingAt) {
            failures.push({
              path: url.pathname,
              reason: req.failure()?.errorText || "?",
            });
          }
        })(),
      );
    page.on("request", (req) => {
      const url = new URL(req.url());
      if (url.hostname !== "proxy.example.com") return;
      const entry = {
        method: req.method(),
        path: url.pathname,
        urlBytes: req.url().length,
        body: req.postData() !== null,
      };
      audit.push(entry);
      pending.set(req, Date.now());
    });
    page.on("requestfinished", (req) => settle(req, true));
    page.on("requestfailed", (req) => settle(req, false));

    await page.goto(
      `${PARENT}/?cap=${encodeURIComponent(cap)}&origin=${encodeURIComponent(ORIGIN)}&base=${encodeURIComponent("/")}`,
    );
    await page.evaluate(() => window.__e2e.start());
    await page.evaluate(() => window.__e2e.hello());
    // The carrier commits on the first uplink probe, so open the stream and
    // push one DATA chunk before waiting for the connected status.
    await page.evaluate(() => window.__e2e.open(1));
    await page.evaluate(
      (chunk) => window.__e2e.data(1, window.__e2e.makePayload(chunk)),
      CHUNK,
    );
    await page.evaluate(() =>
      window.__e2e.waitFor(() => window.__e2e.status === "connected", 60000),
    );
    await page.evaluate(
      ({ total, chunk }) => {
        const payload = window.__e2e.makePayload(total);
        for (let offset = chunk; offset < total; offset += chunk) {
          window.__e2e.data(1, payload.subarray(offset, offset + chunk));
        }
      },
      { total: TOTAL, chunk: CHUNK },
    );
    await page.evaluate(
      (total) =>
        window.__e2e.waitFor(() => window.__e2e.downBytes >= total, 180000),
      TOTAL,
    );
    results.roundtrip = await page.evaluate((total) => {
      const got = window.__e2e.collectData();
      const want = window.__e2e.makePayload(total);
      if (got.length !== want.length)
        return `length ${got.length} != ${want.length}`;
      for (let i = 0; i < want.length; i++) {
        if (got[i] !== want[i]) return `byte ${i}: ${got[i]} != ${want[i]}`;
      }
      return `ok ${want.length} bytes`;
    }, TOTAL);

    // With the stream idle, the next /api/v1/down poll is held for the
    // configured 30 s long-poll window before answering.
    const idleStart = Date.now();
    const held = await new Promise((resolve) => {
      const check = setInterval(() => {
        const found = downPolls.find(
          (poll) =>
            poll.started > idleStart - 60000 && poll.ms >= LONG_POLL_MIN_MS,
        );
        if (found || Date.now() - idleStart > LONG_POLL_MAX_MS + 20000) {
          clearInterval(check);
          resolve(found || null);
        }
      }, 500);
    });
    results.longPollMs = held ? held.ms : null;

    closingAt = Date.now();
    await page.evaluate(() => window.__e2e.closeSession());
    await page.waitForTimeout(2000);
    await Promise.all(settlements);

    for (const entry of audit) {
      if (!entry.path.includes("/api/")) continue;
      if (entry.method !== "GET") {
        results.audit.violations.push(`non-GET ${entry.method} ${entry.path}`);
      }
      if (entry.body) results.audit.violations.push(`body on ${entry.path}`);
      if (entry.urlBytes > URL_BUDGET) {
        results.audit.violations.push(
          `url ${entry.urlBytes}B > ${URL_BUDGET} on ${entry.path}`,
        );
      }
    }
    results.audit.requests = audit.length;
    results.audit.apiRequests = audit.filter((e) =>
      e.path.includes("/api/"),
    ).length;
    results.audit.failed = failures;
  } finally {
    await browser.close();
  }

  const probes = results.probes;
  const checks = [
    ["GET root decoy", probes.get_root === 200],
    ["POST rejected", probes.post === 403 || probes.post === 405],
    ["PUT rejected", probes.put === 403 || probes.put === 405],
    ["DELETE rejected", probes.delete === 403 || probes.delete === 405],
    ["HEAD rejected", probes.head === 405],
    ["long URI -> 414", probes.long_uri === 414],
    ["big header -> 400", probes.big_header === 400],
    ["1MiB roundtrip", String(results.roundtrip).startsWith("ok")],
    [
      "30s long poll",
      results.longPollMs !== null && results.longPollMs >= LONG_POLL_MIN_MS,
    ],
    ["request audit clean", results.audit.violations.length === 0],
    ["no aborted requests", results.audit.failed.length === 0],
  ];
  results.ok = checks.every(([, pass]) => pass);
  for (const [name, pass] of checks) {
    console.log(`${pass ? "PASS" : "FAIL"} ${name}`);
  }
  console.log(`probes=${JSON.stringify(probes)}`);
  console.log(
    `requests=${results.audit.requests} api=${results.audit.apiRequests} longPollMs=${results.longPollMs}`,
  );
  console.log(`roundtrip=${results.roundtrip}`);
}

try {
  await main();
} catch (error) {
  console.error(`e2e error: ${(error && error.stack) || error}`);
  results.ok = false;
}
fs.writeFileSync(
  path.join(outDir, `results-${carrier}.json`),
  JSON.stringify(results, null, 2) + "\n",
);
process.exit(results.ok ? 0 : 1);
