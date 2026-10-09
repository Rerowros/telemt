"use strict";
// Executable regressions for the GET-only carrier page: every HTTPS route
// becomes a canonical query-encoded GET, uplink bodies fragment under the
// page-owned URL budget, and retries/cancellation keep the same contracts.
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const path = require("node:path");

const BOOTSTRAP = "A".repeat(43),
  SESSION = "B".repeat(43),
  URL_BYTES = 7168;

function renderedPage() {
  if (process.argv.includes("--stdin")) return fs.readFileSync(0, "utf8");
  let page = fs.readFileSync(path.join(__dirname, "document.html"), "utf8");
  const modules = {
    RESPONSE: "response",
    REQUEST: "request",
    BUFFER: "buffers",
    RECOVERY: "recovery",
    CONVEYOR: "conveyor",
    DOWNLINK: "downlink",
  };
  page = page.replace("__DIAGNOSTIC_RUNTIME__", "");
  for (const [key, file] of Object.entries(modules))
    if (page.includes("__" + key + "_RUNTIME__"))
      page = page.replace("__" + key + "_RUNTIME__", () =>
        fs.readFileSync(path.join(__dirname, file + ".js"), "utf8"),
      );
  page = page.replace("__RUNTIME__", () =>
    fs.readFileSync(path.join(__dirname, "runtime.js"), "utf8"),
  );
  const values = {
    BOOTSTRAP,
    HOST: "proxy.example.com",
    BASE_PREFIX: "",
    CARRIER_METHOD: "GET",
    NEGOTIATION_ENABLED: "true",
    CANDIDATE_COUNT: 4,
    CARRIER_DEADLINES: "3,5,8,12",
    LONG_POLL_SECS: 25,
    BRIDGE_REQUEST_SECS: 10,
    BRIDGE_RETRY_SECS: 90,
    BRIDGE_RECOVERY_SECS: 15,
    WEBSOCKET_OPEN_SECS: 15,
    RECONNECT_GRACE_SECS: 120,
    CARRIER_PROBE_COALESCE_MS: 0,
    GET_URL_BYTES: URL_BYTES,
    BATCH_LIMIT: 2097152,
    QUEUE_LIMIT: 33554432,
    QUEUE_ITEMS: 16384,
    MAX_STREAMS: 1024,
    STATUS_FUNCTION:
      "state=>{if(port&&!closed)port.postMessage({t:'status',state})}",
    HELLO_TIMEOUT_CALLBACK: "()=>fail('timeout')",
    PAGEHIDE_CALLBACK: "()=>close(true)",
  };
  return page.replace(/__([A-Z_]+)__;?/g, (all, key) =>
    key.startsWith("DIAGNOSTIC_") ? "" : String(values[key] ?? ""),
  );
}
function frame(type, id = 0, payload = []) {
  const data = new Uint8Array(8 + payload.length),
    view = new DataView(data.buffer);
  data[0] = type;
  data[1] = id >>> 16;
  data[2] = id >>> 8;
  data[3] = id;
  view.setUint32(4, payload.length);
  data.set(payload, 8);
  return data.buffer;
}
function join(...frames) {
  const data = new Uint8Array(frames.reduce((n, f) => n + f.byteLength, 0));
  let offset = 0;
  for (const f of frames) {
    data.set(new Uint8Array(f), offset);
    offset += f.byteLength;
  }
  return data.buffer;
}
function decode(encoded) {
  const bytes = Buffer.from(
    encoded.replace(/-/g, "+").replace(/_/g, "/"),
    "base64",
  );
  return new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.length);
}
async function flush() {
  for (let i = 0; i < 40; i++) await Promise.resolve();
}
function environment(page) {
  let now = 1000,
    nextTimer = 1;
  const timers = new Map(),
    events = new Map(),
    requests = [],
    received = [];
  const port = {
    onmessage: null,
    start() {},
    close() {},
    postMessage(value, transfer) {
      received.push(structuredClone(value, { transfer: transfer || [] }));
    },
  };
  const context = {
    ArrayBuffer,
    Uint8Array,
    DataView,
    TextDecoder,
    TextEncoder,
    URL,
    URLSearchParams,
    Headers,
    AbortController,
    ReadableStream,
    structuredClone,
    console,
    btoa: (binary) => Buffer.from(binary, "binary").toString("base64"),
    location: { hash: "", pathname: "/", search: "?bridge=test" },
    history: { replaceState() {} },
    parent: {},
    performance: { now: () => now },
    Date: class extends Date {
      static now() {
        return now;
      }
    },
    document: { visibilityState: "visible", addEventListener() {} },
    addEventListener: (name, fn) => events.set(name, fn),
    setTimeout: (fn, delay) => {
      const id = nextTimer++;
      timers.set(id, { fn, at: now + delay });
      return id;
    },
    clearTimeout: (id) => timers.delete(id),
    fetch: (url, options) =>
      new Promise((resolve, reject) => {
        const request = { url, options, resolve, reject };
        requests.push(request);
        options.signal?.addEventListener(
          "abort",
          () => reject(new Error("aborted")),
          { once: true },
        );
      }),
    WebSocket: class {
      static OPEN = 1;
      static CLOSING = 2;
      constructor() {
        throw new Error("unexpected websocket");
      }
    },
  };
  vm.createContext(context);
  for (const match of page.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g))
    vm.runInContext(match[1], context);
  events.get("message")({
    source: context.parent,
    origin: "http://127.0.0.1:12345",
    data: { t: "tproxy-init", v: 1 },
    ports: [port],
  });
  const send = (data) => port.onmessage({ data });
  const pending = (suffix) =>
    requests.filter((r) => pathname(r.url) === suffix && !r.answered);
  const answer = (request, status, body = null, headers = {}) => {
    request.answered = true;
    request.resolve({
      status,
      headers: new Headers(headers),
      body:
        body === null
          ? null
          : new ReadableStream({
              start(c) {
                c.enqueue(new Uint8Array(body));
                c.close();
              },
            }),
    });
  };
  return {
    context,
    send,
    pending,
    answer,
    received,
    requests,
    async tick(ms) {
      const until = now + ms;
      for (;;) {
        const entry = [...timers]
          .filter(([, t]) => t.at <= until)
          .sort((a, b) => a[1].at - b[1].at)[0];
        if (!entry) break;
        now = entry[1].at;
        timers.delete(entry[0]);
        entry[1].fn();
        await flush();
      }
      now = until;
      await flush();
    },
    close() {
      events.get("pagehide")();
    },
  };
}
function pathname(url) {
  return new URL(url).pathname;
}
function params(url) {
  return new URL(url).searchParams;
}
async function session(page, window = 4) {
  const env = environment(page);
  env.send(frame(16, 0, [1]));
  await flush();
  const request = env.pending("/api/v1/session")[0];
  assert.ok(request, "GET session requested");
  const headers = {
    "X-Session-Token": SESSION,
    "X-Down-Cursor": "0",
    "X-Carrier-Mode": "https",
    "X-Carrier-Attempt": "1",
    "X-Carrier-Candidate-Count": "4",
    "X-Carrier-Deadline": "12",
    "X-Carrier-State": "provisional",
  };
  if (window !== null) headers["X-Telemt-Up-Window"] = String(window);
  env.answer(request, 200, frame(17), headers);
  await flush();
  return env;
}
const tests = [];
function test(name, run) {
  tests.push({ name, run });
}
function ups(env) {
  return env.requests.filter((r) => pathname(r.url) === "/api/v1/up");
}
// Answers every pending fragment of the oldest unfinished uplink in order.
async function drain(env, sequence) {
  const parts = [];
  for (let step = 0; step < 8192; step++) {
    const request = ups(env).filter(
      (r) => !r.answered && params(r.url).get("s") === String(sequence),
    )[0];
    if (!request) {
      await flush();
      continue;
    }
    const query = params(request.url),
      part = query.get("p"),
      total = query.get("pn");
    const last = total === null || part === String(Number(total) - 1);
    env.answer(
      request,
      204,
      null,
      last
        ? {
            "X-Up-Ack": String(sequence),
            "X-Up-Part": part === null ? "0" : part,
          }
        : { "X-Up-Part": part },
    );
    parts.push(request);
    await flush();
    if (last) break;
  }
  return parts;
}
// Issues one uplink whose body needs several fragments.
async function fragmentedUp(env, payload) {
  env.send(join(frame(1, 1), frame(2, 1, payload)));
  await flush();
  const first = ups(env).filter(
    (r) => !r.answered && params(r.url).get("pn") !== null,
  )[0];
  assert.ok(first, "fragmented uplink started");
  return first;
}
function client(env, overrides = {}) {
  env.context.__settings = Object.assign(
    {
      base: () => "https://proxy.example.com",
      method: () => "GET",
      getUrlBytes: () => URL_BYTES,
      retryMs: () => 600,
      requestMs: () => 1000,
      longPollMs: () => 25000,
      batchLimit: () => 2097152,
      closed: () => false,
      cancel: () => {},
      retrying: () => {},
      reason: (error, fallback) => (error && error.telemtReason) || fallback,
      failure: (reason, message) =>
        Object.assign(new Error(message || reason), { telemtReason: reason }),
      read: async (response) => {
        try {
          if (response.body) await response.body.cancel();
        } catch (error) {}
        return null;
      },
    },
    overrides,
  );
  return vm.runInContext(
    "TelemtBridgeRequest.create(globalThis.__settings)",
    env.context,
  );
}

test("the session create request moves every carrier header into the query", async (page) => {
  const env = await session(page);
  const create = env.requests.find(
    (r) => pathname(r.url) === "/api/v1/session",
  );
  assert.equal(create.options.method, "GET");
  assert.ok(!create.options.body, "GET carries no request body");
  const query = params(create.url);
  assert.equal(query.get("t"), BOOTSTRAP);
  assert.match(query.get("n"), /^[1-9][0-9]*$/);
  assert.equal(
    query.get("k"),
    create.options.headers["X-Carrier-Capabilities"],
  );
  assert.equal(query.get("a"), create.options.headers["X-Carrier-Attempt"]);
  // Optional carrier headers appear in the query only when mirrored.
  for (const [header, key] of [
    ["X-Carrier-Failure", "f"],
    ["X-Telemt-Up-Window", "w"],
  ]) {
    if (create.options.headers[header] === undefined)
      assert.equal(query.get(key), null);
    else assert.equal(query.get(key), String(create.options.headers[header]));
  }
  assert.ok(query.get("d").length > 0);
  assert.deepEqual(
    new Uint8Array(decode(query.get("d"))),
    new Uint8Array(frame(16, 0, [1])),
  );
  // Mirrored headers stay on the wire for compatibility.
  assert.equal(create.options.headers.Authorization, "Bearer " + BOOTSTRAP);
  assert.ok(create.url.length <= URL_BYTES);
  env.close();
  await flush();
});

test("fragmented uplinks stay under the budget with ordered unique nonces", async (page) => {
  const env = await session(page);
  const payload = new Array(65536).fill(7);
  const sent = join(frame(1, 1), frame(2, 1, payload));
  env.send(sent);
  await flush();
  const parts = await drain(env, 1);
  const total = Number(params(parts[0].url).get("pn"));
  assert.ok(total > 3, "expected several fragments, got " + total);
  assert.equal(parts.length, total, "every fragment emitted exactly once");
  const seen = new Set(),
    reassembled = [];
  for (const [index, request] of parts.entries()) {
    assert.equal(request.options.method, "GET");
    assert.ok(!request.options.body);
    const query = params(request.url);
    assert.equal(query.get("t"), SESSION);
    assert.equal(query.get("s"), "1");
    assert.equal(query.get("p"), String(index));
    assert.equal(query.get("pn"), String(total));
    assert.ok(!seen.has(query.get("n")), "nonce reused");
    seen.add(query.get("n"));
    assert.ok(
      request.url.length <= URL_BYTES,
      "url over budget: " + request.url.length,
    );
    for (const byte of decode(query.get("d"))) reassembled.push(byte);
  }
  assert.deepEqual(new Uint8Array(reassembled), new Uint8Array(sent));
  env.close();
  await flush();
});

test("intermediate fragments reject acks, wrong indexes, and non-204 status", async (page) => {
  for (const invalid of ["ack", "index", "status"]) {
    const env = await session(page);
    const first = await fragmentedUp(env, new Array(8192).fill(3));
    if (invalid === "ack")
      env.answer(first, 204, null, { "X-Up-Part": "0", "X-Up-Ack": "0" });
    else if (invalid === "index")
      env.answer(first, 204, null, { "X-Up-Part": "1" });
    else env.answer(first, 200, new ArrayBuffer(0), { "X-Up-Part": "0" });
    await flush();
    // The operation must not advance to the next part on a malformed answer.
    assert.ok(
      !ups(env).some((r) => params(r.url).get("p") === "1"),
      invalid + " advanced the transfer",
    );
    env.close();
    await flush();
  }
});

test("a terminal intermediate response stops without sending remaining parts", async (page) => {
  const env = await session(page);
  const first = await fragmentedUp(env, new Array(8192).fill(5));
  env.answer(first, 404, null);
  await flush();
  // No later fragment of this operation was sent after the terminal answer.
  assert.ok(!ups(env).some((r) => params(r.url).get("p") === "1"));
  env.close();
  await flush();
});

test("a lost physical part retries with a fresh nonce and identical data", async (page) => {
  const env = await session(page);
  const first = await fragmentedUp(env, new Array(8192).fill(9));
  env.answer(first, 204, null, { "X-Up-Part": "0" });
  await flush();
  const failed = ups(env).filter((r) => !r.answered)[0];
  assert.ok(failed);
  assert.equal(params(failed.url).get("p"), "1");
  failed.answered = true;
  failed.reject(new Error("lost"));
  await flush();
  await env.tick(400);
  const replay = ups(env).filter((r) => !r.answered)[0];
  assert.ok(
    replay,
    "part 1 replayed, pending=" +
      ups(env).map((r) => params(r.url).get("p") + "/" + !!r.answered),
  );
  const before = params(failed.url),
    after = params(replay.url);
  assert.equal(after.get("p"), "1");
  assert.equal(after.get("d"), before.get("d"));
  assert.equal(after.get("s"), before.get("s"));
  assert.notEqual(after.get("n"), before.get("n"));
  const parts = await drain(env, 1);
  assert.ok(parts.length >= 1);
  env.close();
  await flush();
});

test("the downlink poll is a bodyless GET with cursor metadata", async (page) => {
  const env = await session(page);
  env.send(frame(1, 1));
  await flush();
  const down = env.pending("/api/v1/down")[0];
  assert.ok(down);
  assert.equal(down.options.method, "GET");
  assert.ok(!down.options.body);
  const query = params(down.url);
  assert.equal(query.get("t"), SESSION);
  assert.equal(query.get("c"), "0");
  assert.match(query.get("n"), /^[1-9][0-9]*$/);
  assert.ok(down.url.length <= URL_BYTES);
  env.answer(down, 204, null, { "X-Down-Cursor": "0" });
  await flush();
  const next = env.pending("/api/v1/down")[0];
  assert.ok(next, "long poll replays");
  assert.equal(params(next.url).get("c"), "0");
  env.close();
  await flush();
});

test("close uses one fire-and-forget op=close GET without a body", async (page) => {
  const env = await session(page);
  env.send(frame(1, 1));
  await flush();
  env.send({ t: "close" });
  await flush();
  const close = env.requests.find((r) => params(r.url).get("op") === "close");
  assert.ok(close, "close issued");
  assert.equal(close.options.method, "GET");
  assert.ok(!close.options.body);
  assert.equal(pathname(close.url), "/api/v1/session");
  assert.equal(params(close.url).get("t"), SESSION);
  env.close();
  await flush();
  assert.equal(
    env.requests.filter((r) => params(r.url).get("op") === "close").length,
    1,
  );
});

test("a multi-part uplink survives one loss per part inside the scaled budget", async (page) => {
  const env = environment(page);
  // retryMs*total must cover one failed attempt plus backoff on every part.
  const requestClient = client(env, { retryMs: () => 600 });
  const body = join(
    frame(2, 1, new Array(32768).fill(4)),
    frame(2, 1, new Array(32768).fill(6)),
  );
  const frozen = requestClient.options(
    "POST",
    SESSION,
    body,
    { "X-Up-Seq": "7" },
    null,
  );
  let done = false;
  const operation = requestClient.send("/api/v1/up", frozen, null, null, null);
  operation.then(
    () => {
      done = true;
    },
    () => {
      done = true;
    },
  );
  const failed = new Set();
  let elapsed = 0;
  for (let step = 0; step < 2000 && !done; step++) {
    const request = ups(env).filter((r) => !r.answered)[0];
    if (!request) {
      await env.tick(50);
      elapsed += 50;
      continue;
    }
    const query = params(request.url),
      part = query.get("p");
    if (!failed.has(part)) {
      failed.add(part);
      request.answered = true;
      request.reject(new Error("lost"));
      await flush();
      continue;
    }
    const last = part === String(Number(query.get("pn")) - 1);
    env.answer(
      request,
      204,
      null,
      last ? { "X-Up-Ack": "7", "X-Up-Part": part } : { "X-Up-Part": part },
    );
    await flush();
  }
  const response = await operation;
  assert.equal(response.status, 204);
  assert.ok(
    elapsed > 600,
    "loss per part outlasts one single-part retry budget",
  );
  assert.equal(
    failed.size,
    Number(params(ups(env)[0].url).get("pn")),
    "every part lost once",
  );
  const nonces = new Set(ups(env).map((r) => params(r.url).get("n")));
  assert.equal(
    nonces.size,
    ups(env).length,
    "every physical request owns a nonce",
  );
  env.close();
});

test("direct url construction throws before fetch when the budget is exceeded", async (page) => {
  const env = environment(page);
  const requestClient = client(env, { getUrlBytes: () => 1024 });
  const huge = new ArrayBuffer(4096);
  assert.throws(
    () =>
      requestClient.url(
        "/api/v1/session",
        requestClient.options("POST", BOOTSTRAP, huge, {}, null),
        false,
      ),
    /get url budget exceeded/,
  );
  // A budget below the query overhead leaves no room for even one raw byte.
  const tight = client(env, { getUrlBytes: () => 50 });
  const external = new env.context.AbortController();
  const frozen = tight.options(
    "POST",
    SESSION,
    new ArrayBuffer(64),
    { "X-Up-Seq": "1" },
    external.signal,
  );
  await assert.rejects(
    tight.send("/api/v1/up", frozen, null, null, null),
    /get url budget exceeded/,
  );
  assert.equal(ups(env).length, 0, "a budget failure never reaches fetch");
  external.abort();
  await flush();
  assert.equal(ups(env).length, 0, "no stale listener replays the request");
  env.close();
});

test("single-frame uplinks still travel as one canonical GET", async (page) => {
  const env = await session(page);
  env.send(frame(1, 1));
  await flush();
  const up = ups(env)[0];
  assert.ok(up);
  const query = params(up.url);
  assert.equal(query.get("s"), "1");
  assert.equal(query.get("p"), null, "pn=1 omits the part index");
  assert.equal(query.get("pn"), null);
  assert.deepEqual(
    new Uint8Array(decode(query.get("d"))),
    new Uint8Array(frame(1, 1)),
  );
  env.answer(up, 204, null, { "X-Up-Ack": "1" });
  await flush();
  env.close();
  await flush();
});

test("a one-megabyte batch round-trips through ordered GET fragments", async (page) => {
  const env = await session(page);
  const payload = new Uint8Array(1048576);
  for (let i = 0; i < payload.length; i++) payload[i] = i & 255;
  const first = Array.from(payload.subarray(0, 65536)),
    second = Array.from(payload.subarray(65536));
  const sent = join(frame(1, 1), frame(2, 1, first), frame(2, 1, second));
  env.send(sent);
  await flush();
  const parts = await drain(env, 1);
  const total = Number(params(parts[0].url).get("pn"));
  assert.ok(total > 100, "1 MiB must fragment, got " + total);
  const reassembled = [];
  for (const [index, request] of parts.entries()) {
    const query = params(request.url);
    assert.equal(query.get("p"), String(index));
    assert.equal(query.get("pn"), String(total));
    assert.ok(request.url.length <= URL_BYTES);
    for (const byte of decode(query.get("d"))) reassembled.push(byte);
  }
  assert.deepEqual(new Uint8Array(reassembled), new Uint8Array(sent));
  env.close();
  await flush();
});

test("GET diagnostics emit one query-encoded report and no POST sideband", async (page) => {
  const diagnostic = page.includes("TelemtBridgeDiagnostics");
  const env = environment(page);
  env.send(frame(16, 0, [1]));
  await flush();
  const reports = env.requests.filter(
    (r) => pathname(r.url) === "/api/v1/diagnostic",
  );
  if (!diagnostic) {
    assert.equal(reports.length, 0);
    env.close();
    await flush();
    return;
  }
  assert.ok(reports.length >= 1, "diagnostic reports emitted");
  const events = new Set();
  for (const report of reports) {
    const query = params(report.url);
    assert.equal(report.options.method, "GET");
    assert.ok(!report.options.body);
    assert.equal(query.get("t"), BOOTSTRAP);
    assert.match(query.get("n"), /^[1-9][0-9]*$/);
    const decoded = JSON.parse(
      new TextDecoder().decode(decode(query.get("d"))),
    );
    assert.equal(decoded.v, 1);
    assert.equal(typeof decoded.event, "string");
    events.add(decoded.event);
    assert.ok(report.url.length <= URL_BYTES);
  }
  assert.ok(events.has("runtime_started"));
  assert.ok(!env.requests.some((r) => r.options.method === "POST"));
  env.answer(env.pending("/api/v1/session")[0], 200, frame(17), {
    "X-Session-Token": SESSION,
    "X-Down-Cursor": "0",
    "X-Carrier-Mode": "https",
    "X-Carrier-Attempt": "1",
    "X-Carrier-Candidate-Count": "4",
    "X-Carrier-Deadline": "12",
    "X-Carrier-State": "provisional",
  });
  await flush();
  env.close();
  await flush();
});

test("recovery fetches the policy document with a unique nonce per call", async (page) => {
  const env = environment(page);
  // The page-owned request module supplies the GET nonce the runtime wires in.
  const get = env.context.TelemtBridgeRequest.get;
  env.context.__recovery = {
    budgetMs: () => 15000,
    requestMs: () => 10000,
    url: () => "https://proxy.example.com/?bridge=test",
    nonce: () => get.nonce(),
    token: () => SESSION,
    read: async () => new ArrayBuffer(0),
    cancel: () => {},
    status: () => {},
    restored: () => {},
    replace: async () => {},
    replaceable: () => true,
    reason: (error, fallback) => fallback,
    terminal: () => {},
  };
  const recovery = vm.runInContext(
    "TelemtBridgeRecovery.create(globalThis.__recovery)",
    env.context,
  );
  const first = recovery.recover("network", async () => {
    throw new Error("replay lost");
  });
  await flush();
  const document = env.requests.find(
    (r) => pathname(r.url) === "/" && params(r.url).get("bridge") === "test",
  );
  assert.ok(document, "recovery document requested");
  const nonce = params(document.url).get("n");
  assert.match(nonce || "", /^[1-9][0-9]*$/, "GET recovery carries a nonce");
  env.answer(document, 500, null);
  await first;
  const second = recovery.recover("network", null);
  await flush();
  const again = env.requests.filter(
    (r) => pathname(r.url) === "/" && params(r.url).get("bridge") === "test",
  )[1];
  assert.ok(again, "second recovery reloads the document");
  assert.notEqual(params(again.url).get("n"), nonce);
  recovery.cancel();
  await second;
  env.close();
  await flush();
});

(async () => {
  const page = renderedPage();
  let failed = 0;
  for (const { name, run } of tests) {
    let timer;
    try {
      await Promise.race([
        run(page),
        new Promise((_, reject) => {
          timer = setTimeout(
            () => reject(new Error("test made no bounded progress")),
            5000,
          );
        }),
      ]);
      console.log("ok - " + name);
    } catch (error) {
      failed++;
      console.error("not ok - " + name + "\n" + error.stack);
    } finally {
      clearTimeout(timer);
    }
  }
  if (failed) process.exitCode = 1;
})();
