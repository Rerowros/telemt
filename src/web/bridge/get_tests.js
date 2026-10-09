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

let stdinPage = null;
function renderedPage(overrides = {}) {
  if (process.argv.includes("--stdin")) {
    if (stdinPage === null) stdinPage = fs.readFileSync(0, "utf8");
    let page = stdinPage;
    if (overrides.GET_PARALLEL_PARTS !== undefined)
      page = page.replace(
        /const getParallelParts=\d+;/,
        `const getParallelParts=${overrides.GET_PARALLEL_PARTS};`,
      );
    return page;
  }
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
  const values = Object.assign(
    {
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
      GET_PARALLEL_PARTS: 6,
      BATCH_LIMIT: 2097152,
      QUEUE_LIMIT: 33554432,
      QUEUE_ITEMS: 16384,
      MAX_STREAMS: 1024,
      STATUS_FUNCTION:
        "state=>{if(port&&!closed)port.postMessage({t:'status',state})}",
      HELLO_TIMEOUT_CALLBACK: "()=>fail('timeout')",
      PAGEHIDE_CALLBACK: "()=>close(true)",
    },
    overrides,
  );
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
    nextTimer = 1,
    resourceEntries = [];
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
    performance: {
      now: () => now,
      getEntriesByType: () => resourceEntries,
    },
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
    setProtocol: (protocol) => {
      resourceEntries = [
        {
          name: "https://proxy.example.com/api/v1/up",
          nextHopProtocol: protocol,
        },
      ];
    },
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

test("intermediate fragments reject acks, wrong indexes, and non-204 status", async () => {
  const page = renderedPage({ GET_PARALLEL_PARTS: 1 });
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

test("a terminal intermediate response stops without sending remaining parts", async () => {
  const page = renderedPage({ GET_PARALLEL_PARTS: 1 });
  const env = await session(page);
  const first = await fragmentedUp(env, new Array(8192).fill(5));
  env.answer(first, 404, null);
  await flush();
  // No later fragment of this operation was sent after the terminal answer.
  assert.ok(!ups(env).some((r) => params(r.url).get("p") === "1"));
  env.close();
  await flush();
});

test("a lost physical part retries with a fresh nonce and identical data", async () => {
  const page = renderedPage({ GET_PARALLEL_PARTS: 1 });
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

test("a multi-part uplink survives repeated loss per part inside the scaled budget", async (page) => {
  const env = environment(page);
  // retryMs*total must cover one failed attempt plus backoff on every part;
  // the first loss of each part is reissued at once, the second backs off.
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
  const failed = new Map();
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
    if ((failed.get(part) || 0) < 2) {
      failed.set(part, (failed.get(part) || 0) + 1);
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
    "every part lost",
  );
  assert.ok(
    [...failed.values()].every((losses) => losses === 2),
    "every part lost twice",
  );
  const nonces = new Set(ups(env).map((r) => params(r.url).get("n")));
  assert.equal(
    nonces.size,
    ups(env).length,
    "every physical request owns a nonce",
  );
  env.close();
});

test("a transport failure reissues a fragment or poll once before recovery", async () => {
  const env = environment(renderedPage());
  // Committed carriers allow one attempt per request; the reissue is extra.
  const requestClient = client(env, { parallelParts: () => 1 });
  const body = join(
    frame(2, 1, new Array(6000).fill(71)),
    frame(2, 1, new Array(6000).fill(72)),
  );
  const operation = requestClient.send(
    "/api/v1/up",
    requestClient.options("POST", SESSION, body, { "X-Up-Seq": "7" }, null),
    null,
    1,
    null,
  );
  let failure = null;
  operation.catch((error) => {
    failure = error;
  });
  await flush();
  const lost = ups(env).filter((r) => !r.answered)[0];
  lost.answered = true;
  lost.reject(new TypeError("Failed to fetch"));
  await flush();
  const reissued = ups(env).filter((r) => !r.answered)[0];
  assert.ok(reissued, "the fragment is reissued without waiting for a timer");
  assert.equal(params(reissued.url).get("p"), params(lost.url).get("p"));
  assert.equal(params(reissued.url).get("d"), params(lost.url).get("d"));
  assert.notEqual(params(reissued.url).get("n"), params(lost.url).get("n"));
  // A second transport failure of the same request escalates as before.
  reissued.answered = true;
  reissued.reject(new TypeError("Failed to fetch"));
  await flush();
  assert.ok(failure, "the operation fails over to recovery");
  assert.equal(failure.telemtReason, "network");
  assert.equal(ups(env).filter((r) => !r.answered).length, 0);

  const poll = requestClient.send(
    "/api/v1/down",
    requestClient.options(
      "POST",
      SESSION,
      null,
      { "X-Down-Cursor": "0" },
      null,
    ),
    null,
    1,
    null,
  );
  await flush();
  const first = env.pending("/api/v1/down")[0];
  first.answered = true;
  first.reject(new TypeError("Failed to fetch"));
  await flush();
  const again = env.pending("/api/v1/down")[0];
  assert.ok(again, "the poll is reissued once");
  assert.equal(params(again.url).get("c"), "0");
  env.answer(again, 204, null, {});
  assert.equal((await poll).status, 204);
  env.close();
  await flush();
});

test("a stalled fragment is reissued once while a held closing part escalates", async () => {
  const env = environment(renderedPage());
  // The operation budget outlasts both request deadlines.
  const requestClient = client(env, {
    parallelParts: () => 1,
    retryMs: () => 10000,
  });
  // One 6 KiB frame needs exactly two fragments: part 0 and the final.
  const body = frame(2, 1, new Array(6000).fill(73));
  const operation = requestClient.send(
    "/api/v1/up",
    requestClient.options("POST", SESSION, body, { "X-Up-Seq": "7" }, null),
    null,
    1,
    null,
  );
  let failure = null;
  operation.catch((error) => {
    failure = error;
  });
  await flush();
  const stalled = ups(env).filter((r) => !r.answered)[0];
  assert.equal(params(stalled.url).get("pn"), "2");
  assert.equal(params(stalled.url).get("p"), "0");
  // The fragment never answers; its request deadline fires.
  await env.tick(1000);
  assert.ok(stalled.options.signal.aborted, "the stalled fragment is aborted");
  const reissued = ups(env).filter(
    (r) => !r.answered && !r.options.signal.aborted,
  )[0];
  assert.ok(reissued, "the fragment is reissued instead of failing the op");
  assert.equal(params(reissued.url).get("p"), "0");
  assert.equal(failure, null);
  env.answer(reissued, 204, null, { "X-Up-Part": "0" });
  await flush();
  const closing = ups(env).filter(
    (r) => !r.answered && !r.options.signal.aborted,
  )[0];
  assert.equal(params(closing.url).get("p"), "1");
  // A closing part may be held server-side behind an earlier sequence, so
  // its timeout keeps escalating to recovery.
  await env.tick(1000);
  await flush();
  assert.ok(failure, "the held closing part escalates");
  assert.equal(failure.telemtReason, "timeout");
  env.close();
  await flush();
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

test("non-final parts fly K-wide and the final part waits for all acks", async () => {
  const page = renderedPage({ GET_PARALLEL_PARTS: 3 });
  const env = await session(page);
  env.send(join(frame(1, 1), frame(2, 1, new Array(48 * 1024).fill(11))));
  await flush();
  const inflight = () => ups(env).filter((r) => !r.answered);
  assert.equal(inflight().length, 3, "K workers run parts concurrently");
  const total = Number(params(inflight()[0].url).get("pn"));
  assert.ok(total > 4, "body must exceed the worker count");
  for (let part = 0; part < total - 1; ) {
    const request = inflight()[0];
    assert.ok(request, "a worker is always in flight");
    const index = Number(params(request.url).get("p"));
    assert.equal(index, part, "parts issue in index order");
    env.answer(request, 204, null, { "X-Up-Part": String(index) });
    await flush();
    part++;
  }
  const last = inflight().find(
    (r) => params(r.url).get("p") === String(total - 1),
  );
  assert.ok(last, "final part issues after all non-final acks");
  env.answer(last, 204, null, {
    "X-Up-Ack": "1",
    "X-Up-Part": String(total - 1),
  });
  await flush();
  env.close();
  await flush();
});

test("a terminal answer aborts siblings and returns the response", async () => {
  const env = environment(renderedPage());
  const requestClient = client(env, { parallelParts: () => 3 });
  const body = join(
    frame(2, 1, new Array(48 * 1024).fill(12)),
    frame(2, 1, new Array(48 * 1024).fill(14)),
  );
  const operation = requestClient.send(
    "/api/v1/up",
    requestClient.options("POST", SESSION, body, { "X-Up-Seq": "7" }, null),
    null,
    null,
    null,
  );
  await flush();
  const inflight = () => ups(env).filter((r) => !r.answered);
  assert.equal(inflight().length, 3);
  const total = Number(params(inflight()[0].url).get("pn"));
  assert.ok(total > 4, "parts must outlast the terminal answer");
  env.answer(inflight()[0], 404, null);
  const response = await operation;
  assert.equal(response.status, 404);
  await flush();
  const aborted = ups(env).filter(
    (r) => !r.answered && r.options.signal.aborted,
  );
  assert.equal(aborted.length, 2, "sibling fetches abort on a terminal answer");
  assert.ok(
    !ups(env).some((r) => Number(params(r.url).get("p")) >= 3),
    "no fresh part issued after the terminal answer",
  );
  env.close();
  await flush();
});

test("one part retry keeps sibling parts flowing", async () => {
  const page = renderedPage({ GET_PARALLEL_PARTS: 3 });
  const env = await session(page);
  env.send(join(frame(1, 1), frame(2, 1, new Array(48 * 1024).fill(13))));
  await flush();
  const inflight = () => ups(env).filter((r) => !r.answered);
  const first = inflight()[0];
  env.answer(first, 204, null, { "X-Up-Part": "0" });
  await flush();
  const failed = inflight()[0];
  assert.equal(params(failed.url).get("p"), "1");
  failed.answered = true;
  failed.reject(new Error("lost"));
  await flush();
  assert.ok(
    inflight().some((r) => params(r.url).get("p") !== "1"),
    "siblings keep flowing during a part retry",
  );
  await env.tick(400);
  const replay = inflight().find((r) => params(r.url).get("p") === "1");
  assert.ok(replay, "part 1 retried");
  assert.equal(params(replay.url).get("d"), params(failed.url).get("d"));
  assert.notEqual(params(replay.url).get("n"), params(failed.url).get("n"));
  env.close();
  await flush();
});

test("a 503 fragment answer retries inside the same operation", async () => {
  const env = environment(renderedPage());
  const requestClient = client(env, { parallelParts: () => 3 });
  const body = join(
    frame(2, 1, new Array(48 * 1024).fill(15)),
    frame(2, 1, new Array(48 * 1024).fill(16)),
  );
  const operation = requestClient.send(
    "/api/v1/up",
    requestClient.options("POST", SESSION, body, { "X-Up-Seq": "7" }, null),
    null,
    null,
    null,
  );
  operation.catch(() => {});
  await flush();
  const inflight = () => ups(env).filter((r) => !r.answered);
  const busy = inflight()[0];
  const part = params(busy.url).get("p");
  const sessionsBefore = env.requests.filter(
    (r) => pathname(r.url) === "/api/v1/session",
  ).length;
  env.answer(busy, 503, null);
  await flush();
  await env.tick(400);
  await flush();
  const retry = inflight().find((r) => params(r.url).get("p") === part);
  assert.ok(retry, "a busy fragment retries inside the operation");
  assert.notEqual(params(retry.url).get("n"), params(busy.url).get("n"));
  assert.equal(
    env.requests.filter((r) => pathname(r.url) === "/api/v1/session").length,
    sessionsBefore,
    "503 on a fragment never triggers a session recovery",
  );
  env.close();
  await flush();
});

test("the h1.1 scheduler leaves connections for an active long poll", async () => {
  const env = environment(renderedPage());
  const requestClient = client(env, { parallelParts: () => 8 });
  // One held GET down poll counts against the six-connection envelope.
  const poll = requestClient.send(
    "/api/v1/down",
    requestClient.options(
      "POST",
      SESSION,
      null,
      { "X-Down-Cursor": "0" },
      null,
    ),
    null,
    1,
    null,
  );
  poll.catch(() => {});
  await flush();
  assert.equal(env.pending("/api/v1/down").length, 1);
  const body = join(
    frame(2, 1, new Array(48 * 1024).fill(21)),
    frame(2, 1, new Array(48 * 1024).fill(22)),
  );
  const operation = requestClient.send(
    "/api/v1/up",
    requestClient.options("POST", SESSION, body, { "X-Up-Seq": "7" }, null),
    null,
    null,
    null,
  );
  operation.catch(() => {});
  await flush();
  const inflight = () => ups(env).filter((r) => !r.answered);
  assert.equal(
    inflight().length,
    4,
    "cap is 6 - 1 long poll - 1 reserve despite K=8",
  );
  const request = inflight()[0];
  env.answer(request, 204, null, {
    "X-Up-Part": params(request.url).get("p"),
  });
  await flush();
  assert.ok(
    inflight().length <= 4,
    "grants stay bounded while the poll is held",
  );
  env.close();
  await flush();
});

test("each held long poll tightens the h1.1 parts cap", async () => {
  const env = environment(renderedPage());
  const requestClient = client(env, { parallelParts: () => 8 });
  for (const cursor of ["0", "7"]) {
    const poll = requestClient.send(
      "/api/v1/down",
      requestClient.options(
        "POST",
        SESSION,
        null,
        { "X-Down-Cursor": cursor },
        null,
      ),
      null,
      1,
      null,
    );
    poll.catch(() => {});
  }
  await flush();
  assert.equal(env.pending("/api/v1/down").length, 2);
  const body = join(
    frame(2, 1, new Array(48 * 1024).fill(23)),
    frame(2, 1, new Array(48 * 1024).fill(24)),
  );
  const operation = requestClient.send(
    "/api/v1/up",
    requestClient.options("POST", SESSION, body, { "X-Up-Seq": "7" }, null),
    null,
    null,
    null,
  );
  operation.catch(() => {});
  await flush();
  assert.equal(
    ups(env).filter((r) => !r.answered).length,
    3,
    "cap is 6 - 2 long polls - 1 reserve",
  );
  env.close();
  await flush();
});

test("single-part and final uplink parts bypass the parts pool", async () => {
  // Unknown protocol keeps the conservative h1.1 envelope: cap = 6 - 0 - 1.
  const env = environment(renderedPage());
  const requestClient = client(env, { parallelParts: () => 6 });
  const bulk = (marker) =>
    join(
      frame(2, 1, new Array(48 * 1024).fill(marker)),
      frame(2, 1, new Array(48 * 1024).fill(marker + 1)),
    );
  const uplink = (sequence, body) => {
    const operation = requestClient.send(
      "/api/v1/up",
      requestClient.options(
        "POST",
        SESSION,
        body,
        { "X-Up-Seq": String(sequence) },
        null,
      ),
      null,
      null,
      null,
    );
    operation.catch(() => {});
    return operation;
  };
  const opA = uplink(7, bulk(41));
  await flush();
  const inflight = () => ups(env).filter((r) => !r.answered);
  assert.equal(inflight().length, 5, "five parts hold the h1.1 cap");
  // A single-part operation never queues behind bulk fragments.
  const ping = uplink(9, frame(5, 1, [9]));
  await flush();
  const single = inflight().find((r) => params(r.url).get("s") === "9");
  assert.ok(single, "single-part op dispatched over a saturated pool");
  assert.equal(params(single.url).get("pn"), null);
  env.answer(single, 204, null, { "X-Up-Ack": "9", "X-Up-Part": "0" });
  await flush();
  const pingResponse = await ping;
  assert.equal(pingResponse.status, 204);
  // Drain op A down to its last non-final part, then let op B's workers flood
  // the pool: the closing part must still dispatch without waiting for a slot.
  const seqA = (r) => params(r.url).get("s") === "7";
  const total = Number(params(inflight().find(seqA).url).get("pn"));
  const lastIndex = String(total - 1);
  const nonFinalA = (r) => seqA(r) && params(r.url).get("p") !== lastIndex;
  let acked = 0;
  for (let step = 0; step < 4096 && acked < total - 2; step++) {
    const pending = inflight().filter(nonFinalA);
    if (pending.length) {
      env.answer(pending[0], 204, null, {
        "X-Up-Part": params(pending[0].url).get("p"),
      });
      acked++;
    }
    await flush();
  }
  await flush();
  const lastNonFinal = inflight().find(nonFinalA);
  assert.ok(lastNonFinal, "exactly one non-final part is outstanding");
  uplink(8, bulk(43));
  await flush();
  env.answer(lastNonFinal, 204, null, {
    "X-Up-Part": params(lastNonFinal.url).get("p"),
  });
  await flush();
  const final = inflight().find(
    (r) => seqA(r) && params(r.url).get("p") === lastIndex,
  );
  assert.ok(final, "final part dispatched while the pool stays saturated");
  // The last acknowledged fragment hands its connection to the closing part
  // and the in-flight final then shrinks the pool, so one stays free.
  assert.equal(inflight().length, 5, "final takes the handed-off slot");
  env.answer(final, 204, null, { "X-Up-Ack": "7", "X-Up-Part": lastIndex });
  const response = await opA;
  assert.equal(response.status, 204);
  env.close();
  await flush();
});

// Browser view of the HTTP/1.1 envelope: every unanswered, unaborted carrier
// request holds one of the six connections the browser keeps per origin.
function connectionsInUse(env) {
  return env.requests.filter(
    (r) =>
      !r.answered &&
      !r.options.signal.aborted &&
      ["/api/v1/up", "/api/v1/down"].includes(pathname(r.url)),
  ).length;
}

test("held closing parts shrink the h1.1 pool so a ping finds a free connection", async () => {
  const env = environment(renderedPage());
  const requestClient = client(env, { parallelParts: () => 6 });
  const send = (path, body, headers) => {
    const operation = requestClient.send(
      path,
      requestClient.options("POST", SESSION, body, headers, null),
      null,
      path === "/api/v1/down" ? 1 : null,
      null,
    );
    operation.catch(() => {});
    return operation;
  };
  send("/api/v1/down", null, { "X-Down-Cursor": "0" });
  // Two two-part operations whose closing parts the server holds while an
  // earlier sequence is still being applied.
  const small = (marker) => frame(2, 1, new Array(6000).fill(marker));
  send("/api/v1/up", small(51), { "X-Up-Seq": "7" });
  send("/api/v1/up", small(52), { "X-Up-Seq": "8" });
  await flush();
  for (const sequence of ["7", "8"]) {
    const first = ups(env).find(
      (r) => !r.answered && params(r.url).get("s") === sequence,
    );
    assert.equal(params(first.url).get("p"), "0");
    env.answer(first, 204, null, { "X-Up-Part": "0" });
    await flush();
    assert.ok(
      ups(env).some(
        (r) =>
          !r.answered &&
          params(r.url).get("s") === sequence &&
          params(r.url).get("p") === "1",
      ),
      "closing part dispatched and held",
    );
  }
  // A bulk upload now competes for the remaining connections.
  send(
    "/api/v1/up",
    join(
      frame(2, 1, new Array(48 * 1024).fill(53)),
      frame(2, 1, new Array(48 * 1024).fill(54)),
    ),
    { "X-Up-Seq": "9" },
  );
  await flush();
  assert.ok(
    connectionsInUse(env) < 6,
    `poll + held finals + fragments must leave a connection free (in use: ${connectionsInUse(env)})`,
  );
  const ping = send("/api/v1/up", frame(5, 1, [9]), { "X-Up-Seq": "10" });
  await flush();
  const single = ups(env).find(
    (r) => !r.answered && params(r.url).get("s") === "10",
  );
  assert.ok(single, "the ping is dispatched at once");
  env.answer(single, 204, null, { "X-Up-Ack": "10", "X-Up-Part": "0" });
  assert.equal((await ping).status, 204);
  env.close();
  await flush();
});

test("a finished h1.1 long poll hands its connection to the re-poll first", async () => {
  const env = environment(renderedPage());
  const requestClient = client(env, { parallelParts: () => 8 });
  const poll = (cursor) => {
    const operation = requestClient.send(
      "/api/v1/down",
      requestClient.options(
        "POST",
        SESSION,
        null,
        { "X-Down-Cursor": cursor },
        null,
      ),
      null,
      1,
      null,
    );
    operation.catch(() => {});
    return operation;
  };
  const first = poll("0");
  await flush();
  const operation = requestClient.send(
    "/api/v1/up",
    requestClient.options(
      "POST",
      SESSION,
      join(
        frame(2, 1, new Array(48 * 1024).fill(61)),
        frame(2, 1, new Array(48 * 1024).fill(62)),
      ),
      { "X-Up-Seq": "7" },
      null,
    ),
    null,
    null,
    null,
  );
  operation.catch(() => {});
  await flush();
  const parts = () => ups(env).filter((r) => !r.answered).length;
  const before = parts();
  // The poll loop re-polls as soon as the previous poll settles.
  first.then(() => poll("1"));
  env.answer(env.pending("/api/v1/down")[0], 204, null, {
    "X-Down-Cursor": "1",
  });
  await flush();
  assert.equal(env.pending("/api/v1/down").length, 1, "re-poll issued");
  assert.equal(parts(), before, "no fragment took the poll's connection");
  await env.tick(0);
  assert.equal(parts(), before, "the regrant still sees the held re-poll");
  env.close();
  await flush();
});

test("a multiplexed edge lifts the scheduler cap to K", async () => {
  const env = environment(renderedPage());
  env.setProtocol("h2");
  const requestClient = client(env, { parallelParts: () => 6 });
  const body = join(
    frame(2, 1, new Array(48 * 1024).fill(31)),
    frame(2, 1, new Array(48 * 1024).fill(32)),
  );
  const operation = requestClient.send(
    "/api/v1/up",
    requestClient.options("POST", SESSION, body, { "X-Up-Seq": "7" }, null),
    null,
    null,
    null,
  );
  operation.catch(() => {});
  await flush();
  const inflight = () => ups(env).filter((r) => !r.answered);
  assert.equal(inflight().length, 6, "h2 admits the full K worker pool");
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
