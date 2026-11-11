import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { once } from "node:events";
import http from "node:http";
import { fileURLToPath } from "node:url";
import { after, before, test } from "node:test";

const root = new URL("../", import.meta.url);
let upstream;
let app;
let appPort;
let upstreamPort;
let upstreamRequest;
let transientFailures = 0;
let upstreamPaths = [];
let appOutput = "";
let waitForCancelledUpstream;
let upstreamResponseClosed;
let signalHeldRequest;

function listen(server) {
  server.listen(0, "127.0.0.1");
  return once(server, "listening");
}

async function waitForHealth(port, child) {
  for (let attempt = 0; attempt < 100; attempt++) {
    if (child.exitCode !== null) throw new Error("Router process exited before becoming ready.");
    try {
      const response = await fetch(`http://127.0.0.1:${port}/health`);
      if (response.ok) return;
    } catch {}
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`Router did not become ready. Child output: ${appOutput}`);
}

before(async () => {
  upstream = http.createServer(async (req, res) => {
    let raw = "";
    for await (const chunk of req) raw += chunk;
    upstreamRequest = { url: req.url, authorization: req.headers.authorization, body: JSON.parse(raw) };
    upstreamPaths.push(req.url);
    if (upstreamRequest.body.messages[0]?.content === "cancel-test") {
      upstreamResponseClosed = new Promise((resolve) => res.once("close", resolve));
      waitForCancelledUpstream?.();
      return;
    }
    if (upstreamRequest.body.messages[0]?.content === "hold-test") {
      signalHeldRequest?.();
      setTimeout(() => {
        if (!res.destroyed) {
          res.writeHead(200, { "content-type": "application/json" });
          res.end(JSON.stringify({ choices: [{ message: { content: "finished" } }] }));
        }
      }, 250);
      return;
    }
    if (upstreamRequest.body.messages[0]?.content === "circuit-test") {
      res.writeHead(503, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: { message: "Provider unavailable" } }));
      return;
    }
    if (upstreamRequest.body.stream === true) {
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.end('data: {"choices":[{"delta":{"content":"streamed"}}]}\n\ndata: [DONE]\n\n');
      return;
    }
    if (upstreamRequest.body.messages[0]?.content === "timeout-fallback" && req.url.startsWith("/v1/")) {
      setTimeout(() => {
        if (!res.destroyed) {
          res.writeHead(200, { "content-type": "application/json" });
          res.end(JSON.stringify({ choices: [{ message: { content: "late primary" } }] }));
        }
      }, 200);
      return;
    }
    if (upstreamRequest.body.messages[0]?.content === "timeout-fallback" && req.url.startsWith("/backup/")) {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ choices: [{ message: { content: "fast backup" } }] }));
      return;
    }
    if (upstreamRequest.body.messages[0]?.content === "fallback-test" && req.url.startsWith("/backup/")) {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ choices: [{ message: { content: "backup response" } }] }));
      return;
    }
    if (upstreamRequest.body.messages[0]?.content === "retry-once" && transientFailures++ === 0) {
      res.writeHead(503, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: { message: "Temporarily unavailable" } }));
      return;
    }
    if (upstreamRequest.body.messages[0]?.content === "retry-once") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ choices: [{ message: { content: "recovered" } }] }));
      return;
    }
    res.writeHead(429, { "content-type": "application/json", "retry-after": "0" });
    res.end(JSON.stringify({ error: { message: "Provider rate limit" } }));
  });
  await listen(upstream);
  upstreamPort = upstream.address().port;

  appPort = await new Promise((resolve, reject) => {
    const probe = http.createServer();
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", () => {
      const port = probe.address().port;
      probe.close(() => resolve(port));
    });
  });

  app = spawn(process.execPath, [
    "--experimental-loader=@opentelemetry/instrumentation/hook.mjs",
    "--import",
    "./instrumentation.mjs",
    "server.mjs",
  ], {
    cwd: fileURLToPath(root),
    env: {
      ...process.env,
      HOST: "127.0.0.1",
      PORT: String(appPort),
      PROVIDERS_JSON: JSON.stringify({
        local: {
          baseUrl: `http://127.0.0.1:${upstreamPort}/v1`,
          apiKey: "provider-secret",
          timeoutMs: 100,
          models: { fast: "configured-model" },
        },
        backup: {
          baseUrl: `http://127.0.0.1:${upstreamPort}/backup/v1`,
          apiKey: "backup-secret",
          timeoutMs: 1_000,
          models: { fast: "backup-model" },
        },
        breakerA: {
          baseUrl: `http://127.0.0.1:${upstreamPort}/breaker-a/v1`,
          apiKey: "breaker-a-secret",
          timeoutMs: 1_000,
          models: { breaker: "circuit-model" },
        },
        breakerB: {
          baseUrl: `http://127.0.0.1:${upstreamPort}/breaker-b/v1`,
          apiKey: "breaker-b-secret",
          timeoutMs: 1_000,
          models: { breaker: "circuit-model" },
        },
      }),
      ROUTER_API_KEY: "router-secret",
      MAX_UPSTREAM_RETRIES: "1",
      MAX_CONCURRENT_REQUESTS: "1",
      CIRCUIT_BREAKER_FAILURE_THRESHOLD: "1",
      CIRCUIT_BREAKER_OPEN_MS: "1000",
      RATE_LIMIT_MAX_REQUESTS: "20",
      RATE_LIMIT_WINDOW_MS: "60000",
      REDIS_URL: "",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  app.stdout.setEncoding("utf8").on("data", (chunk) => { appOutput += chunk; });
  app.stderr.setEncoding("utf8").on("data", (chunk) => { appOutput += chunk; });
  await waitForHealth(appPort, app);
});

after(async () => {
  if (app && app.exitCode === null) {
    app.kill();
    await once(app, "exit");
  }
  if (upstream?.listening) {
    upstream.close();
    await once(upstream, "close");
  }
});

test("requires the router key for protected endpoints", async () => {
  const response = await fetch(`http://127.0.0.1:${appPort}/v1/models`);
  assert.equal(response.status, 401);
  assert.match(response.headers.get("x-request-id"), /^[0-9a-f-]{36}$/i);
});

test("exposes liveness and readiness probes", async () => {
  const live = await fetch(`http://127.0.0.1:${appPort}/health/live`);
  assert.equal(live.status, 200);
  assert.deepEqual(await live.json(), { live: true });
  const ready = await fetch(`http://127.0.0.1:${appPort}/health/ready`);
  assert.equal(ready.status, 200);
  assert.deepEqual(await ready.json(), { ready: true });
});

test("protects and exports Prometheus request metrics", async () => {
  const denied = await fetch(`http://127.0.0.1:${appPort}/metrics`);
  assert.equal(denied.status, 401);
  const response = await fetch(`http://127.0.0.1:${appPort}/metrics`, {
    headers: { authorization: "Bearer router-secret" },
  });
  assert.equal(response.status, 200);
  assert.match(response.headers.get("content-type"), /text\/plain/);
  const body = await response.text();
  assert.match(body, /mini_router_http_requests_total/);
  assert.match(body, /route="\/health\/live"/);
});

test("lists the public model aliases", async () => {
  const response = await fetch(`http://127.0.0.1:${appPort}/v1/models`, {
    headers: { authorization: "Bearer router-secret" },
  });
  assert.equal(response.status, 200);
  assert.equal((await response.json()).data[0].id, "fast");
});

test("rejects malformed JSON and empty messages", async () => {
  const url = `http://127.0.0.1:${appPort}/v1/chat/completions`;
  const headers = { authorization: "Bearer router-secret", "content-type": "application/json" };
  const malformed = await fetch(url, { method: "POST", headers, body: "{" });
  assert.equal(malformed.status, 400);
  const empty = await fetch(url, { method: "POST", headers, body: JSON.stringify({ messages: [] }) });
  assert.equal(empty.status, 400);
  const wrongType = await fetch(url, {
    method: "POST",
    headers: { authorization: "Bearer router-secret", "content-type": "text/plain" },
    body: JSON.stringify({ model: "fast", messages: [{ role: "user", content: "hello" }] }),
  });
  assert.equal(wrongType.status, 415);
  const oversized = await fetch(url, {
    method: "POST",
    headers,
    body: "é".repeat(500_001),
  });
  assert.equal(oversized.status, 400);
  assert.match((await oversized.json()).error.message, /too large/);
});

test("forwards requests with the configured model and relays provider errors", async () => {
  const response = await fetch(`http://127.0.0.1:${appPort}/v1/chat/completions`, {
    method: "POST",
    headers: { authorization: "Bearer router-secret", "content-type": "application/json" },
    body: JSON.stringify({ model: "fast", messages: [{ role: "user", content: "hello" }] }),
  });
  assert.equal(response.status, 429);
  assert.deepEqual(await response.json(), { error: { message: "Provider rate limit" } });
  assert.equal(upstreamRequest.url, "/backup/v1/chat/completions");
  assert.equal(upstreamRequest.authorization, "Bearer backup-secret");
  assert.equal(upstreamRequest.body.model, "backup-model");
});

test("retries a temporary upstream server error", async () => {
  transientFailures = 0;
  const response = await fetch(`http://127.0.0.1:${appPort}/v1/chat/completions`, {
    method: "POST",
    headers: { authorization: "Bearer router-secret", "content-type": "application/json" },
    body: JSON.stringify({ model: "fast", messages: [{ role: "user", content: "retry-once" }] }),
  });
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), { choices: [{ message: { content: "recovered" } }] });
  assert.equal(transientFailures, 2);
});

test("falls back to the next provider for a throttled model", async () => {
  upstreamPaths = [];
  const response = await fetch(`http://127.0.0.1:${appPort}/v1/chat/completions`, {
    method: "POST",
    headers: { authorization: "Bearer router-secret", "content-type": "application/json" },
    body: JSON.stringify({ model: "fast", messages: [{ role: "user", content: "fallback-test" }] }),
  });
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), { choices: [{ message: { content: "backup response" } }] });
  assert.deepEqual(upstreamPaths, [
    "/v1/chat/completions",
    "/v1/chat/completions",
    "/backup/v1/chat/completions",
  ]);
});

test("uses the next provider when a provider exceeds its timeout", async () => {
  const response = await fetch(`http://127.0.0.1:${appPort}/v1/chat/completions`, {
    method: "POST",
    headers: { authorization: "Bearer router-secret", "content-type": "application/json" },
    body: JSON.stringify({ model: "fast", messages: [{ role: "user", content: "timeout-fallback" }] }),
  });
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), { choices: [{ message: { content: "fast backup" } }] });
});

test("passes server-sent event responses through for stream requests", async () => {
  const response = await fetch(`http://127.0.0.1:${appPort}/v1/chat/completions`, {
    method: "POST",
    headers: { authorization: "Bearer router-secret", "content-type": "application/json" },
    body: JSON.stringify({ model: "fast", stream: true, messages: [{ role: "user", content: "hello" }] }),
  });
  assert.equal(response.status, 200);
  assert.match(response.headers.get("content-type"), /text\/event-stream/);
  assert.equal(await response.text(), 'data: {"choices":[{"delta":{"content":"streamed"}}]}\n\ndata: [DONE]\n\n');
});

test("aborts the upstream request when the client disconnects", async () => {
  const requestArrived = new Promise((resolve) => { waitForCancelledUpstream = resolve; });
  const clientRequest = http.request({
    hostname: "127.0.0.1",
    port: appPort,
    path: "/v1/chat/completions",
    method: "POST",
    headers: { authorization: "Bearer router-secret", "content-type": "application/json" },
  }, (response) => response.resume());
  clientRequest.on("error", () => {});
  clientRequest.end(JSON.stringify({ model: "fast", messages: [{ role: "user", content: "cancel-test" }] }));
  await requestArrived;
  clientRequest.destroy();
  const cancelled = await Promise.race([
    upstreamResponseClosed.then(() => true),
    new Promise((resolve) => setTimeout(() => resolve(false), 1_000)),
  ]);
  assert.equal(cancelled, true);
});

test("rejects requests above the configured concurrency limit", async () => {
  const requestArrived = new Promise((resolve) => { signalHeldRequest = resolve; });
  const firstResponse = fetch(`http://127.0.0.1:${appPort}/v1/chat/completions`, {
    method: "POST",
    headers: { authorization: "Bearer router-secret", "content-type": "application/json" },
    body: JSON.stringify({ model: "fast", messages: [{ role: "user", content: "hold-test" }] }),
  });
  await requestArrived;
  const rejected = await fetch(`http://127.0.0.1:${appPort}/v1/chat/completions`, {
    method: "POST",
    headers: { authorization: "Bearer router-secret", "content-type": "application/json" },
    body: JSON.stringify({ model: "fast", messages: [{ role: "user", content: "over-limit" }] }),
  });
  assert.equal(rejected.status, 503);
  assert.equal(rejected.headers.get("retry-after"), "1");
  const accepted = await firstResponse;
  assert.equal(accepted.status, 200);
});

test("rejects invalid port configuration at startup", () => {
  const result = spawnSync(process.execPath, ["server.mjs"], {
    cwd: fileURLToPath(root),
    env: { ...process.env, PORT: "70000" },
    encoding: "utf8",
  });
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /PORT must be an integer from 1 to 65535/);
});

test("rejects partially configured legacy provider settings", () => {
  const result = spawnSync(process.execPath, ["server.mjs"], {
    cwd: fileURLToPath(root),
    env: { ...process.env, PROVIDERS_JSON: "", UPSTREAM_BASE_URL: "https://api.example.test/v1", UPSTREAM_API_KEY: "", UPSTREAM_MODEL: "model" },
    encoding: "utf8",
  });
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /Set all legacy UPSTREAM_BASE_URL/);
});

test("rejects an invalid Redis connection URL at startup", () => {
  const result = spawnSync(process.execPath, ["server.mjs"], {
    cwd: fileURLToPath(root),
    env: { ...process.env, REDIS_URL: "http://127.0.0.1:6379" },
    encoding: "utf8",
  });
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /REDIS_URL must use redis:\/\/ or rediss:\/\//);
});

test("requires router authentication when binding to a non-loopback address", () => {
  const result = spawnSync(process.execPath, ["server.mjs"], {
    cwd: fileURLToPath(root),
    env: { ...process.env, HOST: "0.0.0.0", ROUTER_API_KEY: "" },
    encoding: "utf8",
  });
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /ROUTER_API_KEY is required when HOST is not a loopback address/);
});

test("opens provider circuits after repeated failures and returns 503 while all are open", async () => {
  const requestOptions = {
    method: "POST",
    headers: { authorization: "Bearer router-secret", "content-type": "application/json" },
    body: JSON.stringify({ model: "breaker", messages: [{ role: "user", content: "circuit-test" }] }),
  };
  const first = await fetch(`http://127.0.0.1:${appPort}/v1/chat/completions`, requestOptions);
  assert.equal(first.status, 503);
  const second = await fetch(`http://127.0.0.1:${appPort}/v1/chat/completions`, requestOptions);
  assert.equal(second.status, 503);
  assert.match((await second.json()).error.message, /All providers.*temporarily unavailable/);
});

test("limits requests per router key and returns a retry delay", async () => {
  let throttled;
  for (let attempt = 0; attempt < 20; attempt++) {
    const response = await fetch(`http://127.0.0.1:${appPort}/v1/chat/completions`, {
      method: "POST",
      headers: { authorization: "Bearer router-secret", "content-type": "application/json" },
      body: JSON.stringify({ model: "not-configured", messages: [{ role: "user", content: "rate-test" }] }),
    });
    if (response.status === 429) {
      throttled = response;
      break;
    }
    assert.equal(response.status, 404);
  }
  assert.ok(throttled, "expected the configured rate limit to reject a request");
  assert.ok(Number(throttled.headers.get("retry-after")) >= 1);
  assert.match((await throttled.json()).error.message, /rate limit exceeded/i);
});
