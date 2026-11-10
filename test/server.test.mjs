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

function listen(server) {
  server.listen(0, "127.0.0.1");
  return once(server, "listening");
}

async function waitForHealth(port, child) {
  for (let attempt = 0; attempt < 50; attempt++) {
    if (child.exitCode !== null) throw new Error("Router process exited before becoming ready.");
    try {
      const response = await fetch(`http://127.0.0.1:${port}/health`);
      if (response.ok) return;
    } catch {}
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error("Router did not become ready.");
}

before(async () => {
  upstream = http.createServer(async (req, res) => {
    let raw = "";
    for await (const chunk of req) raw += chunk;
    upstreamRequest = { url: req.url, authorization: req.headers.authorization, body: JSON.parse(raw) };
    upstreamPaths.push(req.url);
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
    res.writeHead(429, { "content-type": "application/json" });
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

  app = spawn(process.execPath, ["server.mjs"], {
    cwd: fileURLToPath(root),
    env: {
      ...process.env,
      HOST: "127.0.0.1",
      PORT: String(appPort),
      PROVIDERS_JSON: JSON.stringify({
        local: {
          baseUrl: `http://127.0.0.1:${upstreamPort}/v1`,
          apiKey: "provider-secret",
          models: { fast: "configured-model" },
        },
        backup: {
          baseUrl: `http://127.0.0.1:${upstreamPort}/backup/v1`,
          apiKey: "backup-secret",
          models: { fast: "backup-model" },
        },
      }),
      ROUTER_API_KEY: "router-secret",
    },
    stdio: "ignore",
  });
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
  assert.deepEqual(upstreamPaths, ["/v1/chat/completions", "/backup/v1/chat/completions"]);
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
