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
      UPSTREAM_BASE_URL: `http://127.0.0.1:${upstreamPort}/v1`,
      UPSTREAM_API_KEY: "provider-secret",
      UPSTREAM_MODEL: "configured-model",
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
    body: JSON.stringify({ model: "client-model", messages: [{ role: "user", content: "hello" }] }),
  });
  assert.equal(response.status, 429);
  assert.deepEqual(await response.json(), { error: { message: "Provider rate limit" } });
  assert.equal(upstreamRequest.url, "/v1/chat/completions");
  assert.equal(upstreamRequest.authorization, "Bearer provider-secret");
  assert.equal(upstreamRequest.body.model, "configured-model");
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
