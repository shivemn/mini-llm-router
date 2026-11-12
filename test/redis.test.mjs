import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import http from "node:http";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { after, before, test } from "node:test";

const root = new URL("../", import.meta.url);
const redisUrl = process.env.REDIS_URL;
const redisPrefix = `mini-llm-router-test-${randomUUID()}`;
let upstream;
let upstreamPort;
let upstreamHits = 0;
let appA;
let appB;
let appPortA;
let appPortB;

function listen(server) {
  server.listen(0, "127.0.0.1");
  return once(server, "listening");
}

async function findPort() {
  const probe = http.createServer();
  await listen(probe);
  const port = probe.address().port;
  await new Promise((resolve) => probe.close(resolve));
  return port;
}

async function waitForReady(port, child, output) {
  for (let attempt = 0; attempt < 100; attempt++) {
    if (child.exitCode !== null) throw new Error(`Router exited during startup: ${output()}`);
    try {
      const response = await fetch(`http://127.0.0.1:${port}/health/ready`);
      if (response.status === 200) return;
    } catch {}
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`Router failed to become ready: ${output()}`);
}

function startRouter(port) {
  let output = "";
  const child = spawn(process.execPath, [
    "--experimental-loader=@opentelemetry/instrumentation/hook.mjs",
    "--import",
    "./instrumentation.mjs",
    "server.mjs",
  ], {
    cwd: fileURLToPath(root),
    env: {
      ...process.env,
      HOST: "127.0.0.1",
      PORT: String(port),
      ROUTER_API_KEY: "redis-test-key",
      REDIS_URL: redisUrl,
      REDIS_KEY_PREFIX: redisPrefix,
      PROVIDERS_JSON: JSON.stringify({
        mock: {
          baseUrl: `http://127.0.0.1:${upstreamPort}/v1`,
          apiKey: "mock-provider-key",
          models: { shared: "mock-model" },
        },
      }),
      MAX_UPSTREAM_RETRIES: "0",
      RATE_LIMIT_MAX_REQUESTS: "10",
      RATE_LIMIT_WINDOW_MS: "60000",
      CIRCUIT_BREAKER_FAILURE_THRESHOLD: "1",
      CIRCUIT_BREAKER_OPEN_MS: "30000",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  child.stdout.setEncoding("utf8").on("data", (chunk) => { output += chunk; });
  child.stderr.setEncoding("utf8").on("data", (chunk) => { output += chunk; });
  return { child, output: () => output };
}

async function postChat(port, model = "shared") {
  return fetch(`http://127.0.0.1:${port}/v1/chat/completions`, {
    method: "POST",
    headers: { authorization: "Bearer redis-test-key", "content-type": "application/json" },
    body: JSON.stringify({ model, messages: [{ role: "user", content: "redis integration" }] }),
  });
}

before(async () => {
  if (!redisUrl) return;
  upstream = http.createServer(async (req, res) => {
    for await (const _chunk of req) {}
    upstreamHits++;
    res.writeHead(503, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: { message: "mock provider failure" } }));
  });
  await listen(upstream);
  upstreamPort = upstream.address().port;
  appPortA = await findPort();
  appPortB = await findPort();
  const startedA = startRouter(appPortA);
  const startedB = startRouter(appPortB);
  appA = startedA.child;
  appB = startedB.child;
  await Promise.all([
    waitForReady(appPortA, appA, startedA.output),
    waitForReady(appPortB, appB, startedB.output),
  ]);
});

after(async () => {
  for (const app of [appA, appB]) {
    if (app && app.exitCode === null) {
      app.kill();
      await once(app, "exit");
    }
  }
  if (upstream?.listening) {
    upstream.close();
    await once(upstream, "close");
  }
});

test("shares rate limits and circuit state across router instances", { skip: !redisUrl }, async () => {
  const failed = await postChat(appPortA);
  assert.equal(failed.status, 503);
  assert.equal(upstreamHits, 1);

  const circuitOpen = await postChat(appPortB);
  assert.equal(circuitOpen.status, 503);
  assert.match((await circuitOpen.json()).error.message, /All providers.*temporarily unavailable/);
  assert.equal(upstreamHits, 1, "second instance should see the shared open circuit");

  for (let index = 0; index < 8; index++) {
    const response = await postChat(appPortA, "unknown-model");
    assert.equal(response.status, 404);
  }
  const rateLimited = await postChat(appPortB, "unknown-model");
  assert.equal(rateLimited.status, 429, "second instance should see the shared rate-limit counter");
});
