import http from "node:http";
import { createHash, randomUUID } from "node:crypto";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

function normalizeProvider(name, value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`Provider '${name}' must be an object.`);
  }
  let parsed;
  try {
    parsed = new URL(value.baseUrl);
  } catch {
    throw new Error(`Provider '${name}' baseUrl must be a valid absolute URL.`);
  }
  if (!["http:", "https:"].includes(parsed.protocol) || parsed.username || parsed.password) {
    throw new Error(`Provider '${name}' baseUrl must use HTTP or HTTPS and must not contain credentials.`);
  }
  if (typeof value.apiKey !== "string" || !value.apiKey.trim()) {
    throw new Error(`Provider '${name}' apiKey must be a non-empty string.`);
  }
  const timeoutMs = value.timeoutMs ?? 120_000;
  if (!Number.isInteger(timeoutMs) || timeoutMs < 100 || timeoutMs > 300_000) {
    throw new Error(`Provider '${name}' timeoutMs must be an integer from 100 to 300000.`);
  }
  if (!value.models || typeof value.models !== "object" || Array.isArray(value.models)) {
    throw new Error(`Provider '${name}' models must map public model names to upstream model names.`);
  }
  const models = Object.entries(value.models);
  if (models.length === 0 || models.some(([alias, model]) => !alias.trim() || typeof model !== "string" || !model.trim())) {
    throw new Error(`Provider '${name}' must configure at least one non-empty model mapping.`);
  }
  return {
    name,
    baseUrl: value.baseUrl.replace(/\/+$/, ""),
    apiKey: value.apiKey,
    timeoutMs,
    models: Object.fromEntries(models),
  };
}

function readConfig(env = process.env) {
  const rawPort = env.PORT || "3001";
  const port = Number(rawPort);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error("PORT must be an integer from 1 to 65535.");
  }
  const maxRetries = Number(env.MAX_UPSTREAM_RETRIES ?? "0");
  if (!Number.isInteger(maxRetries) || maxRetries < 0 || maxRetries > 5) {
    throw new Error("MAX_UPSTREAM_RETRIES must be an integer from 0 to 5.");
  }
  const maxConcurrentRequests = Number(env.MAX_CONCURRENT_REQUESTS ?? "100");
  if (!Number.isInteger(maxConcurrentRequests) || maxConcurrentRequests < 1 || maxConcurrentRequests > 1000) {
    throw new Error("MAX_CONCURRENT_REQUESTS must be an integer from 1 to 1000.");
  }
  const circuitFailureThreshold = Number(env.CIRCUIT_BREAKER_FAILURE_THRESHOLD ?? "3");
  if (!Number.isInteger(circuitFailureThreshold) || circuitFailureThreshold < 1 || circuitFailureThreshold > 20) {
    throw new Error("CIRCUIT_BREAKER_FAILURE_THRESHOLD must be an integer from 1 to 20.");
  }
  const circuitOpenMs = Number(env.CIRCUIT_BREAKER_OPEN_MS ?? "30000");
  if (!Number.isInteger(circuitOpenMs) || circuitOpenMs < 1000 || circuitOpenMs > 300000) {
    throw new Error("CIRCUIT_BREAKER_OPEN_MS must be an integer from 1000 to 300000.");
  }
  const rateLimitMaxRequests = Number(env.RATE_LIMIT_MAX_REQUESTS ?? "60");
  if (!Number.isInteger(rateLimitMaxRequests) || rateLimitMaxRequests < 1 || rateLimitMaxRequests > 100_000) {
    throw new Error("RATE_LIMIT_MAX_REQUESTS must be an integer from 1 to 100000.");
  }
  const rateLimitWindowMs = Number(env.RATE_LIMIT_WINDOW_MS ?? "60000");
  if (!Number.isInteger(rateLimitWindowMs) || rateLimitWindowMs < 1000 || rateLimitWindowMs > 3_600_000) {
    throw new Error("RATE_LIMIT_WINDOW_MS must be an integer from 1000 to 3600000.");
  }

  let providers = [];
  if (env.PROVIDERS_JSON) {
    let parsedProviders;
    try {
      parsedProviders = JSON.parse(env.PROVIDERS_JSON);
    } catch {
      throw new Error("PROVIDERS_JSON must be a valid JSON object containing provider configurations.");
    }
    if (!parsedProviders || typeof parsedProviders !== "object" || Array.isArray(parsedProviders)) {
      throw new Error("PROVIDERS_JSON must be a valid JSON object containing provider configurations.");
    }
    providers = Object.entries(parsedProviders).map(([name, value]) => normalizeProvider(name, value));
    if (providers.length === 0) {
      throw new Error("PROVIDERS_JSON must contain at least one provider.");
    }
  } else {
    const legacyValues = [env.UPSTREAM_BASE_URL, env.UPSTREAM_API_KEY, env.UPSTREAM_MODEL];
    if (legacyValues.some(Boolean)) {
      if (legacyValues.some((value) => !value)) {
        throw new Error("Set all legacy UPSTREAM_BASE_URL, UPSTREAM_API_KEY, and UPSTREAM_MODEL values, or leave them all blank.");
      }
      providers = [normalizeProvider("default", {
        baseUrl: env.UPSTREAM_BASE_URL,
        apiKey: env.UPSTREAM_API_KEY,
        models: { [env.UPSTREAM_MODEL]: env.UPSTREAM_MODEL },
      })];
    }
  }

  const host = env.HOST || "127.0.0.1";
  const routerKey = env.ROUTER_API_KEY;
  if (!["127.0.0.1", "::1", "localhost"].includes(host) && !routerKey) {
    throw new Error("ROUTER_API_KEY is required when HOST is not a loopback address.");
  }

  return {
    host,
    port,
    maxRetries,
    maxConcurrentRequests,
    circuitFailureThreshold,
    circuitOpenMs,
    rateLimitMaxRequests,
    rateLimitWindowMs,
    providers,
    routerKey,
  };
}

const config = {
  ...readConfig(),
};

function json(res, status, body) {
  res.writeHead(status, { "content-type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(body));
}

function authorized(req) {
  if (!config.routerKey) return true;
  return req.headers.authorization === `Bearer ${config.routerKey}`;
}

async function readJson(req) {
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    size += bytes.length;
    if (size > 1_000_000) throw new Error("Request body is too large (1 MB maximum).");
    chunks.push(bytes);
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw new Error("Request body must be valid JSON.");
  }
}

const retryableStatuses = new Set([408, 429, 500, 502, 503, 504]);
let activeRequests = 0;
const providerCircuits = new Map();
const rateLimitBuckets = new Map();
const requestMetrics = new Map();
const knownMetricRoutes = new Set(["/health", "/health/live", "/health/ready", "/v1/models", "/v1/chat/completions", "/metrics"]);

function metricRoute(pathname) {
  return knownMetricRoutes.has(pathname) ? pathname : "/other";
}

function recordRequestMetric(method, route, status, durationMs) {
  const key = JSON.stringify([method, route, status]);
  const metric = requestMetrics.get(key) || { method, route, status, count: 0, durationSeconds: 0 };
  metric.count++;
  metric.durationSeconds += durationMs / 1000;
  requestMetrics.set(key, metric);
}

function renderMetrics() {
  const lines = [
    "# HELP mini_router_http_requests_total Completed HTTP requests.",
    "# TYPE mini_router_http_requests_total counter",
  ];
  for (const metric of requestMetrics.values()) {
    const labels = `method="${metric.method}",route="${metric.route}",status="${metric.status}"`;
    lines.push(`mini_router_http_requests_total{${labels}} ${metric.count}`);
  }
  lines.push(
    "# HELP mini_router_request_duration_seconds_sum Total duration of completed HTTP requests.",
    "# TYPE mini_router_request_duration_seconds_sum counter",
    "# HELP mini_router_request_duration_seconds_count Number of completed HTTP requests included in the duration sum.",
    "# TYPE mini_router_request_duration_seconds_count counter",
  );
  for (const metric of requestMetrics.values()) {
    const labels = `method="${metric.method}",route="${metric.route}"`;
    lines.push(`mini_router_request_duration_seconds_sum{${labels}} ${metric.durationSeconds.toFixed(6)}`);
    lines.push(`mini_router_request_duration_seconds_count{${labels}} ${metric.count}`);
  }
  return `${lines.join("\n")}\n`;
}

function rateLimitKey(req) {
  if (config.routerKey) {
    return `key:${createHash("sha256").update(req.headers.authorization || "").digest("hex")}`;
  }
  return `ip:${req.socket.remoteAddress || "unknown"}`;
}

function consumeRateLimit(req) {
  const now = Date.now();
  const key = rateLimitKey(req);
  let bucket = rateLimitBuckets.get(key);
  if (!bucket || now >= bucket.resetAt) {
    if (!bucket && rateLimitBuckets.size >= 10_000) {
      for (const [candidate, value] of rateLimitBuckets) {
        if (now >= value.resetAt) rateLimitBuckets.delete(candidate);
      }
      if (rateLimitBuckets.size >= 10_000) rateLimitBuckets.delete(rateLimitBuckets.keys().next().value);
    }
    bucket = { count: 0, resetAt: now + config.rateLimitWindowMs };
    rateLimitBuckets.set(key, bucket);
  }
  if (bucket.count >= config.rateLimitMaxRequests) {
    return { allowed: false, retryAfter: Math.max(1, Math.ceil((bucket.resetAt - now) / 1000)) };
  }
  bucket.count++;
  return { allowed: true };
}

function circuitFor(provider) {
  if (!providerCircuits.has(provider.name)) {
    providerCircuits.set(provider.name, { failures: 0, openUntil: 0 });
  }
  return providerCircuits.get(provider.name);
}

function recordProviderFailure(provider) {
  const circuit = circuitFor(provider);
  circuit.failures++;
  if (circuit.failures >= config.circuitFailureThreshold) {
    circuit.openUntil = Date.now() + config.circuitOpenMs;
  }
}

function recordProviderSuccess(provider) {
  const circuit = circuitFor(provider);
  circuit.failures = 0;
  circuit.openUntil = 0;
}

function retryDelay(response, attempt) {
  const retryAfter = response.headers.get("retry-after");
  if (retryAfter) {
    const seconds = Number(retryAfter);
    const retryAt = Date.parse(retryAfter);
    const requestedDelay = Number.isFinite(seconds)
      ? seconds * 1000
      : Number.isFinite(retryAt) ? retryAt - Date.now() : NaN;
    if (Number.isFinite(requestedDelay) && requestedDelay >= 0) {
      return Math.min(10_000, requestedDelay);
    }
  }
  return 100 * (2 ** attempt);
}

async function fetchUpstream(provider, body, requestSignal) {
  for (let attempt = 0; attempt <= config.maxRetries; attempt++) {
    try {
      const response = await fetch(`${provider.baseUrl}/chat/completions`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${provider.apiKey}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({ ...body, model: provider.models[body.model] }),
        signal: AbortSignal.any([requestSignal, AbortSignal.timeout(provider.timeoutMs)]),
      });
      if (!retryableStatuses.has(response.status) || attempt === config.maxRetries) return response;
      const delay = retryDelay(response, attempt);
      await response.body?.cancel();
      await new Promise((resolve) => setTimeout(resolve, delay));
      continue;
    } catch (error) {
      if (error.name === "TimeoutError" || requestSignal.aborted || attempt === config.maxRetries) throw error;
      await new Promise((resolve) => setTimeout(resolve, 100 * (2 ** attempt)));
    }
  }
  throw new Error("Upstream retries exhausted.");
}

async function fetchWithFallback(model, body, requestSignal) {
  const candidates = config.providers.filter((provider) => Object.hasOwn(provider.models, model));
  let lastError;
  let openProviders = 0;
  for (let index = 0; index < candidates.length; index++) {
    if (circuitFor(candidates[index]).openUntil > Date.now()) {
      openProviders++;
      continue;
    }
    try {
      const response = await fetchUpstream(candidates[index], body, requestSignal);
      if (response.status >= 500) recordProviderFailure(candidates[index]);
      else recordProviderSuccess(candidates[index]);
      const canFallback = response.status === 429 || retryableStatuses.has(response.status);
      if (canFallback && index < candidates.length - 1) {
        await response.body?.cancel();
        continue;
      }
      return response;
    } catch (error) {
      if (requestSignal.aborted) throw error;
      recordProviderFailure(candidates[index]);
      lastError = error;
    }
  }
  if (openProviders === candidates.length) {
    const error = new Error("All providers for this model are temporarily unavailable.");
    error.statusCode = 503;
    throw error;
  }
  if (lastError) throw lastError;
  throw new Error("No provider is configured for the requested model.");
}

const server = http.createServer(async (req, res) => {
  const pathname = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`).pathname;
  const requestId = randomUUID();
  const startedAt = performance.now();
  const requestAbort = new AbortController();
  res.setHeader("x-request-id", requestId);
  req.once("aborted", () => requestAbort.abort());
  res.once("close", () => {
    if (!res.writableEnded) requestAbort.abort();
  });
  res.once("finish", () => {
    const metricMethod = ["GET", "POST"].includes(req.method) ? req.method : "OTHER";
    recordRequestMetric(metricMethod, metricRoute(pathname), res.statusCode, performance.now() - startedAt);
    console.log(JSON.stringify({
      event: "request_complete",
      requestId,
      method: req.method,
      path: pathname,
      status: res.statusCode,
      durationMs: Math.round(performance.now() - startedAt),
    }));
  });

  if (req.method === "GET" && pathname === "/health/live") {
    return json(res, 200, { live: true });
  }
  if (req.method === "GET" && pathname === "/health/ready") {
    const ready = config.providers.length > 0;
    return json(res, ready ? 200 : 503, { ready });
  }
  if (req.method === "GET" && pathname === "/metrics") {
    if (!authorized(req)) return json(res, 401, { error: { message: "Invalid router API key." } });
    res.writeHead(200, { "content-type": "text/plain; version=0.0.4; charset=utf-8" });
    return res.end(renderMetrics());
  }
  if (req.method === "GET" && pathname === "/health") {
    return json(res, 200, { ok: true, configured: config.providers.length > 0 });
  }

  if (req.method === "GET" && pathname === "/v1/models") {
    if (!authorized(req)) return json(res, 401, { error: { message: "Invalid router API key." } });
    if (config.providers.length === 0) return json(res, 503, { error: { message: "Configure at least one provider first." } });
    const data = config.providers.flatMap((provider) => Object.keys(provider.models).map((model) => ({
      id: model,
      object: "model",
      owned_by: provider.name,
    })));
    return json(res, 200, { object: "list", data });
  }

  if (req.method !== "POST" || pathname !== "/v1/chat/completions") {
    return json(res, 404, { error: { message: "Not found. Try POST /v1/chat/completions or GET /v1/models." } });
  }
  if (!authorized(req)) return json(res, 401, { error: { message: "Invalid router API key." } });
  if (config.providers.length === 0) {
    return json(res, 503, { error: { message: "Configure PROVIDERS_JSON or the legacy UPSTREAM_* settings in .env." } });
  }
  const rateLimit = consumeRateLimit(req);
  if (!rateLimit.allowed) {
    res.setHeader("retry-after", String(rateLimit.retryAfter));
    return json(res, 429, { error: { message: "Request rate limit exceeded. Retry after the indicated delay." } });
  }
  if (activeRequests >= config.maxConcurrentRequests) {
    res.setHeader("retry-after", "1");
    return json(res, 503, { error: { message: "Router is at its concurrent request limit. Retry shortly." } });
  }
  activeRequests++;
  let requestSlotReleased = false;
  const releaseRequestSlot = () => {
    if (requestSlotReleased) return;
    requestSlotReleased = true;
    activeRequests--;
  };
  res.once("finish", releaseRequestSlot);
  res.once("close", releaseRequestSlot);
  if (!/^application\/json(?:\s*;|$)/i.test(req.headers["content-type"] || "")) {
    return json(res, 415, { error: { message: "Content-Type must be application/json." } });
  }

  let body;
  try {
    body = await readJson(req);
  } catch (error) {
    return json(res, 400, { error: { message: error.message } });
  }
  if (!body || !Array.isArray(body.messages) || body.messages.length === 0) {
    return json(res, 400, { error: { message: "Provide a non-empty messages array." } });
  }
  if (typeof body.model !== "string" || !body.model) {
    return json(res, 400, { error: { message: "Provide a model name from GET /v1/models." } });
  }
  if (!config.providers.some((candidate) => Object.hasOwn(candidate.models, body.model))) {
    return json(res, 404, { error: { message: `Unknown model '${body.model}'. List available models with GET /v1/models.` } });
  }

  try {
    const upstream = await fetchWithFallback(body.model, body, requestAbort.signal);
    const contentType = upstream.headers.get("content-type") || "application/json";
    if (body.stream === true) {
      res.writeHead(upstream.status, { "content-type": contentType });
      if (upstream.body) await pipeline(Readable.fromWeb(upstream.body), res);
      else res.end();
      return;
    }
    const responseBody = await upstream.text();
    res.writeHead(upstream.status, { "content-type": contentType });
    res.end(responseBody);
  } catch (error) {
    if (requestAbort.signal.aborted || res.destroyed) return;
    if (res.headersSent) {
      res.destroy(error);
      return;
    }
    const timedOut = error.name === "TimeoutError";
    const statusCode = error.statusCode || (timedOut ? 504 : 502);
    return json(res, statusCode, {
      error: { message: error.statusCode ? error.message : timedOut ? "The upstream provider took too long to respond." : "Could not reach the upstream provider." },
    });
  }
});

server.listen(config.port, config.host, () => {
  console.log(`Mini LLM Router listening at http://${config.host}:${config.port}`);
  if (config.providers.length === 0) {
    console.log("Add provider settings to .env to enable chat requests.");
  }
});

function shutdown(signal) {
  console.log(JSON.stringify({ event: "server_shutdown", signal }));
  server.close(() => process.exit(0));
  const forceClose = setTimeout(() => server.closeAllConnections(), 10_000);
  forceClose.unref();
}

process.once("SIGINT", () => shutdown("SIGINT"));
process.once("SIGTERM", () => shutdown("SIGTERM"));
