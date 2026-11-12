import http from "node:http";
import { readFileSync } from "node:fs";
import { createHash, randomUUID, timingSafeEqual } from "node:crypto";
import { SpanStatusCode, trace } from "@opentelemetry/api";
import { GetSecretValueCommand, SecretsManagerClient } from "@aws-sdk/client-secrets-manager";
import { createClient } from "redis";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { shutdownTelemetry } from "./instrumentation.mjs";
import { adaptProviderResponse, buildProviderRequest } from "./providers/adapters.mjs";

const tracer = trace.getTracer("mini-llm-router");
const openApiDocument = readFileSync(new URL("./openapi.json", import.meta.url), "utf8");
const swaggerHtml = `<!doctype html><html><head><meta charset="utf-8"><title>Mini LLM Router API</title><link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css"></head><body><div id="swagger-ui"></div><script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script><script>SwaggerUIBundle({url:'/openapi.json',dom_id:'#swagger-ui'});</script></body></html>`;

function normalizeProvider(name, value, env = process.env) {
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
  const hasInlineKey = typeof value.apiKey === "string" && Boolean(value.apiKey.trim());
  const hasSecretId = typeof value.apiKeySecretId === "string" && Boolean(value.apiKeySecretId.trim());
  const hasKeyEnvName = typeof value.apiKeyEnv === "string" && /^[A-Za-z_][A-Za-z0-9_]*$/.test(value.apiKeyEnv);
  if (value.apiKeyEnv !== undefined && !hasKeyEnvName) {
    throw new Error(`Provider '${name}' apiKeyEnv must be a valid environment variable name.`);
  }
  const hasEnvKey = hasKeyEnvName && typeof env[value.apiKeyEnv] === "string" && Boolean(env[value.apiKeyEnv].trim());
  if ([hasInlineKey, hasSecretId, hasKeyEnvName].filter(Boolean).length !== 1) {
    throw new Error(`Provider '${name}' must set exactly one of apiKey, apiKeyEnv, or apiKeySecretId.`);
  }
  if (value.apiKeySecretId !== undefined && !hasSecretId) {
    throw new Error(`Provider '${name}' apiKeySecretId must be a non-empty secret name or ARN.`);
  }
  if (hasKeyEnvName && !hasEnvKey) {
    throw new Error(`Provider '${name}' environment variable '${value.apiKeyEnv}' is missing or empty.`);
  }
  const protocol = value.protocol || "openai-compatible";
  if (!["openai-compatible", "anthropic", "gemini"].includes(protocol)) {
    throw new Error(`Provider '${name}' protocol must be openai-compatible, anthropic, or gemini.`);
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
  const routing = value.routing ?? {};
  if (!routing || typeof routing !== "object" || Array.isArray(routing)) throw new Error(`Provider '${name}' routing must be an object.`);
  if (Object.keys(routing).some((key) => !["order", "allow_fallbacks", "data_collection", "zdr"].includes(key))) {
    throw new Error(`Provider '${name}' routing supports order, allow_fallbacks, data_collection, and zdr only.`);
  }
  if (routing.order !== undefined && (!Array.isArray(routing.order) || routing.order.some((item) => typeof item !== "string" || !item.trim()))) {
    throw new Error(`Provider '${name}' routing.order must be an array of provider names.`);
  }
  if (routing.allow_fallbacks !== undefined && typeof routing.allow_fallbacks !== "boolean") throw new Error(`Provider '${name}' routing.allow_fallbacks must be boolean.`);
  if (routing.data_collection !== undefined && !["allow", "deny"].includes(routing.data_collection)) throw new Error(`Provider '${name}' routing.data_collection must be allow or deny.`);
  if (routing.zdr !== undefined && typeof routing.zdr !== "boolean") throw new Error(`Provider '${name}' routing.zdr must be boolean.`);
  if (Object.keys(routing).length && !["openrouter.ai", "www.openrouter.ai"].includes(parsed.hostname)) {
    throw new Error(`Provider '${name}' routing preferences are supported only for OpenRouter.`);
  }
  return {
    name,
    protocol,
    baseUrl: value.baseUrl.replace(/\/+$/, ""),
    apiKey: hasInlineKey ? value.apiKey : hasEnvKey ? env[value.apiKeyEnv] : undefined,
    apiKeySecretId: hasSecretId ? value.apiKeySecretId : undefined,
    timeoutMs,
    models: Object.fromEntries(models),
    routing: Object.freeze({ ...routing }),
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
  let redisUrl;
  if (env.REDIS_URL) {
    let parsedRedisUrl;
    try {
      parsedRedisUrl = new URL(env.REDIS_URL);
    } catch {
      throw new Error("REDIS_URL must be a valid Redis connection URL.");
    }
    if (!["redis:", "rediss:"].includes(parsedRedisUrl.protocol) || !parsedRedisUrl.hostname) {
      throw new Error("REDIS_URL must use redis:// or rediss:// and include a host.");
    }
    redisUrl = env.REDIS_URL;
  }
  const redisKeyPrefix = env.REDIS_KEY_PREFIX || "mini-llm-router";
  if (redisKeyPrefix.length > 100 || /\s/.test(redisKeyPrefix)) {
    throw new Error("REDIS_KEY_PREFIX must be at most 100 characters and contain no whitespace.");
  }
  const cacheTtlSeconds = Number(env.LLM_CACHE_TTL_SECONDS ?? "0");
  if (!Number.isInteger(cacheTtlSeconds) || cacheTtlSeconds < 0 || cacheTtlSeconds > 86400) {
    throw new Error("LLM_CACHE_TTL_SECONDS must be an integer from 0 to 86400.");
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
    providers = Object.entries(parsedProviders).map(([name, value]) => normalizeProvider(name, value, env));
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
  let clientApiKeys = {};
  if (env.CLIENT_API_KEYS_JSON) {
    try { clientApiKeys = JSON.parse(env.CLIENT_API_KEYS_JSON); } catch { throw new Error("CLIENT_API_KEYS_JSON must be a JSON object mapping client names to API keys."); }
    if (!clientApiKeys || typeof clientApiKeys !== "object" || Array.isArray(clientApiKeys) || Object.entries(clientApiKeys).some(([name, key]) => !/^[A-Za-z0-9._-]{1,64}$/.test(name) || typeof key !== "string" || key.length < 32)) {
      throw new Error("CLIENT_API_KEYS_JSON must map client names to API keys of at least 32 characters.");
    }
  }
  const freeOnly = !["0", "false", "no"].includes(String(env.FREE_ONLY_MODE ?? "true").toLowerCase());
  if (freeOnly) {
    for (const provider of providers) {
      const url = new URL(provider.baseUrl);
      const localHost = ["localhost", "127.0.0.1", "::1", "host.docker.internal", "ollama"].includes(url.hostname);
      const openRouter = ["openrouter.ai", "www.openrouter.ai"].includes(url.hostname);
      const freeModels = Object.values(provider.models).every((model) => model === "openrouter/free" || model.endsWith(":free"));
      if (localHost) continue;
      if (!openRouter || !freeModels) {
        throw new Error(`FREE_ONLY_MODE is enabled: provider '${provider.name}' must use OpenRouter free model IDs or a local Ollama endpoint.`);
      }
    }
  }
  if (!["127.0.0.1", "::1", "localhost"].includes(host) && !routerKey && Object.keys(clientApiKeys).length === 0) {
    throw new Error("ROUTER_API_KEY is required when HOST is not a loopback address (or configure CLIENT_API_KEYS_JSON).");
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
    redisUrl,
    redisKeyPrefix,
    cacheTtlSeconds,
    awsRegion: env.AWS_REGION || undefined,
    providers,
    routerKey,
    clientApiKeys,
    freeOnly,
  };
}

const config = {
  ...readConfig(),
};

async function resolveProviderSecrets(providers, region) {
  const secretProviders = providers.filter((provider) => provider.apiKeySecretId);
  if (secretProviders.length === 0) return providers;
  const client = new SecretsManagerClient(region ? { region } : {});
  try {
    return await Promise.all(providers.map(async (provider) => {
      if (!provider.apiKeySecretId) return provider;
      let response;
      try {
        response = await client.send(new GetSecretValueCommand({ SecretId: provider.apiKeySecretId }));
      } catch (error) {
        const reason = error?.name || "SecretsManagerError";
        throw new Error(`Unable to load AWS secret for provider '${provider.name}' (${reason}).`);
      }
      let secretValue = response.SecretString;
      if (secretValue === undefined && response.SecretBinary) {
        secretValue = Buffer.from(response.SecretBinary).toString("utf8");
      }
      if (typeof secretValue !== "string" || !secretValue.trim()) {
        throw new Error(`AWS secret for provider '${provider.name}' has no usable value.`);
      }
      try {
        const parsed = JSON.parse(secretValue);
        if (parsed && typeof parsed.apiKey === "string") secretValue = parsed.apiKey;
      } catch {
        // Plain-text secrets are also supported.
      }
      if (!secretValue.trim()) {
        throw new Error(`AWS secret for provider '${provider.name}' has no usable API key.`);
      }
      const { apiKeySecretId, ...resolvedProvider } = provider;
      return { ...resolvedProvider, apiKey: secretValue };
    }));
  } finally {
    client.destroy();
  }
}

config.providers = await resolveProviderSecrets(config.providers, config.awsRegion);

const redisClient = config.redisUrl ? createClient({
  url: config.redisUrl,
  disableOfflineQueue: true,
  socket: { connectTimeout: 5000 },
}) : undefined;
redisClient?.on("error", () => {
  console.error(JSON.stringify({ event: "redis_client_error" }));
});
if (redisClient) {
  await redisClient.connect();
  console.log(JSON.stringify({ event: "redis_connected" }));
}

function json(res, status, body) {
  res.writeHead(status, { "content-type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(body));
}

function authorized(req) {
  if (!config.routerKey && Object.keys(config.clientApiKeys).length === 0) return true;
  const token = /^Bearer\s+(.+)$/i.exec(req.headers.authorization || "")?.[1];
  if (!token) return false;
  const matches = (expected) => {
    const left = Buffer.from(token);
    const right = Buffer.from(expected);
    return left.length === right.length && timingSafeEqual(left, right);
  };
  return Boolean((config.routerKey && matches(config.routerKey)) || Object.values(config.clientApiKeys).some(matches));
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
const usageTotals = new Map();
const upstreamRateLimits = new Map();
const durationBuckets = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10];
const knownMetricRoutes = new Set(["/health", "/health/live", "/health/ready", "/docs", "/openapi.json", "/v1/models", "/v1/catalog/free-models", "/v1/usage", "/v1/chat/completions", "/metrics"]);

function recordUsage(provider, requestedModel, usage) {
  if (!usage || typeof usage !== "object") return;
  const key = JSON.stringify([provider.name, requestedModel]);
  const total = usageTotals.get(key) || { provider: provider.name, model: requestedModel, requests: 0, promptTokens: 0, completionTokens: 0, totalTokens: 0, cost: 0 };
  total.requests++;
  for (const [field, source] of [["promptTokens", "prompt_tokens"], ["completionTokens", "completion_tokens"], ["totalTokens", "total_tokens"]]) {
    const value = Number(usage[source]);
    if (Number.isFinite(value) && value >= 0) total[field] += value;
  }
  const cost = Number(usage.cost ?? usage.total_cost);
  if (Number.isFinite(cost) && cost >= 0) total.cost += cost;
  usageTotals.set(key, total);
}
let freeCatalogCache = { expiresAt: 0, data: [] };

function metricRoute(pathname) {
  return knownMetricRoutes.has(pathname) ? pathname : "/other";
}

function recordRequestMetric(method, route, status, durationMs) {
  const key = JSON.stringify([method, route, status]);
  const metric = requestMetrics.get(key) || { method, route, status, count: 0, durationSeconds: 0, buckets: Array(durationBuckets.length).fill(0) };
  metric.count++;
  const durationSeconds = durationMs / 1000;
  metric.durationSeconds += durationSeconds;
  durationBuckets.forEach((bound, index) => { if (durationSeconds <= bound) metric.buckets[index]++; });
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
    "# HELP mini_router_request_duration_seconds Request duration histogram.",
    "# TYPE mini_router_request_duration_seconds histogram",
  );
  const histograms = new Map();
  for (const metric of requestMetrics.values()) {
    const key = JSON.stringify([metric.method, metric.route]);
    const histogram = histograms.get(key) || { method: metric.method, route: metric.route, count: 0, durationSeconds: 0, buckets: Array(durationBuckets.length).fill(0) };
    histogram.count += metric.count;
    histogram.durationSeconds += metric.durationSeconds;
    metric.buckets.forEach((count, index) => { histogram.buckets[index] += count; });
    histograms.set(key, histogram);
  }
  for (const histogram of histograms.values()) {
    const labels = `method="${histogram.method}",route="${histogram.route}"`;
    lines.push(`mini_router_request_duration_seconds_sum{${labels}} ${histogram.durationSeconds.toFixed(6)}`);
    lines.push(`mini_router_request_duration_seconds_count{${labels}} ${histogram.count}`);
    durationBuckets.forEach((bound, index) => {
      lines.push(`mini_router_request_duration_seconds_bucket{${labels},le="${bound}"} ${histogram.buckets[index]}`);
    });
    lines.push(`mini_router_request_duration_seconds_bucket{${labels},le="+Inf"} ${histogram.count}`);
  }
  lines.push("# HELP mini_router_upstream_rate_limits_total Upstream HTTP 429 responses by provider.", "# TYPE mini_router_upstream_rate_limits_total counter");
  for (const [provider, count] of upstreamRateLimits) lines.push(`mini_router_upstream_rate_limits_total{provider="${provider.replaceAll("\\", "\\\\").replaceAll("\"", "\\\"").replaceAll("\n", "\\n")}"} ${count}`);
  return `${lines.join("\n")}\n`;
}

function rateLimitKey(req) {
  if (config.routerKey || Object.keys(config.clientApiKeys).length > 0) {
    return `key:${createHash("sha256").update(req.headers.authorization || "").digest("hex")}`;
  }
  return `ip:${req.socket.remoteAddress || "unknown"}`;
}

async function consumeRateLimit(req) {
  const now = Date.now();
  const key = rateLimitKey(req);
  if (redisClient) {
    try {
      const [countValue, ttlValue] = await redisClient.eval(redisRateLimitScript, {
        keys: [`${config.redisKeyPrefix}:rate:${key}`],
        arguments: [String(config.rateLimitWindowMs)],
      });
      const count = Number(countValue);
      if (count > config.rateLimitMaxRequests) {
        return { allowed: false, retryAfter: Math.max(1, Math.ceil(Number(ttlValue) / 1000)) };
      }
      return { allowed: true };
    } catch {
      throw redisStateUnavailable();
    }
  }
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

const redisRateLimitScript = `
local count = redis.call('INCR', KEYS[1])
if count == 1 then redis.call('PEXPIRE', KEYS[1], ARGV[1]) end
local ttl = redis.call('PTTL', KEYS[1])
return { count, ttl }
`;

function redisStateUnavailable() {
  const error = new Error("Shared state store is unavailable.");
  error.statusCode = 503;
  return error;
}

function redisProviderKey(providerName, state) {
  return `${config.redisKeyPrefix}:provider:${encodeURIComponent(providerName)}:${state}`;
}

async function isProviderCircuitOpen(provider) {
  if (!redisClient) return circuitFor(provider).openUntil > Date.now();
  try {
    return (await redisClient.exists(redisProviderKey(provider.name, "open"))) > 0;
  } catch {
    throw redisStateUnavailable();
  }
}

function circuitFor(provider) {
  if (!providerCircuits.has(provider.name)) {
    providerCircuits.set(provider.name, { failures: 0, openUntil: 0 });
  }
  return providerCircuits.get(provider.name);
}

async function recordProviderFailure(provider) {
  if (redisClient) {
    try {
      await redisClient.eval(
        "local n=redis.call('INCR',KEYS[1]); if n==1 then redis.call('PEXPIRE',KEYS[1],ARGV[2]) end; if n>=tonumber(ARGV[1]) then redis.call('SET',KEYS[2],'1','PX',ARGV[2]); redis.call('DEL',KEYS[1]); return 1 end; return 0",
        {
          keys: [redisProviderKey(provider.name, "failures"), redisProviderKey(provider.name, "open")],
          arguments: [String(config.circuitFailureThreshold), String(config.circuitOpenMs)],
        },
      );
      return;
    } catch {
      throw redisStateUnavailable();
    }
  }
  const circuit = circuitFor(provider);
  circuit.failures++;
  if (circuit.failures >= config.circuitFailureThreshold) {
    circuit.openUntil = Date.now() + config.circuitOpenMs;
  }
}

async function recordProviderSuccess(provider) {
  if (redisClient) {
    try {
      await redisClient.del([
        redisProviderKey(provider.name, "failures"),
        redisProviderKey(provider.name, "open"),
      ]);
      return;
    } catch {
      throw redisStateUnavailable();
    }
  }
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
      const response = await tracer.startActiveSpan("llm.provider.request", {
        attributes: {
          "llm.provider.name": provider.name,
          "llm.model.requested": body.model,
          "llm.model.upstream": provider.models[body.model],
          "llm.provider.attempt": attempt + 1,
        },
      }, async (span) => {
        try {
          const request = buildProviderRequest(provider, body);
          const response = await fetch(request.url, {
            method: "POST",
            headers: {
              "content-type": "application/json",
              ...request.headers,
            },
            body: JSON.stringify(request.body),
            signal: AbortSignal.any([requestSignal, AbortSignal.timeout(provider.timeoutMs)]),
          });
          span.setAttribute("http.response.status_code", response.status);
          if (response.status >= 500) span.setStatus({ code: SpanStatusCode.ERROR });
          return response;
        } catch (error) {
          span.recordException(error);
          span.setStatus({ code: SpanStatusCode.ERROR });
          throw error;
        } finally {
          span.end();
        }
      });
      if (!retryableStatuses.has(response.status) || attempt === config.maxRetries) {
        return await adaptProviderResponse(provider, response, body);
      }
      const delay = retryDelay(response, attempt);
      await response.body?.cancel();
      await new Promise((resolve) => setTimeout(resolve, delay));
      continue;
    } catch (error) {
      if (error.statusCode || error.name === "TimeoutError" || requestSignal.aborted || attempt === config.maxRetries) throw error;
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
    if (await isProviderCircuitOpen(candidates[index])) {
      openProviders++;
      continue;
    }
    try {
      const response = await fetchUpstream(candidates[index], body, requestSignal);
      if (response.status === 429) upstreamRateLimits.set(candidates[index].name, (upstreamRateLimits.get(candidates[index].name) || 0) + 1);
      if (response.status >= 500) await recordProviderFailure(candidates[index]);
      else await recordProviderSuccess(candidates[index]);
      const canFallback = response.status === 429 || retryableStatuses.has(response.status);
      if (canFallback && index < candidates.length - 1) {
        await response.body?.cancel();
        continue;
      }
      return { response, provider: candidates[index] };
    } catch (error) {
      if (requestSignal.aborted) throw error;
      if (error.statusCode) throw error;
      await recordProviderFailure(candidates[index]);
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
    const ready = config.providers.length > 0 && (!redisClient || redisClient.isReady);
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

  if (req.method === "GET" && pathname === "/openapi.json") {
    res.writeHead(200, { "content-type": "application/json; charset=utf-8" });
    return res.end(openApiDocument);
  }
  if (req.method === "GET" && pathname === "/docs") {
    res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
    return res.end(swaggerHtml);
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

  if (req.method === "GET" && pathname === "/v1/catalog/free-models") {
    if (!authorized(req)) return json(res, 401, { error: { message: "Invalid router API key." } });
    if (Date.now() >= freeCatalogCache.expiresAt) {
      try {
        const response = await fetch("https://openrouter.ai/api/v1/models", { signal: AbortSignal.timeout(8000) });
        if (!response.ok) throw new Error(`OpenRouter returned HTTP ${response.status}.`);
        const catalog = await response.json();
        freeCatalogCache = {
          expiresAt: Date.now() + 5 * 60_000,
          data: (Array.isArray(catalog.data) ? catalog.data : []).filter((model) =>
            typeof model?.id === "string" && model.id.endsWith(":free") &&
            Number(model?.pricing?.prompt) === 0 && Number(model?.pricing?.completion) === 0,
          ).map(({ id, name, description, context_length: contextLength, pricing }) => ({
            id, name, description, context_length: contextLength, pricing,
          })),
        };
      } catch {
        if (freeCatalogCache.expiresAt <= Date.now()) {
          return json(res, 503, { error: { message: "OpenRouter's free model catalog is temporarily unavailable." } });
        }
      }
    }
    return json(res, 200, { object: "list", data: freeCatalogCache.data, cached: true });
  }

  if (req.method === "GET" && pathname === "/v1/usage") {
    if (!authorized(req)) return json(res, 401, { error: { message: "Invalid router API key." } });
    return json(res, 200, { object: "usage", data: [...usageTotals.values()] });
  }

  if (req.method !== "POST" || pathname !== "/v1/chat/completions") {
    return json(res, 404, { error: { message: "Not found. Try POST /v1/chat/completions or GET /v1/models." } });
  }
  if (!authorized(req)) return json(res, 401, { error: { message: "Invalid router API key." } });
  if (config.providers.length === 0) {
    return json(res, 503, { error: { message: "Configure PROVIDERS_JSON or the legacy UPSTREAM_* settings in .env." } });
  }
  let rateLimit;
  try {
    rateLimit = await consumeRateLimit(req);
  } catch {
    return json(res, 503, { error: { message: "Shared rate-limit store is unavailable." } });
  }
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

  const cacheKey = redisClient && config.cacheTtlSeconds > 0 && body.stream !== true
    ? `${config.redisKeyPrefix}:completion:${createHash("sha256").update(JSON.stringify({ body, providers: config.providers.map(({ name, baseUrl, models }) => ({ name, baseUrl, models })) })).digest("hex")}`
    : undefined;
  if (cacheKey) {
    res.setHeader("x-router-cache", "MISS");
    try {
      const cached = await redisClient.get(cacheKey);
      if (cached) {
        const entry = JSON.parse(cached);
        res.setHeader("x-router-cache", "HIT");
        res.writeHead(entry.status, { "content-type": entry.contentType });
        return res.end(entry.body);
      }
    } catch {
      console.error(JSON.stringify({ event: "completion_cache_read_failed" }));
    }
  }

  try {
    const upstream = await tracer.startActiveSpan("llm.router.route", {
      attributes: { "llm.model.requested": body.model },
    }, async (span) => {
      try {
        const result = await fetchWithFallback(body.model, body, requestAbort.signal);
        span.setAttribute("llm.provider.name", result.provider.name);
        span.setAttribute("http.response.status_code", result.response.status);
        return result;
      } catch (error) {
        span.recordException(error);
        span.setStatus({ code: SpanStatusCode.ERROR });
        throw error;
      } finally {
        span.end();
      }
    });
    const { response: upstreamResponse, provider: upstreamProvider } = upstream;
    const contentType = upstreamResponse.headers.get("content-type") || "application/json";
    if (upstreamResponse.status === 429) {
      const retryAfter = upstreamResponse.headers.get("retry-after");
      if (retryAfter) res.setHeader("retry-after", String(Math.max(1, Math.ceil(retryDelay(upstreamResponse, 0) / 1000))));
    }
    if (body.stream === true) {
      res.writeHead(upstreamResponse.status, { "content-type": contentType });
      if (upstreamResponse.body) await pipeline(Readable.fromWeb(upstreamResponse.body), res);
      else res.end();
      return;
    }
    const responseBody = await upstreamResponse.text();
    if (upstreamResponse.ok) {
      try { recordUsage(upstreamProvider, body.model, JSON.parse(responseBody).usage); } catch { /* Ignore non-JSON provider responses. */ }
    }
    if (cacheKey && upstreamResponse.ok && contentType.includes("application/json")) {
      try {
        await redisClient.set(cacheKey, JSON.stringify({ status: upstreamResponse.status, contentType, body: responseBody }), { EX: config.cacheTtlSeconds });
      } catch {
        console.error(JSON.stringify({ event: "completion_cache_write_failed" }));
      }
    }
    res.writeHead(upstreamResponse.status, { "content-type": contentType });
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
  server.close(async () => {
    await shutdownTelemetry();
    try {
      await redisClient?.close();
    } catch {}
    process.exit(0);
  });
  const forceClose = setTimeout(() => server.closeAllConnections(), 10_000);
  forceClose.unref();
}

process.once("SIGINT", () => shutdown("SIGINT"));
process.once("SIGTERM", () => shutdown("SIGTERM"));
