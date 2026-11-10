import http from "node:http";

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
    models: Object.fromEntries(models),
  };
}

function readConfig(env = process.env) {
  const rawPort = env.PORT || "3001";
  const port = Number(rawPort);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error("PORT must be an integer from 1 to 65535.");
  }
  const maxRetries = Number(env.MAX_UPSTREAM_RETRIES ?? "1");
  if (!Number.isInteger(maxRetries) || maxRetries < 0 || maxRetries > 5) {
    throw new Error("MAX_UPSTREAM_RETRIES must be an integer from 0 to 5.");
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
  } else if (env.UPSTREAM_BASE_URL && env.UPSTREAM_API_KEY && env.UPSTREAM_MODEL) {
    providers = [normalizeProvider("default", {
      baseUrl: env.UPSTREAM_BASE_URL,
      apiKey: env.UPSTREAM_API_KEY,
      models: { [env.UPSTREAM_MODEL]: env.UPSTREAM_MODEL },
    })];
  }

  return {
    host: env.HOST || "127.0.0.1",
    port,
    maxRetries,
    providers,
    routerKey: env.ROUTER_API_KEY,
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
  let raw = "";
  for await (const chunk of req) {
    raw += chunk;
    if (raw.length > 1_000_000) throw new Error("Request body is too large (1 MB maximum).");
  }
  try {
    return JSON.parse(raw);
  } catch {
    throw new Error("Request body must be valid JSON.");
  }
}

const retryableStatuses = new Set([408, 500, 502, 503, 504]);

async function fetchUpstream(provider, body) {
  for (let attempt = 0; attempt <= config.maxRetries; attempt++) {
    try {
      const response = await fetch(`${provider.baseUrl}/chat/completions`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${provider.apiKey}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({ ...body, model: provider.models[body.model] }),
        signal: AbortSignal.timeout(120_000),
      });
      if (!retryableStatuses.has(response.status) || attempt === config.maxRetries) return response;
      await response.body?.cancel();
    } catch (error) {
      if (error.name === "TimeoutError" || attempt === config.maxRetries) throw error;
    }
    await new Promise((resolve) => setTimeout(resolve, 100 * (2 ** attempt)));
  }
  throw new Error("Upstream retries exhausted.");
}

const server = http.createServer(async (req, res) => {
  const pathname = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`).pathname;

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
  const provider = config.providers.find((candidate) => Object.hasOwn(candidate.models, body.model));
  if (!provider) {
    return json(res, 404, { error: { message: `Unknown model '${body.model}'. List available models with GET /v1/models.` } });
  }

  try {
    const upstream = await fetchUpstream(provider, body);
    const contentType = upstream.headers.get("content-type") || "application/json";
    const responseBody = await upstream.text();
    res.writeHead(upstream.status, { "content-type": contentType });
    res.end(responseBody);
  } catch (error) {
    const timedOut = error.name === "TimeoutError";
    return json(res, timedOut ? 504 : 502, {
      error: { message: timedOut ? "The upstream provider took too long to respond." : "Could not reach the upstream provider." },
    });
  }
});

server.listen(config.port, config.host, () => {
  console.log(`Mini LLM Router listening at http://${config.host}:${config.port}`);
  if (config.providers.length === 0) {
    console.log("Add provider settings to .env to enable chat requests.");
  }
});
