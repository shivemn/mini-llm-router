import http from "node:http";

function readConfig(env = process.env) {
  const rawPort = env.PORT || "3001";
  const port = Number(rawPort);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error("PORT must be an integer from 1 to 65535.");
  }

  let baseUrl;
  if (env.UPSTREAM_BASE_URL) {
    let parsed;
    try {
      parsed = new URL(env.UPSTREAM_BASE_URL);
    } catch {
      throw new Error("UPSTREAM_BASE_URL must be a valid absolute URL.");
    }
    if (!["http:", "https:"].includes(parsed.protocol) || parsed.username || parsed.password) {
      throw new Error("UPSTREAM_BASE_URL must use HTTP or HTTPS and must not contain credentials.");
    }
    baseUrl = env.UPSTREAM_BASE_URL.replace(/\/+$/, "");
  }

  return {
    host: env.HOST || "127.0.0.1",
    port,
    baseUrl,
    providerKey: env.UPSTREAM_API_KEY,
    model: env.UPSTREAM_MODEL,
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

const server = http.createServer(async (req, res) => {
  const pathname = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`).pathname;

  if (req.method === "GET" && pathname === "/health") {
    return json(res, 200, { ok: true, configured: Boolean(config.baseUrl && config.providerKey && config.model) });
  }

  if (req.method === "GET" && pathname === "/v1/models") {
    if (!authorized(req)) return json(res, 401, { error: { message: "Invalid router API key." } });
    if (!config.model) return json(res, 503, { error: { message: "Set UPSTREAM_MODEL in .env first." } });
    return json(res, 200, { object: "list", data: [{ id: config.model, object: "model", owned_by: "upstream" }] });
  }

  if (req.method !== "POST" || pathname !== "/v1/chat/completions") {
    return json(res, 404, { error: { message: "Not found. Try POST /v1/chat/completions or GET /v1/models." } });
  }
  if (!authorized(req)) return json(res, 401, { error: { message: "Invalid router API key." } });
  if (!config.baseUrl || !config.providerKey || !config.model) {
    return json(res, 503, { error: { message: "Configure UPSTREAM_BASE_URL, UPSTREAM_API_KEY, and UPSTREAM_MODEL in .env." } });
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

  try {
    const upstream = await fetch(`${config.baseUrl}/chat/completions`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${config.providerKey}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({ ...body, model: config.model }),
      signal: AbortSignal.timeout(120_000),
    });
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
  if (!config.baseUrl || !config.providerKey || !config.model) {
    console.log("Add provider settings to .env to enable chat requests.");
  }
});
