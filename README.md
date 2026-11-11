# Mini LLM Router

A small local project that accepts OpenAI-compatible chat requests and routes each requested model name to a configured upstream provider and model ID. It is a learning starter: it does not yet include a dashboard or encrypted key storage.

## Requirements

- Node.js 20 or newer
- An API key for an OpenAI-compatible provider (the provider may charge for usage)

## Run on Windows PowerShell

1. Copy `.env.example` to `.env`.
2. Edit `.env` and set `PROVIDERS_JSON` with provider names, base URLs, API keys, and public-name-to-upstream-model mappings. Keep `.env` private; it is ignored by Git.
3. In this folder, run:

   ```powershell
   npm start
   ```

The server binds to `127.0.0.1:3001` by default, so it is available only on your own computer. `GET /health` checks that the server is running. If you set `ROUTER_API_KEY`, clients must send it as a Bearer token. Binding to any non-loopback `HOST` requires `ROUTER_API_KEY` at startup.

`MAX_UPSTREAM_RETRIES` controls retries for temporary network failures and upstream HTTP 408, 429, 500, 502, 503, and 504 responses. It defaults to 0 because retrying a chat request can duplicate provider charges, and accepts values from 0 to 5. Retries use exponential backoff; valid `Retry-After` headers are honored up to 10 seconds. Timeouts are not retried.

`MAX_CONCURRENT_REQUESTS` caps in-flight chat requests per process. It defaults to 100 and accepts values from 1 to 1000. Requests above the cap receive HTTP 503 with `Retry-After: 1`.

Each provider has an in-memory circuit breaker. After `CIRCUIT_BREAKER_FAILURE_THRESHOLD` server failures or connection errors (default 3), it is skipped for `CIRCUIT_BREAKER_OPEN_MS` (default 30000 ms) while other configured providers are tried. When all providers for a model are paused, requests receive HTTP 503. Circuit state resets when a provider responds without a server error.

`RATE_LIMIT_MAX_REQUESTS` limits each router key (or source IP when router authentication is disabled) to a fixed-window request count. The default is 60 requests per 60000 ms. Excess requests receive HTTP 429 with a `Retry-After` header. These in-memory limits apply per process.

Set `REDIS_URL` to share rate-limit counters and circuit-breaker state across router processes. Redis operations use atomic commands/scripts; if the shared store becomes unavailable, chat requests fail closed with HTTP 503. Without `REDIS_URL`, these features use process-local memory. `REDIS_KEY_PREFIX` defaults to `mini-llm-router` to isolate keys when Redis is shared.

Requests with `"stream": true` pass the upstream response stream through to the client, including its content type and server-sent event chunks.

Each response includes an `X-Request-ID` header. The server writes one JSON log entry per completed request with its ID, method, path, status, and duration; it does not log request bodies or credentials.

### OpenTelemetry tracing

Set `OTEL_EXPORTER_OTLP_ENDPOINT` to an OTLP/HTTP collector URL to enable traces; leave it blank to keep tracing disabled. The router creates spans for provider selection and individual provider attempts, and the HTTP and `fetch` instrumentations capture inbound and outbound request spans. Trace attributes include provider and model names, status, and attempt number; prompts and API keys are not recorded. The startup command enables the OpenTelemetry ESM loader required by this project's module format. See the [OpenTelemetry Node.js guide](https://opentelemetry.io/docs/languages/js/getting-started/nodejs/) and [ESM support notes](https://github.com/open-telemetry/opentelemetry-js/blob/main/doc/esm-support.md).

On Ctrl+C or SIGTERM, the server stops accepting new connections and allows active requests up to 10 seconds to finish before closing them.

Example provider configuration:

```dotenv
PROVIDERS_JSON={"openai":{"baseUrl":"https://api.openai.com/v1","apiKey":"your-provider-key","timeoutMs":30000,"models":{"fast":"gpt-4o-mini"}},"local":{"baseUrl":"http://127.0.0.1:11434/v1","apiKey":"local-token","models":{"local-model":"llama3.1"}}}
```

Clients can request `fast` or `local-model`; the router sends the mapped upstream model ID. `timeoutMs` is optional per provider, defaults to 120000, and accepts values from 100 to 300000. `GET /v1/models` lists configured public names. If multiple providers define the same public model name, the router tries them in configuration order and falls back on rate limits, temporary server failures, or connection errors. The legacy `UPSTREAM_BASE_URL`, `UPSTREAM_API_KEY`, and `UPSTREAM_MODEL` settings remain supported as a single-provider configuration; if you use them, set all three. The server validates `PORT` and each provider URL, rejects URLs with embedded credentials, requires JSON request content, and enforces a 1 MB request body limit. Leaving provider settings blank is allowed; chat requests return a configuration error until they are set.

Run the automated checks with:

```powershell
npm test
```

GitHub Actions runs this suite and builds the Docker image on pushes and pull requests.

## Run with Docker Compose

Copy `.env.example` to `.env`, configure at least one provider, and set a non-empty `ROUTER_API_KEY`. Then run:

```powershell
docker compose up --build
```

Compose starts a Redis service with persistent data for shared rate-limit and circuit-breaker state. It binds the router port to `127.0.0.1:3001` on the host. The container listens on its internal network interface and requires the router key. The image runs as the unprivileged `node` user and has a liveness health check.

## Try a request

In a second PowerShell window:

```powershell
$headers = @{ Authorization = "Bearer replace-with-your-router-key" }
$body = @{
  model = "any-name"
  messages = @(@{ role = "user"; content = "Explain APIs in one sentence." })
} | ConvertTo-Json -Depth 5
Invoke-RestMethod -Uri http://127.0.0.1:3001/v1/chat/completions -Method Post -Headers $headers -ContentType "application/json" -Body $body
```

If `ROUTER_API_KEY` is blank, omit the `-Headers` argument. Set the request's `model` to one of the public names configured in `PROVIDERS_JSON` (or to `UPSTREAM_MODEL` in legacy mode).

## Endpoints

- `GET /health` — local status and whether provider settings are present.
- `GET /health/live` — process liveness probe.
- `GET /health/ready` — readiness probe; returns HTTP 503 until a provider is configured.
- `GET /metrics` — Prometheus text metrics for request counts and durations; protected by `ROUTER_API_KEY` when configured.
- `GET /v1/models` — configured public model names in OpenAI-style list format.
- `POST /v1/chat/completions` — forwards chat requests and provider responses.

Never commit `.env` or share API keys. This starter is intended for local learning, not for an internet-facing service. Check your provider's terms and pricing before sending requests.
