# Mini LLM Router

A local LLM router that accepts OpenAI-compatible chat requests and routes each requested model name to a configured provider and model ID. The default example uses OpenRouter's free model router; the project also includes Redis shared state, metrics, traces, and a Grafana dashboard.

## Requirements

- Node.js 20 or newer
- An OpenRouter API key for its currently free models (no subscription or card required to start; free usage is rate-limited)

## Run on Windows PowerShell

1. Copy `.env.example` to `.env`.
2. Create a key at [OpenRouter](https://openrouter.ai/keys) and set `OPENROUTER_API_KEY` in `.env`. The example maps the public model name `free` to `openrouter/free`, which selects an available free model.
3. Keep `.env` private; it is ignored by Git.
4. In this folder, run:

   ```powershell
   npm start
   ```

The server binds to `127.0.0.1:3001` by default, so it is available only on your own computer. `GET /health` checks that the server is running. If you set `ROUTER_API_KEY`, clients must send it as a Bearer token. Binding to any non-loopback `HOST` requires `ROUTER_API_KEY` at startup.

`FREE_ONLY_MODE` defaults to `true`. Startup then accepts only OpenRouter's `openrouter/free` or `:free` model IDs, plus local endpoints such as Ollama on `localhost`, `host.docker.internal`, or `ollama`. This prevents a paid OpenRouter model or an unrelated hosted provider from being configured by mistake. Set it to `false` only if you intentionally want paid or other hosted providers.

### Optional AWS Secrets Manager

The default OpenRouter setup reads `OPENROUTER_API_KEY` from `.env` and does not call AWS. Secrets Manager is optional and may incur charges (AWS lists $0.40 per secret per month plus API calls); skip this integration if you need to keep the setup free. [AWS pricing](https://aws.amazon.com/secrets-manager/pricing/)

If you choose to use Secrets Manager, provider configuration accepts `apiKeySecretId`, and the router reads each secret once during startup. Compose mounts the Windows user's `.aws` directory read-only only when using the AWS override file:

```powershell
docker compose -f compose.yaml -f compose.aws.yaml up --build
```

The AWS runtime identity needs `secretsmanager:GetSecretValue`; see the [example IAM policy](aws/secrets-manager-read-policy.example.json). `apiKeyEnv` is also supported to read a key from a named local environment variable.

### Provider protocols

The client endpoint remains `POST /v1/chat/completions`. Set each provider's `protocol` to `openai-compatible` (the default), `anthropic`, or `gemini`. OpenAI-compatible providers receive the chat-completions request directly with the configured model substitution. Anthropic uses the Messages API, and Gemini uses `generateContent` or `streamGenerateContent`; their text responses and SSE text chunks are converted back into OpenAI-style chat-completion responses. The Anthropic base URL can be `https://api.anthropic.com` (or end in `/v1`); the Gemini base URL should include `/v1beta`. These adapters currently handle text messages and common generation settings. Tool calls and provider-specific multimodal inputs require protocol-specific mapping and are not supported by these adapters yet.

`MAX_UPSTREAM_RETRIES` controls retries for temporary network failures and upstream HTTP 408, 429, 500, 502, 503, and 504 responses. It defaults to 0 because retrying a chat request can duplicate provider charges, and accepts values from 0 to 5. Retries use exponential backoff; valid `Retry-After` headers are honored up to 10 seconds. Timeouts are not retried.

`MAX_CONCURRENT_REQUESTS` caps in-flight chat requests per process. It defaults to 100 and accepts values from 1 to 1000. Requests above the cap receive HTTP 503 with `Retry-After: 1`.

Each provider has an in-memory circuit breaker. After `CIRCUIT_BREAKER_FAILURE_THRESHOLD` server failures or connection errors (default 3), it is skipped for `CIRCUIT_BREAKER_OPEN_MS` (default 30000 ms) while other configured providers are tried. When all providers for a model are paused, requests receive HTTP 503. Circuit state resets when a provider responds without a server error.

`RATE_LIMIT_MAX_REQUESTS` limits each router key (or source IP when router authentication is disabled) to a fixed-window request count. The default is 60 requests per 60000 ms. Excess requests receive HTTP 429 with a `Retry-After` header. These in-memory limits apply per process.

Set `REDIS_URL` to share rate-limit counters and circuit-breaker state across router processes. Redis operations use atomic commands/scripts; if the shared store becomes unavailable, chat requests fail closed with HTTP 503. Without `REDIS_URL`, these features use process-local memory. `REDIS_KEY_PREFIX` defaults to `mini-llm-router` to isolate keys when Redis is shared.

Requests with `"stream": true` pass the upstream response stream through to the client, including its content type and server-sent event chunks.

Each response includes an `X-Request-ID` header. The server writes one JSON log entry per completed request with its ID, method, path, status, and duration; it does not log request bodies or credentials.

### OpenTelemetry tracing

Set `OTEL_EXPORTER_OTLP_ENDPOINT` to an OTLP/HTTP collector URL to enable traces; leave it blank to keep tracing disabled. The router creates spans for provider selection and individual provider attempts, and the HTTP and `fetch` instrumentations capture inbound and outbound request spans. Trace attributes include provider and model names, status, and attempt number; prompts and API keys are not recorded. The startup command enables the OpenTelemetry ESM loader required by this project's module format. See the [OpenTelemetry Node.js guide](https://opentelemetry.io/docs/languages/js/getting-started/nodejs/) and [ESM support notes](https://github.com/open-telemetry/opentelemetry-js/blob/main/doc/esm-support.md).

The `/metrics` endpoint exposes Prometheus request counters and duration histograms with bounded routes and status labels. When `ROUTER_API_KEY` is set, metrics require that same key; Compose passes it to Prometheus through a mounted secret. The histogram supports Prometheus percentile calculations such as p50 and p95.

On Ctrl+C or SIGTERM, the server stops accepting new connections and allows active requests up to 10 seconds to finish before closing them.

Example provider configuration:

```dotenv
OPENROUTER_API_KEY=your-openrouter-key
PROVIDERS_JSON={"openrouter":{"protocol":"openai-compatible","baseUrl":"https://openrouter.ai/api/v1","apiKeyEnv":"OPENROUTER_API_KEY","models":{"free":"openrouter/free"}}}
```

Clients can request `fast` or `local-model`; the router sends the mapped upstream model ID. `timeoutMs` is optional per provider, defaults to 120000, and accepts values from 100 to 300000. `GET /v1/models` lists configured public names. If multiple providers define the same public model name, the router tries them in configuration order and falls back on rate limits, temporary server failures, or connection errors. The legacy `UPSTREAM_BASE_URL`, `UPSTREAM_API_KEY`, and `UPSTREAM_MODEL` settings remain supported as a single-provider configuration; if you use them, set all three. The server validates `PORT` and each provider URL, rejects URLs with embedded credentials, requires JSON request content, and enforces a 1 MB request body limit. Leaving provider settings blank is allowed; chat requests return a configuration error until they are set.

Run the automated checks with:

```powershell
npm test
```

GitHub Actions runs this suite and builds the Docker image on pushes and pull requests.

## Run with Docker Compose

Copy `.env.example` to `.env`, add your OpenRouter key, and replace `GRAFANA_ADMIN_PASSWORD`. AWS credentials are not needed for the free OpenRouter setup. Then run:

```powershell
docker compose up --build
```

Compose starts Redis with persistent shared rate-limit and circuit state, Prometheus with 15-day metric retention, Grafana with a provisioned router dashboard, an OpenTelemetry Collector, and Tempo with local trace storage for development. Grafana is available at `http://127.0.0.1:3000` (the default login is `admin`; use the password from `.env`), and Prometheus at `http://127.0.0.1:9090`. The router and monitoring UIs bind to loopback on the host. Tempo's local filesystem storage is for development; use object storage and appropriate retention for production. The router image runs as the unprivileged `node` user and has a liveness health check.

## Try a request

In a second PowerShell window:

```powershell
$headers = @{ Authorization = "Bearer replace-with-your-router-key" }
$body = @{
  model = "free"
  messages = @(@{ role = "user"; content = "Explain APIs in one sentence." })
} | ConvertTo-Json -Depth 5
Invoke-RestMethod -Uri http://127.0.0.1:3001/v1/chat/completions -Method Post -Headers $headers -ContentType "application/json" -Body $body
```

If `ROUTER_API_KEY` is blank, omit the `-Headers` argument. Set the request's `model` to one of the public names configured in `PROVIDERS_JSON` (the default free route is `free`).

## Endpoints

- `GET /health` — local status and whether provider settings are present.
- `GET /health/live` — process liveness probe.
- `GET /health/ready` — readiness probe; returns HTTP 503 until a provider is configured.
- `GET /metrics` — Prometheus text metrics for request counts and durations; protected by `ROUTER_API_KEY` when configured.
- `GET /v1/models` — configured public model names in OpenAI-style list format.
- `GET /v1/catalog/free-models` — cached list of OpenRouter models marked `:free` with zero prompt and completion pricing; protected by `ROUTER_API_KEY` when configured.
- `POST /v1/chat/completions` — forwards chat requests and provider responses.

Never commit `.env` or share API keys. This starter is intended for local learning, not for an internet-facing service. Check your provider's terms and pricing before sending requests.
