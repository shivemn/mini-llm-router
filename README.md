# Mini LLM Router

A small local project that accepts OpenAI-compatible chat requests and routes each requested model name to a configured upstream provider and model ID. It is a learning starter: it does not yet include automatic fallback, streaming, a dashboard, or encrypted key storage.

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

The server binds to `127.0.0.1:3001` by default, so it is available only on your own computer. `GET /health` checks that the server is running. If you set `ROUTER_API_KEY`, clients must send it as a Bearer token.

`MAX_UPSTREAM_RETRIES` controls retries for temporary network failures and upstream HTTP 408, 500, 502, 503, and 504 responses. It defaults to 1 and accepts values from 0 to 5. Each retry waits briefly with exponential backoff; timeouts and rate-limit responses are returned without retrying.

Example provider configuration:

```dotenv
PROVIDERS_JSON={"openai":{"baseUrl":"https://api.openai.com/v1","apiKey":"your-provider-key","models":{"fast":"gpt-4o-mini"}},"local":{"baseUrl":"http://127.0.0.1:11434/v1","apiKey":"local-token","models":{"local-model":"llama3.1"}}}
```

Clients can request `fast` or `local-model`; the router sends the mapped upstream model ID. `GET /v1/models` lists configured public names. If multiple providers define the same public model name, the router tries them in configuration order and falls back on rate limits, temporary server failures, or connection errors. The legacy `UPSTREAM_BASE_URL`, `UPSTREAM_API_KEY`, and `UPSTREAM_MODEL` settings remain supported as a single-provider configuration. The server validates `PORT` and each provider URL, and rejects URLs with embedded credentials. Leaving provider settings blank is allowed; chat requests return a configuration error until they are set.

Run the automated checks with:

```powershell
npm test
```

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
- `GET /v1/models` — configured public model names in OpenAI-style list format.
- `POST /v1/chat/completions` — forwards chat requests and provider responses.

Never commit `.env` or share API keys. This starter is intended for local learning, not for an internet-facing service. Check your provider's terms and pricing before sending requests.
