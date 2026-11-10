# Mini LLM Router

A small local project that accepts an OpenAI-compatible chat request and forwards it to one configured OpenAI-compatible provider. It is a learning starter: it does not yet include multi-provider routing, streaming, a dashboard, or encrypted key storage.

## Requirements

- Node.js 20 or newer
- An API key for an OpenAI-compatible provider (the provider may charge for usage)

## Run on Windows PowerShell

1. Copy `.env.example` to `.env`.
2. Edit `.env` and set the provider's base URL, API key, and model ID. Keep `.env` private; it is ignored by Git.
3. In this folder, run:

   ```powershell
   npm start
   ```

The server binds to `127.0.0.1:3001` by default, so it is available only on your own computer. `GET /health` checks that the server is running. If you set `ROUTER_API_KEY`, clients must send it as a Bearer token.

The server validates `PORT` and requires `UPSTREAM_BASE_URL`, when provided, to be an absolute HTTP or HTTPS URL without embedded credentials. Leaving provider settings blank is allowed; chat requests return a configuration error until they are set.

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

If `ROUTER_API_KEY` is blank, omit the `-Headers` argument. The requested model name is replaced with `UPSTREAM_MODEL`, because this starter routes to one configured model.

## Endpoints

- `GET /health` — local status and whether provider settings are present.
- `GET /v1/models` — the configured model in OpenAI-style list format.
- `POST /v1/chat/completions` — forwards chat requests and provider responses.

Never commit `.env` or share API keys. This starter is intended for local learning, not for an internet-facing service. Check your provider's terms and pricing before sending requests.
