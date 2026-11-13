import assert from "node:assert/strict";
import { test } from "node:test";
import { adaptProviderResponse, buildProviderRequest } from "../providers/adapters.mjs";

const baseBody = {
  model: "fast",
  messages: [{ role: "user", content: "hello" }],
};

test("builds Anthropic Messages API requests", () => {
  const provider = { protocol: "anthropic", baseUrl: "https://api.anthropic.test/v1", apiKey: "test-key", models: { fast: "claude-test" } };
  const request = buildProviderRequest(provider, { ...baseBody, messages: [{ role: "system", content: "Be concise" }, ...baseBody.messages], max_tokens: 80 });
  assert.equal(request.url, "https://api.anthropic.test/v1/messages");
  assert.equal(request.headers["x-api-key"], "test-key");
  assert.deepEqual(request.body.messages, [{ role: "user", content: "hello" }]);
  assert.equal(request.body.system, "Be concise");
  assert.equal(request.body.max_tokens, 80);
});

test("builds Gemini generateContent requests", () => {
  const provider = { protocol: "gemini", baseUrl: "https://generativelanguage.googleapis.test/v1beta", apiKey: "test-key", models: { fast: "gemini-test" } };
  const request = buildProviderRequest(provider, { ...baseBody, max_tokens: 90 });
  assert.equal(request.url, "https://generativelanguage.googleapis.test/v1beta/models/gemini-test:generateContent");
  assert.equal(request.headers["x-goog-api-key"], "test-key");
  assert.deepEqual(request.body.contents, [{ role: "user", parts: [{ text: "hello" }] }]);
  assert.equal(request.body.generationConfig.maxOutputTokens, 90);
});

test("normalizes Anthropic completion text and token usage", async () => {
  const provider = { protocol: "anthropic" };
  const response = await adaptProviderResponse(provider, new Response(JSON.stringify({
    id: "msg-1", type: "message", content: [{ type: "text", text: "hello back" }], stop_reason: "end_turn",
    usage: { input_tokens: 4, output_tokens: 3 },
  })), baseBody);
  const result = await response.json();
  assert.equal(result.choices[0].message.content, "hello back");
  assert.equal(result.choices[0].finish_reason, "stop");
  assert.deepEqual(result.usage, { prompt_tokens: 4, completion_tokens: 3, total_tokens: 7 });
});

test("normalizes Gemini completion text and token usage", async () => {
  const provider = { protocol: "gemini" };
  const response = await adaptProviderResponse(provider, new Response(JSON.stringify({
    responseId: "gemini-1", candidates: [{ content: { parts: [{ text: "hello back" }] }, finishReason: "STOP" }],
    usageMetadata: { promptTokenCount: 5, candidatesTokenCount: 2, totalTokenCount: 7 },
  })), baseBody);
  const result = await response.json();
  assert.equal(result.choices[0].message.content, "hello back");
  assert.equal(result.choices[0].finish_reason, "stop");
  assert.deepEqual(result.usage, { prompt_tokens: 5, completion_tokens: 2, total_tokens: 7 });
});

test("normalizes Anthropic streamed text and final usage", async () => {
  const provider = { protocol: "anthropic" };
  const events = [
    { type: "message_start", message: { id: "msg-stream", usage: { input_tokens: 6 } } },
    { type: "content_block_delta", delta: { type: "text_delta", text: "streamed" } },
    { type: "message_delta", delta: { stop_reason: "end_turn" }, usage: { output_tokens: 4 } },
  ].map((event) => `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`).join("");
  const response = await adaptProviderResponse(provider, new Response(events, { headers: { "content-type": "text/event-stream" } }), { ...baseBody, stream: true });
  const text = await response.text();
  const chunks = [...text.matchAll(/^data: (\{.*\})$/gm)].map((match) => JSON.parse(match[1]));
  assert.equal(chunks.some((chunk) => chunk.choices[0].delta.content === "streamed"), true);
  assert.deepEqual(chunks.find((chunk) => chunk.usage)?.usage, { prompt_tokens: 6, completion_tokens: 4, total_tokens: 10 });
});

test("normalizes Gemini streamed text and final usage", async () => {
  const provider = { protocol: "gemini" };
  const events = [
    { candidates: [{ content: { parts: [{ text: "streamed" }] } }] },
    { candidates: [{ finishReason: "STOP" }], usageMetadata: { promptTokenCount: 6, candidatesTokenCount: 4, totalTokenCount: 10 } },
  ].map((event) => `data: ${JSON.stringify(event)}\n\n`).join("");
  const response = await adaptProviderResponse(provider, new Response(events, { headers: { "content-type": "text/event-stream" } }), { ...baseBody, stream: true });
  const text = await response.text();
  const chunks = [...text.matchAll(/^data: (\{.*\})$/gm)].map((match) => JSON.parse(match[1]));
  assert.equal(chunks.some((chunk) => chunk.choices[0].delta.content === "streamed"), true);
  assert.deepEqual(chunks.find((chunk) => chunk.usage)?.usage, { prompt_tokens: 6, completion_tokens: 4, total_tokens: 10 });
});

test("requests OpenRouter usage in streaming responses only", () => {
  const openRouter = { protocol: "openai-compatible", baseUrl: "https://openrouter.ai/api/v1", apiKey: "test-key", models: { fast: "openrouter/free" } };
  const local = { protocol: "openai-compatible", baseUrl: "http://127.0.0.1:11434/v1", apiKey: "local", models: { fast: "qwen-test" } };
  assert.equal(buildProviderRequest(openRouter, { ...baseBody, stream: true }).body.stream_options.include_usage, true);
  assert.equal(buildProviderRequest(local, { ...baseBody, stream: true }).body.stream_options, undefined);
});
