import { Readable } from "node:stream";

function messageText(content) {
  if (typeof content === "string") return content;
  if (Array.isArray(content) && content.every((part) => part?.type === "text" && typeof part.text === "string")) {
    return content.map((part) => part.text).join("");
  }
  const error = new Error("This provider adapter currently supports text-only message content.");
  error.statusCode = 400;
  throw error;
}

function conversationText(body) {
  if (body.tools !== undefined || body.tool_choice !== undefined || body.functions !== undefined || body.function_call !== undefined) {
    const error = new Error("Tool and function calling are not supported by the Anthropic or Gemini text adapters.");
    error.statusCode = 400;
    throw error;
  }
  if (!Array.isArray(body.messages) || body.messages.some((message) => !["system", "developer", "user", "assistant"].includes(message.role))) {
    const error = new Error("This provider adapter supports system, developer, user, and assistant text messages.");
    error.statusCode = 400;
    throw error;
  }
  return body.messages.map((message) => ({ role: message.role, text: messageText(message.content) }));
}

function endpoint(baseUrl, path) {
  return `${baseUrl.replace(/\/+$/, "")}/${path.replace(/^\/+/, "")}`;
}

export function buildProviderRequest(provider, body) {
  const model = provider.models[body.model];
  if (provider.protocol === "anthropic") {
    const conversation = conversationText(body);
    const system = conversation.filter((message) => ["system", "developer"].includes(message.role)).map((message) => message.text).join("\n\n");
    const messages = conversation.filter((message) => !["system", "developer"].includes(message.role))
      .map((message) => ({ role: message.role === "assistant" ? "assistant" : "user", content: message.text }));
    const path = provider.baseUrl.endsWith("/v1") ? "messages" : "v1/messages";
    const payload = {
      model,
      max_tokens: body.max_tokens ?? body.max_completion_tokens ?? 1024,
      messages,
      stream: body.stream === true,
    };
    if (system) payload.system = system;
    for (const key of ["temperature", "top_p", "top_k", "metadata"]) {
      if (body[key] !== undefined) payload[key] = body[key];
    }
    if (body.stop_sequences !== undefined || body.stop !== undefined) {
      payload.stop_sequences = body.stop_sequences ?? (Array.isArray(body.stop) ? body.stop : [body.stop]);
    }
    return {
      url: endpoint(provider.baseUrl, path),
      headers: { "x-api-key": provider.apiKey, "anthropic-version": "2023-06-01" },
      body: payload,
    };
  }
  if (provider.protocol === "gemini") {
    const conversation = conversationText(body);
    const systemParts = conversation.filter((message) => ["system", "developer"].includes(message.role)).map((message) => ({ text: message.text }));
    const contents = conversation.filter((message) => !["system", "developer"].includes(message.role))
      .map((message) => ({ role: message.role === "assistant" ? "model" : "user", parts: [{ text: message.text }] }));
    const action = body.stream === true ? "streamGenerateContent?alt=sse" : "generateContent";
    const payload = { contents };
    if (systemParts.length) payload.systemInstruction = { parts: systemParts };
    const generationConfig = {};
    if (body.temperature !== undefined) generationConfig.temperature = body.temperature;
    if (body.top_p !== undefined) generationConfig.topP = body.top_p;
    if (body.max_tokens !== undefined || body.max_completion_tokens !== undefined) {
      generationConfig.maxOutputTokens = body.max_tokens ?? body.max_completion_tokens;
    }
    if (body.stop !== undefined) generationConfig.stopSequences = Array.isArray(body.stop) ? body.stop : [body.stop];
    if (Object.keys(generationConfig).length) payload.generationConfig = generationConfig;
    return {
      url: endpoint(provider.baseUrl, `models/${encodeURIComponent(model)}:${action}`),
      headers: { "x-goog-api-key": provider.apiKey },
      body: payload,
    };
  }
  const payload = { ...body, model };
  if (body.stream === true && ["openrouter.ai", "www.openrouter.ai"].includes(new URL(provider.baseUrl).hostname)) {
    const streamOptions = body.stream_options && typeof body.stream_options === "object" && !Array.isArray(body.stream_options)
      ? body.stream_options
      : {};
    payload.stream_options = { ...streamOptions, include_usage: true };
  }
  if (provider.routing && Object.keys(provider.routing).length) payload.provider = provider.routing;
  return {
    url: endpoint(provider.baseUrl, "chat/completions"),
    headers: { authorization: `Bearer ${provider.apiKey}` },
    body: payload,
  };
}

function openAiFinishReason(reason) {
  if (["max_tokens", "MAX_TOKENS"].includes(reason)) return "length";
  if (["stop_sequence", "end_turn", "STOP"].includes(reason)) return "stop";
  if (["tool_use", "MALFORMED_FUNCTION_CALL"].includes(reason)) return "tool_calls";
  return "stop";
}

function chatCompletion({ id, model, text, finishReason, promptTokens = 0, completionTokens = 0 }) {
  return {
    id: id || `chatcmpl-${Date.now()}`,
    object: "chat.completion",
    created: Math.floor(Date.now() / 1000),
    model,
    choices: [{ index: 0, message: { role: "assistant", content: text }, finish_reason: openAiFinishReason(finishReason) }],
    usage: { prompt_tokens: promptTokens, completion_tokens: completionTokens, total_tokens: promptTokens + completionTokens },
  };
}

async function jsonResponse(response, payload) {
  return new Response(JSON.stringify(payload), {
    status: response.status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

async function adaptError(response) {
  const text = await response.text();
  let message = text;
  try {
    const parsed = JSON.parse(text);
    message = parsed.error?.message || parsed.message || text;
  } catch {}
  return jsonResponse(response, { error: { message: String(message).slice(0, 4000) } });
}

async function readJsonResponse(response) {
  const text = await response.text();
  try { return JSON.parse(text); } catch { return undefined; }
}

async function adaptAnthropicResponse(response, body) {
  const result = await readJsonResponse(response);
  if (!result) return jsonResponse(response, { error: { message: "Provider returned an invalid JSON response." } });
  const text = Array.isArray(result.content) ? result.content.filter((part) => part.type === "text").map((part) => part.text || "").join("") : "";
  return jsonResponse(response, chatCompletion({
    id: result.id,
    model: body.model,
    text,
    finishReason: result.stop_reason,
    promptTokens: result.usage?.input_tokens || 0,
    completionTokens: result.usage?.output_tokens || 0,
  }));
}

async function adaptGeminiResponse(response, body) {
  const result = await readJsonResponse(response);
  if (!result) return jsonResponse(response, { error: { message: "Provider returned an invalid JSON response." } });
  const candidate = result.candidates?.[0];
  const text = candidate?.content?.parts?.filter((part) => typeof part.text === "string").map((part) => part.text).join("") || "";
  const usage = result.usageMetadata || {};
  return jsonResponse(response, chatCompletion({
    id: result.responseId,
    model: body.model,
    text,
    finishReason: candidate?.finishReason,
    promptTokens: usage.promptTokenCount || 0,
    completionTokens: usage.candidatesTokenCount || 0,
  }));
}

async function* readSseEvents(stream) {
  if (!stream) return;
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  try {
    while (true) {
      const { done, value } = await reader.read();
      buffer += decoder.decode(value || new Uint8Array(), { stream: !done });
      const blocks = buffer.split(/\r?\n\r?\n/);
      buffer = blocks.pop() || "";
      for (const block of blocks) {
        let event = "message";
        const data = [];
        for (const line of block.split(/\r?\n/)) {
          if (line.startsWith("event:")) event = line.slice(6).trim();
          else if (line.startsWith("data:")) data.push(line.slice(5).trimStart());
        }
        if (data.length) yield { event, data: data.join("\n") };
      }
      if (done) break;
    }
    if (buffer.trim()) {
      let event = "message";
      const data = [];
      for (const line of buffer.split(/\r?\n/)) {
        if (line.startsWith("event:")) event = line.slice(6).trim();
        else if (line.startsWith("data:")) data.push(line.slice(5).trimStart());
      }
      if (data.length) yield { event, data: data.join("\n") };
    }
  } finally {
    reader.releaseLock();
  }
}

function streamChunk(id, model, delta, finishReason = null, usage = undefined) {
  const chunk = { id, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model, choices: [{ index: 0, delta, finish_reason: finishReason }] };
  if (usage) chunk.usage = usage;
  return `data: ${JSON.stringify(chunk)}\n\n`;
}

async function* anthropicStream(response, model) {
  let id = `chatcmpl-${Date.now()}`;
  let promptTokens = 0;
  for await (const item of readSseEvents(response.body)) {
    if (item.data === "[DONE]") break;
    let event;
    try { event = JSON.parse(item.data); } catch { continue; }
    if (event.type === "message_start") {
      id = event.message?.id || id;
      promptTokens = Number(event.message?.usage?.input_tokens) || 0;
      yield streamChunk(id, model, { role: "assistant" });
    } else if (event.type === "content_block_delta" && event.delta?.type === "text_delta") {
      yield streamChunk(id, model, { content: event.delta.text || "" });
    } else if (event.type === "message_delta" && (event.delta?.stop_reason || event.usage)) {
      const completionTokens = Number(event.usage?.output_tokens) || 0;
      yield streamChunk(id, model, {}, event.delta?.stop_reason ? openAiFinishReason(event.delta.stop_reason) : null, {
        prompt_tokens: promptTokens,
        completion_tokens: completionTokens,
        total_tokens: promptTokens + completionTokens,
      });
    } else if (event.type === "error") {
      yield `data: ${JSON.stringify({ error: { message: event.error?.message || "Provider stream failed." } })}\n\n`;
    }
  }
  yield "data: [DONE]\n\n";
}

async function* geminiStream(response, model) {
  const id = `chatcmpl-${Date.now()}`;
  yield streamChunk(id, model, { role: "assistant" });
  for await (const item of readSseEvents(response.body)) {
    let event;
    try { event = JSON.parse(item.data); } catch { continue; }
    const candidate = event.candidates?.[0];
    const text = candidate?.content?.parts?.filter((part) => typeof part.text === "string").map((part) => part.text).join("");
    if (text) yield streamChunk(id, model, { content: text });
    const usage = event.usageMetadata;
    if (candidate?.finishReason || usage) {
      const promptTokens = Number(usage?.promptTokenCount) || 0;
      const completionTokens = Number(usage?.candidatesTokenCount) || 0;
      yield streamChunk(id, model, {}, candidate?.finishReason ? openAiFinishReason(candidate.finishReason) : null, {
        prompt_tokens: promptTokens,
        completion_tokens: completionTokens,
        total_tokens: Number(usage?.totalTokenCount) || promptTokens + completionTokens,
      });
    }
  }
  yield "data: [DONE]\n\n";
}

export async function adaptProviderResponse(provider, response, body) {
  if (!response.ok || provider.protocol === "openai-compatible") return response.ok ? response : adaptError(response);
  if (body.stream === true) {
    const chunks = provider.protocol === "anthropic" ? anthropicStream(response, body.model) : geminiStream(response, body.model);
    const stream = Readable.from((async function* () {
      for await (const chunk of chunks) yield Buffer.from(chunk);
    })(), { objectMode: false });
    return new Response(Readable.toWeb(stream), {
      status: response.status,
      headers: { "content-type": "text/event-stream; charset=utf-8", "cache-control": "no-cache", connection: "keep-alive" },
    });
  }
  return provider.protocol === "anthropic" ? adaptAnthropicResponse(response, body) : adaptGeminiResponse(response, body);
}

