import readline from "node:readline";

const baseUrl = (process.env.MCP_ROUTER_URL || "http://127.0.0.1:3001").replace(/\/+$/, "");
const apiKey = process.env.MCP_ROUTER_API_KEY || process.env.ROUTER_API_KEY;
const protocolVersions = new Set(["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"]);

function send(message) {
  process.stdout.write(`${JSON.stringify(message)}\n`);
}

function error(id, code, message) {
  send({ jsonrpc: "2.0", id, error: { code, message } });
}

async function request(path, options = {}) {
  const headers = { ...(options.headers || {}) };
  if (apiKey) headers.authorization = `Bearer ${apiKey}`;
  const response = await fetch(`${baseUrl}${path}`, {
    ...options,
    headers,
    signal: AbortSignal.timeout(180_000),
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`Router returned HTTP ${response.status}: ${text.slice(0, 1000)}`);
  return text ? JSON.parse(text) : {};
}

const tools = [
  {
    name: "list_models",
    description: "List the model aliases configured in the Mini LLM Router.",
    inputSchema: { type: "object", properties: {}, additionalProperties: false },
  },
  {
    name: "chat_completion",
    description: "Send a text prompt to a configured model through the Mini LLM Router.",
    inputSchema: {
      type: "object",
      properties: {
        model: { type: "string", description: "A configured model alias, such as free or local." },
        prompt: { type: "string", description: "The user prompt to send." },
        system: { type: "string", description: "Optional system instruction." },
      },
      required: ["model", "prompt"],
      additionalProperties: false,
    },
  },
  {
    name: "get_usage",
    description: "Read aggregate provider usage and reported cost from the router.",
    inputSchema: { type: "object", properties: {}, additionalProperties: false },
  },
];

async function callTool(name, args) {
  let result;
  if (name === "list_models") {
    result = await request("/v1/models");
  } else if (name === "get_usage") {
    result = await request("/v1/usage");
  } else if (name === "chat_completion") {
    if (!args || typeof args.model !== "string" || typeof args.prompt !== "string" || !args.prompt.trim()) {
      throw new Error("chat_completion requires a model alias and non-empty prompt.");
    }
    const messages = [];
    if (typeof args.system === "string" && args.system.trim()) messages.push({ role: "system", content: args.system });
    messages.push({ role: "user", content: args.prompt });
    result = await request("/v1/chat/completions", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: args.model, messages }),
    });
  } else {
    throw new Error(`Unknown tool '${name}'.`);
  }
  return { content: [{ type: "text", text: JSON.stringify(result) }] };
}

async function handle(message) {
  const { id, method, params = {} } = message;
  if (typeof method !== "string") return error(id ?? null, -32600, "Invalid JSON-RPC request.");
  if (method === "notifications/initialized" || method === "notifications/cancelled") return;
  if (method === "initialize") {
    const requested = params.protocolVersion;
    const protocolVersion = protocolVersions.has(requested) ? requested : "2025-11-25";
    return send({ jsonrpc: "2.0", id, result: {
      protocolVersion,
      capabilities: { tools: { listChanged: false } },
      serverInfo: { name: "mini-llm-router", version: "1.0.0" },
    } });
  }
  if (method === "ping") return send({ jsonrpc: "2.0", id, result: {} });
  if (method === "tools/list") return send({ jsonrpc: "2.0", id, result: { tools } });
  if (method === "tools/call") {
    try {
      const result = await callTool(params.name, params.arguments || {});
      return send({ jsonrpc: "2.0", id, result });
    } catch (cause) {
      return send({ jsonrpc: "2.0", id, result: { isError: true, content: [{ type: "text", text: cause.message || "Tool call failed." }] } });
    }
  }
  if (id !== undefined) error(id, -32601, `Method not found: ${method}`);
}

const input = readline.createInterface({ input: process.stdin, crlfDelay: Infinity, terminal: false });
input.on("line", (line) => {
  if (!line.trim()) return;
  try {
    const message = JSON.parse(line);
    void handle(message).catch((cause) => {
      console.error(`MCP request failed: ${cause.message || cause}`);
      if (message?.id !== undefined) error(message.id, -32603, "Internal error.");
    });
  } catch {
    error(null, -32700, "Parse error.");
  }
});
