import { NodeSDK } from "@opentelemetry/sdk-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { HttpInstrumentation } from "@opentelemetry/instrumentation-http";
import { UndiciInstrumentation } from "@opentelemetry/instrumentation-undici";

const otlpConfigured = Boolean(
  process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT || process.env.OTEL_EXPORTER_OTLP_ENDPOINT,
);

const sdk = otlpConfigured
  ? new NodeSDK({
      serviceName: process.env.OTEL_SERVICE_NAME || "mini-llm-router",
      traceExporter: new OTLPTraceExporter(),
      instrumentations: [new HttpInstrumentation(), new UndiciInstrumentation()],
    })
  : undefined;

sdk?.start();

export async function shutdownTelemetry() {
  await sdk?.shutdown();
}
