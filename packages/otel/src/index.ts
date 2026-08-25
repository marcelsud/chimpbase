import {
  type Context,
  type Counter,
  type Span,
  SpanStatusCode,
  context,
  trace,
} from "@opentelemetry/api";
import { logs, SeverityNumber } from "@opentelemetry/api-logs";
import { OTLPLogExporter } from "@opentelemetry/exporter-logs-otlp-http";
import { OTLPMetricExporter } from "@opentelemetry/exporter-metrics-otlp-http";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { resourceFromAttributes } from "@opentelemetry/resources";
import {
  BatchLogRecordProcessor,
  LoggerProvider,
} from "@opentelemetry/sdk-logs";
import {
  MeterProvider,
  PeriodicExportingMetricReader,
} from "@opentelemetry/sdk-metrics";
import {
  BatchSpanProcessor,
  BasicTracerProvider,
  type SpanExporter,
  type SpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import { ATTR_SERVICE_NAME } from "@opentelemetry/semantic-conventions";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";

import type {
  ChimpbaseSinkSpan,
  ChimpbaseTelemetryAttributes,
  ChimpbaseTelemetrySink,
} from "@chimpbase/runtime";

const SEVERITY_MAP: Record<string, SeverityNumber> = {
  debug: SeverityNumber.DEBUG,
  error: SeverityNumber.ERROR,
  info: SeverityNumber.INFO,
  warn: SeverityNumber.WARN,
};

export interface ChimpbaseOtelSinkOptions {
  endpoint?: string;
  logExporter?: InstanceType<typeof OTLPLogExporter>;
  metricExporter?: InstanceType<typeof OTLPMetricExporter>;
  serviceName?: string;
  spanProcessor?: SpanProcessor;
  traceExporter?: SpanExporter;
}

function toOtelAttributes(
  scope: { kind: string; name: string },
  attrs: ChimpbaseTelemetryAttributes = {},
): Record<string, string | number | boolean> {
  const result: Record<string, string | number | boolean> = {
    "chimpbase.scope.kind": scope.kind,
    "chimpbase.scope.name": scope.name,
  };
  for (const [key, value] of Object.entries(attrs)) {
    if (value !== null && value !== undefined) {
      result[key] = value;
    }
  }
  return result;
}

/**
 * Creates a ChimpbaseTelemetrySink that exports telemetry via OpenTelemetry.
 *
 * This function registers global OTel providers (TracerProvider, ContextManager).
 * Only call it once per process — multiple calls will conflict.
 *
 * Zero-config: reads OTEL_EXPORTER_OTLP_ENDPOINT and OTEL_SERVICE_NAME from env.
 */
export function createOtelSink(
  options: ChimpbaseOtelSinkOptions = {},
): ChimpbaseTelemetrySink {
  const serviceName =
    options.serviceName ??
    process.env.OTEL_SERVICE_NAME ??
    "chimpbase-app";

  const resource = resourceFromAttributes({
    [ATTR_SERVICE_NAME]: serviceName,
  });

  // Set up AsyncLocalStorage-based context manager for proper span propagation
  const contextManager = new AsyncLocalStorageContextManager();
  contextManager.enable();
  context.setGlobalContextManager(contextManager);

  const spanProcessor =
    options.spanProcessor ??
    new BatchSpanProcessor(
      options.traceExporter ??
        new OTLPTraceExporter(
          (options.endpoint !== undefined && options.endpoint.length > 0)
            ? { url: `${options.endpoint}/v1/traces` }
            : undefined,
        ),
    );
  const tracerProvider = new BasicTracerProvider({
    resource,
    spanProcessors: [spanProcessor],
  });
  trace.setGlobalTracerProvider(tracerProvider);
  const tracer = tracerProvider.getTracer("chimpbase", "0.4.0");

  const metricExporter =
    options.metricExporter ??
    new OTLPMetricExporter(
      (options.endpoint !== undefined && options.endpoint.length > 0)
        ? { url: `${options.endpoint}/v1/metrics` }
        : undefined,
    );
  const meterProvider = new MeterProvider({
    resource,
    readers: [
      new PeriodicExportingMetricReader({ exporter: metricExporter }),
    ],
  });
  const meter = meterProvider.getMeter("chimpbase", "0.4.0");

  const logExporter =
    options.logExporter ??
    new OTLPLogExporter(
      (options.endpoint !== undefined && options.endpoint.length > 0)
        ? { url: `${options.endpoint}/v1/logs` }
        : undefined,
    );
  const loggerProvider = new LoggerProvider({ resource });
  loggerProvider.addLogRecordProcessor(
    new BatchLogRecordProcessor(logExporter),
  );
  const logger = loggerProvider.getLogger("chimpbase", "0.4.0");

  const counters = new Map<string, Counter>();

  function makeSinkSpan(span: Span): ChimpbaseSinkSpan {
    const otelCtx: Context = trace.setSpan(context.active(), span);
    return {
      setAttribute(key, value) {
        span.setAttribute(key, value);
      },
      end(status, errorMessage) {
        span.setStatus(
          status === "ok"
            ? { code: SpanStatusCode.OK }
            : { code: SpanStatusCode.ERROR, message: errorMessage },
        );
        span.end();
      },
      runInContext<T>(fn: () => T | Promise<T>): T | Promise<T> {
        return context.with(otelCtx, fn);
      },
    };
  }

  return {
    onLog(scope, level, message, attributes) {
      logger.emit({
        attributes: toOtelAttributes(scope, attributes),
        body: message,
        severityNumber: SEVERITY_MAP[level] ?? SeverityNumber.INFO,
        severityText: level.toUpperCase(),
      });
    },

    onMetric(scope, name, value, labels) {
      let counter = counters.get(name);
      if (!(counter !== undefined)) {
        counter = meter.createCounter(name);
        counters.set(name, counter);
      }
      counter.add(value, toOtelAttributes(scope, labels));
    },

    startSpan(scope, name, attributes) {
      const span = tracer.startSpan(
        name,
        { attributes: toOtelAttributes(scope, attributes) },
        context.active(),
      );
      return makeSinkSpan(span);
    },

    startHandlerSpan(scope) {
      const spanName = `${scope.kind}:${scope.name}`;
      const span = tracer.startSpan(
        spanName,
        { attributes: toOtelAttributes(scope) },
        context.active(),
      );
      return makeSinkSpan(span);
    },

    async shutdown() {
      await tracerProvider.shutdown();
      await meterProvider.shutdown();
      await loggerProvider.shutdown();
      context.disable();
    },
  };
}
