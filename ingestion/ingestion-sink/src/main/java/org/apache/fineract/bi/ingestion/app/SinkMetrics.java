/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.fineract.bi.ingestion.app;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

public final class SinkMetrics implements AutoCloseable {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final Timer batchDuration;
    private final Timer endToEndLatency;
    private final Counter batches;
    private final Counter fencedRetries;
    private final Counter dbErrors;
    private final Counter holds;
    private final AtomicLong lastSuccessMillis = new AtomicLong(System.currentTimeMillis());
    private HttpServer server;

    public SinkMetrics() {
        new JvmMemoryMetrics().bindTo(registry);
        new JvmGcMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        batchDuration = Timer.builder("fbi_sink_batch_duration")
                .description("Time to apply one batch to the warehouse (COPY + merge + offsets)")
                .register(registry);
        endToEndLatency = Timer.builder("fbi_sink_end_to_end_latency")
                .description("Source commit to warehouse commit")
                .register(registry);
        batches = Counter.builder("fbi_sink_batches_total").register(registry);
        fencedRetries = Counter.builder("fbi_sink_fenced_retries_total")
                .description("Batches rejected by warehouse fencing and re-routed")
                .register(registry);
        dbErrors = Counter.builder("fbi_sink_db_errors_total").register(registry);
        holds = Counter.builder("fbi_sink_holds_total").description("Batches held for paused ownership").register(registry);
    }

    public MeterRegistry registry() {
        return registry;
    }

    public void events(String table, String outcome, long count) {
        if (count > 0) {
            registry.counter("fbi_sink_events_total", "table", table, "outcome", outcome).increment(count);
        }
    }

    public void deadLettered(String reason, long count) {
        registry.counter("fbi_sink_dead_lettered_total", "reason", reason).increment(count);
    }

    public void rowsWritten(String table, String target, long rows) {
        registry.counter("fbi_sink_rows_written_total", "table", table, "target", target).increment(rows);
    }

    public void batch(long nanos) {
        batches.increment();
        batchDuration.record(Duration.ofNanos(nanos));
        lastSuccessMillis.set(System.currentTimeMillis());
    }

    public void latencyMicros(long micros) {
        if (micros >= 0) {
            endToEndLatency.record(Duration.ofNanos(micros * 1_000L));
        }
    }

    public void fenced() {
        fencedRetries.increment();
    }

    public void dbError() {
        dbErrors.increment();
    }

    public void hold() {
        holds.increment();
    }

    public void absentColumn(String table, String column) {
        registry.counter("fbi_sink_absent_column_total", "table", table, "column", column).increment();
    }

    public void offsetGap() {
        registry.counter("fbi_sink_offset_gap_total").increment();
    }

    public long lastSuccessMillis() {
        return lastSuccessMillis.get();
    }

    public void serve(int port, BooleanSupplier healthy) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/metrics", exchange -> {
            byte[] body = render().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.createContext("/health", exchange -> {
            boolean ok = healthy.getAsBoolean();
            byte[] body = (ok ? "OK" : "UNHEALTHY").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(ok ? 200 : 503, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.start();
    }

    private String render() {
        StringBuilder out = new StringBuilder();
        for (Meter meter : registry.getMeters()) {
            Meter.Id id = meter.getId();
            String name = id.getName().replace('.', '_').replace('-', '_');
            StringBuilder labels = new StringBuilder();
            id.getTags().forEach(t -> labels.append(labels.length() == 0 ? "{" : ",")
                    .append(t.getKey().replace('.', '_')).append("=\"").append(t.getValue()).append('"'));
            String tags = labels.length() == 0 ? "" : labels.append('}').toString();
            if (meter instanceof Timer timer) {
                out.append(name).append("_count").append(tags).append(" ").append((double) timer.count()).append("\n");
                out.append(name).append("_sum_seconds").append(tags).append(" ")
                        .append(timer.totalTime(TimeUnit.SECONDS)).append("\n");
                out.append(name).append("_max_seconds").append(tags).append(" ")
                        .append(timer.max(TimeUnit.SECONDS)).append("\n");
            } else {
                for (var measurement : meter.measure()) {
                    String suffix = id.getType() == Meter.Type.COUNTER && !name.endsWith("_total") ? "_total" : "";
                    out.append(name).append(suffix).append(tags).append(" ").append(measurement.getValue()).append("\n");
                    break;
                }
            }
        }
        return out.toString();
    }

    @Override
    public void close() {
        if (server != null) {
            server.stop(0);
        }
        registry.close();
    }
}
