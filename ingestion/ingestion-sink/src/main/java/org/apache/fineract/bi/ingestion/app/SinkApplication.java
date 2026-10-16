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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.Gauge;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.fineract.bi.ingestion.cdc.DebeziumEnvelopeParser;
import org.apache.fineract.bi.ingestion.config.Env;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.fineract.bi.ingestion.contract.ContractLoader;
import org.apache.fineract.bi.ingestion.routing.OwnershipCache;
import org.apache.fineract.bi.ingestion.sink.WarehouseWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SinkApplication {

    private static final Logger LOG = LoggerFactory.getLogger(SinkApplication.class);

    private SinkApplication() {
    }

    public static void main(String[] args) throws Exception {
        String command = args.length > 0 ? args[0] : "run";
        Contract contract = ContractLoader.fromEnvironment();
        Env env = Env.system();
        switch (command) {
            case "provision-topics" -> TopicProvisioner.provision(contract, env);
            case "run" -> run(contract, SinkConfig.from(env, System.getenv()));
            default -> {
                System.err.println("Usage: SinkApplication [run|provision-topics]");
                System.exit(2);
            }
        }
    }

    static void run(Contract contract, SinkConfig config) throws Exception {
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(config.dbUrl());
        hc.setUsername(config.dbUser());
        hc.setPassword(config.dbPassword());
        hc.setMaximumPoolSize(config.dbPoolSize());
        hc.setPoolName("fbi-sink");
        hc.addDataSourceProperty("ApplicationName", config.writerId());
        hc.addDataSourceProperty("reWriteBatchedInserts", "true");
        hc.setAutoCommit(false);

        SinkMetrics metrics = new SinkMetrics();
        try (HikariDataSource ds = new HikariDataSource(hc);
                DeadLetterPublisher dlt = new DeadLetterPublisher(config)) {
            OwnershipCache ownership = new OwnershipCache(ds, 1_000);
            ownership.refresh(true);
            LOG.info("Loaded ownership: {} KAFKA routes across {} registered sources",
                    ownership.snapshot().size(), ownership.snapshot().sourceCount());
            Gauge.builder("fbi_sink_ownership_routes", () -> ownership.snapshot().size()).register(metrics.registry());
            Gauge.builder("fbi_sink_ownership_age_seconds",
                    () -> (System.currentTimeMillis() - ownership.snapshot().loadedAtMillis()) / 1000.0)
                    .register(metrics.registry());

            ProgressReporter progress = new ProgressReporter(ds, config.writerId());
            ScheduledExecutorService timers = Executors.newScheduledThreadPool(2, r -> {
                Thread t = new Thread(r, "sink-timer");
                t.setDaemon(true);
                return t;
            });
            timers.scheduleWithFixedDelay(ownership::refreshQuietly,
                    config.ownershipRefreshMs(), config.ownershipRefreshMs(), TimeUnit.MILLISECONDS);
            timers.scheduleWithFixedDelay(progress::flush,
                    config.progressFlushMs(), config.progressFlushMs(), TimeUnit.MILLISECONDS);

            DebeziumEnvelopeParser parser = new DebeziumEnvelopeParser(contract);
            WarehouseWriter writer = new WarehouseWriter(contract, "KAFKA");
            List<SinkWorker> workers = new ArrayList<>();
            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < config.threads(); i++) {
                SinkWorker w = new SinkWorker(i, config, ds, ownership, parser, writer, dlt, metrics, progress);
                workers.add(w);
                Thread t = new Thread(w);
                threads.add(t);
                t.start();
            }
            metrics.serve(config.metricsPort(), () -> workers.stream().allMatch(SinkWorker::isAlive));
            LOG.info("Sink started: {} workers, group={}, pattern={}, batch={} events/{} ms",
                    config.threads(), config.groupId(), config.topicPattern(),
                    config.maxBatchEvents(), config.maxBatchWaitMs());

            CountDownLatch stopped = new CountDownLatch(1);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("Shutting down sink");
                workers.forEach(SinkWorker::shutdown);
                for (Thread t : threads) {
                    try {
                        t.join(30_000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                progress.flush();
                stopped.countDown();
            }, "sink-shutdown"));

            while (stopped.getCount() > 0) {
                if (workers.stream().anyMatch(w -> !w.isAlive())) {
                    LOG.error("A sink worker died; exiting for restart");
                    workers.forEach(SinkWorker::shutdown);
                    progress.flush();
                    System.exit(1);
                }
                stopped.await(5, TimeUnit.SECONDS);
            }
            timers.shutdownNow();
        } finally {
            metrics.close();
        }
    }
}
