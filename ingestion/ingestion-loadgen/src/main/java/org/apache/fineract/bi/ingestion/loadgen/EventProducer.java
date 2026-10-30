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
package org.apache.fineract.bi.ingestion.loadgen;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class EventProducer {

    private static final Logger LOG = LoggerFactory.getLogger(EventProducer.class);

    record Held(long releaseAt, ProducerRecord<byte[], byte[]> record) {
    }

    private EventProducer() {
    }

    public static void run(Args args, Contract contract) throws Exception {
        String bootstrap = args.get("bootstrap", "localhost:29092");
        int sources = args.getInt("sources", 100);
        String prefix = args.get("source-prefix", "loadsrc");
        int keys = args.getInt("keys", 10_000);
        long total = args.getLong("events", 1_000_000);
        double rate = args.getDouble("rate", 0);
        double burstRate = args.getDouble("burst-rate", 0);
        long burstStart = args.getLong("burst-start-sec", -1);
        long burstDuration = args.getLong("burst-duration-sec", 0);
        double dupRatio = args.getDouble("duplicate-ratio", 0.01);
        double reorderRatio = args.getDouble("reorder-ratio", 0.01);
        double deleteRatio = args.getDouble("delete-ratio", 0.02);
        double skew = args.getDouble("source-skew", 0.0);
        int padding = args.getInt("padding-bytes", 0);
        long seed = args.getLong("seed", 42);
        String out = args.get("ground-truth", "ground-truth.json");
        List<String> tables = new ArrayList<>();
        List<Double> weights = new ArrayList<>();
        for (String part : args.get("tables",
                "m_loan_transaction:50,m_loan:20,m_loan_repayment_schedule:20,m_client:10").split(",")) {
            String[] kv = part.split(":");
            contract.requireTable(kv[0]);
            tables.add(kv[0]);
            weights.add(kv.length > 1 ? Double.parseDouble(kv[1]) : 1.0);
        }
        double[] cumulative = new double[tables.size()];
        double sum = weights.stream().mapToDouble(Double::doubleValue).sum();
        double acc = 0;
        for (int i = 0; i < cumulative.length; i++) {
            acc += weights.get(i) / sum;
            cumulative[i] = acc;
        }

        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.ACKS_CONFIG, args.get("acks", "all"));
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.LINGER_MS_CONFIG, args.get("linger-ms", "20"));
        p.put(ProducerConfig.BATCH_SIZE_CONFIG, args.get("batch-bytes", "262144"));
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, args.get("compression", "zstd"));
        p.put(ProducerConfig.BUFFER_MEMORY_CONFIG, args.get("buffer-bytes", "67108864"));

        long baseLsn = System.currentTimeMillis() * 1_000L;
        long[] lsnBySource = new long[sources];
        java.util.Arrays.fill(lsnBySource, baseLsn);
        GroundTruth truth = new GroundTruth(sources, tables, keys, baseLsn);
        SplittableRandom rnd = new SplittableRandom(seed);
        ArrayDeque<Held> held = new ArrayDeque<>();
        AtomicLong errors = new AtomicLong();
        long sent = 0;
        long duplicates = 0;
        long reordered = 0;
        long deletes = 0;
        long started = System.nanoTime();
        String[] sourceIds = new String[sources];
        for (int s = 0; s < sources; s++) {
            sourceIds[s] = String.format("%s-%05d", prefix, s);
        }

        try (KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(p, new ByteArraySerializer(), new ByteArraySerializer())) {
            for (long i = 0; i < total; i++) {
                int t = pick(cumulative, rnd.nextDouble());
                int s = skew > 0 ? (int) Math.min(sources - 1, Math.floor(sources * Math.pow(rnd.nextDouble(), 1 + skew)))
                        : rnd.nextInt(sources);
                int key = rnd.nextInt(keys) + 1;
                String table = tables.get(t);
                long lsn = ++lsnBySource[s];
                char op;
                if (truth.isLive(s, t, key - 1)) {
                    op = rnd.nextDouble() < deleteRatio ? 'd' : 'u';
                } else {
                    op = 'c';
                }
                truth.record(s, t, key - 1, lsn, op == 'd');
                if (op == 'd') {
                    deletes++;
                }
                long tsMicros = System.currentTimeMillis() * 1_000L;
                String pk = contract.requireTable(table).primaryKey();
                byte[] k = ("{\"" + pk + "\":" + key + "}").getBytes(StandardCharsets.UTF_8);
                byte[] v = SyntheticPayloads.envelope(sourceIds[s], table, pk, key, lsn, op, tsMicros, padding)
                        .getBytes(StandardCharsets.UTF_8);
                ProducerRecord<byte[], byte[]> rec = new ProducerRecord<>(contract.topicFor(table), k, v);
                if (rnd.nextDouble() < reorderRatio) {
                    held.add(new Held(i + 1 + rnd.nextInt(2_000), rec));
                    reordered++;
                } else {
                    send(producer, rec, errors);
                    sent++;
                }
                if (rnd.nextDouble() < dupRatio) {
                    held.add(new Held(i + 1 + rnd.nextInt(2_000), rec));
                    duplicates++;
                }
                while (!held.isEmpty() && held.peekFirst().releaseAt() <= i) {
                    send(producer, held.pollFirst().record(), errors);
                    sent++;
                }
                if ((i & 1023) == 0) {
                    pace(started, i, rate, burstRate, burstStart, burstDuration);
                    if (i % 100_000 == 0 && i > 0) {
                        double secs = (System.nanoTime() - started) / 1e9;
                        LOG.info("generated {} events ({} eps)", i, Math.round(i / secs));
                    }
                }
            }
            while (!held.isEmpty()) {
                send(producer, held.pollFirst().record(), errors);
                sent++;
            }
            producer.flush();
        }
        double seconds = (System.nanoTime() - started) / 1e9;
        if (errors.get() > 0) {
            throw new IllegalStateException(errors.get() + " sends failed; ground truth would be wrong");
        }

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();
        root.put("source_prefix", prefix);
        root.put("base_lsn", baseLsn);
        root.put("generated_events", total);
        root.put("sent_records", sent);
        root.put("duplicates", duplicates);
        root.put("reordered", reordered);
        root.put("deletes", deletes);
        root.put("produce_seconds", seconds);
        root.put("produce_eps", Math.round(total / seconds));
        root.put("started_at_epoch_ms", System.currentTimeMillis() - Math.round(seconds * 1000));
        ObjectNode byTable = root.putObject("tables");
        for (Map.Entry<String, Map<Integer, GroundTruth.Expected>> e : truth.summary().entrySet()) {
            ArrayNode arr = byTable.putArray(e.getKey());
            for (Map.Entry<Integer, GroundTruth.Expected> se : e.getValue().entrySet()) {
                ObjectNode n = arr.addObject();
                n.put("source_id", sourceIds[se.getKey()]);
                n.put("live", se.getValue().live());
                n.put("deleted", se.getValue().deleted());
                n.put("lsn_sum", se.getValue().lsnSum());
                n.put("amount_cents_sum", se.getValue().amountCentsSum());
            }
        }
        mapper.writerWithDefaultPrettyPrinter().writeValue(new File(out), root);
        LOG.info("Produced {} events ({} records incl. {} duplicates, {} reordered, {} deletes) in {}s = {} eps; "
                + "ground truth -> {}", total, sent, duplicates, reordered, deletes,
                String.format("%.1f", seconds), Math.round(total / seconds), out);
    }

    static int pick(double[] cumulative, double r) {
        for (int i = 0; i < cumulative.length; i++) {
            if (r < cumulative[i]) {
                return i;
            }
        }
        return cumulative.length - 1;
    }

    private static void send(KafkaProducer<byte[], byte[]> producer, ProducerRecord<byte[], byte[]> rec,
            AtomicLong errors) {
        producer.send(rec, (md, e) -> {
            if (e != null) {
                errors.incrementAndGet();
                LOG.error("send failed", e);
            }
        });
    }

    static void pace(long startedNanos, long generated, double rate, double burstRate, long burstStart,
            long burstDuration) throws InterruptedException {
        if (rate <= 0) {
            return;
        }
        double elapsed = (System.nanoTime() - startedNanos) / 1e9;
        double expected;
        if (burstRate > 0 && burstStart >= 0 && elapsed >= burstStart) {
            double inBurst = Math.min(elapsed - burstStart, burstDuration);
            double after = Math.max(0, elapsed - burstStart - burstDuration);
            expected = burstStart * rate + inBurst * burstRate + after * rate;
        } else {
            expected = elapsed * rate;
        }
        if (generated > expected) {
            double currentRate = burstRate > 0 && burstStart >= 0 && elapsed >= burstStart
                    && elapsed < burstStart + burstDuration ? burstRate : rate;
            long sleepMs = (long) ((generated - expected) / currentRate * 1000);
            if (sleepMs > 0) {
                Thread.sleep(Math.min(sleepMs, 1000));
            }
        }
    }
}
