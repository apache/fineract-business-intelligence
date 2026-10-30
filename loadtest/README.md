<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Load and correctness testing

The harness implements the plan in [`../test.md`](../test.md): a constant-memory synthetic
Debezium event generator, a real-database workload simulator and a verifier that checks the
warehouse against **ground truth**, not just message counts.

| Tool | What it tests |
|---|---|
| `loadgen produce` | Kafka → sink → warehouse at a controlled rate, with injected duplicates, out-of-order versions of the same key, deletes and payload padding. Writes the expected final state. |
| `loadgen verify` | Waits until, for every source and table, live rows, tombstones, the sum of winning positions and the sum of transaction amounts equal the ground truth; reports end-to-end latency percentiles. |
| `loadgen source-workload` | Real transactions on Fineract-shaped PostgreSQL databases (repayments, loan updates, new loans with schedules, reversals, deletes) — exercises WAL → Debezium as well. |
| `controlplane.cli reconcile-source` | Bucketed full-row checksum of a source database vs the warehouse. |
| `verify_incremental_equivalence.sh` | Proves the incremental dbt build equals a full rebuild after backdated changes, and that a partial build advances only the marts it built. |

## Laptop-safe procedure (≈ 4 GB of Docker memory)

```bash
docker compose up -d warehouse fineract-db
docker compose --profile cdc up -d kafka kafka-init sink          # connect only for real-CDC tests
loadtest/run_stages.sh 1    # functional
loadtest/run_stages.sh 2    # 1M events @ 8k/s
loadtest/run_stages.sh 3    # burst
loadtest/run_stages.sh 4    # consumer outage + drain throughput
loadtest/run_stages.sh 5    # chaos: kill -9 sink, warehouse outage
loadtest/run_stages.sh cleanup   # remove synthetic data before running dbt
```

Every container has a memory limit; the generator is rate-limited by default. Run `cleanup`
before dbt: synthetic sources are real data to dbt and would make it build loan x day history for
them.

Real Debezium path:

```bash
scripts/bootstrap_cdc_sources.sh 3                 # fineract_src_001..003 + replication role + publication
FBI_SRC_DEMO_PASSWORD=fbi_cdc_dev_only docker compose --profile cdc up -d connect
python -m controlplane.cli register-source --source-id cdc-src-001 --tenant-id t1 \
    --host fineract-bi-fineract-db --db fineract_src_001 --user fbi_cdc \
    --secret-ref env:FBI_SRC_DEMO_PASSWORD --engine KAFKA            # repeat per database
FBI_CONNECT_CLUSTERS='{"default":"http://localhost:8083"}' FBI_CONNECT_USER=fbi_connect FBI_CONNECT_PASSWORD=fbi_connect_dev_only \
    FBI_DEBEZIUM_PUBLICATION_MODE=disabled \
    python -m controlplane.cli reconcile-connectors
docker compose --profile loadtest run --rm loadgen source-workload --sources=3 \
    --jdbc-template=jdbc:postgresql://fineract-bi-fineract-db:5432/fineract_src_%03d \
    --db-user=root --db-password=... --seed-loans=200 --tps=100 --duration-sec=60
docker compose --profile python run --rm --no-deps -e FBI_SRC_DEMO_PASSWORD=fbi_cdc_dev_only extractor \
    python -m controlplane.cli reconcile-source --source-id cdc-src-001 --bucket-size 1000
```

## Capacity testing on real hardware

Use the same commands against `compose.kafka-cluster.yaml` or the Kubernetes reference
(`deploy/k8s`), with the generator on a separate VM so it is not the bottleneck. Scale
`SINK_REPLICAS`/`SINK_THREADS`, `TOPIC_PARTITION_SCALE=1`, more `--sources`, and record hardware,
configuration and the verifier report for each run. Isolate stages as in test.md §6:
produce with the sink stopped (Kafka ingest), then start the sink (consumer → PostgreSQL drain).

## Laptop results (12 vCPU, 8 GB Docker)

See `plan.md` §17. Summary: exact correctness in every stage; 8k events/s sustained with
p99 1.5 s; 10k/s burst without backlog; 210k/s Kafka ingest and ≥33k/s sink drain; exact recovery
from `kill -9` and a 30 s warehouse outage; real Debezium from three databases matched the
sources row-for-row.
