# Gold API latency benchmark

Measured against the local `kind` cluster with the full platform running: 50,004 payments in
Iceberg, 8,764 hourly gold buckets, Trino capped at a 512 MB heap, everything sharing a 7.75 GiB
Docker VM on a 10-core laptop.

## Method

Run from **inside the cluster**, against the `api` Service:

```bash
kubectl exec -n data-pipeline deploy/api -- python /tmp/bench_incluster.py
```

Percentiles are nearest-rank, not interpolated — with 60 samples, interpolating between neighbours
invents a latency that was never observed. Cold and warm are reported separately because they are
different systems: cold exercises Trino, Iceberg file pruning, and the HDFS read path; warm
exercises the snapshot cache and little else. Blending them would just average two distributions.

## Results

| scenario | n | err | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|---:|---:|
| cold cache (first call) | 2 | 0 | 52.8 ms | 96.4 ms | 96.4 ms | 96.4 ms |
| hourly (warm cache) | 60 | 0 | 62.8 ms | 221.6 ms | 1917.1 ms | 1917.1 ms |
| hourly filtered (`country_code=NL`) | 60 | 0 | 35.3 ms | 68.9 ms | 131.7 ms | 131.7 ms |
| summary | 60 | 0 | 34.2 ms | 99.1 ms | 119.8 ms | 119.8 ms |

**Filtering is faster than not filtering** — 35 ms p50 against 63 ms — which is the pushdown
working. `country_code=NL` compiles into a `WHERE` predicate, so Trino reads fewer Iceberg files
rather than returning everything for the process to sift.

The 1.9 s outlier in the warm-cache row is a single sample out of sixty, on a node where Trino,
Kafka, HDFS, Spark, and Airflow share 7.75 GiB. It is a JVM pause, not a code path — p95 for the
same scenario is 222 ms.

## A measurement caveat worth repeating

The first run of this benchmark went through `kubectl port-forward` and reported **11 errors out of
60** with a p99 of **5,985 ms**. None of that was real: six sequential calls through the same
tunnel returned HTTP 200 in 56–314 ms, and the API pod had zero restarts. `kubectl port-forward` is
a single userspace TCP relay and it collapses under sustained request rates.

Benchmarking through it measures the tunnel. Those numbers were discarded rather than published —
a benchmark that flatters or maligns the system it claims to measure is worse than no benchmark,
because the number outlives the caveat.

## Reproducing

Under Compose, where the port is published directly and no tunnel is involved:

```bash
docker compose up -d api
bash scripts/bench_api.sh
```
