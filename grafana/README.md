# Pulsar Lite Observability Stack

A one-command Prometheus + Grafana stack that scrapes the broker's
`GET /metrics` endpoint (default `0.0.0.0:8080`).

## Quick start

```bash
# 1. Start the broker (metrics are on by default)
../rust/target/release/pulsar-lite --config ../rust/pulsar-lite.toml

# 2. Start the observability stack
docker compose up -d

# 3. Open
#    Grafana    http://localhost:3000  (admin/admin; anonymous read enabled)
#    Prometheus http://localhost:9090/targets — the pulsar-lite job must be UP
```

Dashboards are provisioned automatically (folder `Pulsar Lite`):

- **Pulsar Lite / Topics** — publish/deliver rates and throughput, entity
  counts, subscription backlog, unacked gate state, redelivery, storage
  size, end-to-end and ledger write latency (P50/P99), entry-size
  distribution.
- **Pulsar Lite / Broker** — broker-level rates (counter `rate()` and
  window-gauge views), connections and rejection reasons, backlog and
  storage totals, write-queue batch sizes, process RSS/CPU.

## Metric naming conventions

- `pulsar_*` families reproduce native Apache Pulsar names and label sets
  verbatim, so official dashboards and PromQL translate directly.
- `pulsar_lite_*` families are extensions with no native counterpart
  (error reasons, redelivery counters, write-queue batch metrics).
- Histograms use the standard Prometheus shape (`_bucket{le=...}` +
  `_sum` + `_count`); query percentiles with `histogram_quantile()`.

## Broker configuration

```toml
[metrics]
enabled = true            # false: no listener, no scrape aggregation
addr = "0.0.0.0:8080"     # /metrics path; must be reachable by the scraper
cluster = "pulsar-lite"   # cluster label value on every family
rate_window_secs = 60     # window for pulsar_rate_in-style gauges
```

Remote broker: change the target in `prometheus/prometheus.yml` from
`host.docker.internal:8080` to your broker address.
