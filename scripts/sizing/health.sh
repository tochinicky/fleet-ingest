#!/usr/bin/env bash
# The sizing run's health verdict, from Prometheus's own data after the fact (S8-6): the steady phase of each run,
# by its UTC window from samples.txt. Prometheus keeps its volume across `compose down`, so this works later as long
# as the stack (or just Prometheus) is up again within its retention.
#   scripts/sizing/health.sh <utc-end, e.g. 2026-10-16T10:04:26Z> [window, default 20m]
set -euo pipefail
end="${1:?utc end of the steady phase}"; window="${2:-20m}"
q() { curl -sfG "http://127.0.0.1:9090/api/v1/query" --data-urlencode "query=$1" --data-urlencode "time=$end" | python3 -c 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(r[0]["value"][1] if r else "no data")'; }
echo "steady window ending $end ($window):"
echo "  ingest lag p95 (s):            $(q "histogram_quantile(0.95, sum(rate(ingest_lag_seconds_bucket[$window])) by (le))")"
echo "  ingest lag max bucket (s):     $(q "histogram_quantile(1, sum(rate(ingest_lag_seconds_bucket[$window])) by (le))")"
echo "  readings stored:               $(q "sum(increase(ingest_readings_stored_total[$window]))")"
echo "  health passes over 2 s:        $(q "sum(increase(fleet_health_job_pass_duration_seconds_count[$window])) - sum(increase(fleet_health_job_pass_duration_seconds_bucket{le=\"2\"}[$window]))")"
echo "  slowest pass p99 (s):          $(q "histogram_quantile(0.99, sum(rate(fleet_health_job_pass_duration_seconds_bucket[$window])) by (le))")"
echo "  health-job failures:           $(q "sum(increase(fleet_health_job_failures_total[$window]))")"
echo "  ingest DB retries:             $(q "sum(increase(ingest_db_retries_total[$window]))")"
echo "  broker drops:                  $(q "max(broker_publish_messages_dropped) - max(broker_publish_messages_dropped offset $window)")"
echo "  container restarts (cAdvisor): $(q "count(changes(container_start_time_seconds{name=~\"fleet-ingest-.*\"}[$window]) > 0)")"
