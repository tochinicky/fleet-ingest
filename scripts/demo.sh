#!/usr/bin/env bash
# The full demo: every fault scenario, each PASS or FAIL, then one summary table.
# Each scenario is PASS, FAIL or INVALID. INVALID means a check couldn't be judged, because the VM wasn't healthy
# before the fault (no quiet baseline, decisions.md S7-7). It is never shown as PASS. Exits 0 only if every scenario
# passed.
#
#   scripts/demo.sh                 # all of them, about 75 minutes on a laptop (scenario 4 alone is about 20)
#   SKIP="4 9" scripts/demo.sh      # leave scenarios out by number
#
# It starts the stack itself (certificates, then `docker compose up`). Each scenario's full output goes to
# demo-results/<timestamp>/<scenario>.log, and the table to demo-results/<timestamp>/summary.txt.
set -uo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
out="$root/demo-results/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$out"
skip=" ${SKIP:-} "

scenarios=(
  "1|duplicate storm|scripts/scenarios/duplicate-storm.ts"
  "2|10% of gateways offline for 2 min|scripts/scenarios/gateways-offline.ts"
  "3a|Postgres down 120 s|scripts/scenarios/postgres-down.ts 3a"
  "3b|bound 5,000, Postgres down 60 s|scripts/scenarios/postgres-down.ts 3b"
  "4|reconnect storm after a 10 min broker outage|scripts/scenarios/reconnect-storm.ts jitter"
  "5|poison messages|scripts/scenarios/poison.ts"
  "6|impersonation and revocation|scripts/scenarios/impersonation.ts"
  "7|kill -9 an ingest replica|scripts/scenarios/kill-replica.ts"
  "7b|ingest-1 down past a reduced bound|scripts/scenarios/replica-down-overflow.ts"
  "8|power loss on a gateway|scripts/scenarios/power-loss.ts"
  "9|the real gateway, 2 min WAN cut|scripts/scenarios/real-gateway.ts"
)

scripts/dev-certs.sh 500 >/dev/null
docker compose -f deploy/compose.yaml up -d --build >/dev/null 2>&1 || { echo "the stack didn't start"; exit 1; }

passed=0
failed=0
invalid=0
printf '%-4s %-46s %-7s %s\n' "#" "scenario" "result" "minutes" | tee "$out/summary.txt"
for entry in "${scenarios[@]}"; do
  IFS='|' read -r id name command <<<"$entry"
  if [[ "$skip" == *" $id "* ]]; then
    printf '%-4s %-46s %-7s\n' "$id" "$name" "SKIP" | tee -a "$out/summary.txt"
    continue
  fi
  start=$(date +%s)
  # shellcheck disable=SC2086
  node $command >"$out/$id.log" 2>&1
  status=$?
  # 2 is the scenarios' INVALID status (EXIT_INVALID in scripts/scenarios/lib.ts).
  case $status in
    0) result=PASS; passed=$((passed + 1)) ;;
    2) result=INVALID; invalid=$((invalid + 1)) ;;
    *) result=FAIL; failed=$((failed + 1)) ;;
  esac
  printf '%-4s %-46s %-7s %s\n' "$id" "$name" "$result" "$(( ($(date +%s) - start + 30) / 60 ))" | tee -a "$out/summary.txt"
  [[ "$result" != PASS ]] && grep -E '^(FAIL|INVALID)' "$out/$id.log" | sed 's/^/       /' | tee -a "$out/summary.txt"
done

echo "logs: $out"
echo "$passed PASS / $failed FAIL / $invalid INVALID" | tee -a "$out/summary.txt"
if (( failed > 0 || invalid > 0 )); then exit 1; fi
