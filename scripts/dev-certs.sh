#!/usr/bin/env bash
# Creates everything the Compose stack needs in deploy/certs (gitignored): the lab CA, the broker certificate, one
# certificate per service (ingest-1, ingest-2, api, ops) and one per fleet-sim gateway (sim-gw-0001 … sim-gw-<n>).
# Safe to run again: existing certificates are kept, missing ones are issued.
#
#   scripts/dev-certs.sh [gateway-count]   (default 10)
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
dir="$here/../deploy/certs"
count="${1:-10}"
"$here/pki.sh" init "$dir"
for cn in ingest-1 ingest-2 api ops; do [[ -f "$dir/$cn.crt" ]] || "$here/pki.sh" issue "$dir" "$cn"; done
"$here/pki.sh" issue-fleet "$dir" sim-gw "$count"
