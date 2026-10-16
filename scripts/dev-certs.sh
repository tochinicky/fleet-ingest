#!/usr/bin/env bash
# Creates everything the Compose stack needs in deploy/certs (gitignored): the lab CA, the broker certificate, one
# certificate per service (ingest-1, ingest-2, api, ops), and in deploy/certs/fleet one per fleet-sim gateway
# (sim-gw-0001 … sim-gw-<n>) with a copy of ca.crt. Each service mounts only its own files; fleet-sim mounts
# deploy/certs/fleet, which never holds the CA key (S8-5). The CA key is never mounted anywhere.
# Safe to run again: existing certificates are kept, missing ones are issued.
#
#   scripts/dev-certs.sh [gateway-count]   (default 10)
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
dir="$here/../deploy/certs"
count="${1:-10}"
"$here/pki.sh" init "$dir"
for cn in ingest-1 ingest-2 api ops; do [[ -f "$dir/$cn.crt" ]] || "$here/pki.sh" issue "$dir" "$cn"; done
# The dynsec admin (S8-5) can kick any client and change dynsec's roles, so its key is kept apart: deploy/certs is
# mounted into service containers, deploy/admin/certs into none. It's issued by the same CA, then moved.
admin="$here/../deploy/admin/certs"
if [[ ! -f "$admin/dynsec-admin.crt" ]]; then
  mkdir -p "$admin"
  "$here/pki.sh" issue "$dir" dynsec-admin
  mv "$dir/dynsec-admin.crt" "$dir/dynsec-admin.key" "$admin/"
  chmod 600 "$admin/dynsec-admin.key"
fi
PKI_OUT="$dir/fleet" "$here/pki.sh" issue-fleet "$dir" sim-gw "$count"
cp "$dir/ca.crt" "$dir/fleet/ca.crt"
chmod 600 "$dir/ca.key"
