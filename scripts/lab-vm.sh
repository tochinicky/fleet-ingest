#!/usr/bin/env bash
# The lab VM (decisions.md S7-12): demos, scenarios and integration runs happen on a separate Linux machine, not on a
# laptop's Docker VM, whose 4 GB distorted every timing. This script only moves files and runs commands over SSH;
# starting, stopping and creating the machine is done elsewhere.
#
#   LAB_HOST=user@host [LAB_SSH_KEY=path] scripts/lab-vm.sh sync           # the working tree (and the gateway repo, if set)
#   LAB_HOST=user@host [LAB_SSH_KEY=path] scripts/lab-vm.sh run <command>  # runs in ~/fleet-ingest with Node from nvm
#   LAB_HOST=user@host [LAB_SSH_KEY=path] scripts/lab-vm.sh pull-results   # demo-results/ back to this checkout
#
# Optional: LAB_DIR (remote directory, default fleet-ingest), LAB_GATEWAY_REPO (a local checkout of the .NET gateway
# repo, synced to ~/home-energy-gateway for scenario 9).
#
# Never synced: .git and everything .gitignore ignores (node_modules, demo-results, spike data, certificates and keys,
# .env, Terraform state). The lab's certificates are made on the lab machine by scripts/dev-certs.sh, so no CA key
# ever leaves the machine it was made on.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
: "${LAB_HOST:?set LAB_HOST to user@host}"
remote_dir="${LAB_DIR:-fleet-ingest}"
ssh_opts=(-o BatchMode=yes -o ServerAliveInterval=30)
if [[ -n "${LAB_SSH_KEY:-}" ]]; then ssh_opts+=(-i "$LAB_SSH_KEY"); fi
rsync_ssh="ssh ${ssh_opts[*]}"
sync_excludes=(
  --exclude .git --exclude node_modules --exclude demo-results --exclude '*.log' --exclude .env
  --exclude certs --exclude '*.key' --exclude '*.pem' --exclude 'spikes/*/data'
  --exclude .terraform --exclude '*.tfstate*' --exclude terraform.tfvars --exclude '*.tfplan' --exclude production.env
)

case "${1:-}" in
  sync)
    # The excludes mirror .gitignore (macOS's openrsync ignores a `:- .gitignore` filter, so they're spelled out):
    # certificates, keys, .env, Terraform state, results, node_modules stay here. On the lab machine those paths are
    # protected from --delete, so its own certificates survive a sync.
    rsync -az --delete -e "$rsync_ssh" "${sync_excludes[@]}" "$root/" "$LAB_HOST:$remote_dir/"
    if [[ -n "${LAB_GATEWAY_REPO:-}" ]]; then
      rsync -az --delete -e "$rsync_ssh" --exclude bin --exclude obj --exclude .git \
        "${LAB_GATEWAY_REPO%/}/" "$LAB_HOST:home-energy-gateway/"
    fi
    ;;
  run)
    shift
    [[ $# -gt 0 ]] || { echo "usage: $0 run <command>" >&2; exit 2; }
    # shellcheck disable=SC2029 # the command is meant to expand on the lab machine
    ssh "${ssh_opts[@]}" "$LAB_HOST" "cd $remote_dir && . ~/.nvm/nvm.sh >/dev/null && $*"
    ;;
  pull-results)
    mkdir -p "$root/demo-results"
    rsync -az -e "$rsync_ssh" "$LAB_HOST:$remote_dir/demo-results/" "$root/demo-results/"
    ;;
  *)
    echo "usage: $0 sync | run <command> | pull-results" >&2
    exit 2
    ;;
esac
