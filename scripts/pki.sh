#!/usr/bin/env bash
# Dev PKI for the fleet-ingest lab (D6): one CA, a server certificate for the broker, one client
# certificate per gateway and per service, and revocation with a CRL. It uses `openssl ca`, which keeps a small
# database of what it issued (index.txt), so revoking and regenerating the CRL work like a real CA.
# Dev only: the CA key sits unencrypted on the operator's disk (mode 600). It's never mounted into a container and
# never deployed (S8-5): services mount only their own certificate and key, ca.crt and crl.pem. A real fleet keeps it
# in an HSM or a managed CA, and gateways get their keys at manufacturing or enrolment, never from a script.
#
# PKI_OUT=<dir> writes issued certificates and keys there instead of <dir> (the CA stays in <dir>): fleet-sim's
# gateways go to <dir>/fleet, the one directory a container mounts whole, so it can't hold the CA key.
#
#   scripts/pki.sh init  <dir>                     CA, broker server cert, empty CRL
#   scripts/pki.sh issue <dir> <cn>...             a client cert per CN (CN = gateway ID or service name)
#   scripts/pki.sh issue-fleet <dir> <prefix> <n>  <prefix>-0001 … <prefix>-<n>, like fleet-sim's gateway IDs
#   scripts/pki.sh revoke <dir> <cn>               revoke that CN's certificate and regenerate the CRL
#
# Every certificate uses an EC P-256 key: small and fast to issue, so 500 gateway certificates take seconds.

set -euo pipefail

cmd="${1:-}"
dir="${2:-}"
[[ -n "$cmd" && -n "$dir" ]] || { sed -n '2,15p' "$0"; exit 2; }

ca_conf() {
  # The `openssl ca` configuration: where its database lives, what it signs, and for how long.
  cat > "$dir/ca.cnf" <<EOF
[ ca ]
default_ca = lab_ca

[ lab_ca ]
dir               = $dir
database          = \$dir/index.txt
new_certs_dir     = \$dir/issued
serial            = \$dir/serial
crlnumber         = \$dir/crlnumber
certificate       = \$dir/ca.crt
private_key       = \$dir/ca.key
default_md        = sha256
default_days      = 825
default_crl_days  = 30
policy            = any_name
unique_subject    = no
copy_extensions   = copy

[ any_name ]
commonName = supplied

[ client_ext ]
basicConstraints = critical, CA:FALSE
keyUsage         = critical, digitalSignature
extendedKeyUsage = clientAuth

[ server_ext ]
basicConstraints = critical, CA:FALSE
keyUsage         = critical, digitalSignature
extendedKeyUsage = serverAuth
subjectAltName   = DNS:mosquitto, DNS:localhost, IP:127.0.0.1
EOF
}

new_key() {
  # An EC P-256 private key in PEM, readable by the container users (dev only, see header).
  openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$1" 2>/dev/null
  chmod 644 "$1"
}

sign() {
  # Creates a key and a CSR for CN=$1, has the CA sign it with extension section $2, and writes $1.crt / $1.key
  # into $PKI_OUT (default: the CA's own directory).
  local cn="$1" ext="$2" out="${PKI_OUT:-$dir}"
  mkdir -p "$out"
  new_key "$out/$cn.key"
  openssl req -new -key "$out/$cn.key" -subj "/CN=$cn" -out "$out/$cn.csr" 2>/dev/null
  openssl ca -batch -config "$dir/ca.cnf" -extensions "$ext" -notext -in "$out/$cn.csr" -out "$out/$cn.crt" 2>/dev/null
  rm -f "$out/$cn.csr"
}

regenerate_crl() {
  # The CRL lists every revoked serial, signed by the CA; the broker reads it as `crlfile`.
  openssl ca -batch -config "$dir/ca.cnf" -gencrl -out "$dir/crl.pem" 2>/dev/null
  chmod 644 "$dir/crl.pem"
}

case "$cmd" in
  init)
    mkdir -p "$dir/issued"
    [[ -f "$dir/ca.crt" ]] && { echo "CA already exists in $dir"; exit 0; }
    : > "$dir/index.txt"
    echo 1000 > "$dir/serial"
    echo 1000 > "$dir/crlnumber"
    ca_conf
    new_key "$dir/ca.key"
    chmod 600 "$dir/ca.key"
    openssl req -x509 -new -key "$dir/ca.key" -sha256 -days 3650 -subj "/CN=fleet-ingest lab CA" \
      -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" -out "$dir/ca.crt"
    sign mosquitto server_ext
    regenerate_crl
    echo "CA, broker certificate and CRL in $dir"
    ;;
  issue)
    shift 2
    for cn in "$@"; do sign "$cn" client_ext; done
    echo "issued: $*"
    ;;
  issue-fleet)
    prefix="${3:?prefix}"; count="${4:?count}"
    for ((i = 1; i <= count; i++)); do
      cn=$(printf "%s-%04d" "$prefix" "$i")
      [[ -f "${PKI_OUT:-$dir}/$cn.crt" ]] || sign "$cn" client_ext
    done
    echo "fleet certificates $prefix-0001 … $(printf "%s-%04d" "$prefix" "$count") in $dir"
    ;;
  revoke)
    cn="${3:?cn}"
    openssl ca -batch -config "$dir/ca.cnf" -revoke "$dir/$cn.crt" 2>/dev/null
    regenerate_crl
    echo "revoked $cn; CRL regenerated"
    ;;
  *)
    sed -n '2,15p' "$0"; exit 2
    ;;
esac
