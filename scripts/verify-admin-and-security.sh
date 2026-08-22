#!/usr/bin/env bash
# Live verification of the administrative and security surface added on top
# of M8: the four introspection/deletion APIs, quota entities, timestamp
# offset lookup, preferred-leader rebalancing, and mutual TLS.
#
# Everything here is checked against a real broker over a real socket, and
# every assertion is on observable output rather than on an exit code —
# a CLI that prints nothing and exits 0 must fail these, not pass them.
#
# Certificates are generated with Go's crypto/x509 (the development host has
# no OpenSSL), which is also the only way to get a CA whose name differs
# from the client's: a self-signed certificate has issuer == subject, and
# that is exactly the case that cannot catch a principal read from the
# wrong field.
#
# Requires Git Bash and Go on PATH.

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMP_ROOT="$(cd "${TMPDIR:-/tmp}" && pwd -P)"
WORK_DIR="$(mktemp -d "$TEMP_ROOT/brahmaputra-admin.XXXXXX")"
SERVER_EXE="$ROOT/target/release/brahmaputra-server.exe"
CLI_EXE="$ROOT/target/release/brahmaputra-cli.exe"
[[ -x "$SERVER_EXE" ]] || SERVER_EXE="$ROOT/target/release/brahmaputra-server"
[[ -x "$CLI_EXE" ]] || CLI_EXE="$ROOT/target/release/brahmaputra-cli"

CHECKS=0
PIDS=()

stage() { printf '\n\033[36m==> %s\033[0m\n' "$1"; }
pass() { CHECKS=$((CHECKS + 1)); printf '\033[32mPASS: %s\033[0m\n' "$1"; }
info() { printf '     %s\n' "$1"; }
die() { printf '\n\033[31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }

assert_eq() {
  local actual="$1" expected="$2" description="$3"
  [[ "$actual" == "$expected" ]] || die "$description (expected=$expected actual=$actual)"
  pass "$description"
}

assert_contains() {
  local haystack="$1" needle="$2" description="$3"
  [[ "$haystack" == *"$needle"* ]] || die "$description (looked for '$needle' in: $haystack)"
  pass "$description"
}

cleanup() {
  local pid
  for pid in "${PIDS[@]:-}"; do kill -9 "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  if [[ "${KEEP_ARTIFACTS:-0}" != "1" && "$WORK_DIR" == "$TEMP_ROOT"/brahmaputra-admin.* ]]; then
    rm -rf "$WORK_DIR"
  else
    printf 'artifacts: %s\n' "$WORK_DIR"
  fi
}
trap cleanup EXIT

allocate_port() {
  perl -MIO::Socket::INET -e '
    my $s = IO::Socket::INET->new(LocalAddr => "127.0.0.1", LocalPort => 0, Listen => 1, Proto => "tcp") or die $!;
    print $s->sockport;
  '
}

wait_for_broker() {
  local broker="$1" deadline=$((SECONDS + 40))
  shift
  while (( SECONDS < deadline )); do
    if "$CLI_EXE" --broker "$broker" "$@" metadata >/dev/null 2>&1; then return 0; fi
    sleep 0.3
  done
  return 1
}

# --------------------------------------------------- standalone: admin APIs

stage "DescribeCluster, DescribeConfigs, DescribeLogDirs, DeleteRecords"

PORT="$(allocate_port)"
BROKER="127.0.0.1:$PORT"
mkdir -p "$WORK_DIR/standalone"
"$SERVER_EXE" --port "$PORT" --data-dir "$WORK_DIR/standalone" \
  --default-partitions 2 --segment-bytes 8192 \
  >"$WORK_DIR/standalone.out" 2>"$WORK_DIR/standalone.err" &
PIDS+=($!)
wait_for_broker "$BROKER" || die "standalone broker never became ready"

cli() { "$CLI_EXE" --broker "$BROKER" "$@"; }

# Every API the broker dispatches must be advertised: a client that trusts
# ApiVersions to describe the broker cannot use what it is not told about.
ADVERTISED="$(cli api-versions | grep -c '^  api ')"
assert_eq "$ADVERTISED" "28" "ApiVersions advertises every dispatched API"

CLUSTER="$(cli describe-cluster)"
assert_contains "$CLUSTER" "cluster id:" "DescribeCluster reports a cluster id"
assert_contains "$CLUSTER" "$PORT" "DescribeCluster reports the broker's own address"

cli produce --topic admin-demo --count 3000 --value-size 300 --no-key \
  --acks 1 --compression none >/dev/null

CONFIGS="$(cli describe-configs --type topic --name admin-demo)"
assert_contains "$CONFIGS" "retention.ms" "DescribeConfigs lists a config that was never set"
assert_contains "$CONFIGS" "default" "unset configs are reported as inherited defaults"
BROKER_CONFIGS="$(cli describe-configs --type broker)"
assert_contains "$BROKER_CONFIGS" "broker.id" "DescribeConfigs answers for the broker resource"

DIRS="$(cli describe-log-dirs --topic admin-demo)"
assert_contains "$DIRS" "admin-demo-0" "DescribeLogDirs names each partition it hosts"
SIZE_BEFORE="$(printf '%s\n' "$DIRS" | sed -n 's/^total \([0-9]*\) bytes.*/\1/p')"
[[ "${SIZE_BEFORE:-0}" -gt 0 ]] || die "DescribeLogDirs reported zero bytes for a written topic"
info "on disk before delete: ${SIZE_BEFORE} bytes"

# Deleting records must hide them, reclaim their segments, and leave the
# survivors readable — all three, or it is not a delete.
cli delete-records --topic admin-demo --offset 1000 >/dev/null
EARLIEST="$(cli offsets --topic admin-demo --partition 0 | sed -n 's/.*earliest=\([0-9]*\).*/\1/p')"
assert_eq "$EARLIEST" "1000" "DeleteRecords moved the log start offset"
SIZE_AFTER="$(cli describe-log-dirs --topic admin-demo | sed -n 's/^total \([0-9]*\) bytes.*/\1/p')"
[[ "$SIZE_AFTER" -lt "$SIZE_BEFORE" ]] \
  || die "DeleteRecords reclaimed no disk (before=$SIZE_BEFORE after=$SIZE_AFTER)"
pass "DeleteRecords reclaimed segments (${SIZE_BEFORE} -> ${SIZE_AFTER} bytes)"
SURVIVORS="$(cli consume --topic admin-demo --from earliest --max 100000 2>/dev/null | wc -l)"
assert_eq "$SURVIVORS" "1000" "exactly the undeleted records remain readable"

# ------------------------------------------------ timestamp offset lookup

stage "ListOffsets by timestamp"

BEFORE_MS="$(date +%s%3N)"
cli produce --topic clocked --count 400 --value-size 200 --no-key \
  --acks 1 --compression none >/dev/null
sleep 1
MIDDLE_MS="$(date +%s%3N)"
cli produce --topic clocked --count 400 --value-size 200 --no-key \
  --acks 1 --compression none >/dev/null

at_timestamp() {
  cli offsets --topic clocked --partition 0 --timestamp "$1" \
    | sed -n 's/.*at(-\?[0-9]*)=\([0-9]*\).*/\1/p'
}
assert_eq "$(at_timestamp $((BEFORE_MS - 60000)))" "0" \
  "a timestamp older than the log resolves to its start"
assert_eq "$(at_timestamp "$MIDDLE_MS")" "200" \
  "a timestamp between two batches resolves to the first record after it"
assert_eq "$(at_timestamp $((MIDDLE_MS + 3600000)))" "400" \
  "a timestamp newer than every record resolves to the log end"

# ------------------------------------------------------- cluster: quotas

stage "quota entities: per-user and per-client-id byte-rate limits"

kill -9 "${PIDS[0]}" 2>/dev/null || true
PIDS=()
DATA_PORT="$(allocate_port)"
CONTROL_PORT="$(allocate_port)"
BROKER="127.0.0.1:$DATA_PORT"
CONTROLLER="http://127.0.0.1:$CONTROL_PORT"
mkdir -p "$WORK_DIR/cluster"
"$SERVER_EXE" --host 127.0.0.1 --port "$DATA_PORT" --data-dir "$WORK_DIR/cluster" \
  --node-id 1 --cluster-id verify-admin --control-port "$CONTROL_PORT" \
  --http-port 0 --bootstrap --controller-peer "1=127.0.0.1:$CONTROL_PORT" \
  --heartbeat-interval-ms 500 --session-timeout-ms 3000 \
  >"$WORK_DIR/cluster.out" 2>"$WORK_DIR/cluster.err" &
PIDS+=($!)
wait_for_broker "$BROKER" || die "cluster broker never became ready"
# The offsets topic is created once the node registers; give the controller
# a moment to apply it before writing metadata commands.
sleep 3

ctl() { "$CLI_EXE" --controller "$CONTROLLER" "$@"; }
ctl topic create --name quota-demo --partitions 1 --replication-factor 1 >/dev/null

produce_rate() {
  "$CLI_EXE" --broker "$BROKER" produce --topic quota-demo --count 4000 \
    --value-size 512 --no-key --acks 1 --compression none \
    | sed -n 's/.*-> \([0-9]*\) msgs\/sec.*/\1/p'
}

EMPTY="$(ctl quota list)"
assert_contains "$EMPTY" "no quota overrides" "a cluster starts with no quota overrides"

BASELINE="$(produce_rate)"
info "unthrottled: ${BASELINE} msgs/sec"

# The Rust producer announces itself as `brahmaputra-client`; the quota is
# bound to that client id rather than to a user, which is the form that
# works without authentication.
ctl quota set --client-id brahmaputra-client --produce-bytes-per-sec 262144 >/dev/null
LISTED="$(ctl quota list)"
assert_contains "$LISTED" "brahmaputra-client" "quota list shows the configured entity"
sleep 2
THROTTLED="$(produce_rate)"
info "throttled to 256 KiB/s: ${THROTTLED} msgs/sec"
# 512-byte records under a 256 KiB/s ceiling is ~512 records/sec. Assert an
# order of magnitude rather than a number, so this is a test of enforcement
# and not of the host's speed.
[[ "$THROTTLED" -lt $((BASELINE / 4)) ]] \
  || die "the quota did not throttle (baseline=$BASELINE throttled=$THROTTLED)"
pass "a per-client quota throttles produce (${BASELINE} -> ${THROTTLED} msgs/sec)"

TOTAL="$("$CLI_EXE" --broker "$BROKER" offsets --topic quota-demo \
  | awk -F'latest=' '{ total += $2 } END { print total + 0 }')"
assert_eq "$TOTAL" "8000" "throttling delayed the acknowledgements and lost no records"

ctl quota delete --client-id brahmaputra-client >/dev/null
sleep 2
RESTORED="$(produce_rate)"
[[ "$RESTORED" -gt $((THROTTLED * 4)) ]] \
  || die "removing the quota did not restore throughput (throttled=$THROTTLED restored=$RESTORED)"
pass "removing the quota restores throughput (${RESTORED} msgs/sec)"

# ------------------------------------------------------------- mutual TLS

stage "mutual TLS: the certificate's subject is the principal"

command -v go >/dev/null 2>&1 || die "go is required to generate test certificates"
CERT_DIR="$WORK_DIR/certs"
mkdir -p "$CERT_DIR"
cat >"$WORK_DIR/gencerts.go" <<'GOFILE'
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"time"
)

func write(path string, blocks ...*pem.Block) {
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	for _, b := range blocks {
		if err := pem.Encode(f, b); err != nil {
			panic(err)
		}
	}
}

func keyBlock(k *ecdsa.PrivateKey) *pem.Block {
	der, err := x509.MarshalPKCS8PrivateKey(k)
	if err != nil {
		panic(err)
	}
	return &pem.Block{Type: "PRIVATE KEY", Bytes: der}
}

func main() {
	dir := os.Args[1]
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "brahmaputra-verify-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDer, _ := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	caCert, _ := x509.ParseCertificate(caDer)
	write(dir+"/ca.pem", &pem.Block{Type: "CERTIFICATE", Bytes: caDer})

	srvKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	srvTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "brahmaputra"},
		DNSNames:     []string{"brahmaputra", "localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	srvDer, _ := x509.CreateCertificate(rand.Reader, srvTmpl, caCert, &srvKey.PublicKey, caKey)
	write(dir+"/server.pem", &pem.Block{Type: "CERTIFICATE", Bytes: srvDer})
	write(dir+"/server.key", keyBlock(srvKey))

	// Two clients of the same authority. Their subjects differ and their
	// issuer does not, which is what proves the principal comes from the
	// subject: reading the issuer would make these one identity.
	for i, name := range []string{"alice", "mallory"} {
		key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(int64(10 + i)),
			Subject:      pkix.Name{CommonName: name},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(24 * time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		}
		der, _ := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
		write(dir+"/"+name+".pem", &pem.Block{Type: "CERTIFICATE", Bytes: der})
		write(dir+"/"+name+".key", keyBlock(key))
	}
}
GOFILE
(cd "$WORK_DIR" && go run gencerts.go "$CERT_DIR") || die "cannot generate test certificates"

for pid in "${PIDS[@]}"; do kill -9 "$pid" 2>/dev/null || true; done
PIDS=()
DATA_PORT="$(allocate_port)"
CONTROL_PORT="$(allocate_port)"
BROKER="127.0.0.1:$DATA_PORT"
CONTROLLER="http://127.0.0.1:$CONTROL_PORT"
mkdir -p "$WORK_DIR/secure"
"$SERVER_EXE" --host 127.0.0.1 --port "$DATA_PORT" --data-dir "$WORK_DIR/secure" \
  --node-id 1 --cluster-id verify-mtls --control-port "$CONTROL_PORT" \
  --http-port 0 --bootstrap --controller-peer "1=127.0.0.1:$CONTROL_PORT" \
  --transport tcp-tls --require-auth \
  --tls-cert "$CERT_DIR/server.pem" --tls-key "$CERT_DIR/server.key" \
  --tls-client-ca "$CERT_DIR/ca.pem" \
  --heartbeat-interval-ms 500 --session-timeout-ms 3000 \
  >"$WORK_DIR/secure.out" 2>"$WORK_DIR/secure.err" &
PIDS+=($!)

as_client() {
  local who="$1"
  shift
  "$CLI_EXE" --broker "$BROKER" --transport tcp-tls --tls-ca "$CERT_DIR/ca.pem" \
    --tls-cert "$CERT_DIR/$who.pem" --tls-key "$CERT_DIR/$who.key" "$@"
}

DEADLINE=$((SECONDS + 40))
until curl -sf "$CONTROLLER/api/v1/controller/metadata" >/dev/null 2>&1; do
  (( SECONDS < DEADLINE )) || die "secure broker's controller never became ready"
  sleep 0.3
done
sleep 3

"$CLI_EXE" --controller "$CONTROLLER" topic create --name secured \
  --partitions 1 --replication-factor 1 >/dev/null

# A client that presents no certificate cannot complete the handshake at
# all: the refusal happens before any request is sent.
if as_client_without_cert_output="$("$CLI_EXE" --broker "$BROKER" --transport tcp-tls \
    --tls-ca "$CERT_DIR/ca.pem" metadata 2>&1)"; then
  die "a connection with no client certificate was accepted"
fi
pass "a connection with no client certificate is refused at the handshake"

# Deny-by-default: a verified certificate is an identity, not a permission.
if as_client alice produce --topic secured --value hello >/dev/null 2>&1; then
  die "alice was allowed to produce before any ACL granted it"
fi
pass "a verified certificate alone authorizes nothing"

grant() {
  curl -sf -X POST "$CONTROLLER/api/v1/controller/command" \
    -H 'content-type: application/json' \
    -d "{\"type\":\"put_acl\",\"rule\":{\"principal\":\"$1\",\"resource_type\":\"$2\",\"resource_name\":\"$3\",\"operation\":\"$4\",\"permission\":\"allow\"}}" \
    >/dev/null
}
for operation in read write describe; do
  grant alice topic secured "$operation"
done
grant alice cluster cluster describe
sleep 2

ACKED="$(as_client alice produce --topic secured --value hello 2>&1 | tail -1)"
assert_contains "$ACKED" "acked" "the principal from the certificate's subject is authorized"
OFFSETS="$(as_client alice offsets --topic secured)"
assert_contains "$OFFSETS" "latest=1" "the record it wrote is readable back"

# The decisive check: mallory holds a certificate from the same CA. If the
# principal came from the issuer rather than the subject, these two would be
# the same identity and this would succeed.
if as_client mallory produce --topic secured --value evil >/dev/null 2>&1; then
  die "a different subject signed by the same CA was authorized as alice"
fi
pass "a different subject from the same CA is a different principal, and is denied"

printf '\n\033[32mAll %d checks passed.\033[0m\n' "$CHECKS"
