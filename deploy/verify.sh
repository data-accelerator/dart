#!/usr/bin/env bash
# Verify a DART deployment on a real Kubernetes cluster.
#
#   deploy/verify.sh [-n namespace] [-i image] [-p dns|k8s]
#
# This exists because some properties cannot be established by unit tests: that
# the image actually runs unprivileged with no shell, that the probes and admin
# plane work, that a *peer* hit happens across a real network hop, and that
# concurrent demand for one block collapses to few origin fetches rather than one
# per instance.
#
# -p selects the discovery provider under test: "dns" (default; the plain dart
# image) or "k8s" (the dart-k8s image variant, EndpointSlice watch; applies
# rbac.yaml and points -discover at the k8s scheme). Every check below is
# provider-agnostic — that is the point: convergence is identical either way.
#
# It is intentionally assertive rather than informational: every check either
# passes or fails the run, so it is usable in CI.
set -euo pipefail

NS=dart-verify
IMAGE=dart:dev
PROVIDER=dns
while getopts "n:i:p:h" opt; do
  case $opt in
    n) NS=$OPTARG ;;
    i) IMAGE=$OPTARG ;;
    p) PROVIDER=$OPTARG ;;
    h) sed -n '2,16p' "$0"; exit 0 ;;
    *) exit 2 ;;
  esac
done
case $PROVIDER in
  dns|k8s) ;;
  *) echo "unknown provider '$PROVIDER' (want dns or k8s)" >&2; exit 2 ;;
esac

HERE=$(cd "$(dirname "$0")" && pwd)
PASS=0; FAIL=0
ok()   { printf '  \033[32mPASS\033[0m %s\n' "$1"; PASS=$((PASS+1)); }
bad()  { printf '  \033[31mFAIL\033[0m %s\n' "$1"; FAIL=$((FAIL+1)); }
step() { printf '\n\033[1m%s\033[0m\n' "$1"; }
check() { if [ "$2" = "$3" ]; then ok "$1 ($2)"; else bad "$1: got '$2', want '$3'"; fi; }

k() { kubectl -n "$NS" "$@"; }
# tb runs a command in the toolbox pod. Checks go through it so they exercise real
# in-cluster networking, and because DART's distroless image has no shell to exec.
tb() { k exec dart-toolbox -- "$@"; }
curl_tb() { tb curl -sS --max-time 120 "$@"; }

# metric NAME LABELS POD -> the current counter value, or 0 if absent.
# Absent and zero are the same thing for a counter that has never been touched,
# and Prometheus exporters legitimately omit untouched series.
metric() {
  local name=$1 labels=$2 pod=$3
  curl_tb "http://${pod}.dart-peers.${NS}.svc:19147/metrics" \
    | awk -v pat="^${name}${labels}" '$0 ~ pat {v=$2} END {printf "%d", v+0}'
}
blocks_from() { metric dart_block_source_total "\{source=\"$1\"\}" "$2"; }

# sha256 of an object fetched through a DART instance.
via_dart() {
  local pod=$1 path=$2 extra=${3:-}
  # shellcheck disable=SC2086
  curl_tb $extra "http://${pod}.dart-peers.${NS}.svc:19145/dart/http://dart-origin.${NS}.svc/${path}" \
    | sha256sum | cut -d' ' -f1
}
via_origin() {
  local path=$1 extra=${2:-}
  # shellcheck disable=SC2086
  curl_tb $extra "http://dart-origin.${NS}.svc/${path}" | sha256sum | cut -d' ' -f1
}

trap 'echo; echo "--- recent dart logs ---"; k logs -l app=dart-verify --tail=30 --prefix 2>/dev/null || true' ERR

step "Deploying to namespace $NS (image $IMAGE, discovery $PROVIDER)"
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
k apply -f "$HERE/k8s/test-fixtures.yaml" >/dev/null
# Substitute the image without needing kustomize or envsubst.
manifest=$(sed "s#image: dart:dev#image: ${IMAGE}#" "$HERE/k8s/statefulset.yaml")
if [ "$PROVIDER" = k8s ]; then
  # The k8s scheme watches EndpointSlices, which needs the namespaced RBAC and a
  # service account, and its spec names the Service rather than a DNS name.
  k apply -f "$HERE/k8s/rbac.yaml" >/dev/null
  manifest=$(printf '%s\n' "$manifest" | sed \
    -e 's#-discover=dns:dart-peers\.\$(POD_NAMESPACE)\.svc\.cluster\.local:19146#-discover=k8s:$(POD_NAMESPACE)/dart-peers#' \
    -e 's#^    spec:$#    spec:\n      serviceAccountName: dart#')
fi
printf '%s\n' "$manifest" | k apply -f - >/dev/null

step "Waiting for readiness"
k wait --for=condition=ready pod/dart-toolbox --timeout=120s >/dev/null
k rollout status deploy/dart-origin --timeout=180s >/dev/null
# rollout status on a StatefulSet waits for all replicas to be ready, which is the
# real assertion here: the readiness probe hits /healthz, so this passing means the
# binary started, took the cache lock and is serving its admin plane.
if k rollout status statefulset/dart --timeout=180s >/dev/null; then
  ok "all 3 replicas became ready (image runs; /healthz serving)"
else
  bad "replicas did not become ready"
  exit 1
fi

step "Image and process properties"
check "runs as non-root uid 65532" \
  "$(k get pod dart-0 -o jsonpath='{.spec.securityContext.runAsUser}')" "65532"
# A distroless image has no shell. Confirming that is worth a check: it is the
# property that makes `kubectl exec` impossible, which is why the toolbox exists.
if k exec dart-0 -- /bin/sh -c true 2>/dev/null; then
  bad "a shell is present in the runtime image"
else
  ok "no shell in the runtime image (distroless)"
fi

step "Admin plane"
check "healthz" "$(curl_tb -o /dev/null -w '%{http_code}' "http://dart-0.dart-peers.${NS}.svc:19147/healthz")" "200"
for ep in metrics admin/stats admin/members admin/ring; do
  code=$(curl_tb -o /dev/null -w '%{http_code}' "http://dart-0.dart-peers.${NS}.svc:19147/${ep}")
  check "/${ep}" "$code" "200"
done
step "Discovery: all three converge on the same membership"
# Membership is discovered, not configured, so it is asynchronous and must be waited
# for rather than assumed. Each pod resolves the headless Service for addresses and
# then exchanges rosters to learn identities; nothing here tells a pod who its peers
# are, so this is a genuine assertion rather than a read-back of configuration.
members_of() {
  curl_tb "http://${1}.dart-peers.${NS}.svc:19147/admin/members" \
    | grep -o '"id" *: *"[^"]*"' | sed 's/.*"\([^"]*\)"$/\1/' | sort | tr '\n' ',' 
}
epoch_of() {
  # The epoch is rendered as a *quoted* decimal string, not a JSON number, because
  # it is a full uint64 and JSON numbers are doubles. Match digits either way.
  curl_tb "http://${1}.dart-peers.${NS}.svc:19147/admin/members" \
    | grep -o '"epoch"[^,}]*' | grep -o '[0-9][0-9]*'
}

converged=false
for _ in $(seq 1 30); do
  a=$(members_of dart-0); b=$(members_of dart-1); c=$(members_of dart-2)
  if [ "$a" = "dart-0,dart-1,dart-2," ] && [ "$a" = "$b" ] && [ "$b" = "$c" ]; then
    converged=true; break
  fi
  sleep 2
done
if $converged; then
  ok "all three discovered each other (membership: ${a%,})"
else
  bad "did not converge within 60s: dart-0=[$a] dart-1=[$b] dart-2=[$c]"
fi

# The epoch only works as a convergence token if nodes that agree on membership
# compute the same value, which is why it covers ID and weight but not liveness.
ea=$(epoch_of dart-0); eb=$(epoch_of dart-1); ec=$(epoch_of dart-2)
if [ -n "$ea" ] && [ "$ea" = "$eb" ] && [ "$eb" = "$ec" ]; then
  ok "epochs agree across nodes ($ea)"
else
  bad "epochs disagree: $ea / $eb / $ec"
fi

step "Correctness: bytes through DART match the origin"
# The fixture content is position-sensitive, so this also catches block-assembly
# and range-mapping errors rather than only truncation.
want=$(via_origin blob.bin)
check "64MiB object, full GET" "$(via_dart dart-0 blob.bin)" "$want"
check "64MiB object, second GET (now cached)" "$(via_dart dart-0 blob.bin)" "$want"
# A range that deliberately starts and ends mid-block.
rng='-r 1048570-3145738'
check "unaligned range" "$(via_dart dart-0 blob.bin "$rng")" "$(via_origin blob.bin "$rng")"
# 1 MiB + 1 byte: the tail block holds a single byte.
check "short tail block" "$(via_dart dart-0 odd.bin)" "$(via_origin odd.bin)"

step "Cache: a repeat read does not return to the origin"
before_origin=$(blocks_from origin dart-0)
before_cache=$(blocks_from cache dart-0)
via_dart dart-0 blob.bin >/dev/null
after_origin=$(blocks_from origin dart-0)
after_cache=$(blocks_from cache dart-0)
if [ "$after_cache" -gt "$before_cache" ]; then
  ok "cache hits increased ($before_cache -> $after_cache)"
else
  bad "cache hits did not increase ($before_cache -> $after_cache)"
fi
check "origin fetches unchanged on a warm read" "$after_origin" "$before_origin"

step "P2P: a cold instance is served by a peer, not the origin"
# This is the check that needs a real cluster. dart-0 has the object warm; ask a
# different instance for it and require that at least some blocks arrive from a
# peer. Which instance owns which chunk is decided by HRW, so we look for peer
# traffic anywhere among the other two rather than predicting the owner.
peer_before=0; peer_after=0
for p in dart-1 dart-2; do peer_before=$((peer_before + $(blocks_from peer $p))); done
for p in dart-1 dart-2; do via_dart $p blob.bin >/dev/null; done
for p in dart-1 dart-2; do peer_after=$((peer_after + $(blocks_from peer $p))); done
if [ "$peer_after" -gt "$peer_before" ]; then
  ok "blocks served from a peer ($peer_before -> $peer_after)"
else
  bad "no peer hits ($peer_before -> $peer_after); P2P is not working"
fi

step "Origin amplification stays bounded"
# The arithmetic is exact, so this can be a tight bound rather than a gesture.
#
#   blob.bin is 64 MiB at -block-size=1MiB          -> 64 blocks
#   odd.bin is 1 MiB + 1 B                          ->  2 blocks
#   ideal total, cluster-wide                       -> 66 origin fetches
#
# Each block is fetched from origin once, by whichever node owns its chunk; the
# other two get it from that node. Size probes do not count (they go straight to the
# fetcher, not through the block path). With no sharing at all, three instances
# reading everything would be ~198.
#
# 100 leaves room for a hedge or a failover racing an owner while still failing
# clearly if fan-out is broken (~198) or half-broken (~132).
total_origin=0
for p in dart-0 dart-1 dart-2; do total_origin=$((total_origin + $(blocks_from origin $p))); done
echo "  total origin block fetches across 3 instances: $total_origin (ideal 66, no sharing ~198)"
if [ "$total_origin" -lt 100 ]; then
  ok "origin fetches bounded ($total_origin < 100; ideal 66)"
else
  bad "origin fetches too high ($total_origin, ideal 66); blocks are not being shared"
fi

step "Instance lock: one cache directory, one process"
# store.LockDir must refuse a second instance on the same directory. Prove it by
# starting a second dart against dart-0's mount and requiring a non-zero exit.
if k debug pod/dart-0 --image="$IMAGE" --target=dart -q --attach=false \
      --container=locktest -- /dart -cache-dir=/var/cache/dart -listen=:19999 >/dev/null 2>&1; then
  sleep 8
  st=$(k get pod dart-0 -o jsonpath='{.status.ephemeralContainerStatuses[?(@.name=="locktest")].state.terminated.exitCode}' 2>/dev/null || true)
  if [ "${st:-}" != "" ] && [ "${st:-0}" -ne 0 ]; then
    ok "a second instance on the same cache dir exited $st"
  else
    bad "a second instance did not fail (exit code '${st:-none}'); the lock is not holding"
  fi
else
  echo "  SKIP  ephemeral containers unavailable on this cluster"
fi

step "Result"
printf '  %d passed, %d failed\n' "$PASS" "$FAIL"
if [ "$FAIL" -ne 0 ]; then
  echo "  (namespace $NS left in place for inspection; delete with: kubectl delete ns $NS)"
  exit 1
fi
echo "  clean up with: kubectl delete ns $NS"
