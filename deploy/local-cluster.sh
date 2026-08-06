#!/usr/bin/env bash
# Run a DART cluster on one machine and report what it is doing.
#
#   deploy/local-cluster.sh up       start N nodes, wait for ready + convergence
#   deploy/local-cluster.sh status   one-shot report
#   deploy/local-cluster.sh watch    repeat, showing throughput between samples
#   deploy/local-cluster.sh urls     request URLs, ready to paste
#   deploy/local-cluster.sh logs [A] tail node logs
#   deploy/local-cluster.sh down     stop everything
#
# This is an observation rig, not a test: `up` leaves the cluster running so you can
# drive it by hand and watch the effect.
#
# It does not run or configure an origin. In prefix mode a request carries the whole
# upstream URL, so DART needs no origin at startup and you can point every request
# at whatever real origin you like:
#
#   http://127.0.0.1:18150/dart/https://your-registry/v2/repo/blobs/sha256:...
#
# Set UPSTREAM=<url> to have the printed examples filled in for you.
#
# Reporting reads DART's own metrics only, so nothing is required of the origin: the
# node counts what it received from peers versus from upstream itself.
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT=$PWD
RUN=${DART_RUN_DIR:-$ROOT/.dart-local}

# --- configuration (override via environment) --------------------------------
NODES=${NODES:-3}
UPSTREAM=${UPSTREAM:-}              # optional: only used to print concrete examples
CACHE_SIZE=${CACHE_SIZE:-2GiB}
MEM_SIZE=${MEM_SIZE:-256MiB}
BLOCK_SIZE=${BLOCK_SIZE:-4MiB}
CHUNK_SIZE=${CHUNK_SIZE:-64MiB}     # below the 256MiB default so a test object
                                    # spans several chunks, and therefore has
                                    # several owners rather than one
PREFIX=${PREFIX:-dart}
DISCOVER_INTERVAL=${DISCOVER_INTERVAL:-1s}
CLIENT_BASE=${CLIENT_BASE:-18150}
PEER_BASE=${PEER_BASE:-18160}
ADMIN_BASE=${ADMIN_BASE:-18170}
# Anything extra to pass to every node, e.g.
#   DART_FLAGS="-registry=https://registry-1.docker.io -registry-auth=/path/creds.json"
DART_FLAGS=${DART_FLAGS:-}

GO=${GO:-$(command -v go || echo /usr/local/go/bin/go)}

# Node 0 is A, 1 is B, ... Letters read better in a report than ordinals.
node_id()     { printf "\\$(printf '%03o' $((65 + $1)))"; }
client_port() { echo $((CLIENT_BASE + $1)); }
peer_port()   { echo $((PEER_BASE + $1)); }
admin_port()  { echo $((ADMIN_BASE + $1)); }

bold()  { printf '\033[1m%s\033[0m\n' "$1"; }
green() { printf '\033[32m%s\033[0m' "$1"; }
red()   { printf '\033[31m%s\033[0m' "$1"; }
dim()   { printf '\033[2m%s\033[0m\n' "$1"; }

# The shape overlaybd uses: the prefix front end takes the entire upstream URL.
dart_url() { echo "http://127.0.0.1:$(client_port "$1")/${PREFIX}/${2}"; }
example_upstream() { echo "${UPSTREAM:-https://YOUR-ORIGIN/path/to/blob}"; }

# --- metric helpers ----------------------------------------------------------
# metric <admin-port> <name> [label-match] -> value, or 0 when the series is absent
# (an exporter legitimately omits a counter that has never been touched).
metric() {
  local port=$1 name=$2 match=${3:-}
  curl -sf --max-time 3 "http://127.0.0.1:${port}/metrics" 2>/dev/null \
    | awk -v n="$name" -v m="$match" '
        index($0, n) == 1 && (m == "" || index($0, m) > 0) { v = $NF }
        END { printf "%.0f", v + 0 }'
}
blocks_from() { metric "$(admin_port "$1")" dart_block_source_total "source=\"$2\""; }
bytes_dir()   { metric "$(admin_port "$1")" dart_bytes_total "direction=\"$2\""; }

members_json() { curl -sf --max-time 3 "http://127.0.0.1:$(admin_port "$1")/admin/members" 2>/dev/null; }
member_ids()   { members_json "$1" | grep -o '"id" *: *"[^"]*"' | sed 's/.*"\([^"]*\)"$/\1/' | sort | tr '\n' ' '; }
# The epoch is a quoted decimal string, not a JSON number: it is a full uint64 and
# JSON numbers are doubles.
epoch_of()     { members_json "$1" | grep -o '"epoch"[^,}]*' | grep -o '[0-9][0-9]*'; }
is_ready()     { curl -sf --max-time 2 -o /dev/null "http://127.0.0.1:$(admin_port "$1")/healthz" 2>/dev/null; }

human() {
  awk -v b="$1" 'BEGIN{
    split("B KiB MiB GiB TiB", u, " ");
    i=1; while (b >= 1024 && i < 5) { b /= 1024; i++ }
    printf (i==1 ? "%d %s" : "%.1f %s"), b, u[i]
  }'
}

# --- up ----------------------------------------------------------------------
cmd_up() {
  if pgrep -x dart >/dev/null 2>&1; then
    red "dart is already running; run 'down' first"; echo; exit 1
  fi
  mkdir -p "$RUN"

  bold "Building"
  "$GO" build -o "$RUN/dart" ./cmd/dart
  echo "  $("$RUN/dart" -version)"

  # Every node seeds from every node's peer port. Discovery still does the real work:
  # a seed is only an address, and the stable identity placement needs comes from the
  # roster exchange that follows.
  local seeds=""
  for i in $(seq 0 $((NODES - 1))); do
    seeds="${seeds}${seeds:+,}127.0.0.1:$(peer_port "$i")"
  done

  bold "Starting $NODES nodes"
  rm -f "$RUN"/*.log
  for i in $(seq 0 $((NODES - 1))); do
    local id; id=$(node_id "$i")
    rm -rf "$RUN/cache-$id"; mkdir -p "$RUN/cache-$id"
    # shellcheck disable=SC2086
    "$RUN/dart" \
      -self-id="$id" \
      -discover="static:$seeds" \
      -peer-advertise="127.0.0.1:$(peer_port "$i")" \
      -discover-interval="$DISCOVER_INTERVAL" \
      -listen="127.0.0.1:$(client_port "$i")" \
      -peer-listen="127.0.0.1:$(peer_port "$i")" \
      -admin="127.0.0.1:$(admin_port "$i")" \
      -prefix="$PREFIX" \
      -cache-dir="$RUN/cache-$id" \
      -cache-size="$CACHE_SIZE" -mem-size="$MEM_SIZE" \
      -block-size="$BLOCK_SIZE" -chunk-size="$CHUNK_SIZE" \
      $DART_FLAGS \
      >"$RUN/$id.log" 2>&1 &
    echo "  $id  client=$(client_port "$i")  peer=$(peer_port "$i")  admin=$(admin_port "$i")"
  done

  bold "Waiting for ready"
  local ok=1
  for _ in $(seq 1 40); do
    ok=1
    for i in $(seq 0 $((NODES - 1))); do is_ready "$i" || ok=0; done
    [ "$ok" = 1 ] && break
    sleep 0.5
  done
  if [ "$ok" != 1 ]; then
    red "  not all nodes became ready"; echo
    tail -n 6 "$RUN"/*.log
    exit 1
  fi
  echo "  $(green "all $NODES ready")"

  bold "Waiting for membership to converge"
  local want conv=0
  want=$(for i in $(seq 0 $((NODES - 1))); do node_id "$i"; echo -n ' '; done)
  for _ in $(seq 1 60); do
    conv=1
    local first_epoch=""
    for i in $(seq 0 $((NODES - 1))); do
      [ "$(member_ids "$i")" = "$want" ] || conv=0
      local e; e=$(epoch_of "$i")
      [ -z "$first_epoch" ] && first_epoch=$e
      [ "$e" = "$first_epoch" ] || conv=0
    done
    [ "$conv" = 1 ] && break
    sleep 0.5
  done
  if [ "$conv" = 1 ]; then
    echo "  $(green converged): [$(member_ids 0)] epoch $(epoch_of 0)"
  else
    red "  did not converge"; echo
    for i in $(seq 0 $((NODES - 1))); do
      echo "    $(node_id "$i"): [$(member_ids "$i")] epoch $(epoch_of "$i")"
    done
  fi

  echo
  bold "Settings"
  echo "  cache=$CACHE_SIZE  mem=$MEM_SIZE  block=$BLOCK_SIZE  chunk=$CHUNK_SIZE"
  [ -n "$DART_FLAGS" ] && echo "  extra: $DART_FLAGS"
  echo
  cmd_urls
}

# --- urls --------------------------------------------------------------------
cmd_urls() {
  local up; up=$(example_upstream)
  bold "Send requests here (the shape overlaybd uses)"
  dim "  http://127.0.0.1:<client-port>/${PREFIX}/<full upstream URL>"
  for i in $(seq 0 $((NODES - 1))); do
    echo "  $(node_id "$i")  $(dart_url "$i" "$up")"
  done
  if [ -z "$UPSTREAM" ]; then
    echo
    dim "  Set UPSTREAM=<url> to have these printed concretely."
  fi
  echo
  bold "Examples"
  cat <<EOF
  U=$(dart_url 0 "$up")

  # whole object
  curl -o /dev/null -w 'speed=%{speed_download} B/s time=%{time_total}s\n' "\$U"

  # a 4 MiB range, the way a block device reads
  curl -o /dev/null -r 0-4194303 "\$U"

  # 32 parallel random 1 MiB reads (overlaybd's lazy-load pattern)
  for i in \$(seq 32); do
    off=\$(( (RANDOM * 32768) % 1000000000 ))
    curl -s -o /dev/null -r \$off-\$((off + 1048575)) "\$U" &
  done; wait

  # the same object from every node at once, to see peer fan-out
$(for i in $(seq 0 $((NODES - 1))); do echo "  curl -s -o /dev/null $(dart_url "$i" "$up") &"; done)
  wait

  # baseline: straight to the origin, bypassing DART
  curl -o /dev/null -w 'speed=%{speed_download} B/s\n' $up
EOF
  echo
  dim "  status:  deploy/local-cluster.sh status"
  dim "  watch:   deploy/local-cluster.sh watch      (throughput between samples)"
  dim "  stop:    deploy/local-cluster.sh down"
}

# --- status ------------------------------------------------------------------
cmd_status() {
  bold "Nodes"
  printf '  %-4s %-6s %-7s %9s %9s %9s %11s %11s %8s\n' \
    NODE READY MEMBERS CACHE PEER ORIGIN DELIVERED UPSTREAM CIRCUITS
  dim '       (CACHE/PEER/ORIGIN = block reads by source; DELIVERED/UPSTREAM = wire bytes)'
  local t_cache=0 t_peer=0 t_origin=0 t_served=0 t_oin=0 t_pin=0
  for i in $(seq 0 $((NODES - 1))); do
    local id ready ch ph og sv oin co n
    id=$(node_id "$i")
    if is_ready "$i"; then ready=$(green ok); else ready=$(red down); fi
    ch=$(blocks_from "$i" cache); ph=$(blocks_from "$i" peer); og=$(blocks_from "$i" origin)
    sv=$(bytes_dir "$i" client); oin=$(bytes_dir "$i" origin_in)
    co=$(metric "$(admin_port "$i")" dart_peer_circuits_open)
    n=$(member_ids "$i" | wc -w)
    printf '  %-4s %-15s %-7s %9s %9s %9s %11s %11s %8s\n' \
      "$id" "$ready" "$n" "$ch" "$ph" "$og" "$(human "$sv")" "$(human "$oin")" "$co"
    t_cache=$((t_cache + ch)); t_peer=$((t_peer + ph)); t_origin=$((t_origin + og))
    t_served=$((t_served + sv)); t_oin=$((t_oin + oin))
    t_pin=$((t_pin + $(bytes_dir "$i" peer_in)))
  done
  printf '  %-4s %-6s %-7s %9s %9s %9s %11s %11s\n' \
    ALL '' '' "$t_cache" "$t_peer" "$t_origin" "$(human "$t_served")" "$(human "$t_oin")"

  local blocks=$((t_cache + t_peer + t_origin))
  if [ "$blocks" -gt 0 ]; then
    echo
    # Block counts are per-read attribution: two concurrent readers that coalesce
    # onto one upstream fetch are two origin-sourced reads. Byte counts are wire
    # bytes, so that same pair transfers one object's worth. The two answer
    # different questions and will not divide into each other.
    printf '  block reads: %d  (cache %d%%, peer %d%%, origin %d%%)\n' \
      "$blocks" $((t_cache * 100 / blocks)) $((t_peer * 100 / blocks)) $((t_origin * 100 / blocks))
    printf '  origin offload: %s%% of block reads were satisfied inside the cluster\n' \
      "$(green $(( (t_cache + t_peer) * 100 / blocks )))"
    if [ "$t_served" -gt 0 ]; then
      printf '  bytes on the wire: delivered %s, from upstream %s, between peers %s\n' \
        "$(human "$t_served")" "$(human "$t_oin")" "$(human "$t_pin")"
      printf '  amplification: %s%% of delivered bytes were pulled from upstream\n' \
        "$(green $((t_oin * 100 / t_served)))"
      dim "    (100% means every byte served cost an upstream byte; lower is the point)"
    fi
  else
    dim "  no traffic yet"
  fi

  local e0 same=1
  e0=$(epoch_of 0)
  for i in $(seq 0 $((NODES - 1))); do [ "$(epoch_of "$i")" = "$e0" ] || same=0; done
  echo
  if [ "$same" = 1 ]; then
    echo "  epoch $(green "$e0") agreed by all nodes"
  else
    red "  epochs disagree:"; echo
    for i in $(seq 0 $((NODES - 1))); do echo "    $(node_id "$i") $(epoch_of "$i")"; done
  fi
}

# --- watch -------------------------------------------------------------------
cmd_watch() {
  local iv=${1:-2}
  bold "Sampling every ${iv}s (Ctrl-C to stop)"
  dim "  as the cluster warms, ORIGIN-IN should fall towards zero while CLIENT holds up"
  echo
  local -a p_cli p_pin p_oin
  local first=1
  while true; do
    local -a c_cli c_pin c_oin
    for i in $(seq 0 $((NODES - 1))); do
      c_cli[i]=$(bytes_dir "$i" client)
      c_pin[i]=$(bytes_dir "$i" peer_in)
      c_oin[i]=$(bytes_dir "$i" origin_in)
    done

    if [ "$first" = 1 ]; then
      first=0
    else
      printf '  %-4s %14s %14s %14s\n' NODE CLIENT PEER-IN ORIGIN-IN
      local tc=0 tp=0 to=0
      for i in $(seq 0 $((NODES - 1))); do
        local dc=$(( (c_cli[i] - p_cli[i]) / iv ))
        local dp=$(( (c_pin[i] - p_pin[i]) / iv ))
        local do_=$(( (c_oin[i] - p_oin[i]) / iv ))
        printf '  %-4s %12s/s %12s/s %12s/s\n' "$(node_id "$i")" \
          "$(human $dc)" "$(human $dp)" "$(human $do_)"
        tc=$((tc + dc)); tp=$((tp + dp)); to=$((to + do_))
      done
      printf '  %-4s %12s/s %12s/s %12s/s' ALL "$(human $tc)" "$(human $tp)" "$(human $to)"
      if [ "$tc" -gt 0 ]; then printf '   (origin = %d%% of delivered)' $((to * 100 / tc)); fi
      echo; echo "  ---"
    fi
    p_cli=("${c_cli[@]}"); p_pin=("${c_pin[@]}"); p_oin=("${c_oin[@]}")
    sleep "$iv"
  done
}

# --- logs / down -------------------------------------------------------------
cmd_logs() {
  if [ $# -gt 0 ]; then tail -f "$RUN/$1.log"; else tail -n 20 "$RUN"/*.log; fi
}

cmd_down() {
  local n=0
  for p in $(pgrep -x dart 2>/dev/null); do kill "$p" 2>/dev/null && n=$((n + 1)); done
  sleep 1
  for p in $(pgrep -x dart 2>/dev/null); do kill -9 "$p" 2>/dev/null; done
  echo "stopped $n process(es); run dir $RUN kept (logs, caches)"
}

case "${1:-}" in
  up)     shift; cmd_up "$@" ;;
  status) shift; cmd_status "$@" ;;
  watch)  shift; cmd_watch "$@" ;;
  urls)   shift; cmd_urls "$@" ;;
  logs)   shift; cmd_logs "$@" ;;
  down)   shift; cmd_down "$@" ;;
  *)      sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//' ; exit 2 ;;
esac
