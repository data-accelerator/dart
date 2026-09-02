#!/usr/bin/env bash
# check-docs.sh — documentation governance floor checks.
#
# What this enforces (hard failures):
#   1. docs/README.md index <-> docs files stay in sync
#   2. backticked Test*/Benchmark*/Example*/Fuzz* names in docs exist as test
#      functions in *_test.go files
#   3. every exported top-level identifier and exported method (go doc -all)
#      is named (word-boundary match) in its package document
#   4. every docs/*.md carries the required sections of its sanctioned shape
#      (DOC-SHAPES block in docs/README.md is the single source of truth)
#   5. ADR integrity: unique numbers, required metadata fields, Supersedes
#      symmetry, index coverage, Binds path existence
#   6. CANONICAL-COPY blocks are identical to their CANONICAL source
#      (modulo leading indentation)
#   7. every non-superseded ADR is referenced by number from current law
#      (docs/*.md or AGENTS.md, excluding docs/adr/)
#
# Plus one warning (never fails): relative .md links that point nowhere.
#
# This is a FLOOR check: it guarantees presence and lockstep, not quality or
# semantics. Known limits: check 3 covers single-line const/var declarations
# and methods, not grouped const members without individual doc comments, and
# a symbol only appearing in example code still counts as "documented";
# check 4 matches required words at non-letter boundaries in heading text
# (first-word aliases included), so "## Instability notes" does not satisfy
# Stability but "## 6. Determinism / Stability Contract" does; check 7 sees
# only that a reference EXISTS, not that it sits where a reader needs it —
# anchoring policy (temptation sites for rejected records) stays with
# docs/adr/README.md and review.
# Meaning stays with human/agent review.
#
# Requires: bash, grep, awk, go (for check 3). No network, no LLM.

set -u
cd "$(dirname "$0")/.."

fail=0
err()  { echo "FAIL: $*" >&2; fail=$((fail+1)); }
warn() { echo "WARN: $*" >&2; }
ok()   { echo "ok: $*"; }

# ---------------------------------------------------------------- check 1
# Index <-> files.
index_links=$(grep -oE '\]\(\./[A-Za-z0-9_/-]+\.md\)' docs/README.md | sed 's/^](\.\///; s/)$//' | sort -u)
for f in docs/*.md docs/adr/README.md; do
  rel=${f#docs/}
  [ "$rel" = "README.md" ] && continue
  if ! grep -qx "$rel" <<<"$index_links"; then
    err "check1: $f is not linked from the docs/README.md index"
  fi
done
while IFS= read -r rel; do
  [ -z "$rel" ] && continue
  [ -f "docs/$rel" ] || err "check1: index links docs/$rel, which does not exist"
done <<<"$index_links"
ok "check1: index <-> files"

# ---------------------------------------------------------------- check 2
# Backticked test names in docs exist as test functions.
testfiles=$(find internal cmd providers -name '*_test.go' 2>/dev/null)
[ -n "$testfiles" ] || err "check2: no *_test.go files found at all"
for doc in docs/*.md; do
  names=$(grep -oE '`(Test|Benchmark|Example|Fuzz)[A-Za-z0-9_]+`' "$doc" | tr -d '`' | sort -u)
  while IFS= read -r n; do
    [ -z "$n" ] && continue
    if ! grep -qE "func ${n}\(" $testfiles 2>/dev/null; then
      err "check2: $doc mentions \`$n\`, but no 'func $n(' exists in any *_test.go"
    fi
  done <<<"$names"
done
ok "check2: documented test names exist"

# ---------------------------------------------------------------- check 3
# Exported symbols appear in their package document.
doc_for_pkg() {
  case "$1" in
    metrics|admin) echo "docs/observability.md" ;;
    *)             echo "docs/$1.md" ;;
  esac
}
for d in internal/*/; do
  pkg=${d%/}; pkg=${pkg#internal/}
  doc=$(doc_for_pkg "$pkg")
  [ -f "$doc" ] || { err "check3: no document for internal/$pkg (expected $doc)"; continue; }
  syms=$(go doc -all "./internal/$pkg" 2>/dev/null | awk '
    /^(func|type|var|const) [A-Z][A-Za-z0-9_]*/ { s=$2; sub(/\(.*/, "", s); print s }
    /^func \([a-z]+ \*?[A-Z][A-Za-z0-9_]*\) [A-Z][A-Za-z0-9_]*/ {
      s=$0; sub(/^func \([a-z]+ \*?[A-Z][A-Za-z0-9_]*\) /, "", s); sub(/\(.*/, "", s); print s
    }' | sort -u)
  while IFS= read -r s; do
    [ -z "$s" ] && continue
    if ! grep -qE "(^|[^A-Za-z0-9_])${s}([^A-Za-z0-9_]|\$)" "$doc"; then
      err "check3: exported symbol $s (internal/$pkg) is not named in $doc"
    fi
  done <<<"$syms"
done
ok "check3: exported symbols named in package docs"

# ---------------------------------------------------------------- check 4
# Required sections per sanctioned shape, sourced from the DOC-SHAPES block.
shapes=$(awk '/<!-- DOC-SHAPES/{on=1; next} /-->/&&on{exit} on' docs/README.md)
[ -n "$shapes" ] || { err "check4: DOC-SHAPES block missing in docs/README.md"; shapes=""; }

shape_line() { grep -E "^$1:" <<<"$shapes" | sed "s/^$1: *//"; }
# Canonical keyword -> regex of heading alternatives (aliases included).
kw_regex() {
  local alts="$1"
  # first words of every side of every alias pair that mentions this keyword
  local pairs
  pairs=$(grep -E '^heading-aliases:' <<<"$shapes" | sed 's/^heading-aliases: *//' | tr ';' '\n')
  while IFS= read -r pair; do
    case "$pair" in
      *"$1"*)
        while IFS= read -r side; do
          side=$(echo "$side" | sed 's/^ *//; s/ *$//')
          [ -n "$side" ] && alts="$alts|${side%% *}"
        done <<<"$(echo "$pair" | sed 's/<=>/\n/g')"
        ;;
    esac
  done <<<"$pairs"
  echo "$alts"
}
# keyword list: template-concept -> heading regex (first-word match, aliases applied)
KEYWORDS="Overview Concepts API Invariants Concurrency Stability Testing Limitations Usage Behavior"
for doc in docs/*.md; do
  base=${doc#docs/}
  [ "$base" = "README.md" ] && continue
  shape="package"
  a=$(grep -E '^assignments:' <<<"$shapes" | sed 's/^assignments: *//')
  for kv in ${a//;/ }; do
    [ "${kv%%=*}" = "docs/$base" ] && shape=${kv#*=}
  done
  line=$(shape_line "$shape")
  [ -n "$line" ] || { err "check4: shape '$shape' (for $doc) is not defined in DOC-SHAPES"; continue; }
  for kw in $KEYWORDS; do
    case " $line " in
      *" $kw"*) : ;;   # keyword appears in this shape's spec -> required
      *) continue ;;
    esac
    rx=$(kw_regex "$kw")
    # Alternatives are anchored on non-letter boundaries: "## Instability
    # notes" must NOT satisfy the Stability requirement. The numeric
    # "N. " prefix used by convention is optional, so a plain "## Overview"
    # (no number) still matches.
    if ! grep -qiE "^#{1,4} +([0-9.]+ +)?(.* )?($rx)([^A-Za-z]|$)" "$doc"; then
      err "check4: $doc (shape $shape) lacks a required '$kw' section heading"
    fi
  done
done
ok "check4: required sections per sanctioned shape"

# ---------------------------------------------------------------- check 5
# ADR integrity.
adr_readme=docs/adr/README.md
[ -f "$adr_readme" ] || err "check5: $adr_readme missing"
nums=""
for f in docs/adr/[0-9][0-9][0-9][0-9]-*.md; do
  [ -e "$f" ] || continue
  n=$(basename "$f" | cut -c1-4)
  case " $nums " in *" $n "*) err "check5: duplicate ADR number $n";; esac
  nums="$nums $n"
  for field in 'Status: ' 'Date: ' 'Supersedes: ' 'Binds: '; do
    grep -q "^- $field" "$f" || err "check5: $f lacks a '- $field...' metadata line"
  done
  status=$(sed -n 's/^- Status: //p' "$f" | head -1)
  case "$status" in
    proposed|accepted|rejected) : ;;
    "superseded by ADR-"[0-9][0-9][0-9][0-9])
      sn=${status#superseded by ADR-}
      sup=$(ls docs/adr/${sn}-*.md 2>/dev/null | head -1)
      [ -n "$sup" ] || err "check5: $f is superseded by ADR-$sn, which does not exist"
      [ -n "$sup" ] && ! grep -q "^- Supersedes: ADR-$n" "$sup" \
        && err "check5: $f says superseded by ADR-$sn, but $sup does not say 'Supersedes: ADR-$n'"
      ;;
    *) err "check5: $f has an unrecognized Status: $status" ;;
  esac
  sup=$(sed -n 's/^- Supersedes: //p' "$f" | head -1)
  case "$sup" in
    none) : ;;
    ADR-[0-9][0-9][0-9][0-9])
      ls docs/adr/${sup#ADR-}-*.md >/dev/null 2>&1 || err "check5: $f supersedes $sup, which does not exist" ;;
    *) err "check5: $f has an unrecognized Supersedes: $sup" ;;
  esac
  if [ -f "$adr_readme" ] && ! grep -q "$n" "$adr_readme"; then
    err "check5: ADR-$n is not listed in $adr_readme"
  fi
  # Binds path existence: path-like tokens must resolve. (Process
  # substitution, not a pipe: err() must run in THIS shell — a piped loop is
  # a subshell and its failure count would be silently lost.)
  while IFS= read -r tok; do
    tok=${tok%,}; tok=${tok%.}
    if [ "${tok%.md}" != "$tok" ]; then
      [ -f "$tok" ] || err "check5: $f binds $tok, which does not exist"
    else
      [ -d "$tok" ] || [ -f "$tok" ] || err "check5: $f binds $tok, which does not exist"
    fi
  done < <(sed -n 's/^- Binds: //p' "$f" | grep -oE '(docs|internal|cmd|providers)/[A-Za-z0-9_/.,-]+' || true)
done
ok "check5: ADR integrity"

# ---------------------------------------------------------------- check 6
# Canonical copies match their sources (modulo leading indentation).
canon_files=$(grep -rl '<!-- CANONICAL' AGENTS.md CONTRIBUTING.md docs 2>/dev/null || true)
ids=$(grep -rhoE '<!-- CANONICAL id="[a-z0-9-]+" -->' $canon_files 2>/dev/null | sed 's/.*id="//; s/".*//' | sort -u)
dedent() { sed 's/^[[:space:]]*//'; }
block() { # file, open-regex, id
  awk -v id="$3" -v open="$2" '
    $0 ~ open && $0 ~ "id=\"" id "\"" { on=1; next }
    on && $0 ~ "<!-- /CANONICAL" && $0 ~ "id=\"" id "\"" { on=0; next }
    on' "$1"
}
for id in $ids; do
  srcfile=$(grep -l "<!-- CANONICAL id=\"$id\" -->" $canon_files 2>/dev/null | head -1)
  [ -n "$srcfile" ] || { err "check6: no CANONICAL source for id '$id'"; continue; }
  src=$(block "$srcfile" '<!-- CANONICAL id=' "$id" | dedent)
  for f in $canon_files; do
    # every copy of this id in file f
    srcattr=$(grep -oE "<!-- CANONICAL-COPY source=\"[^\"]+\" id=\"$id\" -->" "$f" 2>/dev/null | sed 's/.*source="//; s/".*//' | sort -u)
    [ -z "$srcattr" ] && continue
    [ "$srcattr" = "$srcfile" ] || err "check6: $f copies id '$id' from '$srcattr', but the CANONICAL lives in $srcfile"
    copy=$(block "$f" '<!-- CANONICAL-COPY source=' "$id" | dedent)
    [ "$copy" = "$src" ] || err "check6: CANONICAL-COPY '$id' in $f has drifted from its source $srcfile"
  done
done
ok "check6: canonical copies in lockstep"

# ---------------------------------------------------------------- check 7
# ADR back-references: every non-superseded ADR is cited by number from at
# least one current-law document (docs/*.md or AGENTS.md — never docs/adr/,
# whose index would make the check vacuous). An ADR nothing references is
# invisible to the entry-file + links reading path: the real failure mode is
# not unwritten decisions but unread ones. Superseded records are exempt —
# they stay reachable through the Supersedes chain check 5 validates.
law_files=$(ls docs/*.md AGENTS.md 2>/dev/null || true)
if [ -z "$law_files" ]; then
  err "check7: no current-law documents found (docs/*.md, AGENTS.md)"
fi
for f in docs/adr/[0-9][0-9][0-9][0-9]-*.md; do
  [ -e "$f" ] || continue
  n=$(basename "$f" | cut -c1-4)
  status=$(sed -n 's/^- Status: //p' "$f" | head -1)
  case "$status" in "superseded by ADR-"*) continue ;; esac
  # The digit guard stops a longer number satisfying a shorter one
  # (ADR-00010 must not count for ADR-0001); the boundary is otherwise
  # deliberately loose — "ADR-0001." and "(see ADR-0001)" are the normal
  # citation shapes. NB: the \$ EOL anchor relies on double-quote collapse.
  if [ -n "$law_files" ] && ! grep -qE "ADR-$n([^0-9]|\$)" $law_files; then
    err "check7: $f ($status) is not referenced from any current-law document (docs/*.md or AGENTS.md)"
  fi
done
ok "check7: ADR back-references"

# ------------------------------------------------- link warning (soft)
for doc in docs/*.md docs/adr/*.md AGENTS.md CONTRIBUTING.md README.md; do
  [ -f "$doc" ] || continue
  grep -oE '\]\(\./[^)#]+\.md(#[^)]*)?\)' "$doc" 2>/dev/null | sed 's/^](\.\///; s/#.*//; s/)$//' | sort -u | \
  while IFS= read -r rel; do
    [ -z "$rel" ] && continue
    target="$(dirname "$doc")/$rel"
    [ -e "$target" ] || warn "link: $doc links ./$rel, which does not exist"
  done
done

echo
if [ "$fail" -gt 0 ]; then
  echo "check-docs: $fail check failure(s)" >&2
  exit 1
fi
echo "check-docs: all floor checks passed"
