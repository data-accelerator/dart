#!/usr/bin/env bash
# report-docs-advisories.sh — upsert ONE sticky PR comment carrying the
# advisory (non-blocking) notes from a check-docs.sh run.
#
# Best-effort by design: this is reporting, never gating. It always exits 0,
# tolerates a missing notes file, an absent PR number (push to main), and a
# read-only token (fork PRs) — in the last case the notes still sit in the
# job log, which is where a fork contributor sees them anyway.
#
# The comment is idempotent: an existing comment carrying the marker is
# edited in place rather than duplicated, so the PR thread gets a single
# living note instead of one comment per push.
#
# Usage: report-docs-advisories.sh <check-docs-output-file> <pr-number>
# Env:   GITHUB_REPOSITORY (owner/repo), GH_TOKEN.
#
# Requires: bash, grep, gh. No network beyond the GitHub API.

set -u
out=${1:-check-docs.out}
pr=${2:-}
marker='<!-- check-docs-advisory -->'

[ -n "$pr" ] || { echo "report-advisories: no PR number; skipping"; exit 0; }
[ -f "$out" ] || { echo "report-advisories: $out not found; skipping"; exit 0; }

notes=$(grep '^ADVISORY' "$out" || true)
fence='```'
if [ -n "$notes" ]; then
  body="$marker
**Documentation advisory notes** — these did not fail CI. Each needs a disposition in this PR thread: fixed, or waived with a stated reason (see docs/README.md, Enforcement tiers).

$fence
$notes
$fence"
else
  body="$marker
Documentation advisory notes: none open. ✅"
fi

repo=${GITHUB_REPOSITORY:-}
if [ -z "$repo" ]; then
  # Local dry-run: show what would be posted.
  echo "report-advisories: GITHUB_REPOSITORY unset; would post:"
  printf '%s\n' "$body"
  exit 0
fi

# Find a prior sticky comment by the FULL marker literal, so a user comment
# merely quoting the phrase is never mistaken for the bot's own comment.
existing=$(gh api "repos/$repo/issues/$pr/comments" --paginate \
  --jq '.[] | select(.body | contains("<!-- check-docs-advisory -->")) | .id' 2>/dev/null | head -1 || true)

if [ -n "$existing" ]; then
  gh api "repos/$repo/issues/comments/$existing" -X PATCH -f body="$body" >/dev/null 2>&1 \
    || echo "report-advisories: could not update comment (read-only token?); notes are in the job log"
elif [ -n "$notes" ]; then
  gh api "repos/$repo/issues/$pr/comments" -f body="$body" >/dev/null 2>&1 \
    || echo "report-advisories: could not post comment (read-only token?); notes are in the job log"
else
  echo "report-advisories: no notes and no prior comment; nothing to do"
fi
exit 0
