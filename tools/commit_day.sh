#!/usr/bin/env bash
# Make ONE real commit for a roadmap day (1-10). Reads tools/days.txt.
#
#   tools/commit_day.sh 1            # stage Day 1 files and commit
#   tools/commit_day.sh 2 --push     # also push to 'origin' if configured
#
# Safety rules (on purpose, no backdating, no empty commits):
#   * refuses if nothing is staged for that day
#   * refuses to commit days out of order
#   * refuses a second commit on the same calendar day unless --allow-same-day
#   * uses the real current time; it never sets GIT_AUTHOR_DATE / GIT_COMMITTER_DATE
set -euo pipefail
cd "$(dirname "$0")/.."

DAY="${1:-}"; shift || true
ALLOW_SAME_DAY=0; PUSH=0
for a in "$@"; do
  case "$a" in
    --allow-same-day) ALLOW_SAME_DAY=1 ;;
    --push) PUSH=1 ;;
    *) echo "unknown option: $a" >&2; exit 2 ;;
  esac
done
[[ "$DAY" =~ ^([1-9]|10)$ ]] || { echo "usage: tools/commit_day.sh <1-10> [--allow-same-day] [--push]" >&2; exit 2; }

[ -d .git ] || git init -q -b main
git config user.name  >/dev/null || { echo "Set your identity first: git config user.name \"Your Name\"" >&2; exit 1; }
git config user.email >/dev/null || { echo "Set your identity first: git config user.email you@example.com" >&2; exit 1; }

LINE="$(grep -E "^${DAY}\|" tools/days.txt)"
MSG="$(cut -d'|' -f2 <<<"$LINE")"
FILES="$(cut -d'|' -f3 <<<"$LINE")"

DONE=0; git rev-parse HEAD >/dev/null 2>&1 && DONE="$(git rev-list --count HEAD)"
if [ "$DONE" -ne $((DAY - 1)) ]; then
  echo "Day $DAY must be commit number $DAY, but the repository has $DONE commit(s). Commit days in order." >&2; exit 1
fi
if [ "$DONE" -gt 0 ] && [ "$ALLOW_SAME_DAY" -eq 0 ]; then
  LAST="$(git log -1 --format=%cd --date=short)"
  if [ "$LAST" = "$(date +%F)" ]; then
    echo "The previous commit was made today ($LAST). Run this on a different day, or pass --allow-same-day." >&2; exit 1
  fi
fi

for f in $FILES; do [ -e "$f" ] || { echo "missing file for Day $DAY: $f" >&2; exit 1; }; done
git add -- $FILES
if git diff --cached --quiet; then echo "Nothing staged for Day $DAY - refusing to create an empty commit." >&2; exit 1; fi

git diff --cached --stat
git commit -q -m "$MSG"
echo "Committed Day $DAY: $MSG"
git log -1 --format='  %h  %ad' --date=iso
if [ "$PUSH" -eq 1 ]; then
  if git remote get-url origin >/dev/null 2>&1; then git push -u origin HEAD; else echo "No 'origin' remote configured; skipping push." >&2; fi
fi
