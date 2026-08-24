#!/usr/bin/env bash
# scripts/lint-shellcheck.sh — shellcheck over this repo's shell scripts.
#
# THE ROSTER LIVES HERE AND ONLY HERE.  `lein lint` (via scripts/lint.sh) and the
# granular `lein lint-shellcheck` both run this file, so the list cannot say one
# thing to one caller and another to the other.  A roster stated twice is one that
# drifts, and the drift is silent: the entry point reading the shorter list
# reports the same clean row over fewer scripts.
#
# Named one by one rather than globbed, so adding a script is a decision — and so
# the list has to be kept.  A script missing from it is one nothing checks, which
# is how a `mktemp -t` form that works on BSD and dies on GNU coreutils reaches a
# runner.  Add the file here in the same commit that adds the file; the
# completeness check below fails until you do.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT" || exit 1

SCRIPTS=(
  scripts/lint.sh
  scripts/lint-shellcheck.sh
  scripts/check-reflection.sh
  scripts/link-checkouts.sh
  scripts/update-badges.sh
)

if ! command -v shellcheck >/dev/null 2>&1; then
  echo "shellcheck not found on PATH." >&2
  echo "Install: brew install shellcheck (macOS), or your distro's shellcheck package" >&2
  exit 127
fi

# Both directions, because each fails silently on its own.  A roster naming a
# script the tree no longer has leaves shellcheck reading a shorter list; a script
# the tree has and the roster does not is the one nothing checks.  Neither shows
# up in shellcheck's own exit status, which is why they are asked here.
drift=0

for f in "${SCRIPTS[@]}"; do
  [[ -f "$f" ]] && continue
  echo "lint-shellcheck: $f is in the roster and not in the tree — drop it here." >&2
  drift=1
done

# Every shell script under scripts/ must be in the roster.  A new one added is a
# decision to state here rather than one to infer.
while IFS= read -r f; do
  printf '%s\n' "${SCRIPTS[@]}" | grep -qxF "$f" && continue
  echo "lint-shellcheck: $f is a shell script nothing checks — add it to the roster in $0." >&2
  drift=1
done < <(find scripts -name '*.sh' | sort)

(( drift == 0 )) || exit 1

shellcheck "${SCRIPTS[@]}"
