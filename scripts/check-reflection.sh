#!/usr/bin/env bash
# scripts/check-reflection.sh — a reflection warning fails the build instead of scrolling past.
#
# `:global-vars {*warn-on-reflection* true}` is on in project.clj, but the flag
# only WARNS — nothing greps the output, so a warning prints on every run and sits
# unnoticed.  This is the grep.
#
# WHAT IT COVERS.  `lein check` compiles the namespaces on the source path, so
# this pass is **`src`** — the adapter's one namespace (vaelii.postgres.snapshot),
# which requires the engine's snapshot protocol (vaelii.impl.io.snapshot).  Compiling
# it loads core SOURCE through the checkouts/vaelii symlink
# (scripts/link-checkouts.sh) under the same flag, so a reflection leak in the
# code paths the adapter touches surfaces here too.  The test tree is not compiled
# here (that would need a live-server gate to mean anything); `lein test` compiles
# it under the same flag when a server is configured.
#
#   scripts/check-reflection.sh                  # compile src, fail on any warning
#   REFLECTION_LOG=path/to.log scripts/…         # lint a captured log, compiling nothing
#
# The second form lets this script have a test: feed it a log with a known warning
# and it must exit 1.
set -uo pipefail

# The three the compiler emits under the flag.  Auto-boxing and the primitive-recur
# note are not literally reflection, but they are the same class of silent cost and
# the same fix — a hint at the call site.
readonly PATTERN='Reflection warning|Auto-boxing|recur arg for primitive'

# Third-party sources only.  A dependency whose current release still reflects is a
# fact about that dependency; ours is a defect, fixable at the call site with a type
# hint.  Adding one of our own files here is the thing this script exists to stop.
# Entries are matched as fixed strings against the warning line.
readonly -a ALLOW=()

log=""
# Inline rather than a `cleanup` function: a function only a trap invokes reads as
# uncalled to shellcheck (SC2329).  Single quotes, so `$log` is the value at exit.
trap '[[ -n "$log" && -z "${REFLECTION_LOG:-}" ]] && rm -f "$log"' EXIT

if [[ -n "${REFLECTION_LOG:-}" ]]; then
  log="$REFLECTION_LOG"
  if [[ ! -r "$log" ]]; then
    echo "check-reflection: cannot read REFLECTION_LOG=$log" >&2
    exit 2
  fi
else
  # The `.XXXXXX` is mandatory: BSD `mktemp -t` takes a PREFIX and appends its own
  # randomness, GNU coreutils takes a TEMPLATE and rejects fewer than three X's.  A
  # bare prefix works on macOS and dies on every Linux runner.  `set -e` is off, so
  # a failed mktemp would leave `$log` empty and every line below would report on a
  # file that does not exist — check it rather than inherit that.
  log="$(mktemp -t vaelii-reflect.XXXXXX)" || log=""
  if [[ -z "$log" ]]; then
    echo "check-reflection: mktemp failed; no scratch file to compile into" >&2
    exit 2
  fi
  if ! lein check >"$log" 2>&1; then
    echo "check-reflection: compilation failed — the warnings below are incidental" >&2
    tail -40 "$log" >&2
    exit 2
  fi
fi

hits="$(grep -E "$PATTERN" "$log" || true)"

if [[ -n "$hits" && ${#ALLOW[@]} -gt 0 ]]; then
  for pat in "${ALLOW[@]}"; do
    hits="$(printf '%s\n' "$hits" | grep -vF "$pat" || true)"
  done
fi

if [[ -z "$hits" ]]; then
  echo "no reflection warnings (src)"
  exit 0
fi

printf '%s\n' "$hits"
printf '\n%s reflection/boxing warning(s) — hint the call site.\n' \
       "$(printf '%s\n' "$hits" | grep -c .)" >&2
exit 1
