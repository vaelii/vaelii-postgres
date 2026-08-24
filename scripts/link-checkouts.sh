#!/usr/bin/env bash
# scripts/link-checkouts.sh [-f] [PREFIX] [SUFFIX] — wire up the dev-local
# checkout symlink so Leiningen resolves vaelii core from live source instead of
# an installed snapshot jar:
#
#   checkouts/vaelii -> ../../vaelii
#
# The adapter reaches into `vaelii.impl.io.snapshot`, a seam core is free to
# change.  A checkout is how that break surfaces the moment it lands rather than
# at somebody's next `lein install`.  A stale snapshot is the failure it avoids,
# and that failure is quiet: the jar does not fail loudly, it silently lacks
# whatever the seam grew since it was built.
#
# checkouts/ is gitignored, so the link is not committed — rerun after a fresh
# clone.  Idempotent (ln -snf).  A missing target is skipped with a WARN unless
# -f / --force is given (a dangling link is harmless and resolves once ../vaelii
# is cloned).
#
# PREFIX / SUFFIX wrap the *target repo dir name* so a modified core clone can be
# linked while the checkout name stays fixed (Leiningen matches a checkout by the
# project.clj it finds, not by the directory name):
# `link-checkouts.sh "" -wip` points checkouts/vaelii at ../../vaelii-wip.
set -euo pipefail
cd "$(dirname "$0")/.."   # repo root
mkdir -p checkouts

force=0
while [[ "${1:-}" == -* ]]; do
  case "$1" in
    -f|--force) force=1; shift ;;
    -h|--help)  sed -n '2,20p' "$0"; exit 0 ;;
    --) shift; break ;;
    *)  echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

prefix="${1:-}"
suffix="${2:-}"

# $1 = checkout name (fixed — Leiningen resolves the artifact against it)
# $2 = repo dir name (prefix/suffix applied here)
link() {
  local name="$1" repo="$2"
  local target="../../${prefix}${repo}${suffix}"
  # target is relative to checkouts/, so test it from there.
  if [[ $force -eq 0 && ! -e "checkouts/$target" ]]; then
    echo "WARN: skipping checkouts/$name -> $target (target missing; -f to link anyway)" >&2
    return
  fi
  ln -snf "$target" "checkouts/$name"
  echo "linked checkouts/$name -> $target"
}

link vaelii vaelii
