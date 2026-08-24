#!/usr/bin/env bash
# scripts/update-badges.sh — single source of truth for the README badge row.
#
# Trimmed port of vaelii core's scripts/update-badges.sh. Measures a few
# heuristics from the tree, then regenerates the ENTIRE badge block between the
# `<!-- badges:start ... -->` / `<!-- badges:end -->` markers in README.md. Each
# badge SVG is rendered LOCALLY (no network — the Verdana advance-width table is
# baked in) and committed under .github/badges/; the README references the local
# files. Colors are a perceptually-even OKLCH rainbow: N hues spaced evenly around
# the OKLCH circle at a fixed, bright lightness, so adjacent badges are equally
# distinguishable and black text reads cleanly on every one.
#
# Badges (left to right): license | release | tests | loc | tables | docstrings
#   - license / release: static, read from project.clj (:license name + version).
#   - tests: deftest count, measured from the test tree.
#   - loc: source lines, measured from src.
#   - tables: CREATE TABLE statements in the adapter's schema DDL.
#   - docstrings: % of top-level defns carrying a docstring — the one
#     code-quality figure surfaced as a badge.
#
# A full scorecard prints to stderr; the README rewrite is the main act.
# Repo identity (GH slug, version, license) is derived from the git remote +
# project.clj, so this file is byte-identical across the sibling adapters.
#
# Usage:
#   scripts/update-badges.sh            # measure + rewrite the badge block
#   scripts/update-badges.sh --dry-run  # measure + print, leave README alone
set -euo pipefail

# One level up: this lives in scripts/, beside every other script here.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DRY=0
for a in "$@"; do
  case "$a" in
    --dry-run|-n) DRY=1 ;;
  esac
done

# ---- CONFIG ----
# Perceptually-even OKLCH rainbow. The N badge colors are N hues spaced evenly
# around the OKLCH hue circle at fixed lightness + chroma. Lightness is kept
# high/bright so dark text reads on every badge (>= ~0.78 keeps them all dark).
OKLCH_L=0.82            # lightness 0..1 (higher = brighter; >= 0.78 => dark text)
OKLCH_C=0.12            # chroma (higher = more vivid; auto-clamped to sRGB gamut)

# Print N hexes (no #), evenly spaced around the OKLCH hue circle at OKLCH_L /
# OKLCH_C: OKLCH -> OKLab -> linear sRGB -> gamma sRGB, clamped to gamut.
rainbow_palette() {
  perl -e '
    my ($N,$L,$C)=@ARGV;
    sub g { my $x=shift; $x=$x<=0.0031308?12.92*$x:1.055*($x**(1/2.4))-0.055; $x<0?0:($x>1?1:$x) }
    for my $i (0..$N-1) {
      my $H=6.28318530718*$i/$N; my $a=$C*cos($H); my $b=$C*sin($H);
      my $l=($L+0.3963377774*$a+0.2158037573*$b)**3;
      my $m=($L-0.1055613458*$a-0.0638541728*$b)**3;
      my $s=($L-0.0894841775*$a-1.2914855480*$b)**3;
      my $R= 4.0767416621*$l-3.3077115913*$m+0.2309699292*$s;
      my $G=-1.2684380046*$l+2.6097574011*$m-0.3413193965*$s;
      my $B=-0.0041960863*$l-0.7034186147*$m+1.7076147010*$s;
      printf "%02x%02x%02x ", int(g($R)*255+0.5), int(g($G)*255+0.5), int(g($B)*255+0.5);
    }
  ' "$1" "$OKLCH_L" "$OKLCH_C"
}

# File links are RELATIVE repo paths (org-agnostic; render on GitHub wherever the
# repo lives). GH is only used for the one link that can't be a file: the
# releases page, and it is the PUBLISHED org rather than whatever remote this
# clone happens to carry. Derived from the remote, the badge pointed wherever the
# checkout sat — so regenerating in a private dev tree rewrote the release badge
# to a URL the public cannot open, and the carve's internal-name scan caught it.
# Where a project is published is a fact about the project, not a property of a
# checkout. Same spelling as core's and the plugin's scripts/update-badges.sh.
REPO_NAME=$(basename "$ROOT")
GH="vaelii/$REPO_NAME"

# Static badge values: license + version, read from project.clj.
VERSION=$(grep -m1 -E '^\(defproject' project.clj | grep -oE '"[^"]+"' | head -1 | tr -d '"' || true)
LICENSE_NAME=$(grep -m1 -A1 -E ':license' project.clj | grep -oE ':name[[:space:]]*"[^"]+"' | grep -oE '"[^"]+"' | tr -d '"' || true)
[[ -z "$VERSION" ]] && VERSION="dev"
[[ -z "$LICENSE_NAME" && -f LICENSE ]] && grep -qi 'apache license' LICENSE && LICENSE_NAME="Apache-2.0"
[[ -z "$LICENSE_NAME" ]] && LICENSE_NAME="see LICENSE"

# Self-hosted badge SVGs, rendered by make_badge below.
BADGES_DIR=".github/badges"
MSG_TEXT=000            # message text color (true black on the bright OKLCH color)
LABEL_BG=2b2b2b        # darkened key (label) background; white label text on it
BADGE_SCALE=1.2        # enlarge factor (1 = the native ~20px tall; 1.2 ~= 24px)

MAIN='src'; TEST='test'; DOCS='docs'
clj=(--include='*.clj')

# The adapter's schema file — the docstrings + tables badges link to it. One
# snapshot.clj under src/vaelii/<db>/; derived so this stays shareable.
SNAPSHOT_CLJ=$(find "$MAIN" -name 'snapshot.clj' | head -1)
[[ -z "$SNAPSHOT_CLJ" ]] && SNAPSHOT_CLJ="$MAIN"

# count lines matching a pattern, tolerating zero matches (grep exits 1).
count() { local n; n=$("$@" | wc -l | tr -d ' '); echo "${n:-0}"; }
g() { grep "$@" || true; }

# Render a badge SVG to $1. Args: outfile label message color
#
# Layout is the classic two-part badge: a dark key (label) box and a bright
# message box, each sized to its text plus 5px of padding either side. Text
# advance widths come from the baked-in Verdana 11px table, so this needs neither
# the network nor a local copy of the font; every <text> also carries textLength,
# so the glyph run is painted at exactly the width the box was sized from.
make_badge() {
  perl - "$1" "$2" "$3" "$4" "$BADGE_SCALE" "$MSG_TEXT" "$LABEL_BG" <<'PERL'
use strict; use warnings;
my ($out,$label,$msg,$color,$scale,$msgtext,$labelbg) = @ARGV;

# Verdana 11px advance widths in hundredths of a px, printable ASCII 32..126.
my @W = split ' ', <<'TBL';
387 433 505 900 699 1184 799 295 500 500 699 900 400 500 400 500 699
699 699 699 699 699 699 699 699 699 500 500 900 900 900 600 1100 752
754 768 848 696 632 853 827 463 500 762 612 927 823 866 663 866 765
752 678 805 752 1088 754 677 754 500 500 500 900 699 699 661 685 573
685 655 387 685 696 302 379 651 302 1070 696 668 685 685 469 573 433
696 651 900 651 651 578 698 500 698 900
TBL
sub tw {                        # text width in whole px (0 for empty)
  my $s = shift; return 0 unless length $s;
  my $t = 0;
  for my $c (split //, $s) {
    my $o = ord $c;
    $t += ($o >= 32 && $o <= 126) ? $W[$o - 32] : $W[ord('m') - 32];
  }
  return int($t / 100 + 0.5);
}
sub esc { my $s = shift; $s =~ s/&/&amp;/g; $s =~ s/</&lt;/g; $s =~ s/>/&gt;/g; $s =~ s/"/&quot;/g; $s }

my $PAD = 5;
my $ltw = tw($label);   my $mtw = tw($msg);
my $lw  = $ltw ? $ltw + 2*$PAD : 0;
my $mw  = $mtw + 2*$PAD;
my $w   = $lw + $mw;
# Text centres. A two-part badge nudges the label +1px and the message -1px off
# the geometric centre of its box, which keeps the pair optically balanced across
# the divider; a label-less badge takes no nudge.
my $lx  = ($lw/2 + 1) * 10;
my $mx  = ($lw + $mw/2 + ($lw ? -1 : 0)) * 10;
my $aria = $ltw ? esc($label).": ".esc($msg) : esc($msg);
my ($el,$em) = (esc($label), esc($msg));

# One text run: a blurred shadow, a flat shadow, then the glyphs themselves.
sub run {
  my ($x,$tl,$txt,$shadow,$fill) = @_;
  sprintf('<g transform="scale(.1)"><g aria-hidden="true" fill="%s">'
        . '<text x="%d" y="150" fill-opacity=".8" filter="url(#blur)" textLength="%d">%s</text>'
        . '<text x="%d" y="150" fill-opacity=".3" textLength="%d">%s</text></g>'
        . '<text x="%d" y="140" textLength="%d"%s>%s</text></g>',
        $shadow, $x, $tl, $txt, $x, $tl, $txt, $x, $tl, $fill, $txt);
}
my $body = "";
$body .= run($lx, $ltw*10, $el, '#010101', '')                  if $ltw;
$body .= run($mx, $mtw*10, $em, '#ccc',    qq{ fill="#$msgtext"});

my $svg = sprintf(
  '<svg xmlns="http://www.w3.org/2000/svg" width="%.0f" height="%.0f" viewBox="0 0 %d 20" role="img" aria-label="%s">'
. '<title>%s</title><filter id="blur"><feGaussianBlur stdDeviation="16"/></filter>'
. '<linearGradient id="s" x2="0" y2="100%%"><stop offset="0" stop-color="#bbb" stop-opacity=".1"/><stop offset="1" stop-opacity=".1"/></linearGradient>'
. '<clipPath id="r"><rect width="%d" height="20" rx="3"/></clipPath>'
. '<g clip-path="url(#r)"><rect width="%d" height="20" fill="#%s"/><rect x="%d" width="%d" height="20" fill="#%s"/>'
. '<rect width="%d" height="20" fill="url(#s)"/></g>'
. '<g fill="#fff" text-anchor="middle" font-family="Verdana,Geneva,DejaVu Sans,sans-serif" text-rendering="geometricPrecision" font-size="110">%s</g></svg>',
  $w*$scale, 20*$scale, $w, $aria, $aria, $w,
  $lw, ($ltw ? $labelbg : $color), $lw, $mw, $color, $w, $body);

open my $fh, '>', $out or die "$out: $!"; print $fh $svg; close $fh;
PERL
}

# ---- gather raw counts ----
loc_src=$(find "$MAIN" -name '*.clj' -exec cat {} + | wc -l | tr -d ' ')
loc_test=0; [[ -d "$TEST" ]] && loc_test=$(find "$TEST" -name '*.clj' -exec cat {} + | wc -l | tr -d ' ')
loc_doc=0;  [[ -d "$DOCS" ]] && loc_doc=$(find "$DOCS" -name '*.md' -exec cat {} + | wc -l | tr -d ' ')

defns=$(count g -rhE '^\(defn ' "$MAIN" "${clj[@]}")
docd=$({ grep -rhEA1 '^\(defn ' "$MAIN" "${clj[@]}" || true; } | { grep -cE '^\s+"' || true; } | tr -d ' ')
snake=$(count g -rhE '^\(defn?-? [a-z]*_' "$MAIN" "${clj[@]}")
commented=$(count g -rhE '^[[:space:]]*;;+[[:space:]]*\([a-z*!?-][^`—]*\)[[:space:]]*$' "$MAIN" "${clj[@]}")

tests=$(count g -rhoE '\(deftest[[:space:]]' "$TEST" "${clj[@]}")
tables=$({ grep -rhoiE 'create table( if not exists)? [a-z_]+' "$MAIN" "${clj[@]}" || true; } | sort -u | wc -l | tr -d ' ')
loc_fmt=$(awk -v n="$loc_src" 'BEGIN{ if (n>=9950) printf "%.0fk", n/1000; else if (n>=1000) printf "%.1fk", n/1000; else printf "%d", n }')

# ---- docstring coverage + scorecard (one awk pass); stdout = docstring % ----
docstrings=$(awk -v loc_src="$loc_src" -v loc_test="$loc_test" -v loc_doc="$loc_doc" \
  -v defns="$defns" -v docd="$docd" -v snake="$snake" -v commented="$commented" \
  -v repo="$REPO_NAME" '
  BEGIN{
    doc_cov    = defns>0    ? 100.0*docd/defns        : 0
    test_ratio = loc_src>0  ? loc_test/loc_src         : 0
    doc_ratio  = loc_src>0  ? loc_doc/loc_src          : 0
    cm_per1k   = loc_src>0  ? 1000.0*commented/loc_src : 0

    bar="------------------------------------------------------------"
    printf "\n  %s BADGE SCORECARD\n  %s\n", toupper(repo), bar > "/dev/stderr"
    printf "  source %d loc | tests %d loc | docs %d loc\n\n", loc_src, loc_test, loc_doc > "/dev/stderr"

    printf "  CODE QUALITY  (docstring coverage is the badge; rest are diagnostics)\n" > "/dev/stderr"
    printf "    docstring coverage          %7.1f%%   (badge)\n", doc_cov > "/dev/stderr"
    printf "    test:source ratio           %7.2fx\n", test_ratio > "/dev/stderr"
    printf "    doc:source ratio            %7.2fx\n", doc_ratio > "/dev/stderr"
    printf "    naming               %5d snake_case defns\n", snake > "/dev/stderr"
    printf "    commented-out code          %5.2f/1k src lines\n", cm_per1k > "/dev/stderr"
    printf "  %s\n\n", bar > "/dev/stderr"

    printf "%.0f\n", doc_cov   # stdout: docstring coverage %
  }')

# ---- the badge row (script is source of truth) ----
keys=(license release tests loc tables docstrings)
msgs=("$LICENSE_NAME" "v$VERSION" "$tests" "$loc_fmt" "$tables" "${docstrings}%")
links=(
  "LICENSE"                           # license    -> the license file
  "https://github.com/${GH}/releases" # release    -> releases page (only absolute repo link)
  "test"                              # tests      -> the test tree
  "src"                               # loc        -> the source tree
  "$SNAPSHOT_CLJ"                     # tables     -> the schema DDL
  "$SNAPSHOT_CLJ"                     # docstrings -> the docstring-rich adapter
)

# Sized from `keys`, so adding/dropping a badge is one edit: the palette comes up
# short and `set -u` aborts if a hand-count fell out of sync.
read -r -a RAINBOW <<< "$(rainbow_palette "${#keys[@]}")"

[[ "$DRY" == 0 ]] && mkdir -p "$BADGES_DIR"
BLOCK=""
for i in "${!keys[@]}"; do
  slug=${keys[$i]// /-}
  file="$BADGES_DIR/$slug.svg"
  [[ "$DRY" == 0 ]] && make_badge "$file" "${keys[$i]}" "${msgs[$i]}" "${RAINBOW[$i]}"
  BLOCK+="[![${keys[$i]}](${file})](${links[$i]})"$'\n'
done

{ echo "  link targets:"
  echo "    tables     -> $SNAPSHOT_CLJ"
  echo "    docstrings -> $SNAPSHOT_CLJ"; } >&2

if [[ "$DRY" == 1 ]]; then
  printf '%s' "$BLOCK" >&2
  echo "  (dry run: README not modified)" >&2
else
  # First run: plant the marker pair right under the H1.
  if ! grep -q '<!-- badges:start' README.md; then
    perl -i -0pe 's/\A(#[^\n]*\n)\n*/$1\n<!-- badges:start: regenerated by scripts\/update-badges.sh (do not hand-edit) -->\n<!-- badges:end -->\n\n/s' README.md
  fi
  BLOCK="$BLOCK" perl -i -0pe '
    my $b = $ENV{BLOCK};
    s/(<!-- badges:start.*?-->\n).*?(<!-- badges:end -->)/$1$b$2/s
      or die "badge markers not found in README.md (expected <!-- badges:start --> ... <!-- badges:end -->)\n";
  ' README.md
  echo "  README badges regenerated: ${#keys[@]} badges | docstrings ${docstrings}%" >&2
fi
