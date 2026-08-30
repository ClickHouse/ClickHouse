#!/usr/bin/env bash
set -euo pipefail

# Continuously advance every open pull request that involves you - the ones you
# authored, the ones assigned to you, and the ones you've contributed to
# (commented on or reviewed) - by fanning out a pool of workers, each running
# the `/continue-pr-auto` skill in its own git worktree. By default all three
# categories are selected; restrict with --mine / --assigned / --related (which
# may be combined). PRs carrying the `hold` label are always skipped.
#
# Instead of statically splitting the PRs across shards, the main process keeps
# a shared work queue of all of those PRs and hands the next PR to whichever
# worker becomes free first, so the work is distributed evenly regardless of how
# long each PR takes.
#
# Output is intentionally terse: one line when a PR is assigned to a worker, and
# when it finishes a status line plus a one-to-two sentence summary of what was
# done. The finish status is one of:
#   PUSHED          - the worker pushed new commits (the PR head advanced)
#   MERGED / CLOSED - the PR's state changed
#   NO-CHANGE       - clean run, nothing pushed (e.g. already green / nothing to do)
#   NEEDS-ATTENTION - clean run, nothing pushed, but still CONFLICTING: needs a
#                     human decision (resolve a huge conflict, or close as obsolete)
#   FAILED / TIMEOUT - the worker errored or hit the per-PR timeout
# A clean agent exit does not by itself mean progress, so the status is based
# on whether the PR head advanced rather than just the exit code.
#
# The per-PR lines are colored with a 24-bit (truecolor) color derived from a
# hash of the PR number, using the same YCbCr scheme ClickHouse uses to color
# its log messages (see base/base/terminalColors.cpp). The full `/continue-pr-auto`
# transcript of each PR is written to a per-PR log file under
# tmp/continue-all-prs/ instead of the terminal.
#
# Usage:
#   utils/continue-all-prs.sh [--workers N] [options]
#
# Two-model example:
#   utils/continue-all-prs.sh --agent codex \
#       --triage-model gpt-5.6-luna --model gpt-5.6-sol
#
# Options:
#   --workers N           Number of parallel workers / worktrees (default: 1).
#   --mine                Select PRs you authored.
#   --assigned            Select PRs assigned to you.
#   --related             Select PRs you've contributed to (commented/reviewed),
#                         but only ones that look abandoned - no activity by
#                         anyone but you in the last week (RELATED_STALE_DAYS) -
#                         and whose author is not listed in exclude-authors.txt
#                         (next to this script). Authors there are still updated
#                         via --mine (your own PRs) and --assigned.
#                         --mine/--assigned/--related are combinable; if none is
#                         given, all three are selected.
#   --worktree-base PATH  Base path for worker worktrees; worker i lives at
#                         "<PATH>-<i>". Default: "<main-repo>-prworker".
#   --timeout SECONDS     Total time budget per PR, shared across all
#                         continuation turns (default: 7200). Raised because a
#                         verify-build after merging master can be slow.
#   --ccache-dir PATH     Shared ccache directory for all workers (default:
#                         existing $CCACHE_DIR, else ~/.cache/ccache). A warm
#                         shared cache keeps post-merge rebuilds fast.
#   --ccache-size SIZE    ccache max size via CCACHE_MAXSIZE (default: 200G).
#   --agent AGENT         Agent CLI for each worker: claude (default) or codex.
#   --model MODEL         Model for each worker. By default, use the selected
#                         agent's configured default. Codex accepts Codex and
#                         general-purpose GPT models. For recognized GPT models,
#                         API cost is estimated from input, cached-input and
#                         output token usage using the model's published rates.
#   --triage-model MODEL  Enable two-model mode. MODEL performs the initial PR
#                         check and can perform a clean base-branch merge. If a
#                         conflict or code change is needed, it hands
#                         a task description to the coding model selected by
#                         --model. Requires --model and `bwrap`.
#   --effort LEVEL        Reasoning effort for each worker; default: medium.
#                         Passed as `--effort` to `claude` and as the
#                         `model_reasoning_effort` setting to `codex`.
#   --api-key KEY         Use a custom API key for the workers. `claude` reads
#                         `ANTHROPIC_API_KEY`; `codex` logs into a worker-local
#                         `CODEX_HOME`. NOTE: visible in `ps`
#                         while running; prefer --api-key-file.
#   --api-key-file FILE   Read the custom API key from FILE (not shown
#                         in `ps`). Default: whatever API key or login the
#                         selected agent already uses.
#   --no-status           Disable the persistent bottom status bar (two lines:
#                         elapsed, rounds, ok/fail counts, cost and token totals,
#                         plus the list of PR numbers needing attention). The bar
#                         is shown by default on a TTY.
#   --max-continue N      Max agent turns per PR (per model in two-model mode).
#                         The worker runs once and is then resumed (same session)
#                         until it signals it is done or this cap is hit
#                         (default: 4). The worker is told not to run anything in
#                         the background and to push before ending each turn, so
#                         prepared changes don't get stranded unpushed.
#   --once                Process every open PR once and exit, instead of
#                         looping forever in rounds.
#   --skip-submodules     Create worker worktrees without hardlinking submodules
#                         (faster; only safe if `/continue-pr-auto` won't build).
#   --color WHEN          auto (default) | always | never.
#   --dry-run             Don't touch worktrees or run `/continue-pr-auto`; just list
#                         the PRs and simulate processing to preview the
#                         distribution and coloring.
#   --help                Show this help.
#
# Truecolor in screen/tmux:
#   The per-PR colors use 24-bit SGR sequences ("\033[38;2;R;G;Bm"). For these to
#   show up correctly:
#     * Your outer terminal must support truecolor (most modern ones do;
#       COLORTERM=truecolor or 24bit indicates support).
#     * GNU screen >= 4.7.0: add "truecolor on" to ~/.screenrc (and use a
#       256-color TERM, e.g. "term screen-256color"). Older screen collapses
#       truecolor to the nearest 256-color cell or drops it.
#     * tmux: add `set -as terminal-features ',*:RGB'` (tmux >= 3.2) or
#       `set -ga terminal-overrides ',*:Tc'` to ~/.tmux.conf.
#   If colors look wrong, run with --color never.
#
# Advanced/testing:
#   Set CONTINUE_ALL_PRS_PRS_FILE=<file> to read the PR list (lines of
#   "<number>\t<title>") from a file instead of querying GitHub.
#   Set CONTINUE_ALL_PRS_MIN_FREE_GB=<integer> to override the minimum free
#   disk space maintained by worktree cleanup (default: 100).

REPO="ClickHouse/ClickHouse"

# The Python terminal renderer that draws the status bar (sits next to this script).
STATUS_RENDERER="${BASH_SOURCE[0]%/*}/continue-all-prs-status.py"

# Authors whose PRs are excluded from --related updates (one login per line;
# blank/`#` lines ignored). Sits next to this script.
EXCLUDE_AUTHORS_FILE="${BASH_SOURCE[0]%/*}/exclude-authors.txt"
declare -A EXCLUDED_AUTHOR

# Bright magenta for the orchestrator's own messages, to distinguish from the
# per-PR (hash-colored) lines.
S=$'\033[1;35m'
R=$'\033[0m'
RESET=$'\033[0m'

WORKERS=1
WORKTREE_BASE=""
TIMEOUT=7200
MAX_CONTINUE=4
RELATED_STALE_DAYS=7   # a "related" PR is processed only if nobody but me has acted within this many days
ONCE=0
SKIP_SUBMODULES=0
COLOR_WHEN="auto"
DRY_RUN=0
CCACHE_DIR_OPT=""      # shared ccache dir for all workers (default: existing $CCACHE_DIR, else ~/.cache/ccache)
CCACHE_SIZE="200G"     # ccache max size, applied via CCACHE_MAXSIZE env (not persisted to ccache.conf)
EFFORT="medium"        # reasoning effort passed to each worker
AGENT="claude"         # agent CLI used by workers: claude or codex
MODEL=""               # model passed to the selected agent (empty -> its configured default)
TRIAGE_MODEL=""        # optional first-pass model; MODEL becomes the coding model
SHOW_STATUS=1          # show the persistent bottom status bar (TTY only; --no-status disables)
API_KEY=""             # custom provider API key for worker processes (--api-key)
API_KEY_FILE=""        # ...or read it from this file (safer: not visible in `ps`)
API_KEY_PROVIDED=0      # whether either custom-key option was supplied
MIN_FREE_GB="${CONTINUE_ALL_PRS_MIN_FREE_GB:-100}"

# PR selection modes (combinable). If none are given, all are enabled.
MODE_MINE=0       # PRs I authored
MODE_ASSIGNED=0   # PRs assigned to me
MODE_RELATED=0    # PRs I've contributed to (commented on or reviewed)
MODE_ANY=0        # whether any mode was explicitly requested

usage()
{
    # Print the leading comment header (the first contiguous block of comment
    # lines after the shebang/set), stripping the leading "# ".
    awk 'NR > 2 { if ($0 ~ /^#/) { started = 1; sub(/^# ?/, ""); print } else if (started) exit }' "$0"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --workers)        WORKERS="$2"; shift 2 ;;
        --worktree-base)  WORKTREE_BASE="$2"; shift 2 ;;
        --timeout)        TIMEOUT="$2"; shift 2 ;;
        --max-continue)   MAX_CONTINUE="$2"; shift 2 ;;
        --ccache-dir)     CCACHE_DIR_OPT="$2"; shift 2 ;;
        --ccache-size)    CCACHE_SIZE="$2"; shift 2 ;;
        --agent)          AGENT="$2"; shift 2 ;;
        --model)          MODEL="$2"; shift 2 ;;
        --triage-model)   TRIAGE_MODEL="$2"; shift 2 ;;
        --effort)         EFFORT="$2"; shift 2 ;;
        --no-status)      SHOW_STATUS=0; shift ;;
        --api-key)        API_KEY="$2"; API_KEY_PROVIDED=1; shift 2 ;;
        --api-key-file)   API_KEY_FILE="$2"; API_KEY_PROVIDED=1; shift 2 ;;
        --once)           ONCE=1; shift ;;
        --skip-submodules) SKIP_SUBMODULES=1; shift ;;
        --color)          COLOR_WHEN="$2"; shift 2 ;;
        --dry-run)        DRY_RUN=1; shift ;;
        --mine)           MODE_MINE=1;     MODE_ANY=1; shift ;;
        --assigned)       MODE_ASSIGNED=1; MODE_ANY=1; shift ;;
        --related)        MODE_RELATED=1;  MODE_ANY=1; shift ;;
        --help|-h)        usage; exit 0 ;;
        *) echo "${S}Unknown option: $1${R}" >&2; echo "Run with --help for usage." >&2; exit 1 ;;
    esac
done

if ! [[ "$WORKERS" =~ ^[0-9]+$ ]] || (( WORKERS < 1 )); then
    echo "${S}Error: --workers must be a positive integer${R}" >&2
    exit 1
fi

if ! [[ "$MIN_FREE_GB" =~ ^[0-9]+$ ]] || (( MIN_FREE_GB < 1 )); then
    echo "${S}Error: CONTINUE_ALL_PRS_MIN_FREE_GB must be a positive integer${R}" >&2
    exit 1
fi

case "$AGENT" in
    claude|codex) ;;
    *) echo "${S}Error: --agent must be claude or codex${R}" >&2; exit 1 ;;
esac

if [[ -n "$TRIAGE_MODEL" && -z "$MODEL" ]]; then
    echo "${S}Error: --triage-model requires --model for the coding handoff${R}" >&2
    exit 1
fi

if [[ -n "$TRIAGE_MODEL" ]] && ! command -v bwrap >/dev/null 2>&1; then
    echo "${S}Error: --triage-model requires bwrap (Bubblewrap) to isolate Git credentials${R}" >&2
    exit 1
fi

MAIN_REPO="$(git rev-parse --show-toplevel)"
[[ -n "$WORKTREE_BASE" ]] || WORKTREE_BASE="${MAIN_REPO}-prworker"
# `git worktree list` only reports absolute paths, so every guard comparing a
# worker path against that list (`is_registered_worktree`, and through it
# `prepare_worktree_for_task` and the cleanup helpers) requires an absolute
# base. A relative `--worktree-base` would otherwise pass `ensure_worktree`,
# which canonicalizes on its own, and then fail the first per-PR round with
# `Refusing to clean an unexpected worktree`.
WORKTREE_BASE="$(realpath -m "$WORKTREE_BASE")"

# Install a defense-in-depth pre-push hook for ordinary worker pushes. The hook
# is applied through command-scope environment configuration, so it also covers
# branches checked out in linked worktrees without changing the user's repository
# configuration. `git push --no-verify` bypasses hooks; the worker prompt's
# explicit prohibition and safety gates remain the authoritative enforcement.
PUSH_HOOKS_DIR="$(cd "$MAIN_REPO/utils/continue-all-prs-hooks" && pwd -P)"
[[ -x "$PUSH_HOOKS_DIR/pre-push" ]] \
    || { echo "${S}Error: missing executable pre-push hook: $PUSH_HOOKS_DIR/pre-push${R}" >&2; exit 1; }
GIT_CONFIG_SLOT="${GIT_CONFIG_COUNT:-0}"
[[ "$GIT_CONFIG_SLOT" =~ ^[0-9]+$ ]] \
    || { echo "${S}Error: GIT_CONFIG_COUNT must be a non-negative integer${R}" >&2; exit 1; }
printf -v "GIT_CONFIG_KEY_${GIT_CONFIG_SLOT}" '%s' core.hooksPath
printf -v "GIT_CONFIG_VALUE_${GIT_CONFIG_SLOT}" '%s' "$PUSH_HOOKS_DIR"
export "GIT_CONFIG_KEY_${GIT_CONFIG_SLOT}" "GIT_CONFIG_VALUE_${GIT_CONFIG_SLOT}"
export GIT_CONFIG_COUNT=$((GIT_CONFIG_SLOT + 1))

# No mode flag given -> select all categories (the default behavior).
if (( ! MODE_ANY )); then
    MODE_MINE=1; MODE_ASSIGNED=1; MODE_RELATED=1
fi

# Custom API key for worker processes. `claude` reads its key from the environment.
# `codex` is logged in with the key in a worker-local `CODEX_HOME` before it starts,
# so it cannot inherit or overwrite an ambient Codex login.
# Prefer --api-key-file: an inline --api-key is visible in `ps`.
if [[ -n "$API_KEY_FILE" ]]; then
    [[ -r "$API_KEY_FILE" ]] || { echo "${S}Error: --api-key-file not readable: $API_KEY_FILE${R}" >&2; exit 1; }
    API_KEY="$(tr -d ' \t\r\n' < "$API_KEY_FILE")"
fi
if (( API_KEY_PROVIDED )); then
    [[ -n "$API_KEY" ]] || { echo "${S}Error: --api-key must not be empty${R}" >&2; exit 1; }
    if [[ "$AGENT" == "claude" ]]; then
        export ANTHROPIC_API_KEY="$API_KEY"
    fi
    CUSTOM_KEY=1
else
    CUSTOM_KEY=0
fi

# Published OpenAI API prices in USD per million tokens. The Codex CLI reports
# input tokens including cached input, so cost calculation subtracts cached
# tokens before applying the uncached-input rate. This is an API-price estimate;
# ChatGPT subscription usage is not billed per token.
CODEX_INPUT_PRICE=""
CODEX_CACHED_INPUT_PRICE=""
CODEX_CACHE_WRITE_INPUT_PRICE=""
CODEX_OUTPUT_PRICE=""
CODEX_LONG_CONTEXT_PRICING=0

configure_codex_pricing()
{
    CODEX_INPUT_PRICE=""
    CODEX_CACHED_INPUT_PRICE=""
    CODEX_CACHE_WRITE_INPUT_PRICE=""
    CODEX_OUTPUT_PRICE=""
    CODEX_LONG_CONTEXT_PRICING=0
    case "$1" in
        gpt-5.6|gpt-5.6-sol|gpt-5.6-sol-*)
            CODEX_INPUT_PRICE=5; CODEX_CACHED_INPUT_PRICE=0.5; CODEX_CACHE_WRITE_INPUT_PRICE=6.25; CODEX_OUTPUT_PRICE=30; CODEX_LONG_CONTEXT_PRICING=1 ;;
        gpt-5.6-terra|gpt-5.6-terra-*)
            CODEX_INPUT_PRICE=2; CODEX_CACHED_INPUT_PRICE=0.2; CODEX_CACHE_WRITE_INPUT_PRICE=2.5; CODEX_OUTPUT_PRICE=12; CODEX_LONG_CONTEXT_PRICING=1 ;;
        gpt-5.6-luna|gpt-5.6-luna-*)
            CODEX_INPUT_PRICE=0.2; CODEX_CACHED_INPUT_PRICE=0.02; CODEX_CACHE_WRITE_INPUT_PRICE=0.25; CODEX_OUTPUT_PRICE=1.2; CODEX_LONG_CONTEXT_PRICING=1 ;;
        gpt-5.4|gpt-5.4-20*)
            CODEX_INPUT_PRICE=2.5; CODEX_CACHED_INPUT_PRICE=0.25; CODEX_OUTPUT_PRICE=15; CODEX_LONG_CONTEXT_PRICING=1 ;;
        gpt-5.5-pro|gpt-5.5-pro-*|gpt-5.4-pro|gpt-5.4-pro-*)
            CODEX_INPUT_PRICE=30; CODEX_CACHED_INPUT_PRICE=30; CODEX_OUTPUT_PRICE=180; CODEX_LONG_CONTEXT_PRICING=1 ;;
        gpt-5.5|gpt-5.5-20*)
            CODEX_INPUT_PRICE=5; CODEX_CACHED_INPUT_PRICE=0.5; CODEX_OUTPUT_PRICE=30; CODEX_LONG_CONTEXT_PRICING=1 ;;
        gpt-5.4-mini|gpt-5.4-mini-*)
            CODEX_INPUT_PRICE=0.75; CODEX_CACHED_INPUT_PRICE=0.075; CODEX_OUTPUT_PRICE=4.5 ;;
        gpt-5.4-nano|gpt-5.4-nano-*)
            CODEX_INPUT_PRICE=0.2; CODEX_CACHED_INPUT_PRICE=0.02; CODEX_OUTPUT_PRICE=1.25 ;;
        gpt-5.3-codex|gpt-5.3-codex-*|gpt-5.2|gpt-5.2-*)
            CODEX_INPUT_PRICE=1.75; CODEX_CACHED_INPUT_PRICE=0.175; CODEX_OUTPUT_PRICE=14 ;;
        gpt-5.1|gpt-5.1-*|gpt-5|gpt-5-20*|gpt-5-codex|gpt-5-codex-*)
            CODEX_INPUT_PRICE=1.25; CODEX_CACHED_INPUT_PRICE=0.125; CODEX_OUTPUT_PRICE=10 ;;
        gpt-5-mini|gpt-5-mini-*)
            CODEX_INPUT_PRICE=0.25; CODEX_CACHED_INPUT_PRICE=0.025; CODEX_OUTPUT_PRICE=2 ;;
        gpt-5-nano|gpt-5-nano-*)
            CODEX_INPUT_PRICE=0.05; CODEX_CACHED_INPUT_PRICE=0.005; CODEX_OUTPUT_PRICE=0.4 ;;
    esac
}

estimate_codex_cost()
{
    local input_tokens="$1" cached_input_tokens="$2" output_tokens="$3" cache_write_input_tokens="$4"
    [[ -n "$CODEX_INPUT_PRICE" ]] || { printf '0'; return; }
    awk -v i="$input_tokens" -v ci="$cached_input_tokens" -v o="$output_tokens" -v cwi="$cache_write_input_tokens" \
        -v ip="$CODEX_INPUT_PRICE" -v cip="$CODEX_CACHED_INPUT_PRICE" -v cwip="${CODEX_CACHE_WRITE_INPUT_PRICE:-$CODEX_INPUT_PRICE}" -v op="$CODEX_OUTPUT_PRICE" \
        -v long="$CODEX_LONG_CONTEXT_PRICING" '
        BEGIN {
            uncached = i - ci - cwi;
            if (uncached < 0) uncached = 0;
            input_multiplier = (long && i > 272000) ? 2 : 1;
            output_multiplier = (long && i > 272000) ? 1.5 : 1;
            printf "%.9f", ((uncached * ip + ci * cip + cwi * cwip) * input_multiplier + o * op * output_multiplier) / 1000000;
        }'
}

if [[ "$AGENT" == "codex" ]]; then
    configure_codex_pricing "$MODEL"
fi

# ----------------------------------------------------------------------------
# Shared ccache. Every worker builds against one warm cache so a rebuild after
# merging master reuses cached objects instead of compiling cold (the main
# reason verify-builds were blowing past the per-PR budget). ClickHouse's build
# picks up ccache automatically; we only point all workers at the same dir and
# raise the size via env (not persisted to ccache.conf).
# ----------------------------------------------------------------------------
if [[ -n "$CCACHE_DIR_OPT" ]]; then
    export CCACHE_DIR="$CCACHE_DIR_OPT"
elif [[ -n "${CCACHE_DIR:-}" ]]; then
    export CCACHE_DIR
else
    export CCACHE_DIR="$HOME/.cache/ccache"
fi
export CCACHE_MAXSIZE="$CCACHE_SIZE"
mkdir -p "$CCACHE_DIR" 2>/dev/null || true

# ----------------------------------------------------------------------------
# Color handling
# ----------------------------------------------------------------------------

case "$COLOR_WHEN" in
    always) COLOR=1 ;;
    never)  COLOR=0 ;;
    auto)   if [[ -t 1 ]]; then COLOR=1; else COLOR=0; fi ;;
    *) echo "${S}Error: --color must be auto, always or never${R}" >&2; exit 1 ;;
esac

if (( ! COLOR )); then
    S=""; R=""; RESET=""
fi

# Replicates ClickHouse's intHash64 (src/Common/HashTable/Hash.h): a MurmurHash3
# 64-bit finalizer. Bash integers are 64-bit; multiplication wraps mod 2^64 just
# like the C++ code. `>> 33` is masked to emulate a logical (unsigned) shift.
lsr33() { echo $(( ($1 >> 33) & 0x7FFFFFFF )); }
inthash64()
{
    local x=$1
    x=$(( x ^ $(lsr33 "$x") ))
    x=$(( x * 0xff51afd7ed558ccd ))
    x=$(( x ^ $(lsr33 "$x") ))
    x=$(( x * 0xc4ceb9fe1a85ec53 ))
    x=$(( x ^ $(lsr33 "$x") ))
    echo "$x"
}

# Maps a PR number to a 24-bit foreground SGR sequence, replicating setColor in
# base/base/terminalColors.cpp: a constant-luminance YCbCr color where the
# chroma comes from the low two bytes of the hash. awk does the float math.
pr_color_seq()
{
    if (( ! COLOR )); then printf ''; return; fi
    local h cb cr
    h=$(inthash64 "$1")
    cb=$(( h & 0xFF ))
    cr=$(( (h >> 8) & 0xFF ))
    awk -v cb="$cb" -v cr="$cr" 'BEGIN{
        y = 128;
        r = int(y + 1.402 * (cr - 128));
        g = int(y - 0.344136 * (cb - 128) - 0.714136 * (cr - 128));
        b = int(y + 1.772 * (cb - 128));
        if (r < 0) r = 0; if (r > 255) r = 255;
        if (g < 0) g = 0; if (g > 255) g = 255;
        if (b < 0) b = 0; if (b > 255) b = 255;
        printf "\033[38;2;%d;%d;%dm", r, g, b;
    }'
}

emit()
{
    # emit <color-seq> <message>
    if (( COLOR )); then printf '%s%s%s\n' "$1" "$2" "$RESET"; else printf '%s\n' "$2"; fi
}

banner() { echo "${S}$*${R}"; }

# ----------------------------------------------------------------------------
# Status bar. The terminal is owned by a single-writer Python renderer
# (utils/continue-all-prs-status.py): the whole script's stdout is piped to it,
# and it prints incoming log lines scrolling above a two-line status bar pinned
# to the bottom (elapsed / rounds / ok-fail / cost / token totals, plus the list
# of PR numbers needing attention). Driving the terminal from one process avoids
# the cross-process cursor races a bash-only bar suffered from. The counters the
# renderer reads are maintained here in $STATSFILE / $NAFILE.
# ----------------------------------------------------------------------------
STATUS_ENABLED=0
START=0
ORIG_OUT=""

stats_init() { printf '0 0 0 0 0 0 0 0 0\n' > "$STATSFILE"; : > "$NAFILE"; }

# stats_add <d_rounds> <d_ok> <d_fail> <d_in> <d_out> <d_cachein> <d_cacheout> <d_triage_cost> <d_coding_cost>
# Writes atomically (tmp + mv) so the renderer never reads a torn/empty file.
stats_add()
{
    { flock 7
      local cur; cur=$(cat "$STATSFILE" 2>/dev/null || echo '0 0 0 0 0 0 0 0 0')
      awk -v cur="$cur" -v r="$1" -v s="$2" -v f="$3" -v i="$4" -v o="$5" -v ci="$6" -v co="$7" -v tc="$8" -v cc="$9" \
        'BEGIN { split(cur, x, " ");
                 printf "%d %d %d %d %d %d %d %.6f %.6f\n",
                        x[1]+r, x[2]+s, x[3]+f, x[4]+i, x[5]+o, x[6]+ci, x[7]+co, x[8]+tc, x[9]+cc }' \
        > "$STATSFILE.tmp" && mv -f "$STATSFILE.tmp" "$STATSFILE"
    } 7>>"$STATSLOCK"
}

# needs-attention list: reset at the start of each round, appended (deduped) as
# PRs finish NEEDS-ATTENTION, so the list reflects the current round.
na_reset() { { flock 7; : > "$NAFILE"; } 7>>"$STATSLOCK"; }
na_add()
{
    { flock 7
      grep -qxF "$1" "$NAFILE" 2>/dev/null || echo "$1" >> "$NAFILE"
    } 7>>"$STATSLOCK"
}

# Pipe this script's stdout (banners + all worker lines) through the renderer,
# which owns the real terminal. No-op if disabled or the renderer is missing.
status_start()
{
    (( STATUS_ENABLED )) || return 0
    command -v python3 >/dev/null 2>&1 && [[ -f "$STATUS_RENDERER" ]] || { STATUS_ENABLED=0; return 0; }
    exec {ORIG_OUT}>&1
    exec > >(CAP_STATS="$STATSFILE" CAP_NA="$NAFILE" CAP_START="$START" \
             exec python3 "$STATUS_RENDERER")
}

status_stop()
{
    (( STATUS_ENABLED )) || return 0
    STATUS_ENABLED=0
    [[ -n "$ORIG_OUT" ]] || return 0
    exec >&"$ORIG_OUT"                             # close pipe -> renderer EOF -> it restores
    printf '\033[r' >&"$ORIG_OUT" 2>/dev/null || true   # guarantee the scroll region is reset
}

# Distill a one-to-two sentence summary of what the worker did from its log.
# Both supported agents write their final message to the log, so
# the opening of that message is a natural summary. We take the first one or two
# sentences, but when those are conversational filler ("I've completed ...") we
# fall back to the first markdown header instead, since `/continue-pr-auto` usually
# titles its conclusion there (e.g. "## PR #N is obsolete ...").
summarize_log()
{
    local f="$1" clean sent hdr s
    [[ -s "$f" ]] || { printf '(no output)'; return 0; }

    clean=$(sed -e 's/\x1b\[[0-9;]*m//g' "$f")

    sent=$(printf '%s' "$clean" | tr '\n\t' '  ' \
        | sed -E -e 's/[#>*`]+/ /g' -e 's/  +/ /g' -e 's/^ +//' -e 's/ +$//' \
        | sed -E 's/^(([^.!?]*[.!?]){1,2}).*/\1/')

    hdr=$(printf '%s' "$clean" | grep -m1 -E '^#{1,6}[[:space:]]+' \
        | sed -E 's/^#{1,6}[[:space:]]+//; s/[[:space:]]+$//' || true)

    if [[ -n "$hdr" ]] && printf '%s' "$sent" \
        | grep -qiE '^(I have|I.ve|Here is|Here.s|Done|Sure|Okay|OK|Let me|I.ll|I will|I.m|Alright|Got it)'; then
        s="$hdr"
    else
        s="$sent"
    fi

    [[ -n "$s" ]] || s="$hdr"
    (( ${#s} > 240 )) && s="${s:0:237}..."
    [[ -n "$s" ]] && printf '%s' "$s" || printf '(no summary)'
}

# Warn once if 24-bit colors are unlikely to render under screen/tmux.
maybe_color_hint()
{
    (( COLOR )) || return 0
    case "${COLORTERM:-}" in
        truecolor|24bit) return 0 ;;
    esac
    case "${TERM:-}" in
        screen*)
            banner "Note: under GNU screen, 24-bit colors need 'truecolor on' in ~/.screenrc (screen >= 4.7) and a 256-color TERM. Use --color never to disable." ;;
        tmux*)
            banner "Note: under tmux, 24-bit colors need 'set -as terminal-features \",*:RGB\"' in ~/.tmux.conf. Use --color never to disable." ;;
    esac
}

# ----------------------------------------------------------------------------
# Worktree setup
# ----------------------------------------------------------------------------

# Hardlink the main repo's submodule git data into the worktree and materialize
# the submodule working trees, without any network access. Faithful adaptation
# of the create-worktree skill's hardlink procedure.
setup_worktree_submodules()
{
    local wt="$1"
    local git_common_dir git_dir wt_entry cpu cw jobs gitlinks gl_count sm_count
    local cp_pid parent_status modules_status init_pid

    git_common_dir=$(git -C "$MAIN_REPO" rev-parse --git-common-dir)
    case "$git_common_dir" in
        /*) git_dir="$git_common_dir" ;;
        *)  git_dir="$MAIN_REPO/$git_common_dir" ;;
    esac
    wt_entry=$(basename "$wt")

    ( cp -al "$git_dir/modules" "$git_dir/worktrees/$wt_entry/modules" ) &
    cp_pid=$!

    parent_status=0
    git -C "$wt" -c checkout.workers=0 -c core.fsync=none -c gc.auto=0 \
        checkout -q -f HEAD -- . || parent_status=$?

    modules_status=0
    wait "$cp_pid" || modules_status=$?

    if (( parent_status != 0 )); then echo "${S}FAILED: parent checkout for $wt${R}" >&2; return 1; fi
    if (( modules_status != 0 )); then echo "${S}FAILED: cp -al modules for $wt${R}" >&2; return 1; fi

    git -C "$wt" submodule init &
    init_pid=$!
    find "$git_dir/worktrees/$wt_entry/modules" \
        \( -name config -o -name config.worktree \) -exec \
        sed -i "s|worktree = .*/contrib/|worktree = $wt/contrib/|" {} +
    wait "$init_pid" || { echo "${S}FAILED: submodule init for $wt${R}" >&2; return 1; }

    cpu=$(nproc)
    cw=8; (( cw > cpu )) && cw=$cpu
    jobs=$(( cpu / cw )); (( jobs < 1 )) && jobs=1

    if git -C "$wt" config --file .gitmodules --get-regexp '^submodule\..*\.update$' 2>/dev/null \
        | grep -q ' !'; then
        echo "${S}FAILED: custom submodule update command unsupported on local hardlink path${R}" >&2
        return 1
    fi

    gitlinks=$(git -C "$wt" ls-files -s \
        | sed -n 's/^160000 \([0-9a-f][0-9a-f]*\) 0[[:space:]]\(.*\)$/\1 \2/p')
    gl_count=$(printf '%s\n' "$gitlinks" | sed -n '$=')
    sm_count=$(git -C "$wt" config --file .gitmodules --get-regexp '^submodule\..*\.path$' | sed -n '$=')
    if [[ "${gl_count:-0}" != "${sm_count:-0}" ]]; then
        echo "${S}FAILED: gitlink count ${gl_count:-0} does not match .gitmodules count ${sm_count:-0}${R}" >&2
        return 1
    fi

    # Largest-first scheduling: prime the queue with known-heavy submodules.
    {
        for sp in \
            contrib/llvm-project contrib/google-cloud-cpp contrib/aws \
            contrib/openssl contrib/icu contrib/boost contrib/rust_vendor \
            contrib/sysroot contrib/grpc contrib/arrow contrib/curl \
            contrib/rocksdb contrib/postgres contrib/wasmtime; do
            printf '%s\n' "$gitlinks" | awk -v p="$sp" '$2 == p { print; exit }'
        done
        printf '%s\n' "$gitlinks"
    } \
        | awk '!seen[$2]++ { print }' \
        | while IFS=' ' read -r commit sp; do printf '%s\0%s\0' "$commit" "$sp"; done \
        | xargs -0 -r -n2 -P "$jobs" sh -c '
            wt=$1; gd=$2; we=$3; cw=$4; commit=$5; sp=$6
            [ -n "$commit" ] && [ -n "$sp" ] || { echo "FAILED: empty submodule tuple" >&2; exit 1; }
            mgd="$gd/worktrees/$we/modules/$sp"
            mwt="$wt/$sp"
            mkdir -p "$mwt" || exit 1
            printf "gitdir: %s\n" "$mgd" > "$mwt/.git" || exit 1
            git --git-dir="$mgd" --work-tree="$mwt" \
                -c advice.detachedHead=false \
                -c checkout.workers="$cw" \
                -c checkout.thresholdForParallelism=100 \
                -c index.threads=true \
                -c core.fsync=none \
                -c gc.auto=0 \
                checkout -q -f --detach "$commit" \
                || { echo "FAILED: $sp: commit $commit missing from local mirror" >&2; exit 1; }
          ' sh "$wt" "$git_dir" "$wt_entry" "$cw" \
        || { echo "${S}ERROR: submodule checkout failed for $wt. If a commit is missing from the local mirror, run: git -C $MAIN_REPO submodule update --init${R}" >&2; return 1; }
}

# Create the worktree for a worker if it does not exist yet, otherwise reuse it.
# Reusing an arbitrary pre-existing directory would make the worker cleanup
# sequence operate on state which does not belong to this orchestrator.
ensure_worktree()
{
    local wt="$1"
    local canonical_wt

    canonical_wt=$(realpath -m "$wt")

    if git -C "$MAIN_REPO" worktree list --porcelain | grep -xF "worktree $canonical_wt" >/dev/null; then
        banner "Reusing existing worktree: $canonical_wt"
        return 0
    fi
    if [[ -e "$canonical_wt" ]]; then
        echo "${S}ERROR: path exists but is not a registered worktree: $canonical_wt${R}" >&2
        return 1
    fi

    banner "Creating worktree: $canonical_wt"
    git -C "$MAIN_REPO" worktree add --no-checkout --detach "$canonical_wt" HEAD

    if (( SKIP_SUBMODULES )); then
        git -C "$canonical_wt" -c checkout.workers=0 -c core.fsync=none -c gc.auto=0 checkout -q -f HEAD -- .
    else
        setup_worktree_submodules "$canonical_wt"
    fi
}

# Return the available space, in KiB, on the filesystem containing the worker
# pool. `df -P` keeps the available-space column stable across platforms.
free_space_kib()
{
    df -Pk "$(dirname "$WORKTREE_BASE")" | awk 'NR == 2 { print $4 }'
}

is_registered_worktree()
{
    local candidate="$1"
    git -C "$MAIN_REPO" worktree list --porcelain \
        | grep -xF "worktree $candidate" >/dev/null
}

is_managed_worktree()
{
    local candidate="$1"
    [[ "$candidate" != "$MAIN_REPO" && "$candidate" == "$WORKTREE_BASE-"* ]] \
        && [[ "${candidate#"$WORKTREE_BASE"-}" =~ ^[0-9]+$ ]]
}

is_active_worker_worktree()
{
    local candidate="$1" i
    for (( i = 0; i < WORKERS; ++i )); do
        [[ "$candidate" == "${WORKTREE_BASE}-${i}" ]] && return 0
    done
    return 1
}

# List processes whose current directory is the worker or one of its
# descendants. Agents can accidentally leave servers running after their turn;
# those processes race with `git clean` by recreating files as it removes them.
worktree_processes()
{
    local wt="$1" proc pid

    while IFS= read -r proc; do
        pid=${proc##*/}
        [[ "$pid" != "$$" && "$pid" != "$BASHPID" ]] || continue
        printf '%s\n' "$pid"
    done < <(
        find /proc -mindepth 2 -maxdepth 2 -type l -name cwd \
            \( -lname "$wt" -o -lname "$wt (deleted)" -o -lname "$wt/*" \) \
            -printf '%h\n' 2>/dev/null
    )
}

stop_worktree_processes()
{
    local wt="$1" pid attempt
    local -a pids=()

    if ! is_active_worker_worktree "$wt" || ! is_registered_worktree "$wt"; then
        echo "Refusing to stop processes in an unexpected worktree: $wt" >&2
        return 1
    fi

    mapfile -t pids < <(worktree_processes "$wt")
    (( ${#pids[@]} )) || return 0
    echo "Stopping leftover processes in $wt: ${pids[*]}" >&2

    for pid in "${pids[@]}"; do
        kill -TERM "$pid" 2>/dev/null || sudo -n kill -TERM "$pid" 2>/dev/null || true
    done

    # A watchdog can replace a server while the first process snapshot is
    # being terminated. Rescan before escalating so the replacement is also
    # removed. `SIGKILL` is appropriate here: these are orphaned processes from
    # a completed task, and cleanup cannot safely proceed while they are live.
    for attempt in 1 2 3; do
        mapfile -t pids < <(worktree_processes "$wt")
        (( ${#pids[@]} )) || return 0
        for pid in "${pids[@]}"; do
            kill -KILL "$pid" 2>/dev/null || sudo -n kill -KILL "$pid" 2>/dev/null || true
        done
        sleep 0.1
    done

    mapfile -t pids < <(worktree_processes "$wt")
    (( ! ${#pids[@]} )) || {
        echo "Could not stop leftover processes in $wt: ${pids[*]}" >&2
        return 1
    }
}

# Remove registered worktrees nested below a directory before deleting that
# directory. Git otherwise leaves their administrative entries behind and can
# refuse to remove the parent. Deepest paths go first.
remove_registered_descendants()
{
    local parent="$1" candidate
    local -a descendants=()

    mapfile -t descendants < <(
        git -C "$MAIN_REPO" worktree list --porcelain \
            | sed -n 's/^worktree //p' \
            | awk -v prefix="$parent/" 'index($0, prefix) == 1 { print length($0) "\t" $0 }' \
            | sort -rn \
            | cut -f2-
    )
    for candidate in "${descendants[@]}"; do
        is_managed_worktree "$candidate" || {
            echo "Refusing to remove unmanaged nested worktree: $candidate" >&2
            return 1
        }
        if ! git -C "$MAIN_REPO" worktree remove --force "$candidate"; then
            echo "Retrying worktree removal with elevated permissions: $candidate" >&2
            sudo -n git -c safe.directory="$MAIN_REPO" -C "$MAIN_REPO" worktree remove --force "$candidate"
        fi
    done
}

# Align the worker's submodule checkouts with the gitlinks recorded in the
# superproject HEAD, honoring `--skip-submodules`. `--no-fetch` is load-bearing
# for isolation: `.gitmodules` is pull-request content, so a submodule whose
# objects are absent locally must fail here instead of fetching an
# author-chosen URL with the operator's credentials.
align_worker_submodules()
{
    local wt="$1"

    if (( SKIP_SUBMODULES )); then
        # Restore initialized submodules to the superproject gitlinks without
        # initializing absent ones. `submodule update` can nevertheless
        # initialize submodules selected by `submodule.active`, so check out
        # the recorded SHA directly in the submodules that `foreach` found.
        git -C "$wt" submodule foreach --quiet --recursive \
            'git checkout --detach -q "$sha1"'
    else
        git -C "$wt" submodule update --init --checkout --force --recursive --no-fetch
    fi
}

# Prepare a reusable worker for a new PR. Root-level build directories remain
# available for reuse; all other tracked, untracked and ignored changes go.
prepare_worktree_for_task()
{
    local wt="$1"

    if ! is_active_worker_worktree "$wt" || ! is_registered_worktree "$wt"; then
        echo "Refusing to clean an unexpected worktree: $wt" >&2
        return 1
    fi

    stop_worktree_processes "$wt"
    git -C "$wt" reset --hard -q HEAD
    git -C "$wt" checkout --detach -q HEAD
    remove_registered_descendants "$wt"
    # `git submodule foreach` visits only initialized submodules. Always clean
    # those existing worktrees: `--skip-submodules` avoids initialization, but
    # must not let dirt from an earlier non-skipped run leak into the next PR.
    # shellcheck disable=SC2016 # `$PWD` expands in each `git submodule foreach` shell.
    git -C "$wt" submodule foreach --quiet --recursive \
        'git reset --hard -q HEAD && { git clean -ffdx -e "/build*/" || { echo "Retrying cleanup with elevated permissions: $PWD" >&2; sudo -n git -c safe.directory="$PWD" -C "$PWD" clean -ffdx -e "/build*/"; }; }' >/dev/null
    align_worker_submodules "$wt"
    # Integration tests can leave root-owned artifacts, and stale servers can
    # briefly recreate files while cleanup is traversing a directory. Retry
    # the identical, path-scoped cleanup after stopping any replacement
    # processes. If elevation is unavailable, fail closed.
    local attempt
    for attempt in 1 2 3; do
        git -C "$wt" clean -ffdx -e '/build*/' && return 0
        stop_worktree_processes "$wt"
        echo "Retrying cleanup with elevated permissions ($attempt/3): $wt" >&2
        sudo -n git -c safe.directory="$wt" -C "$wt" clean -ffdx -e '/build*/' && return 0
    done
    echo "Worktree cleanup did not converge after 3 attempts: $wt" >&2
    return 1
}

remove_managed_cache_dir()
{
    local wt="$1" candidate="$2" base
    base="${candidate##*/}"

    if ! is_registered_worktree "$wt" || ! is_managed_worktree "$wt" \
        || [[ "$candidate" != "$wt/"* ]] \
        || [[ "${candidate%/*}" != "$wt" ]] \
        || { [[ "$base" != tmp ]] && [[ "$base" != build* ]]; }; then
            echo "Refusing to remove unexpected cache directory: $candidate" >&2
            return 1
    fi

    banner "Low disk space: removing $candidate"
    remove_registered_descendants "$candidate"
    if ! rm -rf -- "$candidate"; then
        echo "Retrying cleanup with elevated permissions: $candidate" >&2
        sudo -n rm -rf -- "$candidate"
    fi
}

# Run only while the worker pool is idle. First discard old `tmp` and `build*`
# directories, then stale managed worktrees, until the requested reserve is
# restored. Active worker roots are never removed wholesale.
cleanup_worktrees_if_disk_low()
{
    (( DRY_RUN )) && return 0

    local minimum_kib=$(( MIN_FREE_GB * 1024 * 1024 )) available wt candidate mtime
    local -a worktrees=() cache_candidates=() stale_worktrees=()
    local -A worktree_mtime=()
    available=$(free_space_kib)
    [[ "$available" =~ ^[0-9]+$ ]] || {
        echo "${S}Error: could not determine available disk space${R}" >&2
        return 1
    }
    (( available < minimum_kib )) || return 0

    banner "Low disk space: $(( available / 1024 / 1024 )) GiB free; cleaning managed worktrees to restore ${MIN_FREE_GB} GiB"
    mapfile -t worktrees < <(git -C "$MAIN_REPO" worktree list --porcelain | sed -n 's/^worktree //p')

    for wt in "${worktrees[@]}"; do
        is_managed_worktree "$wt" || continue
        [[ -d "$wt" ]] || continue
        # Cache this before removing child directories, which changes the
        # worktree root's mtime and would destroy the old-to-new ordering.
        worktree_mtime["$wt"]=$(stat -c %Y "$wt")
        while IFS= read -r -d '' candidate; do
            mtime=$(stat -c %Y "$candidate")
            cache_candidates+=("$mtime"$'\t'"$wt"$'\t'"$candidate")
        done < <(find "$wt" -mindepth 1 -maxdepth 1 -type d \( -name tmp -o -name 'build*' \) -print0 2>/dev/null)
    done

    if (( ${#cache_candidates[@]} )); then
        mapfile -t cache_candidates < <(printf '%s\n' "${cache_candidates[@]}" | sort -n)
        for candidate in "${cache_candidates[@]}"; do
            IFS=$'\t' read -r mtime wt candidate <<< "$candidate"
            [[ -d "$candidate" ]] || continue
            remove_managed_cache_dir "$wt" "$candidate"
            available=$(free_space_kib)
            (( available >= minimum_kib )) && return 0
        done
    fi

    for wt in "${worktrees[@]}"; do
        is_managed_worktree "$wt" || continue
        is_active_worker_worktree "$wt" && continue
        [[ -d "$wt" ]] || continue
        mtime=${worktree_mtime["$wt"]:-$(stat -c %Y "$wt")}
        stale_worktrees+=("$mtime"$'\t'"$wt")
    done
    if (( ${#stale_worktrees[@]} )); then
        mapfile -t stale_worktrees < <(printf '%s\n' "${stale_worktrees[@]}" | sort -n)
        for candidate in "${stale_worktrees[@]}"; do
            wt=${candidate#*$'\t'}
            # Removing a parent also unregisters its nested worktrees, which
            # may still occur later in this snapshot.
            is_registered_worktree "$wt" || continue
            if ! is_managed_worktree "$wt" || is_active_worker_worktree "$wt"; then
                echo "Refusing to remove unexpected worktree: $wt" >&2
                return 1
            fi
            banner "Low disk space: removing stale worktree $wt"
            remove_registered_descendants "$wt"
            if ! git -C "$MAIN_REPO" worktree remove --force "$wt"; then
                echo "Retrying worktree removal with elevated permissions: $wt" >&2
                sudo -n git -c safe.directory="$MAIN_REPO" -C "$MAIN_REPO" worktree remove --force "$wt"
            fi
            available=$(free_space_kib)
            (( available >= minimum_kib )) && return 0
        done
    fi

    echo "${S}Error: only $(( available / 1024 / 1024 )) GiB free after cleaning managed worktrees; ${MIN_FREE_GB} GiB required${R}" >&2
    return 1
}

# ----------------------------------------------------------------------------
# Work queue (shared file + flock). Workers pop the next PR atomically, so the
# next free worker always gets the next PR -> even distribution.
# ----------------------------------------------------------------------------

QUEUEFILE=""
LOCKFILE=""
LOGDIR="${MAIN_REPO}/tmp/continue-all-prs"
STATSFILE="$LOGDIR/stats"
STATSLOCK="$LOGDIR/stats.lock"
NAFILE="$LOGDIR/needs-attention"
CLEANUP_FAILURE_FILE="$LOGDIR/cleanup-failure"
declare -a WORKER_PIDS=()

cleanup_worker_codex_auth()
{
    [[ "$AGENT" == "codex" ]] || return 0

    local wt
    for wt in "${WT[@]-}"; do
        # Triage receives a disposable `CODEX_HOME` inside its private clone.
        # Remove it here too: `stop_workers` can run before `run_continue_pr`
        # reaches its normal cleanup path.
        rm -rf "$wt/tmp/continue-all-prs/triage-repository/.triage-codex-home" 2>/dev/null || true
        # The worker-local `CODEX_HOME` is per-run state, not just credentials.
        # `continue-pr-auto` keeps `tmp/continue-all-prs/` between pull requests,
        # so leaving `config.toml`, plugin, or MCP state written by one run would
        # silently become the starting state of the next, unrelated one.
        [[ "$CUSTOM_KEY" != 1 ]] || rm -rf "$wt/tmp/continue-all-prs/codex-home" 2>/dev/null || true
    done
}

stop_workers()
{
    local roots own_pgid entry pid pgid
    local -a targets
    local -a active_workers=() live_jobs=()
    local -A target_groups=() worker_pid_set=()

    # `WORKER_PIDS` retains exited child PIDs until `wait` completes. Resolve
    # roots through the shell's live job table so a recycled PID can never be
    # mistaken for a worker and signalled during interrupt handling.
    for pid in "${WORKER_PIDS[@]}"; do
        worker_pid_set["$pid"]=1
    done
    mapfile -t live_jobs < <(jobs -pr)
    for pid in "${live_jobs[@]}"; do
        [[ -n "${worker_pid_set[$pid]:-}" ]] && active_workers+=("$pid")
    done
    (( ${#active_workers[@]} )) || { cleanup_worker_codex_auth; return 0; }
    roots="${active_workers[*]}"
    own_pgid=$(ps -o pgid= -p "$$" 2>/dev/null | tr -d ' ' || true)

    # Snapshot the complete descendant tree before sending any signal. Commands
    # started by an agent can create nested process groups, so killing only the
    # worker's original group is insufficient.
    mapfile -t targets < <(
        ps -eo pid=,ppid=,pgid= | awk -v roots="$roots" '
            BEGIN {
                count = split(roots, root, " ");
                for (i = 1; i <= count; ++i) selected[root[i]] = 1;
            }
            {
                pid[NR] = $1;
                parent[$1] = $2;
                group[$1] = $3;
            }
            END {
                changed = 1;
                while (changed) {
                    changed = 0;
                    for (i = 1; i <= NR; ++i) {
                        p = pid[i];
                        if (!selected[p] && selected[parent[p]]) {
                            selected[p] = 1;
                            changed = 1;
                        }
                    }
                }
                for (i = 1; i <= NR; ++i) {
                    p = pid[i];
                    if (selected[p]) print p, group[p];
                }
            }'
    )

    for entry in "${targets[@]}"; do
        read -r pid pgid <<< "$entry"
        [[ -n "$pgid" && "$pgid" != "$own_pgid" ]] && target_groups["$pgid"]=1
    done

    # Freeze every captured group first so no descendant can spawn more work
    # while shutdown is in progress, then terminate all groups and individual
    # processes. Never signal the orchestrator's own process group.
    for pgid in "${!target_groups[@]}"; do
        kill -STOP -- "-$pgid" 2>/dev/null || true
    done
    for pgid in "${!target_groups[@]}"; do
        kill -KILL -- "-$pgid" 2>/dev/null || true
    done
    for entry in "${targets[@]}"; do
        read -r pid pgid <<< "$entry"
        kill -KILL "$pid" 2>/dev/null || true
    done
    if (( ${#active_workers[@]} )); then
        wait "${active_workers[@]}" 2>/dev/null || true
    fi
    cleanup_worker_codex_auth
    WORKER_PIDS=()
}

cleanup()
{
    stop_workers
    status_stop   # reset the scroll region and kill the updater first
    [[ -n "${QUEUEFILE:-}" ]] && rm -f "$QUEUEFILE" "$QUEUEFILE.tmp" 2>/dev/null || true
    [[ -n "${LOCKFILE:-}" ]] && rm -f "$LOCKFILE" 2>/dev/null || true
    rm -f "$STATSLOCK" "$STATSFILE.tmp" 2>/dev/null || true
}
trap cleanup EXIT
trap 'trap - INT TERM; echo; banner "Interrupted, stopping..."; stop_workers; exit 130' INT TERM

# Markers the worker prints on their own lines when it finishes or hands off.
DONE_MARKER='<<<CONTINUE-PR-DONE>>>'
HANDOFF_MARKER='<<<CONTINUE-PR-HANDOFF>>>'

# Added to every worker session. Forbids backgrounding work (the
# root cause of merges that were prepared but never pushed: the worker started a
# build in the background and ended its turn waiting for a notification that
# never comes in single-shot `--print` mode).
STEER_PROMPT="You are running in a non-interactive, single-shot batch session. Do NOT run any commands in the background and do NOT defer work expecting to be notified later - there is no later notification, and any background process is killed when your turn ends. Run builds, tests, and every long-running command synchronously in the foreground so they finish within your turn, and complete ALL work - including pushing your commits with 'git push' - before you end your turn. Preserve the existing remote PR history: only add commits, never rebase, reset onto another base, amend published commits, force-push, use a '+' refspec, bypass hooks with --no-verify, or delete the remote branch. Before committing, inspect the staged name-status and stat, stage only explicit intended paths, and reject unrelated files or mass changes. Before pushing, require the freshly fetched remote PR head to be an ancestor of local HEAD and inspect the complete diff against the base branch; a scope or lineage anomaly is a hard safety stop to report, never something to work around. A passing (green) CI does NOT mean the PR is done: always fetch and address unresolved review comments and reviewer feedback - including automated or bot reviews such as clickhouse-gh[bot], and review threads that are merely COMMENTED rather than blocking - even when every check passes or the PR is already approved. Do not signal done while there are unaddressed review comments, unless addressing them genuinely requires a human decision. If the PR is CONFLICTING, resolve the conflicts (merge the base branch, resolve, and rework to build) and push whenever you have access; a contested/reserved/superseded note does not block mechanical conflict resolution. Determine pushability from repository and author ownership before maintainerCanModify: a same-repository PR and any PR authored by the authenticated gh user are pushable regardless of maintainerCanModify; that field only blocks pushing another author's cross-repository fork. If you cannot push (e.g. another author's fork with maintainer edits disabled), supersede it: open your own PR from the main repo (crediting the author; re-author the commits yourself if the author has not signed the CLA) and close theirs with a comment linking the new PR - unless the change is obsolete, already fixed, or already superseded, in which case say so specifically rather than a bare 'needs attention'. Every GitHub comment you post (PR comments, issue comments, and review-thread replies) MUST begin with the 🕵 symbol followed by a space, so automated comments are identifiable. When, and only when, the PR is fully handled (changes pushed, or you have determined that no change is needed or that it needs a human decision), end your final message with a line containing exactly: ${DONE_MARKER}"

# Sent on each resume to nudge the worker to finish.
NUDGE_PROMPT="Continue where you left off and finish the task. Reminder: do not use background tasks - run everything synchronously and push your commits before finishing. Preserve remote PR history and obey the staged-diff, full-PR-diff, and fast-forward-only safety gates; never force-push or bypass the pre-push hook. A green CI does NOT mean you are done - also address unresolved review comments and reviewer feedback (including automated/bot reviews and COMMENTED, non-blocking threads). A same-repository PR or a PR authored by the authenticated gh user is pushable even when maintainerCanModify is false; only use that field for another author's cross-repository fork. Any build started in a previous turn was killed when that turn ended; re-run it in the foreground if you still need to verify. When the PR is fully handled, end your final message with a line containing exactly: ${DONE_MARKER}"

TRIAGE_STEER_PROMPT="You are the triage model in a two-model workflow. Inspect the PR, its merge status, CI failures, and unresolved review feedback, then decide whether completing it requires writing code. You may finish and push the work yourself only when no source, test, or documentation changes are needed beyond a clean merge of the latest base branch. If any other code change, including a merge conflict, is needed, do not implement it. End with a handoff block containing a line exactly equal to ${HANDOFF_MARKER}, followed by a concise but sufficiently detailed task description for the coding model: include the diagnosis, relevant files or failures, reviewer requirements, work already performed, and the verification still needed. If you fully handle the PR yourself, use ${DONE_MARKER} as usual and do not emit ${HANDOFF_MARKER}."

TRIAGE_NUDGE_PROMPT="Continue the initial triage. Only complete a clean base-branch merge yourself. If any other source, test, or documentation change is needed, stop and hand it to the coding model by emitting ${HANDOFF_MARKER} on its own line followed by a detailed task description. Emit ${DONE_MARKER} only if the PR is fully handled."

# The prompt is not a security boundary: a triage model can still edit the
# worktree. It may leave no changes, or exactly the conflict-free merge that
# this function prepared for it. Anything else must be discarded before the
# coding model receives the worktree.
triage_state_is_safe()
{
    local wt="$1" start_head="$2" base_head="$3"
    local head first_parent second_parent extra_parent merge_base expected_tree actual_tree index

    [[ -z "$(git -C "$wt" status --porcelain)" ]] || return 1
    head=$(git -C "$wt" rev-parse HEAD) || return 1
    [[ "$head" == "$start_head" ]] && return 0

    read -r first_parent second_parent extra_parent < <(git -C "$wt" show -s --format='%P' HEAD)
    [[ "$first_parent" == "$start_head" && "$second_parent" == "$base_head" && -z "$extra_parent" ]] || return 1
    # Git 2.34 lacks the two-argument `merge-tree` form with `--write-tree`.
    # Build the three-tree merge in a private index instead. `write-tree`
    # rejects unmerged entries, so this only yields a tree for a clean merge.
    merge_base=$(git -C "$wt" merge-base "$start_head" "$base_head") || return 1
    mkdir -p "$wt/tmp/continue-all-prs" || return 1
    index=$(cd "$wt" && mktemp "$PWD/tmp/continue-all-prs/triage-merge-index.XXXXXX") || return 1
    rm -f "$index"
    if ! GIT_INDEX_FILE="$index" git -C "$wt" read-tree -m "$merge_base" "$start_head" "$base_head" \
        || ! expected_tree=$(GIT_INDEX_FILE="$index" git -C "$wt" write-tree); then
        rm -f "$index"
        return 1
    fi
    rm -f "$index"
    actual_tree=$(git -C "$wt" rev-parse "$head^{tree}") || return 1
    [[ "$actual_tree" == "$expected_tree" ]]
}

# Canonical clone URL of the repository the pull requests belong to. Triage
# must never take the base branch from an inherited `origin`, which can point
# at a fork of the operator's checkout.
repo_clone_url()
{
    printf 'https://github.com/%s.git\n' "$REPO"
}

# Check out the actual PR head before triage begins, and return the data needed
# to validate and push a mechanical base merge.  A detached checkout avoids
# reserving one shared local branch when multiple workers process the same PR.
prepare_triage_worktree()
{
    local wt="$1" number="$2" deadline="$3"
    local meta base_ref head_ref head_repo_url head_owner author cross_repo maintainer_can_modify
    local fetch_url push_url base_repo_url pushable=0

    meta=$(run_with_deadline "$deadline" gh pr view "$number" --repo "$REPO" \
        --json baseRefName,headRefName,headRepository,headRepositoryOwner,author,isCrossRepository,maintainerCanModify \
        --jq '[.baseRefName, .headRefName, (.headRepository.url // ""), (.headRepositoryOwner.login // ""), (.author.login // ""), (.isCrossRepository | tostring), (.maintainerCanModify | tostring)] | @tsv') || return $?
    IFS=$'\t' read -r base_ref head_ref head_repo_url head_owner author cross_repo maintainer_can_modify <<< "$meta"
    [[ -n "$base_ref" && -n "$head_ref" && -n "$head_repo_url" ]] || return 1

    fetch_url="$head_repo_url"
    if [[ "$cross_repo" == "true" ]]; then
        if [[ "$head_owner" == "$GH_USER" || "$author" == "$GH_USER" || "$maintainer_can_modify" == "true" ]]; then
            pushable=1
        fi
    else
        pushable=1
    fi

    # Fetching by URL avoids adding a per-fork remote to the shared Git config.
    # Use full `refs/heads/` refspecs: a short name is ambiguous, and a fork
    # that carries both a branch and a tag called `$head_ref` would make
    # `FETCH_HEAD` resolve to the tag, so triage would inspect a tree that is
    # not the PR head.  `headRefName` is controlled by the fork, so this is
    # reachable from outside.
    run_with_deadline "$deadline" git -C "$wt" fetch -q "$fetch_url" "refs/heads/$head_ref" || return $?
    git -C "$wt" checkout --detach -q FETCH_HEAD || return 1
    # Resolve the base branch from the pull request's own repository, by
    # explicit URL. The triage clone inherits `origin` from the operator's
    # checkout, which may be a fork: fetching `origin/$base_ref` from it would
    # validate a merge of the fork's base branch instead of the real one.
    base_repo_url=$(repo_clone_url)
    run_with_deadline "$deadline" git -C "$wt" fetch -q "$base_repo_url" \
        "+refs/heads/$base_ref:refs/remotes/origin/$base_ref" || return $?
    push_url="$head_repo_url"
    printf '%s\t%s\t%s\t%s\t%s\n' "$(git -C "$wt" rev-parse HEAD)" "$(git -C "$wt" rev-parse "origin/$base_ref")" "$head_ref" "$push_url" "$pushable"
}

# Triage is untrusted: give it a private clone rather than the worker's
# worktree and common Git directory. Its only permitted result is a validated
# merge commit, transferred by the orchestrator after it leaves the sandbox.
create_triage_clone()
{
    local wt="$1" triage_wt="$2" deadline="$3"

    rm -rf "$triage_wt"
    run_with_deadline "$deadline" git clone -q --shared --no-checkout "$wt" "$triage_wt" || return $?
    # Cloning a local path points `origin` at the worker's directory. Use the
    # canonical repository instead of whatever `origin` the operator's checkout
    # carries, so that everything triage resolves through `origin` - the base
    # branch above all - comes from the pull request's own repository.
    git -C "$triage_wt" remote set-url origin "$(repo_clone_url)"
}

# The private triage clone starts without a checkout. Materialize its
# submodules only after `prepare_triage_worktree` has checked out the actual
# PR head, so builds and source inspection see the same revision as triage.
#
# `.gitmodules` is pull-request content, and this runs in the orchestrator,
# before the Bubblewrap namespace and the scrubbed Git environment exist: a
# plain `git submodule update --init` would resolve author-chosen URLs with the
# operator's SSH agent, credential helpers, and tokens still available, so a
# `.gitmodules` pointing at `ssh://git@attacker.example/...` would make the
# worker authenticate to that endpoint. Take every submodule URL from the
# trusted worker's checkout instead, keyed by submodule path, and leave
# submodules the worker does not know - the only thing a `.gitmodules` change
# can introduce - unmaterialized.
setup_triage_submodules()
{
    local triage_wt="$1" wt="$2" deadline="$3"

    (( SKIP_SUBMODULES )) && return 0
    materialize_trusted_submodules "$triage_wt" "$wt" "$deadline"
}

# One level of submodules, then the same rule for their own submodules: a
# nested `.gitmodules` is pull-request content as well, so its URLs are no more
# trustworthy than the top-level ones.
materialize_trusted_submodules()
{
    local repo="$1" trusted="$2" deadline="$3"
    local record key value name path
    local -A trusted_path=() trusted_url=() url_of_path=()
    local -a paths=()

    [[ -e "$repo/.gitmodules" && -e "$trusted/.gitmodules" ]] || return 0

    while IFS= read -r -d '' record; do
        key=${record%%$'\n'*}
        [[ "$key" != "$record" ]] && value=${record#*$'\n'} || value=""
        case "$key" in
            submodule.*.path) name=${key#submodule.}; trusted_path[${name%.path}]="$value" ;;
            submodule.*.url) name=${key#submodule.}; trusted_url[${name%.url}]="$value" ;;
        esac
    done < <(git config -f "$trusted/.gitmodules" --list -z)
    for name in "${!trusted_path[@]}"; do
        [[ -n "${trusted_url[$name]:-}" ]] || continue
        url_of_path[${trusted_path[$name]}]="${trusted_url[$name]}"
    done

    while IFS= read -r -d '' record; do
        key=${record%%$'\n'*}
        [[ "$key" != "$record" ]] && value=${record#*$'\n'} || value=""
        [[ "$key" == submodule.*.path ]] || continue
        name=${key#submodule.}
        name=${name%.path}
        path="$value"
        [[ -n "${url_of_path[$path]:-}" ]] || continue
        git -C "$repo" config "submodule.$name.url" "${url_of_path[$path]}" || return 1
        paths+=("$path")
    done < <(git config -f "$repo/.gitmodules" --list -z)

    (( ${#paths[@]} )) || return 0
    # Allow only the transports the trusted URLs actually use, so a redirect or
    # an unexpected URL form cannot reach an arbitrary remote helper either.
    run_with_deadline "$deadline" git -C "$repo" \
        -c protocol.allow=never -c protocol.https.allow=always -c protocol.file.allow=always \
        submodule update --checkout -- "${paths[@]}" || return $?

    for path in "${paths[@]}"; do
        materialize_trusted_submodules "$repo/$path" "$trusted/$path" "$deadline" || return $?
    done
}

# Use the remaining per-PR budget for setup operations as well as agent turns.
# `timeout` returns 124 when the budget expires, which `run_continue_pr`
# reports as the ordinary per-PR timeout outcome.
run_with_deadline()
{
    local deadline="$1" remaining
    shift

    remaining=$(( deadline - $(date +%s) ))
    (( remaining > 0 )) || return 124
    timeout "$remaining" "$@"
}

# `git clone --shared` lets the triage clone read the worker's objects, not the
# other way round: the pull-request head and the base branch that triage
# fetched live only in the clone's own object store. Import them into the
# trusted worker, or the checkout below fails with `reference is not a tree`.
import_triage_objects()
{
    local wt="$1" triage_wt="$2" oid
    local -a missing=()

    for oid in "${@:3}"; do
        git -C "$wt" cat-file -e "$oid^{commit}" 2>/dev/null || missing+=("$oid")
    done
    (( ${#missing[@]} )) || return 0
    git -C "$wt" fetch -q --no-tags "$triage_wt" "${missing[@]}"
}

recreate_validated_triage_merge()
{
    local wt="$1" start_head="$2" base_head="$3" triage_wt="$4"

    import_triage_objects "$wt" "$triage_wt" "$start_head" "$base_head" || return 1

    # Do not transfer the triage commit object. Even with the expected parents
    # and tree, its author, committer, and message are untrusted. Recreate the
    # clean merge in the trusted worker instead.
    git -C "$wt" checkout --detach -q "$start_head" || return 1
    if ! git -C "$wt" merge --no-ff --no-commit "$base_head"; then
        git -C "$wt" merge --abort || true
        return 1
    fi
    # This synthetic merge is a mechanical handoff step, not a commit the
    # operator authors: never make it depend on the local signing setup. With
    # `commit.gpgSign=true` configured, an otherwise clean validated merge
    # would fail here on any machine where signing needs interaction.
    git -C "$wt" -c commit.gpgSign=false commit -m "Merge base branch into pull request head" || return 1
    # The recreated merge can advance submodule gitlinks (a base-branch
    # submodule bump merges cleanly). Without realignment the worker's
    # submodule worktrees stay on the old revisions and the coding model
    # starts from a dirty tree, building against stale submodule sources.
    align_worker_submodules "$wt"
}

discard_untrusted_triage_changes()
{
    local wt="$1" start_head="$2" log="$3"

    echo "Discarding non-mechanical changes made by the triage model before coding handoff." >> "$log"
    git -C "$wt" reset --hard "$start_head" >> "$log"
    git -C "$wt" clean -fd >> "$log"
}

prepare_triage_sandbox_config()
{
    local wt="$1" config source_config key

    config="${2:-$wt/tmp/continue-all-prs/triage-git-config}"

    # Worktrees share the main repository's config. Never mutate it for
    # triage: use a private copy mounted over the common config only in the
    # Bubblewrap namespace. Besides credential helpers, remove all HTTPS
    # extra headers, SSH command overrides, and include files, which can carry
    # authentication tokens.
    mkdir -p "${config%/*}" || return 1
    source_config=$(git -C "$wt" rev-parse --path-format=absolute --git-path config) || return 1
    cp "$source_config" "$config" || return 1
    while IFS= read -r key; do
        git config --file "$config" --unset-all "$key" || return 1
    done < <(git config --file "$config" --name-only --get-regexp '^credential(\..*)?\.helper$' || true)
    while IFS= read -r key; do
        git config --file "$config" --unset-all "$key" || return 1
    done < <(git config --file "$config" --name-only --get-regexp '^http\..*\.extraheader$' || true)
    git config --file "$config" --unset-all core.sshCommand 2>/dev/null || true
    while IFS= read -r key; do
        git config --file "$config" --unset-all "$key" || return 1
    done < <(git config --file "$config" --name-only --get-regexp '^(include|includeif\..*)\.path$' || true)
    # Remote URLs and URL rewrites can embed credentials too. Triage fetches
    # pull-request heads by explicit public URL, and only needs `origin` for
    # the public base repository, so discard every inherited remote endpoint
    # and URL rewrite before restoring that one auth-free fetch URL.
    while IFS= read -r key; do
        git config --file "$config" --unset-all "$key" || return 1
    done < <(git config --file "$config" --name-only --get-regexp '^remote\..*\.(url|pushurl)$' || true)
    while IFS= read -r key; do
        git config --file "$config" --unset-all "$key" || return 1
    done < <(git config --file "$config" --name-only --get-regexp '^url\..*\.(insteadof|pushinsteadof)$' || true)
    git config --file "$config" remote.origin.url "https://github.com/${REPO}.git" || return 1
    printf '%s\n' "$config"
}

# Run /continue-pr-auto in a worktree, resuming the same session until the worker
# signals completion (DONE_MARKER), the per-PR time budget (TIMEOUT, shared
# across all turns and models) is exhausted, or the continuation cap
# (MAX_CONTINUE per model) is hit. In two-model mode, the triage model can emit
# HANDOFF_MARKER to start a fresh coding-model session in the same worktree.
# Writes the full transcript to $log and the final turn to $log.last. Returns
# the exit code of the last turn (124 if the time budget was exhausted).
run_continue_pr()
{
    local wt="$1" number="$2" log="$3"
    local url="https://github.com/$REPO/pull/$number"
    local sid deadline iter phase_iter ec now remaining build_steer prompt usage codex_home last_message
    local phase active_model active_wt system_prompt turn_prompt handoff triage_start_head triage_base_head triage_head_ref triage_push_url triage_pushable triage_wt triage_sandbox_config triage_agent_home triage_codex_home triage_home triage_current_head
    local -a codex_env active_codex_env
    local u_i u_o u_ci u_co u_cost triage_cost coding_cost
    local -a model_args triage_git_args triage_sandbox_args
    sid=""
    phase="coding"
    [[ -n "$TRIAGE_MODEL" ]] && phase="triage"
    handoff=""
    triage_start_head=""
    triage_base_head=""
    triage_head_ref=""
    triage_push_url=""
    triage_pushable=0
    # The documented budget covers every triage operation, including clone,
    # metadata retrieval, and fetches before the first agent turn.
    deadline=$(( $(date +%s) + TIMEOUT ))
    if [[ "$phase" == "triage" ]]; then
        triage_wt="$wt/tmp/continue-all-prs/triage-repository"
        create_triage_clone "$wt" "$triage_wt" "$deadline" || return $?
        local triage_metadata
        triage_metadata=$(prepare_triage_worktree "$triage_wt" "$number" "$deadline") || return $?
        setup_triage_submodules "$triage_wt" "$wt" "$deadline" || return $?
        IFS=$'\t' read -r triage_start_head triage_base_head triage_head_ref triage_push_url triage_pushable <<< "$triage_metadata"
    fi
    : > "$log"
    iter=0
    phase_iter=0
    ec=0

    # Codex API-key authentication is configured state, not an environment
    # variable consumed by `codex exec`. Keep that state private to this worker
    # and remove it after the run, so a requested key cannot fall back to or
    # modify the caller's ambient Codex login.
    codex_home=""
    codex_env=()
    if [[ "$AGENT" == "codex" && "$CUSTOM_KEY" == 1 ]]; then
        codex_home="$wt/tmp/continue-all-prs/codex-home"
        codex_env=("CODEX_HOME=$codex_home")
        rm -rf "$codex_home"
        mkdir -p "$codex_home"
        now=$(date +%s)
        remaining=$(( deadline - now ))
        (( remaining > 0 )) || return 124
        printf '%s\n' "$API_KEY" | timeout "$remaining" env CODEX_HOME="$codex_home" codex login --with-api-key >> "$log" 2>&1 || {
            ec=$?
            rm -rf "$codex_home"
            return "$ec"
        }
    fi

    if [[ "$phase" == "triage" ]]; then
        triage_sandbox_config=$(prepare_triage_sandbox_config "$triage_wt" "$wt/tmp/continue-all-prs/triage-git-config") || return 1
        triage_home="$triage_wt/.triage-home"
        mkdir -p "$triage_home" || return 1
        # The private clone prevents shared Git metadata from persisting. Use
        # an empty home and a sanitized, read-only clone config so the triage
        # model cannot discover host credentials or authenticate a push.
        # A private PID namespace with its own procfs is part of that boundary:
        # without it the host `/proc` stays visible and the triage model can
        # read `/proc/<pid>/environ` of the orchestrator or of any other
        # same-UID process and recover the credentials scrubbed from its own
        # environment.
        triage_sandbox_args=(
            bwrap
            --ro-bind / /
            --unshare-pid
            --proc /proc
            --die-with-parent
            --dev /dev
            --bind "$triage_wt" "$triage_wt"
            --ro-bind "$triage_sandbox_config" "$triage_wt/.git/config"
            --tmpfs "$triage_home"
            --setenv HOME "$triage_home"
            # `gh` resolves its configuration directory as `GH_CONFIG_DIR`,
            # then `$XDG_CONFIG_HOME/gh`, and only then `$HOME/.config/gh`.
            # Redirecting `XDG_CONFIG_HOME` into the private home closes that
            # middle path: without it a triage turn can still run
            # `gh auth token` on a host that uses an XDG configuration layout.
            --setenv XDG_CONFIG_HOME "$triage_home/.config"
            --setenv GIT_CONFIG_GLOBAL /dev/null
            --setenv GIT_CONFIG_SYSTEM /dev/null
            --setenv GIT_CONFIG_NOSYSTEM 1
            --setenv GIT_TERMINAL_PROMPT 0
            --setenv GIT_ASKPASS /bin/false
        )
        # `HOME` above gives Git an empty configuration chain. Hide the host
        # credential locations too, so an agent cannot read a token directly
        # and embed it in a push URL.
        [[ ! -d "$HOME/.config/gh" ]] || triage_sandbox_args+=(--tmpfs "$HOME/.config/gh")
        [[ ! -d "$HOME/.config/git" ]] || triage_sandbox_args+=(--tmpfs "$HOME/.config/git")
        [[ ! -d "$HOME/.ssh" ]] || triage_sandbox_args+=(--tmpfs "$HOME/.ssh")
        [[ ! -e "$HOME/.gitconfig" ]] || triage_sandbox_args+=(--ro-bind /dev/null "$HOME/.gitconfig")
        [[ ! -e "$HOME/.git-credentials" ]] || triage_sandbox_args+=(--ro-bind /dev/null "$HOME/.git-credentials")
        [[ ! -e "$HOME/.netrc" ]] || triage_sandbox_args+=(--ro-bind /dev/null "$HOME/.netrc")
        [[ "${GH_CONFIG_DIR:-}" != /* || ! -d "$GH_CONFIG_DIR" ]] || triage_sandbox_args+=(--tmpfs "$GH_CONFIG_DIR")
        # The redirected `XDG_CONFIG_HOME` above hides these from `gh` and Git,
        # but mask the host directories as well so their absolute paths stay
        # unreadable even when the agent opens them directly.
        if [[ "${XDG_CONFIG_HOME:-}" == /* ]]; then
            [[ ! -d "$XDG_CONFIG_HOME/gh" ]] || triage_sandbox_args+=(--tmpfs "$XDG_CONFIG_HOME/gh")
            [[ ! -d "$XDG_CONFIG_HOME/git" ]] || triage_sandbox_args+=(--tmpfs "$XDG_CONFIG_HOME/git")
        fi
        # `--ro-bind / /` keeps the outer checkout readable by absolute path,
        # and its real Git configuration can carry authentication (a standard
        # `actions/checkout` run stores an `http.*.extraheader` token there).
        # Only the triage clone's own config is replaced with the sanitized
        # copy above, so mask the worker's common and per-worktree config
        # files too; the main repository shares the same common config.
        local host_git_config host_worktree_config
        host_git_config=$(git -C "$wt" rev-parse --path-format=absolute --git-path config) || return 1
        host_worktree_config=$(git -C "$wt" rev-parse --path-format=absolute --git-path config.worktree) || return 1
        [[ ! -e "$host_git_config" ]] || triage_sandbox_args+=(--ro-bind /dev/null "$host_git_config")
        [[ ! -e "$host_worktree_config" ]] || triage_sandbox_args+=(--ro-bind /dev/null "$host_worktree_config")
        if [[ "$AGENT" == "codex" ]]; then
            if [[ "$CUSTOM_KEY" == 1 ]]; then
                triage_agent_home="$codex_home"
            else
                triage_agent_home="${CODEX_HOME:-$HOME/.codex}"
            fi
            # Give triage a disposable copy of Codex state. The sandbox may
            # write auth, plugin, and MCP state, but none of it can reach the
            # worker's real `CODEX_HOME` or survive the triage phase.
            triage_codex_home="$triage_wt/.triage-codex-home"
            rm -rf "$triage_codex_home"
            mkdir -p "$triage_codex_home" || return 1
            [[ ! -d "$triage_agent_home" ]] || cp -a "$triage_agent_home/." "$triage_codex_home" || return 1
            triage_sandbox_args+=(--bind "$triage_codex_home" "$triage_home/.codex")
            triage_sandbox_args+=(--setenv CODEX_HOME "$triage_home/.codex")
        fi
    else
        triage_sandbox_args=()
    fi

    while :; do
        iter=$(( iter + 1 ))
        phase_iter=$(( phase_iter + 1 ))
        now=$(date +%s)
        remaining=$(( deadline - now ))
        (( remaining > 0 )) || { ec=124; break; }
        (( phase_iter > MAX_CONTINUE )) && break

        if [[ "$phase" == "triage" ]]; then
            active_model="$TRIAGE_MODEL"
            active_wt="$triage_wt"
            # The outer worktree is read-only in the Bubblewrap namespace and
            # does not contain the merge being validated. Keep triage builds
            # in its writable private clone instead.
            mkdir -p "$active_wt/build" || return 1
            build_steer="A persistent, ccache-backed build directory for this worktree is at ${active_wt}/build. Reuse it for any build - do not delete it; let ninja rebuild incrementally - and build only the affected targets. ccache is shared and warm across all workers (CCACHE_DIR=${CCACHE_DIR}), so a rebuild after merging master should be far faster than a cold build; never run a full from-scratch rebuild when an incremental one suffices."
            system_prompt="$STEER_PROMPT $build_steer $TRIAGE_STEER_PROMPT"
            turn_prompt="$TRIAGE_NUDGE_PROMPT"
        else
            active_model="$MODEL"
            active_wt="$wt"
            build_steer="A persistent, ccache-backed build directory for this worktree is at ${active_wt}/build. Reuse it for any build - do not delete it; let ninja rebuild incrementally - and build only the affected targets. ccache is shared and warm across all workers (CCACHE_DIR=${CCACHE_DIR}), so a rebuild after merging master should be far faster than a cold build; never run a full from-scratch rebuild when an incremental one suffices."
            system_prompt="$STEER_PROMPT $build_steer"
            turn_prompt="$NUDGE_PROMPT"
        fi
        model_args=()
        [[ -n "$active_model" ]] && model_args=(--model "$active_model")
        active_codex_env=("${codex_env[@]}")
        triage_git_args=()
        if [[ "$phase" == "triage" ]]; then
            triage_git_args=(
                env -u GH_TOKEN -u GITHUB_TOKEN -u GITHUB_PAT -u GH_CONFIG_DIR
                -u SSH_AUTH_SOCK -u GIT_SSH_COMMAND -u GIT_SSH -u GIT_CONFIG -u GIT_CONFIG_PARAMETERS -u GIT_CONFIG_COUNT
            )
            # Command-scope Git configuration is injected through numbered
            # `GIT_CONFIG_KEY_*` / `GIT_CONFIG_VALUE_*` variables. It takes
            # precedence over the sanitized clone config, so do not inherit
            # any of it into the triage sandbox.
            local git_config_var
            for git_config_var in "${!GIT_CONFIG_KEY_@}" "${!GIT_CONFIG_VALUE_@}"; do
                triage_git_args+=(-u "$git_config_var")
            done
            # Bubblewrap supplies the private `CODEX_HOME`; do not override it
            # with the host-side custom-key directory after entering the sandbox.
            active_codex_env=()
            last_message="$active_wt/.continue-pr-last-message"
        else
            last_message="$log.last"
        fi

        echo "===== turn $iter ($phase model ${active_model:-default}, session ${sid:-pending}, ${remaining}s budget left) =====" >> "$log"
        ec=0
        if [[ "$AGENT" == "claude" ]]; then
            if (( phase_iter == 1 )); then
                sid=$(cat /proc/sys/kernel/random/uuid 2>/dev/null \
                    || uuidgen 2>/dev/null \
                    || python3 -c 'import uuid; print(uuid.uuid4())')
                prompt="/continue-pr-auto $url"
                if [[ -n "$handoff" ]]; then
                    prompt+=$'\n\nThe triage model handed off this task. Validate its diagnosis, then complete the work:\n'
                    prompt+="$handoff"
                fi
                ( cd "$active_wt" && "${triage_sandbox_args[@]}" "${triage_git_args[@]}" timeout "$remaining" claude --dangerously-skip-permissions --print \
                    --output-format json --effort "$EFFORT" "${model_args[@]}" \
                    --session-id "$sid" --append-system-prompt "$system_prompt" \
                    "$prompt"</dev/null ) > "$log.json" 2>"$log.err" || ec=$?
            else
                ( cd "$active_wt" && "${triage_sandbox_args[@]}" "${triage_git_args[@]}" timeout "$remaining" claude --dangerously-skip-permissions --print \
                    --output-format json --effort "$EFFORT" "${model_args[@]}" \
                    --resume "$sid" --append-system-prompt "$system_prompt" \
                    "$turn_prompt"</dev/null ) > "$log.json" 2>"$log.err" || ec=$?
            fi

            # Extract the final message text and accumulate token/cost usage.
            if jq -e . "$log.json" >/dev/null 2>&1; then
                jq -r '.result // ""' "$log.json" > "$log.last"
                usage=$(jq -r '[(.usage.input_tokens//0),(.usage.output_tokens//0),(.usage.cache_creation_input_tokens//0),(.usage.cache_read_input_tokens//0),(.total_cost_usd//0)]|@tsv' "$log.json" 2>/dev/null)
                IFS=$'\t' read -r u_i u_o u_ci u_co u_cost <<< "$usage"
                triage_cost=0
                coding_cost=0
                if [[ "$phase" == "triage" ]]; then triage_cost="${u_cost:-0}"; else coding_cost="${u_cost:-0}"; fi
                stats_add 0 0 0 "${u_i:-0}" "${u_o:-0}" "${u_ci:-0}" "${u_co:-0}" "$triage_cost" "$coding_cost"
            else
                cat "$log.err" 2>/dev/null > "$log.last" || true
            fi
        else
            rm -f "$last_message"
            if (( phase_iter == 1 )); then
                prompt="/continue-pr-auto $url

${system_prompt}"
                if [[ -n "$handoff" ]]; then
                    prompt+=$'\n\nThe triage model handed off this task. Validate its diagnosis, then complete the work:\n'
                    prompt+="$handoff"
                fi
                ( cd "$active_wt" && "${triage_sandbox_args[@]}" "${triage_git_args[@]}" env "${active_codex_env[@]}" timeout "$remaining" codex exec \
                    --dangerously-bypass-approvals-and-sandbox --json \
                    --config "model_reasoning_effort=$EFFORT" "${model_args[@]}" \
                    --output-last-message "$last_message" - <<< "$prompt" \
                ) > "$log.json" 2>"$log.err" || ec=$?
                sid=$(jq -Rrs '[splits("\n") | fromjson? | select(.type == "thread.started") | .thread_id][0] // empty' "$log.json" 2>/dev/null || true)
                if [[ -z "$sid" ]] && (( ec == 0 )); then
                    echo "Codex did not report a session ID" >> "$log.err"
                    ec=1
                fi
            else
                ( cd "$active_wt" && "${triage_sandbox_args[@]}" "${triage_git_args[@]}" env "${active_codex_env[@]}" timeout "$remaining" codex exec resume \
                    --dangerously-bypass-approvals-and-sandbox --json \
                    --config "model_reasoning_effort=$EFFORT" "${model_args[@]}" \
                    --output-last-message "$last_message" "$sid" - <<< "$turn_prompt" \
                ) > "$log.json" 2>"$log.err" || ec=$?
            fi

            # Codex emits JSONL and does not currently report cost. Its cached
            # input tokens map to the status bar's cache-read counter.
            if [[ -s "$log.json" ]]; then
                usage=$(jq -Rrs '
                    [splits("\n") | fromjson?] as $events
                    | [([$events[] | select(.type == "turn.completed") | (.usage.input_tokens // 0)] | add // 0),
                     ([$events[] | select(.type == "turn.completed") | (.usage.output_tokens // 0)] | add // 0),
                     ([$events[] | select(.type == "turn.completed") | (.usage.cache_write_input_tokens // 0)] | add // 0),
                     ([$events[] | select(.type == "turn.completed") | (.usage.cached_input_tokens // 0)] | add // 0),
                     0] | @tsv' "$log.json" 2>/dev/null)
                IFS=$'\t' read -r u_i u_o u_ci u_co u_cost <<< "$usage"
                configure_codex_pricing "$active_model"
                u_cost=$(estimate_codex_cost "${u_i:-0}" "${u_co:-0}" "${u_o:-0}" "${u_ci:-0}")
                triage_cost=0
                coding_cost=0
                if [[ "$phase" == "triage" ]]; then triage_cost="${u_cost:-0}"; else coding_cost="${u_cost:-0}"; fi
                stats_add 0 0 0 "${u_i:-0}" "${u_o:-0}" "${u_ci:-0}" "${u_co:-0}" "$triage_cost" "$coding_cost"
            fi
            if [[ "$phase" == "triage" && -f "$last_message" ]]; then
                cp "$last_message" "$log.last" || return 1
            fi
            if [[ ! -s "$log.last" ]]; then
                {
                    jq -Rrs -r 'splits("\n") | fromjson? | select(.type == "error") | .message // empty' "$log.json" 2>/dev/null
                    sed '/^Reading .*from stdin/d' "$log.err" 2>/dev/null || true
                } > "$log.last"
            fi
        fi
        cat "$log.last" >> "$log"

        # A triage handoff starts a fresh session with the coding model but keeps
        # the same worktree and deadline. Check it before DONE_MARKER so an
        # accidental extra completion marker cannot suppress requested coding.
        if [[ "$phase" == "triage" ]] && grep -qE "^${HANDOFF_MARKER}[[:space:]]*$" "$log.last"; then
            handoff=$(cat "$log.last")
            if triage_state_is_safe "$triage_wt" "$triage_start_head" "$triage_base_head"; then
                triage_current_head=$(git -C "$triage_wt" rev-parse HEAD) || return 1
                if [[ "$triage_current_head" != "$triage_start_head" ]]; then
                    recreate_validated_triage_merge "$wt" "$triage_start_head" "$triage_base_head" "$triage_wt" || return 1
                fi
            else
                discard_untrusted_triage_changes "$triage_wt" "$triage_start_head" "$log"
            fi
            echo "===== handoff from $TRIAGE_MODEL to $MODEL =====" >> "$log"
            rm -rf "$triage_codex_home"
            phase="coding"
            triage_sandbox_args=()
            phase_iter=0
            sid=""
            continue
        fi

        # Done when the worker emits the marker on its own line; also stop on a
        # hard failure or timeout. A `SIGKILL` can affect all concurrent Codex
        # processes at once (for example, an external resource manager). Once
        # Codex reports a session ID, resume that session within the existing
        # turn and time limits instead of discarding its completed work.
        if grep -qE "^${DONE_MARKER}[[:space:]]*$" "$log.last"; then
            if [[ "$phase" != "triage" ]] || triage_state_is_safe "$triage_wt" "$triage_start_head" "$triage_base_head"; then
                if [[ "$phase" == "triage" ]]; then
                    triage_current_head=$(git -C "$triage_wt" rev-parse HEAD) || return 1
                    # An unchanged triage worktree did not perform the only
                    # update triage is allowed to make. It is safe, but there
                    # is nothing to recreate or push.
                    [[ "$triage_current_head" != "$triage_start_head" ]] || break
                    recreate_validated_triage_merge "$wt" "$triage_start_head" "$triage_base_head" "$triage_wt" || return 1
                    if [[ "$triage_pushable" != "1" ]]; then
                        handoff=$(cat "$log.last")
                        echo "===== automatic handoff from $TRIAGE_MODEL to $MODEL because the validated merge cannot be pushed =====" >> "$log"
                        rm -rf "$triage_codex_home"
                        phase="coding"
                        triage_sandbox_args=()
                        phase_iter=0
                        sid=""
                        continue
                    fi
                fi
                if [[ "$phase" == "triage" ]] && ! git -C "$wt" push "$triage_push_url" "HEAD:refs/heads/$triage_head_ref" >> "$log" 2>&1; then
                    handoff=$(cat "$log.last")
                    echo "===== automatic handoff from $TRIAGE_MODEL to $MODEL after the validated triage update could not be pushed =====" >> "$log"
                    rm -rf "$triage_codex_home"
                    phase="coding"
                    triage_sandbox_args=()
                    phase_iter=0
                    sid=""
                    continue
                fi
                break
            fi
            handoff=$(cat "$log.last")
            discard_untrusted_triage_changes "$triage_wt" "$triage_start_head" "$log"
            echo "===== automatic handoff from $TRIAGE_MODEL to $MODEL after non-mechanical triage changes =====" >> "$log"
            rm -rf "$triage_codex_home"
            phase="coding"
            triage_sandbox_args=()
            phase_iter=0
            sid=""
            continue
        fi
        # If triage used its continuation allowance without an explicit marker,
        # escalate to the coding model rather than misclassifying the PR as
        # handled. A killed Codex session can also take this handoff path on
        # its last permitted triage turn; otherwise the next iteration would
        # reach the cap before resetting its nonzero exit code.
        if [[ "$phase" == "triage" ]] && (( phase_iter == MAX_CONTINUE )); then
            if (( ec == 0 )) || { [[ "$AGENT" == "codex" && -n "$sid" ]] && (( ec == 137 )); }; then
                handoff=$(cat "$log.last")
                if triage_state_is_safe "$triage_wt" "$triage_start_head" "$triage_base_head"; then
                    triage_current_head=$(git -C "$triage_wt" rev-parse HEAD) || return 1
                    if [[ "$triage_current_head" != "$triage_start_head" ]]; then
                        recreate_validated_triage_merge "$wt" "$triage_start_head" "$triage_base_head" "$triage_wt" || return 1
                    fi
                else
                    discard_untrusted_triage_changes "$triage_wt" "$triage_start_head" "$log"
                fi
                echo "===== automatic handoff from $TRIAGE_MODEL to $MODEL after $MAX_CONTINUE turns =====" >> "$log"
                rm -rf "$triage_codex_home"
                phase="coding"
                triage_sandbox_args=()
                phase_iter=0
                sid=""
                continue
            fi
        fi

        if [[ "$AGENT" == "codex" && -n "$sid" ]] && (( ec == 137 )); then
            echo "Codex was killed; resuming session $sid on the next turn." >> "$log"
            continue
        fi
        (( ec != 0 )) && break
    done

    [[ -z "$codex_home" ]] || rm -rf "$codex_home"
    [[ -z "${triage_codex_home:-}" ]] || rm -rf "$triage_codex_home"
    return "$ec"
}

process_pr()
{
    local i="$1" wt="$2" number="$3" title="$4"
    local color ts log ec outcome status mark summary cleanup_failed=0
    local before_sha after pr_state pr_mergeable pr_review after_sha pushed

    color=$(pr_color_seq "$number")
    ts=$(date +%H:%M:%S)
    emit "$color" "$ts  ->  worker $i  ASSIGNED  PR #$number  $title"

    if (( DRY_RUN )); then
        sleep $(( (RANDOM % 3) + 1 + (number % 3) ))
        outcome="DRY-RUN"
        mark=".. "
        status="DRY-RUN (not processed)"
        summary="(dry run)"
    else
        # Clean the reused worker before any per-PR work. Besides isolating
        # PRs from each other's leftovers, this keeps the worktree safe for
        # direct writes such as `recreate_validated_triage_merge`, which must
        # not inherit untracked files from a previous failed PR.
        log="$LOGDIR/pr-$number.log"
        : > "$log"
        if ! prepare_worktree_for_task "$wt" >> "$log" 2>&1; then
            printf 'Worktree cleanup failed before starting PR #%s.\n' "$number" > "$log.last"
            cat "$log.last" >> "$log"
            ec=1
            cleanup_failed=1
            printf '%s\t%s\n' "$i" "$wt" >> "$CLEANUP_FAILURE_FILE"
        else
            ec=0
        fi

        # PR head before the work, so we can tell whether the worker actually
        # pushed anything (a clean agent exit does NOT imply progress: the
        # /continue-pr-auto skill exits 0 when it finds nothing to do, or when it
        # punts an outward-facing decision such as closing an obsolete PR).
        before_sha=$(gh pr view "$number" --repo "$REPO" --json headRefOid \
            --jq '.headRefOid' 2>/dev/null || echo "")

        if (( ec == 0 )); then
            run_continue_pr "$wt" "$number" "$log" || ec=$?
        fi

        # Detach HEAD so the PR branch isn't held by this worktree, letting a
        # different worker check it out in a later round.
        git -C "$wt" checkout --detach -q 2>/dev/null || true

        after=$(gh pr view "$number" --repo "$REPO" \
            --json state,mergeable,reviewDecision,headRefOid \
            --jq '"\(.state)\t\(.mergeable)\t\(.reviewDecision // "NONE")\t\(.headRefOid)"' \
            2>/dev/null || printf 'UNKNOWN\tUNKNOWN\tNONE\t')
        IFS=$'\t' read -r pr_state pr_mergeable pr_review after_sha <<< "$after"

        pushed=0
        if [[ -n "$before_sha" && -n "$after_sha" && "$before_sha" != "$after_sha" ]]; then
            pushed=1
        fi

        # Classify the outcome. A clean exit is split into PUSHED (made
        # progress) vs NO-CHANGE (nothing pushed); NO-CHANGE while still
        # CONFLICTING means the PR needs a human decision (e.g. resolve a huge
        # conflict, or close as obsolete) -> flagged NEEDS-ATTENTION, not OK.
        if   (( ec == 124 )); then              outcome="TIMEOUT";          mark="XX "
        elif (( ec != 0 ));   then              outcome="FAILED(exit $ec)"; mark="XX "
        elif [[ "$pr_state" == MERGED ]]; then  outcome="MERGED";           mark="OK "
        elif [[ "$pr_state" == CLOSED ]]; then  outcome="CLOSED";           mark="OK "
        elif (( pushed ));    then              outcome="PUSHED";           mark="OK "
        elif [[ "$pr_mergeable" == CONFLICTING ]]; then outcome="NEEDS-ATTENTION"; mark="!! "
        else                                    outcome="NO-CHANGE";        mark=".. "
        fi

        status="$outcome; state=$pr_state mergeable=$pr_mergeable review=$pr_review"
        summary=$(summarize_log "$log.last")
    fi

    # Count the PR as processed ok (clean run) or not (errored/timed out).
    case "$outcome" in
        FAILED*|TIMEOUT) stats_add 0 0 1 0 0 0 0 0 0 ;;
        *)               stats_add 0 1 0 0 0 0 0 0 0 ;;
    esac
    [[ "$outcome" == NEEDS-ATTENTION ]] && na_add "$number"

    ts=$(date +%H:%M:%S)
    emit "$color" "$ts  $mark  worker $i  FINISHED  PR #$number  $title  --  $status"
    emit "$color" "            ^- $summary"
    (( cleanup_failed == 0 ))
}

worker()
{
    local i="$1" wt="$2"
    local line number title

    # The parent enables job control only to place this worker in its own
    # process group. Disable it inside the worker so `timeout`, the agent, and
    # agent-started commands stay in that same group rather than creating a
    # nested foreground process group that the interrupt trap cannot reach.
    set +m

    # Per-worker file descriptor for the queue lock (its own open-file
    # description, so flock mutually excludes between workers).
    exec 9>"$LOCKFILE"

    while true; do
        flock 9
        line=$(head -n 1 "$QUEUEFILE" 2>/dev/null || true)
        if [[ -n "$line" ]]; then
            tail -n +2 "$QUEUEFILE" > "$QUEUEFILE.tmp" 2>/dev/null || true
            mv -f "$QUEUEFILE.tmp" "$QUEUEFILE"
        fi
        flock -u 9

        [[ -z "$line" ]] && break

        IFS=$'\t' read -r number title <<< "$line"
        # A cleanup failure makes this worktree unsafe for another assignment
        # in the same round. Leave the remaining queue to healthy workers
        # instead of reporting the same failure for every subsequent PR.
        process_pr "$i" "$wt" "$number" "$title" || break
    done

    exec 9>&-
}

# Return 0 (true) if a "related" PR looks abandoned: nobody other than me has
# acted on it (pushed a commit, commented, or reviewed) since `cutoff`. If the
# activity data can't be fetched, treat it as active (skip) - fail closed rather
# than jump into a PR we can't assess.
related_is_abandoned()
{
    local number="$1" cutoff="$2" verdict
    verdict=$(gh pr view "$number" --repo "$REPO" --json commits,comments,reviews 2>/dev/null \
        | jq -r --arg me "$GH_USER" --arg cutoff "$cutoff" '
            def others:
              ([ .commits[]?  | select((.authors // [] | map(.login) | index($me)) | not) | .committedDate ]
             + [ .comments[]? | select(.author.login  != $me) | .createdAt ]
             + [ .reviews[]?  | select(.author.login  != $me) | .submittedAt ]);
            (others | map(select(. != null)) | max) as $last
            | if ($last == null) or ($last < $cutoff) then "abandoned" else "active" end
          ' 2>/dev/null)
    [[ "$verdict" == "abandoned" ]]
}

# Load exclude-authors.txt into EXCLUDED_AUTHOR (lowercased logins).
load_excluded_authors()
{
    [[ -r "$EXCLUDE_AUTHORS_FILE" ]] || return 0
    local line
    while IFS= read -r line || [[ -n "$line" ]]; do
        line="${line%%#*}"                 # strip trailing comment
        line="${line//[[:space:]]/}"       # strip all whitespace
        [[ -n "$line" ]] && EXCLUDED_AUTHOR["${line,,}"]=1
    done < "$EXCLUDE_AUTHORS_FILE"
}

fetch_prs()
{
    if [[ -n "${CONTINUE_ALL_PRS_PRS_FILE:-}" ]]; then
        cat "$CONTINUE_ALL_PRS_PRS_FILE"
        return 0
    fi

    # Search the selected categories of open PRs that involve me:
    #   --mine      -> authored by me            (--author)     -> always processed
    #   --assigned  -> assigned to me            (--assignee)   -> always processed
    #   --related   -> contributed to by me      (--commenter, --reviewed-by)
    # Each result is tagged: "always" (mine/assigned) or related-only. Results
    # are unioned, `hold`-labeled PRs dropped, collapsed to one record per PR
    # (keeping "always" if it matched any always category), and sorted by last
    # update (oldest first). Related-only PRs are then kept only if they look
    # abandoned - no activity by anyone but me within RELATED_STALE_DAYS - and
    # their author is not in exclude-authors.txt.
    local cutoff candidates
    cutoff=$(date -u -d "${RELATED_STALE_DAYS} days ago" +%Y-%m-%dT%H:%M:%SZ)

    candidates=$( {
        if (( MODE_MINE )); then
            gh search prs --repo "$REPO" --state open --author @me --limit 1000 \
                --json number,title,updatedAt,labels,author | jq -c 'map(. + {always:true})'
        fi
        if (( MODE_ASSIGNED )); then
            gh search prs --repo "$REPO" --state open --assignee @me --limit 1000 \
                --json number,title,updatedAt,labels,author | jq -c 'map(. + {always:true})'
        fi
        if (( MODE_RELATED )); then
            gh search prs --repo "$REPO" --state open --commenter @me --limit 1000 \
                --json number,title,updatedAt,labels,author | jq -c 'map(. + {always:false})'
            gh search prs --repo "$REPO" --state open --reviewed-by @me --limit 1000 \
                --json number,title,updatedAt,labels,author | jq -c 'map(. + {always:false})'
        fi
    } | jq -s -r '
        add
        | map(select((.labels // []) | map(.name) | index("hold") | not))
        | group_by(.number)
        | map({ number:    .[0].number,
                title:     .[0].title,
                author:    (.[0].author.login // ""),
                updatedAt: (map(.updatedAt) | max),
                always:    (any(.[]; .always)) })
        | sort_by(.updatedAt)
        | .[] | [ .number, (.always | tostring), .author, .updatedAt, .title ] | @tsv' )

    local number always author updatedAt title
    while IFS=$'\t' read -r number always author updatedAt title; do
        [[ -n "$number" ]] || continue
        if [[ "$always" == "true" ]]; then
            # mine / assigned to me -> always processed, regardless of author.
            printf '%s\t%s\n' "$number" "$title"
            continue
        fi
        # related-only: skip if the author is excluded from --related updates.
        [[ -n "${EXCLUDED_AUTHOR[${author,,}]:-}" ]] && continue
        if [[ "$updatedAt" < "$cutoff" ]]; then
            # No activity by anyone (including me) in the window -> abandoned.
            printf '%s\t%s\n' "$number" "$title"
        elif related_is_abandoned "$number" "$cutoff"; then
            printf '%s\t%s\n' "$number" "$title"
        fi
    done <<< "$candidates"
}

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

GH_USER=""
if [[ -z "${CONTINUE_ALL_PRS_PRS_FILE:-}" ]]; then
    GH_USER="$(gh api user --jq '.login')"
fi

maybe_color_hint
load_excluded_authors

modes=()
(( MODE_MINE ))     && modes+=("mine")
(( MODE_ASSIGNED )) && modes+=("assigned")
(( MODE_RELATED ))  && modes+=("related")
MODES_DESC=$(IFS=,; echo "${modes[*]}")
MODES_DESC=${MODES_DESC//,/, }

banner "Main repo:       $MAIN_REPO"
banner "Workers:         $WORKERS"
banner "Worktree base:   ${WORKTREE_BASE}-{0..$((WORKERS - 1))}"
[[ -n "$GH_USER" ]] && banner "GitHub user:     $GH_USER"
banner "Selecting:       $MODES_DESC"
(( MODE_RELATED )) && (( ${#EXCLUDED_AUTHOR[@]} )) && banner "Excluded (related): ${!EXCLUDED_AUTHOR[*]}"
if [[ -n "$TRIAGE_MODEL" ]]; then
    banner "Per-PR timeout:  ${TIMEOUT}s (shared across up to ${MAX_CONTINUE} turns per model)"
else
    banner "Per-PR timeout:  ${TIMEOUT}s (shared across up to ${MAX_CONTINUE} turns)"
fi
banner "ccache:          ${CCACHE_DIR} (max ${CCACHE_MAXSIZE})"
banner "Agent:           ${AGENT}"
if [[ -n "$TRIAGE_MODEL" ]]; then
    banner "Triage model:    ${TRIAGE_MODEL}"
    banner "Coding model:    ${MODEL}"
elif [[ -n "$MODEL" ]]; then
    banner "Model:           ${MODEL}"
fi
if [[ "$AGENT" == "codex" ]]; then
    if [[ -n "$TRIAGE_MODEL" ]]; then
        configure_codex_pricing "$TRIAGE_MODEL"
        if [[ -n "$CODEX_INPUT_PRICE" ]]; then
            banner "Triage pricing:  input \$${CODEX_INPUT_PRICE}, cached \$${CODEX_CACHED_INPUT_PRICE}, output \$${CODEX_OUTPUT_PRICE} per MTok"
        else
            banner "Triage pricing:  unavailable for ${TRIAGE_MODEL}; cost will exclude its usage"
        fi
        configure_codex_pricing "$MODEL"
        if [[ -n "$CODEX_INPUT_PRICE" ]]; then
            banner "Coding pricing:  input \$${CODEX_INPUT_PRICE}, cached \$${CODEX_CACHED_INPUT_PRICE}, output \$${CODEX_OUTPUT_PRICE} per MTok"
        else
            banner "Coding pricing:  unavailable for ${MODEL}; cost will exclude its usage"
        fi
    elif [[ -n "$CODEX_INPUT_PRICE" ]]; then
        banner "Pricing:         input \$${CODEX_INPUT_PRICE}, cached \$${CODEX_CACHED_INPUT_PRICE}, output \$${CODEX_OUTPUT_PRICE} per MTok"
    else
        banner "Pricing:         unavailable for ${MODEL:-configured default}; cost will exclude Codex usage"
    fi
fi
banner "Effort:          ${EFFORT}"
banner "Disk reserve:    ${MIN_FREE_GB} GiB (managed worktrees are cleaned below this)"
(( CUSTOM_KEY )) && banner "API key:         custom (…${API_KEY: -4})"
(( DRY_RUN )) && banner "DRY RUN: not creating worktrees or running /continue-pr-auto"
echo ""

mkdir -p "$LOGDIR"
START=$(date +%s)
stats_init

# Per-worker worktree paths.
declare -a WT
for (( i = 0; i < WORKERS; i++ )); do
    WT[i]="${WORKTREE_BASE}-${i}"
done

# Create worktrees up front (unless dry-running).
if (( ! DRY_RUN )); then
    cleanup_worktrees_if_disk_low
    for (( i = 0; i < WORKERS; i++ )); do
        ensure_worktree "${WT[i]}"
    done
    echo ""
fi

QUEUEFILE="$(mktemp "$LOGDIR/queue.XXXXXX")"
LOCKFILE="$(mktemp "$LOGDIR/lock.XXXXXX")"

# Pin the status bar to the bottom of the terminal (TTY only).
if (( SHOW_STATUS )) && [[ -t 1 ]]; then STATUS_ENABLED=1; fi
status_start

ROUND=0
while true; do
    cleanup_worktrees_if_disk_low
    rm -f "$CLEANUP_FAILURE_FILE"
    ROUND=$((ROUND + 1))
    stats_add 1 0 0 0 0 0 0 0 0
    na_reset
    banner "===== Round ${ROUND}: fetching open PRs ====="

    PRS="$(fetch_prs || true)"

    if [[ -z "$PRS" ]]; then
        banner "No open PRs found. Sleeping 60s before retrying..."
        (( ONCE )) && break
        sleep 60
        continue
    fi

    COUNT=$(printf '%s\n' "$PRS" | grep -c . || true)
    banner "Round ${ROUND}: ${COUNT} PR(s) to process across ${WORKERS} worker(s)"
    echo ""

    printf '%s\n' "$PRS" > "$QUEUEFILE"

    # Job control gives every asynchronous worker its own process group. This
    # lets the interrupt trap terminate the worker and all of its descendants
    # immediately instead of waiting for a running agent command to return.
    set -m
    WORKER_PIDS=()
    for (( i = 0; i < WORKERS; i++ )); do
        worker "$i" "${WT[i]}" &
        WORKER_PIDS+=($!)
    done
    set +m
    wait "${WORKER_PIDS[@]}" || true
    WORKER_PIDS=()

    if [[ -s "$CLEANUP_FAILURE_FILE" ]]; then
        banner "Worktree cleanup failed; stopping instead of retrying in another round"
        exit 1
    fi

    echo ""
    banner "===== Round ${ROUND} complete ====="
    echo ""

    (( ONCE )) && break
done
