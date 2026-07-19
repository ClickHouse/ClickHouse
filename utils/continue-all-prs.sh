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
# A clean `claude` exit does not by itself mean progress, so the status is based
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
# Options:
#   --workers N           Number of parallel workers / worktrees (default: 1).
#   --mine                Select PRs you authored.
#   --assigned            Select PRs assigned to you.
#   --related             Select PRs you've contributed to (commented/reviewed),
#                         but only ones that look abandoned - no activity by
#                         anyone but you in the last week (RELATED_STALE_DAYS).
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
#   --effort LEVEL        Reasoning effort for each worker `claude` (--effort);
#                         default: medium.
#   --no-status           Disable the persistent bottom status bar (elapsed,
#                         rounds, ok/fail counts, cost, token totals). The bar is
#                         shown by default on a TTY.
#   --max-continue N      Max `claude` turns per PR. The worker runs once and is
#                         then resumed (same session) until it signals it is done
#                         or this cap is hit (default: 4). The worker is told not
#                         to run anything in the background and to push before
#                         ending each turn, so prepared changes don't get stranded
#                         unpushed.
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

REPO="ClickHouse/ClickHouse"

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
EFFORT="medium"        # reasoning effort passed to each worker `claude` (--effort)
SHOW_STATUS=1          # show the persistent bottom status bar (TTY only; --no-status disables)

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
        --effort)         EFFORT="$2"; shift 2 ;;
        --no-status)      SHOW_STATUS=0; shift ;;
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

MAIN_REPO="$(git rev-parse --show-toplevel)"
[[ -n "$WORKTREE_BASE" ]] || WORKTREE_BASE="${MAIN_REPO}-prworker"

# No mode flag given -> select all categories (the default behavior).
if (( ! MODE_ANY )); then
    MODE_MINE=1; MODE_ASSIGNED=1; MODE_RELATED=1
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

# Terminal writes are serialized on fd 6 (the TTY lock) against the status-bar
# redraw, so scrolling log lines never land on the reserved status line.
emit()
{
    # emit <color-seq> <message>
    local line
    if (( COLOR )); then line="${1}${2}${RESET}"; else line="$2"; fi
    if (( ${STATUS_ENABLED:-0} )); then
        { flock 6; printf '%s\n' "$line"; } 6>>"${TTYLOCK:-/dev/null}"
    else
        printf '%s\n' "$line"
    fi
}

banner()
{
    if (( ${STATUS_ENABLED:-0} )); then
        { flock 6; echo "${S}$*${R}"; } 6>>"${TTYLOCK:-/dev/null}"
    else
        echo "${S}$*${R}"
    fi
}

# ----------------------------------------------------------------------------
# Status bar: a persistent reverse-video line pinned to the bottom of the
# terminal (via a DECSTBM scroll region) showing elapsed time, rounds,
# ok/fail PR counts, total cost, and token totals. Counters live in $STATSFILE
# (updated under $STATSLOCK); a background updater redraws the bar. TTY only.
# ----------------------------------------------------------------------------
STATUS_ENABLED=0
STATUS_PID=""
START=0

stats_init() { printf '0 0 0 0 0 0 0 0\n' > "$STATSFILE"; }

# stats_add <d_rounds> <d_ok> <d_fail> <d_in> <d_out> <d_cachein> <d_cacheout> <d_cost>
stats_add()
{
    { flock 7
      local cur; cur=$(cat "$STATSFILE" 2>/dev/null || echo '0 0 0 0 0 0 0 0')
      awk -v cur="$cur" -v r="$1" -v s="$2" -v f="$3" -v i="$4" -v o="$5" -v ci="$6" -v co="$7" -v c="$8" \
        'BEGIN { split(cur, x, " ");
                 printf "%d %d %d %d %d %d %d %.6f\n",
                        x[1]+r, x[2]+s, x[3]+f, x[4]+i, x[5]+o, x[6]+ci, x[7]+co, x[8]+c }' \
        > "$STATSFILE"
    } 7>>"$STATSLOCK"
}

humanize() { awk -v n="$1" 'BEGIN{ if(n>=1e9)printf"%.2fG",n/1e9; else if(n>=1e6)printf"%.2fM",n/1e6; else if(n>=1e3)printf"%.1fk",n/1e3; else printf"%d",n }'; }
fmt_elapsed() { local s=$1; printf '%02d:%02d:%02d' $((s/3600)) $(((s%3600)/60)) $((s%60)); }

render_status()
{
    local cur r s f i o ci co c now el
    cur=$(cat "$STATSFILE" 2>/dev/null || echo '0 0 0 0 0 0 0 0')
    read -r r s f i o ci co c <<< "$cur"
    now=$(date +%s); el=$(( now - START ))
    printf 'continue-all-prs | %s | round %d | ok %d fail %d | $%.2f | in %s out %s cache-in %s cache-out %s' \
        "$(fmt_elapsed "$el")" "$r" "$s" "$f" "$c" \
        "$(humanize "$i")" "$(humanize "$o")" "$(humanize "$ci")" "$(humanize "$co")"
}

draw_status()
{
    (( STATUS_ENABLED )) || return 0
    local rows cols text
    rows=$(tput lines 2>/dev/null || echo 24)
    cols=$(tput cols 2>/dev/null || echo 100)
    text=$(render_status); text=${text:0:cols}
    # save cursor (DECSC), go to last row, clear it, print reverse-video, restore (DECRC)
    { flock 6; printf '\0337\033[%d;1H\033[2K\033[7m%s\033[0m\0338' "$rows" "$text"; } 6>>"$TTYLOCK"
}

status_updater() { while :; do draw_status; sleep "${STATUS_INTERVAL:-2}"; done; }

status_start()
{
    (( STATUS_ENABLED )) || return 0
    local rows; rows=$(tput lines 2>/dev/null || echo 24)
    # reserve the bottom row: scroll region = rows 1..(rows-1); park the cursor inside it
    printf '\033[1;%dr\033[%d;1H' "$(( rows - 1 ))" "$(( rows - 1 ))"
    draw_status
    status_updater & STATUS_PID=$!
}

status_stop()
{
    (( STATUS_ENABLED )) || return 0
    [[ -n "$STATUS_PID" ]] && kill "$STATUS_PID" 2>/dev/null || true
    local rows; rows=$(tput lines 2>/dev/null || echo 24)
    # reset the scroll region to the full screen and clear the status line
    printf '\033[r\033[%d;1H\033[2K' "$rows"
    STATUS_ENABLED=0
}

# Distill a one-to-two sentence summary of what the worker did from its log.
# In plain `--print` mode `claude` writes only its final message to the log, so
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
ensure_worktree()
{
    local wt="$1"

    if git -C "$MAIN_REPO" worktree list --porcelain | grep -qxF "worktree $wt"; then
        banner "Reusing existing worktree: $wt"
        return 0
    fi
    if [[ -e "$wt" ]]; then
        banner "Path exists but is not a registered worktree, reusing as-is: $wt"
        return 0
    fi

    banner "Creating worktree: $wt"
    git -C "$MAIN_REPO" worktree add --no-checkout --detach "$wt" HEAD

    if (( SKIP_SUBMODULES )); then
        git -C "$wt" -c checkout.workers=0 -c core.fsync=none -c gc.auto=0 checkout -q -f HEAD -- .
    else
        setup_worktree_submodules "$wt"
    fi
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
TTYLOCK="$LOGDIR/tty.lock"

cleanup()
{
    status_stop   # reset the scroll region and kill the updater first
    [[ -n "${QUEUEFILE:-}" ]] && rm -f "$QUEUEFILE" "$QUEUEFILE.tmp" 2>/dev/null || true
    [[ -n "${LOCKFILE:-}" ]] && rm -f "$LOCKFILE" 2>/dev/null || true
    rm -f "$STATSLOCK" "$TTYLOCK" 2>/dev/null || true
}
trap cleanup EXIT
trap 'echo; banner "Interrupted, stopping..."; exit 130' INT TERM

# Marker the worker prints (on its own line) when it considers the PR finished.
DONE_MARKER='<<<CONTINUE-PR-DONE>>>'

# Appended to the system prompt of every turn. Forbids backgrounding work (the
# root cause of merges that were prepared but never pushed: the worker started a
# build in the background and ended its turn waiting for a notification that
# never comes in single-shot `--print` mode).
STEER_PROMPT="You are running in a non-interactive, single-shot batch session. Do NOT run any commands in the background and do NOT defer work expecting to be notified later - there is no later notification, and any background process is killed when your turn ends. Run builds, tests, and every long-running command synchronously in the foreground so they finish within your turn, and complete ALL work - including pushing your commits with 'git push' - before you end your turn. A passing (green) CI does NOT mean the PR is done: always fetch and address unresolved review comments and reviewer feedback - including automated or bot reviews such as clickhouse-gh[bot], and review threads that are merely COMMENTED rather than blocking - even when every check passes or the PR is already approved. Do not signal done while there are unaddressed review comments, unless addressing them genuinely requires a human decision. If the PR is CONFLICTING, resolve the conflicts (merge the base branch, resolve, and rework to build) and push whenever you can push - your own PRs, and fork PRs where maintainerCanModify is true; a contested/reserved/superseded note does not block mechanical conflict resolution. If you cannot push (e.g. a fork with maintainer edits disabled), supersede it: open your own PR from the main repo (crediting the author; re-author the commits yourself if the author has not signed the CLA) and close theirs with a comment linking the new PR - unless the change is obsolete, already fixed, or already superseded, in which case say so specifically rather than a bare 'needs attention'. When, and only when, the PR is fully handled (changes pushed, or you have determined that no change is needed or that it needs a human decision), end your final message with a line containing exactly: ${DONE_MARKER}"

# Sent on each resume to nudge the worker to finish.
NUDGE_PROMPT="Continue where you left off and finish the task. Reminder: do not use background tasks - run everything synchronously and push your commits before finishing. A green CI does NOT mean you are done - also address unresolved review comments and reviewer feedback (including automated/bot reviews and COMMENTED, non-blocking threads). Any build started in a previous turn was killed when that turn ended; re-run it in the foreground if you still need to verify. When the PR is fully handled, end your final message with a line containing exactly: ${DONE_MARKER}"

# Run /continue-pr-auto in a worktree, resuming the same session until the worker
# signals completion (DONE_MARKER), the per-PR time budget (TIMEOUT, shared
# across all turns) is exhausted, or the continuation cap (MAX_CONTINUE) is hit.
# Writes the full transcript to $log and the final turn to $log.last. Returns
# the exit code of the last turn (124 if the time budget was exhausted).
run_continue_pr()
{
    local wt="$1" number="$2" log="$3"
    local url="https://github.com/$REPO/pull/$number"
    local sid deadline iter ec now remaining build_steer
    sid=$(cat /proc/sys/kernel/random/uuid 2>/dev/null \
        || uuidgen 2>/dev/null \
        || python3 -c 'import uuid; print(uuid.uuid4())')
    # Steer the worker to a persistent, ccache-backed build directory in this
    # worktree so rebuilds are incremental instead of cold each pass.
    build_steer="A persistent, ccache-backed build directory for this worktree is at ${wt}/build. Reuse it for any build - do not delete it; let ninja rebuild incrementally - and build only the affected targets. ccache is shared and warm across all workers (CCACHE_DIR=${CCACHE_DIR}), so a rebuild after merging master should be far faster than a cold build; never run a full from-scratch rebuild when an incremental one suffices."
    deadline=$(( $(date +%s) + TIMEOUT ))
    : > "$log"
    iter=0
    ec=0

    while :; do
        iter=$(( iter + 1 ))
        now=$(date +%s)
        remaining=$(( deadline - now ))
        (( remaining > 0 )) || { ec=124; break; }
        (( iter > MAX_CONTINUE )) && break

        echo "===== turn $iter (session $sid, ${remaining}s budget left) =====" >> "$log"
        ec=0
        if (( iter == 1 )); then
            ( cd "$wt" && timeout "$remaining" claude --dangerously-skip-permissions --print \
                --output-format json --effort "$EFFORT" \
                --session-id "$sid" --append-system-prompt "$STEER_PROMPT $build_steer" \
                "/continue-pr-auto $url"</dev/null ) > "$log.json" 2>"$log.err" || ec=$?
        else
            ( cd "$wt" && timeout "$remaining" claude --dangerously-skip-permissions --print \
                --output-format json --effort "$EFFORT" \
                --resume "$sid" --append-system-prompt "$STEER_PROMPT $build_steer" \
                "$NUDGE_PROMPT"</dev/null ) > "$log.json" 2>"$log.err" || ec=$?
        fi

        # Extract the final message text and accumulate token/cost usage. On a
        # timeout/crash the JSON is empty or partial, so fall back to stderr.
        if jq -e . "$log.json" >/dev/null 2>&1; then
            jq -r '.result // ""' "$log.json" > "$log.last"
            local usage u_i u_o u_ci u_co u_cost
            usage=$(jq -r '[(.usage.input_tokens//0),(.usage.output_tokens//0),(.usage.cache_creation_input_tokens//0),(.usage.cache_read_input_tokens//0),(.total_cost_usd//0)]|@tsv' "$log.json" 2>/dev/null)
            IFS=$'\t' read -r u_i u_o u_ci u_co u_cost <<< "$usage"
            stats_add 0 0 0 "${u_i:-0}" "${u_o:-0}" "${u_ci:-0}" "${u_co:-0}" "${u_cost:-0}"
        else
            { cat "$log.err" 2>/dev/null || true; } > "$log.last"
        fi
        cat "$log.last" >> "$log"

        # Done when the worker emits the marker on its own line; also stop on any
        # hard failure or timeout of a turn.
        grep -qE "^${DONE_MARKER}[[:space:]]*$" "$log.last" && break
        (( ec != 0 )) && break
    done

    return "$ec"
}

process_pr()
{
    local i="$1" wt="$2" number="$3" title="$4"
    local color ts log ec outcome status mark summary
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
        # PR head before the work, so we can tell whether the worker actually
        # pushed anything (a clean `claude` exit does NOT imply progress: the
        # /continue-pr-auto skill exits 0 when it finds nothing to do, or when it
        # punts an outward-facing decision such as closing an obsolete PR).
        before_sha=$(gh pr view "$number" --repo "$REPO" --json headRefOid \
            --jq '.headRefOid' 2>/dev/null || echo "")

        log="$LOGDIR/pr-$number.log"
        ec=0
        run_continue_pr "$wt" "$number" "$log" || ec=$?

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
        FAILED*|TIMEOUT) stats_add 0 0 1 0 0 0 0 0 ;;
        *)               stats_add 0 1 0 0 0 0 0 0 ;;
    esac

    ts=$(date +%H:%M:%S)
    emit "$color" "$ts  $mark  worker $i  FINISHED  PR #$number  $title  --  $status"
    emit "$color" "            ^- $summary"
}

worker()
{
    local i="$1" wt="$2"
    local line number title

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
        process_pr "$i" "$wt" "$number" "$title" || true
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
    # abandoned - no activity by anyone but me within RELATED_STALE_DAYS.
    local cutoff candidates
    cutoff=$(date -u -d "${RELATED_STALE_DAYS} days ago" +%Y-%m-%dT%H:%M:%SZ)

    candidates=$( {
        if (( MODE_MINE )); then
            gh search prs --repo "$REPO" --state open --author @me --limit 1000 \
                --json number,title,updatedAt,labels | jq -c 'map(. + {always:true})'
        fi
        if (( MODE_ASSIGNED )); then
            gh search prs --repo "$REPO" --state open --assignee @me --limit 1000 \
                --json number,title,updatedAt,labels | jq -c 'map(. + {always:true})'
        fi
        if (( MODE_RELATED )); then
            gh search prs --repo "$REPO" --state open --commenter @me --limit 1000 \
                --json number,title,updatedAt,labels | jq -c 'map(. + {always:false})'
            gh search prs --repo "$REPO" --state open --reviewed-by @me --limit 1000 \
                --json number,title,updatedAt,labels | jq -c 'map(. + {always:false})'
        fi
    } | jq -s -r '
        add
        | map(select((.labels // []) | map(.name) | index("hold") | not))
        | group_by(.number)
        | map({ number:    .[0].number,
                title:     .[0].title,
                updatedAt: (map(.updatedAt) | max),
                always:    (any(.[]; .always)) })
        | sort_by(.updatedAt)
        | .[] | [ .number, (.always | tostring), .updatedAt, .title ] | @tsv' )

    local number always updatedAt title
    while IFS=$'\t' read -r number always updatedAt title; do
        [[ -n "$number" ]] || continue
        if [[ "$always" == "true" ]]; then
            printf '%s\t%s\n' "$number" "$title"
        elif [[ "$updatedAt" < "$cutoff" ]]; then
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
banner "Per-PR timeout:  ${TIMEOUT}s (shared across up to ${MAX_CONTINUE} turns)"
banner "ccache:          ${CCACHE_DIR} (max ${CCACHE_MAXSIZE})"
banner "Effort:          ${EFFORT}"
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
    ROUND=$((ROUND + 1))
    stats_add 1 0 0 0 0 0 0 0
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

    pids=()
    for (( i = 0; i < WORKERS; i++ )); do
        worker "$i" "${WT[i]}" &
        pids+=($!)
    done
    wait "${pids[@]}" || true

    echo ""
    banner "===== Round ${ROUND} complete ====="
    echo ""

    (( ONCE )) && break
done
