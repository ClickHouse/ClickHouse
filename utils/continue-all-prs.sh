#!/usr/bin/env bash
set -euo pipefail

# Continuously advance every open pull request that involves you - the ones you
# authored, the ones assigned to you, and the ones you've contributed to
# (commented on or reviewed) - by fanning out a pool of workers, each running
# the `/continue-pr` skill in its own git worktree.
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
#                     human decision (resolve a huge conflict, or close as
#                     obsolete). A comment is posted on the PR (once) to flag it.
#   FAILED / TIMEOUT - the worker errored or hit the per-PR timeout
# A clean `claude` exit does not by itself mean progress, so the status is based
# on whether the PR head advanced rather than just the exit code.
#
# The per-PR lines are colored with a 24-bit (truecolor) color derived from a
# hash of the PR number, using the same YCbCr scheme ClickHouse uses to color
# its log messages (see base/base/terminalColors.cpp). The full `/continue-pr`
# transcript of each PR is written to a per-PR log file under
# tmp/continue-all-prs/ instead of the terminal.
#
# Usage:
#   utils/continue-all-prs.sh [--workers N] [options]
#
# Options:
#   --workers N           Number of parallel workers / worktrees (default: 1).
#   --worktree-base PATH  Base path for worker worktrees; worker i lives at
#                         "<PATH>-<i>". Default: "<main-repo>-prworker".
#   --timeout SECONDS     Per-PR timeout for `/continue-pr` (default: 3600).
#   --once                Process every open PR once and exit, instead of
#                         looping forever in rounds.
#   --skip-submodules     Create worker worktrees without hardlinking submodules
#                         (faster; only safe if `/continue-pr` won't build).
#   --color WHEN          auto (default) | always | never.
#   --dry-run             Don't touch worktrees or run `/continue-pr`; just list
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
TIMEOUT=3600
ONCE=0
SKIP_SUBMODULES=0
COLOR_WHEN="auto"
DRY_RUN=0

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
        --once)           ONCE=1; shift ;;
        --skip-submodules) SKIP_SUBMODULES=1; shift ;;
        --color)          COLOR_WHEN="$2"; shift 2 ;;
        --dry-run)        DRY_RUN=1; shift ;;
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
    if (( COLOR )); then
        printf '%s%s%s\n' "$1" "$2" "$RESET"
    else
        printf '%s\n' "$2"
    fi
}

banner() { echo "${S}$*${R}"; }

# Distill a one-to-two sentence summary of what the worker did from its log.
# In plain `--print` mode `claude` writes only its final message to the log, so
# the opening of that message is a natural summary. We take the first one or two
# sentences, but when those are conversational filler ("I've completed ...") we
# fall back to the first markdown header instead, since `/continue-pr` usually
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

cleanup()
{
    [[ -n "${QUEUEFILE:-}" ]] && rm -f "$QUEUEFILE" "$QUEUEFILE.tmp" 2>/dev/null || true
    [[ -n "${LOCKFILE:-}" ]] && rm -f "$LOCKFILE" 2>/dev/null || true
}
trap cleanup EXIT
trap 'echo; banner "Interrupted, stopping..."; exit 130' INT TERM

# Hidden marker so we comment at most once per PR (across rounds and runs).
NEEDS_ATTENTION_MARKER='<!-- continue-all-prs: needs-attention -->'

# Leave a comment on a PR that needs a human decision, unless we already did.
# Prints one of: posted | exists | failed.
post_needs_attention_comment()
{
    local number="$1" summary="$2" existing
    existing=$(gh pr view "$number" --repo "$REPO" --json comments \
        --jq '.comments[].body' 2>/dev/null || echo "")
    if printf '%s' "$existing" | grep -qF "$NEEDS_ATTENTION_MARKER"; then
        printf 'exists'
        return 0
    fi
    if gh pr comment "$number" --repo "$REPO" --body "$NEEDS_ATTENTION_MARKER
An automated \`/continue-pr\` pass could not advance this PR: it is still \`CONFLICTING\` and nothing was pushed, so it needs a manual decision - resolve the conflict against the base branch, or close it if it is obsolete.

Summary from the automated pass: $summary" >/dev/null 2>&1; then
        printf 'posted'
    else
        printf 'failed'
    fi
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
        # /continue-pr skill exits 0 when it finds nothing to do, or when it
        # punts an outward-facing decision such as closing an obsolete PR).
        before_sha=$(gh pr view "$number" --repo "$REPO" --json headRefOid \
            --jq '.headRefOid' 2>/dev/null || echo "")

        log="$LOGDIR/pr-$number.log"
        ec=0
        ( cd "$wt" && timeout "$TIMEOUT" claude --dangerously-skip-permissions --print \
            "/continue-pr https://github.com/$REPO/pull/$number" </dev/null ) \
            > "$log" 2>&1 || ec=$?

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
        summary=$(summarize_log "$log")

        # A PR that needs a human decision always gets a comment (posted once).
        if [[ "$outcome" == NEEDS-ATTENTION ]]; then
            case "$(post_needs_attention_comment "$number" "$summary")" in
                posted) status+="; commented" ;;
                exists) status+="; comment exists" ;;
                *)      status+="; comment failed" ;;
            esac
        fi
    fi

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

fetch_prs()
{
    if [[ -n "${CONTINUE_ALL_PRS_PRS_FILE:-}" ]]; then
        cat "$CONTINUE_ALL_PRS_PRS_FILE"
        return 0
    fi

    # Process every open PR that involves me, in three categories:
    #   * authored by me                 (--author)
    #   * assigned to me                 (--assignee)
    #   * contributed to by me           (--commenter, --reviewed-by)
    # The four searches are unioned, deduplicated by PR number, and sorted by
    # last update (oldest first), so a PR that matches several categories is
    # processed only once.
    {
        gh search prs --repo "$REPO" --state open --author      @me --limit 1000 --json number,title,updatedAt
        gh search prs --repo "$REPO" --state open --assignee    @me --limit 1000 --json number,title,updatedAt
        gh search prs --repo "$REPO" --state open --commenter   @me --limit 1000 --json number,title,updatedAt
        gh search prs --repo "$REPO" --state open --reviewed-by @me --limit 1000 --json number,title,updatedAt
    } | jq -s -r '
        add
        | unique_by(.number)
        | sort_by(.updatedAt)
        | .[] | "\(.number)\t\(.title)"
    '
}

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

GH_USER=""
if [[ -z "${CONTINUE_ALL_PRS_PRS_FILE:-}" ]]; then
    GH_USER="$(gh api user --jq '.login')"
fi

maybe_color_hint

banner "Main repo:       $MAIN_REPO"
banner "Workers:         $WORKERS"
banner "Worktree base:   ${WORKTREE_BASE}-{0..$((WORKERS - 1))}"
[[ -n "$GH_USER" ]] && banner "GitHub user:     $GH_USER (PRs authored / assigned / contributed)"
banner "Per-PR timeout:  ${TIMEOUT}s"
(( DRY_RUN )) && banner "DRY RUN: not creating worktrees or running /continue-pr"
echo ""

mkdir -p "$LOGDIR"

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

ROUND=0
while true; do
    ROUND=$((ROUND + 1))
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
