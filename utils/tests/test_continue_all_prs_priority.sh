#!/usr/bin/env bash
set -euo pipefail

# Exercises the priority ordering in continue-all-prs.sh:
#   * PRs are processed highest-priority first (blocker > pr-critical-bugfix > rest);
#   * the round advances to a lower priority only when the current one makes no
#     progress;
#   * as soon as a priority makes progress the round skips the lower priorities.
#
# GitHub is mocked (CONTINUE_ALL_PRS_PRS_FILE supplies the PR list with a labels
# column, and a mock `gh`/`claude` on PATH stand in for the real tools). A PR
# whose number is listed in MOCK_PUSHED reports a changed head after the run, so
# it is classified PUSHED (progress); all others are NO-CHANGE.

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
mkdir -p "$repo/tmp"
scratch=$(mktemp -d "$repo/tmp/test-continue-all-prs-priority.XXXXXX")
bin="$scratch/bin"
worktree_base="$scratch/worktree"
pr_file="$scratch/prs"

export CONTINUE_ALL_PRS_MIN_FREE_GB=1

cleanup()
{
    git -C "$repo" worktree remove --force "${worktree_base}-0" 2>/dev/null || true
    rm -rf "$scratch"
}
trap cleanup EXIT

mkdir -p "$bin"

# One PR per priority tier: #101 blocker, #102 pr-critical-bugfix, #103 plain.
printf '%s\n' \
    $'101\tBlocker PR\tblocker' \
    $'102\tCritical bugfix PR\tpr-critical-bugfix' \
    $'103\tOrdinary PR\t' > "$pr_file"

# Mock `gh pr view`: returns the head SHA before the run and a state tuple after.
# A PR in MOCK_PUSHED gets a different "after" SHA, so the script sees a push.
printf '%s\n' \
    '#!/usr/bin/env bash' \
    'if [[ "${1:-}" == pr && "${2:-}" == view ]]; then' \
    '    n="$3"' \
    '    after="sha-$n"' \
    '    case " ${MOCK_PUSHED:-} " in *" $n "*) after="sha-$n-new" ;; esac' \
    '    if [[ "$*" == *state,mergeable* ]]; then' \
    '        printf "OPEN\tMERGEABLE\tNONE\t%s\n" "$after"' \
    '    else' \
    '        printf "sha-%s\n" "$n"' \
    '    fi' \
    '    exit 0' \
    'fi' \
    'exit 0' > "$bin/gh"
chmod +x "$bin/gh"

# Mock `claude`: emits a single JSON result that signals completion immediately.
printf '%s\n' \
    '#!/usr/bin/env bash' \
    'printf "%s\n" "{\"result\":\"handled\n<<<CONTINUE-PR-DONE>>>\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"cache_creation_input_tokens\":0,\"cache_read_input_tokens\":0},\"total_cost_usd\":0}"' > "$bin/claude"
chmod +x "$bin/claude"

run()
{
    # run <log> [MOCK_PUSHED value]
    local log="$1" pushed="${2:-}"
    PATH="$bin:$PATH" CONTINUE_ALL_PRS_PRS_FILE="$pr_file" MOCK_PUSHED="$pushed" \
        "$repo/utils/continue-all-prs.sh" --agent claude --timeout 60 --once \
            --skip-submodules --no-status --worktree-base "$worktree_base" --color never \
            > "$log" 2>&1
}

# The line number at which "ASSIGNED  PR #<n>" appears (empty if not processed).
assigned_line() { grep -n "ASSIGNED  PR #$2" "$1" | head -n1 | cut -d: -f1; }

# ---------------------------------------------------------------------------
# 1. No progress anywhere: every priority is visited, in order, all PRs run.
# ---------------------------------------------------------------------------
run "$scratch/all.log"

grep -q 'Priority 0 (blocker): 1 PR'            "$scratch/all.log"
grep -q 'Priority 1 (pr-critical-bugfix): 1 PR' "$scratch/all.log"
grep -q 'Priority 2 (other): 1 PR'              "$scratch/all.log"
grep -q 'Priority 0 (blocker) needs no action; advancing'            "$scratch/all.log"
grep -q 'Priority 1 (pr-critical-bugfix) needs no action; advancing' "$scratch/all.log"

l101=$(assigned_line "$scratch/all.log" 101)
l102=$(assigned_line "$scratch/all.log" 102)
l103=$(assigned_line "$scratch/all.log" 103)
[[ -n "$l101" && -n "$l102" && -n "$l103" ]] || { echo 'Expected all three PRs to run' >&2; exit 1; }
(( l101 < l102 && l102 < l103 )) || { echo 'PRs were not processed in priority order' >&2; exit 1; }

# ---------------------------------------------------------------------------
# 2. Top priority makes progress: lower priorities are skipped this round.
# ---------------------------------------------------------------------------
run "$scratch/blocker-progress.log" 101

grep -q 'Priority 0 (blocker) made progress; skipping lower priorities' "$scratch/blocker-progress.log"
[[ -n "$(assigned_line "$scratch/blocker-progress.log" 101)" ]] || { echo 'Blocker PR should have run' >&2; exit 1; }
if grep -q 'ASSIGNED  PR #102' "$scratch/blocker-progress.log" \
    || grep -q 'ASSIGNED  PR #103' "$scratch/blocker-progress.log"; then
    echo 'Lower-priority PRs must be skipped once the blocker made progress' >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# 3. Progress at the middle priority: the top advances, the rest is skipped.
# ---------------------------------------------------------------------------
run "$scratch/critical-progress.log" 102

grep -q 'Priority 0 (blocker) needs no action; advancing'              "$scratch/critical-progress.log"
grep -q 'Priority 1 (pr-critical-bugfix) made progress; skipping'      "$scratch/critical-progress.log"
[[ -n "$(assigned_line "$scratch/critical-progress.log" 101)" ]] || { echo 'Blocker PR should have run' >&2; exit 1; }
[[ -n "$(assigned_line "$scratch/critical-progress.log" 102)" ]] || { echo 'Critical-bugfix PR should have run' >&2; exit 1; }
if grep -q 'ASSIGNED  PR #103' "$scratch/critical-progress.log"; then
    echo 'The lowest priority must be skipped once the middle one made progress' >&2
    exit 1
fi

echo 'test_continue_all_prs_priority: OK'
