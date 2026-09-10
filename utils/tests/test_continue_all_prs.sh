#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
mkdir -p "$repo/tmp"
scratch=$(mktemp -d "$repo/tmp/test-continue-all-prs.XXXXXX")
bin="$scratch/bin"
worktree_base="$scratch/worktree"
pr_file="$scratch/prs"
runner_pid=""

# Keep this test independent of the host filesystem capacity. The production
# default is deliberately high, but these assertions do not cover low-space
# cleanup.
export CONTINUE_ALL_PRS_MIN_FREE_GB=1

cleanup()
{
    [[ -z "$runner_pid" ]] || kill "$runner_pid" 2>/dev/null || true
    git -C "$repo" worktree remove --force "${worktree_base}-0" 2>/dev/null || true
    rm -rf "$scratch"
}
trap cleanup EXIT

mkdir -p "$bin"
printf '1\tMocked PR\n' > "$pr_file"

printf '%s\n' '#!/usr/bin/env bash' 'printf "{\\"headRefOid\\":\\"mock-sha\\"}\\n"' > "$bin/gh"
chmod +x "$bin/gh"

printf '%s\n' \
    '#!/usr/bin/env bash' \
    'set -euo pipefail' \
    'if [[ "$1" == exec ]]; then' \
    '    mkdir -p "$CODEX_HOME"' \
    '    printf key > "$CODEX_HOME/auth.json"' \
    '    [[ -z "${CODEX_TEST_READY:-}" ]] || : > "$CODEX_TEST_READY"' \
    '    if [[ -n "${CODEX_TEST_EXEC_SLEEP:-}" ]]; then sleep "$CODEX_TEST_EXEC_SLEEP"; exit 0; fi' \
    '    while [[ $# -gt 0 ]]; do' \
    '        if [[ "$1" == --output-last-message ]]; then printf "<<<CONTINUE-PR-DONE>>>\\n" > "$2"; break; fi' \
    '        shift' \
    '    done' \
    '    printf "{\\"type\\":\\"thread.started\\",\\"thread_id\\":\\"mock\\"}\\n"' \
    '    exit 0' \
    'fi' \
    'output_last_message=""' \
    'for (( i = 1; i <= $#; ++i )); do' \
    '    if [[ "${!i}" == "--output-last-message" ]]; then' \
    '        next=$(( i + 1 ))' \
    '        output_last_message="${!next}"' \
    '    fi' \
    'done' \
    'if [[ "$1" == exec && "$2" == resume ]]; then' \
    '    printf "%s\\n" "<<<CONTINUE-PR-DONE>>>" > "$output_last_message"' \
    '    printf "%s\\n" "{\\"type\\":\\"turn.completed\\"}"' \
    '    exit 0' \
    'fi' \
    'if [[ "$1" == exec ]]; then' \
    '    printf "%s\\n" "working" > "$output_last_message"' \
    '    printf "%s\\n" "{\\"type\\":\\"thread.started\\",\\"thread_id\\":\\"mock-session\\"}"' \
    '    exit 0' \
    'fi' \
    'exit 0' > "$bin/codex"
chmod +x "$bin/codex"

PATH="$bin:$PATH" CONTINUE_ALL_PRS_PRS_FILE="$pr_file" CODEX_TEST_EXEC_SLEEP=10 \
    "$repo/utils/continue-all-prs.sh" --agent codex --api-key test-key --timeout 1 --once \
        --skip-submodules --no-status --worktree-base "$worktree_base" --color never > "$scratch/timeout.log" 2>&1

grep -q 'TIMEOUT' "$scratch/timeout.log"
[[ ! -e "${worktree_base}-0/tmp/continue-all-prs/codex-home/auth.json" ]]

worker="${worktree_base}-0"
submodule="$worker/contrib/FP16"
git clone --quiet --shared "$repo/contrib/FP16" "$submodule"
git -C "$submodule" checkout --detach --quiet HEAD~1
printf 'dirty\n' > "$submodule/continue-all-prs-test-untracked"
[[ -n "$(git -C "$submodule" status --short)" ]]

PATH="$bin:$PATH" CONTINUE_ALL_PRS_PRS_FILE="$pr_file" \
    "$repo/utils/continue-all-prs.sh" --agent codex --api-key test-key --timeout 1 --once \
        --skip-submodules --no-status --worktree-base "$worktree_base" --color never > "$scratch/submodule-cleanup.log" 2>&1

[[ -z "$(git -C "$worker" status --short)" ]]
[[ -z "$(git -C "$submodule" status --short)" ]]
[[ "$(git -C "$submodule" rev-parse HEAD)" == "$(git -C "$worker" rev-parse HEAD:contrib/FP16)" ]]

relative_worktree_base=${worktree_base#"$repo/"}
git -C "$repo" worktree remove --force "${worktree_base}-0" 2>/dev/null || true
rm -rf "${worktree_base}-0"
git -C "$repo" worktree add --no-checkout --detach "${worktree_base}-0" HEAD
(
    cd "$repo"
    PATH="$bin:$PATH" CONTINUE_ALL_PRS_PRS_FILE="$pr_file" \
        "$repo/utils/continue-all-prs.sh" --agent codex --timeout 10 --once --skip-submodules --no-status \
            --worktree-base "$relative_worktree_base" --color never
) > "$scratch/relative-reuse.log" 2>&1
grep -q "Reusing existing worktree: ${worktree_base}-0" "$scratch/relative-reuse.log"

unregistered_worktree_base="$scratch/unregistered-worktree"
mkdir -p "${unregistered_worktree_base}-0"
if PATH="$bin:$PATH" CONTINUE_ALL_PRS_PRS_FILE="$pr_file" \
    "$repo/utils/continue-all-prs.sh" --agent codex --timeout 1 --once \
        --skip-submodules --no-status --worktree-base "$unregistered_worktree_base" --color never \
        > "$scratch/unregistered-worktree.log" 2>&1; then
    echo 'Expected an unregistered worker path to be rejected' >&2
    exit 1
fi
grep -q 'path exists but is not a registered worktree' "$scratch/unregistered-worktree.log"
rmdir "${unregistered_worktree_base}-0"

# A superseding PR creates a replacement branch on origin. The worker hook must
# allow that new ref while still enforcing ancestry for existing refs.
git -C "$repo" rev-parse HEAD > "$scratch/local-oid"
local_oid=$(<"$scratch/local-oid")
printf 'refs/heads/continue-pr-1-replacement %s refs/heads/continue-pr-1-replacement 0000000000000000000000000000000000000000\n' "$local_oid" |
    "$repo/utils/continue-all-prs-hooks/pre-push"
grep -q 'PUSH_MODE=supersede' "$repo/.claude/skills/continue-pr-auto/SKILL.md"
grep -q 'PUSH_BRANCH="continue-pr-${PR_NUMBER}-<short-desc>"' "$repo/.claude/skills/continue-pr-auto/SKILL.md"

PATH="$bin:$PATH" CONTINUE_ALL_PRS_PRS_FILE="$pr_file" CODEX_TEST_EXEC_SLEEP=30 \
    CODEX_TEST_READY="$scratch/login-ready" "$repo/utils/continue-all-prs.sh" --agent codex --api-key test-key \
        --timeout 60 --once --skip-submodules --no-status --worktree-base "$worktree_base" --color never \
        > "$scratch/interrupt.log" 2>&1 &
runner_pid=$!

for _ in {1..100}; do
    [[ -e "$scratch/login-ready" ]] && break
    sleep 0.05
done
[[ -e "$scratch/login-ready" ]]
kill -TERM "$runner_pid"
wait "$runner_pid" || true
runner_pid=""

[[ ! -e "${worktree_base}-0/tmp/continue-all-prs/codex-home/auth.json" ]]
