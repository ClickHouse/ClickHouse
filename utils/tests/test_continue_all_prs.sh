#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
mkdir -p "$repo/tmp"
scratch=$(mktemp -d "$repo/tmp/test-continue-all-prs.XXXXXX")
bin="$scratch/bin"
worktree_base="$scratch/worktree"
pr_file="$scratch/prs"
runner_pid=""

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
