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
    'if [[ "$1" == login ]]; then' \
    '    mkdir -p "$CODEX_HOME"' \
    '    printf key > "$CODEX_HOME/auth.json"' \
    '    [[ -z "${CODEX_TEST_READY:-}" ]] || : > "$CODEX_TEST_READY"' \
    '    sleep "${CODEX_TEST_LOGIN_SLEEP:-0}"' \
    '    exit 0' \
    'fi' \
    'exit 0' > "$bin/codex"
chmod +x "$bin/codex"

PATH="$bin:$PATH" CONTINUE_ALL_PRS_PRS_FILE="$pr_file" CODEX_TEST_LOGIN_SLEEP=10 \
    "$repo/utils/continue-all-prs.sh" --agent codex --api-key test-key --timeout 1 --once \
        --skip-submodules --no-status --worktree-base "$worktree_base" --color never > "$scratch/timeout.log" 2>&1

grep -q 'TIMEOUT' "$scratch/timeout.log"
[[ ! -e "${worktree_base}-0/tmp/continue-all-prs/codex-home/auth.json" ]]

# Shutdown must also remove the disposable triage `CODEX_HOME`, including when
# the regular per-PR cleanup has not run yet.
source <(sed -n '/^cleanup_worker_codex_auth()/,/^}/p' "$repo/utils/continue-all-prs.sh")
cleanup_wt="$scratch/cleanup-worktree"
mkdir -p "$cleanup_wt/tmp/continue-all-prs/codex-home" \
    "$cleanup_wt/tmp/continue-all-prs/triage-repository/.triage-codex-home"
: > "$cleanup_wt/tmp/continue-all-prs/codex-home/auth.json"
: > "$cleanup_wt/tmp/continue-all-prs/triage-repository/.triage-codex-home/auth.json"
AGENT=codex
CUSTOM_KEY=1
WT=("$cleanup_wt")
cleanup_worker_codex_auth
[[ ! -e "$cleanup_wt/tmp/continue-all-prs/codex-home/auth.json" ]]
[[ ! -e "$cleanup_wt/tmp/continue-all-prs/triage-repository/.triage-codex-home/auth.json" ]]

# Load and exercise the sandbox-config helper directly. The mounted config
# must not retain credentials embedded in remotes or URL-rewrite rules.
source <(sed -n '/^prepare_triage_sandbox_config()/,/^}/p' "$repo/utils/continue-all-prs.sh")
triage_repo="$scratch/triage-repository"
git init -q "$triage_repo"
git -C "$triage_repo" remote add origin 'https://user:SECRET_TOKEN@example.com/ClickHouse/ClickHouse.git'
git -C "$triage_repo" remote set-url --push origin 'https://user:SECRET_TOKEN@example.com/ClickHouse/ClickHouse.git'
git -C "$triage_repo" config url.'https://user:SECRET_TOKEN@example.com/'.insteadOf 'https://example.com/'
triage_config=$(REPO='ClickHouse/ClickHouse' prepare_triage_sandbox_config "$triage_repo" "$scratch/triage-git-config")
[[ "$(git config --file "$triage_config" --get remote.origin.url)" == 'https://github.com/ClickHouse/ClickHouse.git' ]]
! git config --file "$triage_config" --get-regexp '^remote\..*\.(url|pushurl)$' | grep -v '^remote\.origin\.url '
! git config --file "$triage_config" --get-regexp '^url\..*\.(insteadof|pushinsteadof)$'

PATH="$bin:$PATH" CONTINUE_ALL_PRS_PRS_FILE="$pr_file" CODEX_TEST_LOGIN_SLEEP=30 \
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
