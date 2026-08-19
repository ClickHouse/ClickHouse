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
    'if [[ "$1" == login ]]; then' \
    '    mkdir -p "$CODEX_HOME"' \
    '    printf key > "$CODEX_HOME/auth.json"' \
    '    [[ -z "${CODEX_TEST_READY:-}" ]] || : > "$CODEX_TEST_READY"' \
    '    sleep "${CODEX_TEST_LOGIN_SLEEP:-0}"' \
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

PATH="$bin:$PATH" CONTINUE_ALL_PRS_PRS_FILE="$pr_file" CODEX_TEST_LOGIN_SLEEP=10 \
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
# must not retain credentials embedded in remotes, URL-rewrite rules,
# URL-scoped credential helpers, or SSH-command overrides.
source <(sed -n '/^prepare_triage_sandbox_config()/,/^}/p' "$repo/utils/continue-all-prs.sh")
triage_repo="$scratch/triage-repository"
git init -q "$triage_repo"
git -C "$triage_repo" remote add origin 'https://user:SECRET_TOKEN@example.com/ClickHouse/ClickHouse.git'
git -C "$triage_repo" remote set-url --push origin 'https://user:SECRET_TOKEN@example.com/ClickHouse/ClickHouse.git'
git -C "$triage_repo" config url.'https://user:SECRET_TOKEN@example.com/'.insteadOf 'https://example.com/'
git -C "$triage_repo" config credential.https://github.com.helper test-helper
git -C "$triage_repo" config core.sshCommand test-helper
triage_config=$(REPO='ClickHouse/ClickHouse' prepare_triage_sandbox_config "$triage_repo" "$scratch/triage-git-config")
[[ "$(git config --file "$triage_config" --get remote.origin.url)" == 'https://github.com/ClickHouse/ClickHouse.git' ]]
! git config --file "$triage_config" --get-regexp '^remote\..*\.(url|pushurl)$' | grep -v '^remote\.origin\.url '
! git config --file "$triage_config" --get-regexp '^url\..*\.(insteadof|pushinsteadof)$'
! git config --file "$triage_config" --get-regexp '^credential(\..*)?\.helper$'
! git config --file "$triage_config" --get core.sshCommand

# Command-scope Git config has higher priority than the sanitized clone config.
# The triage environment must remove every numbered carrier, not only
# `GIT_CONFIG` itself.
command_scope_git_args=(env -u GIT_CONFIG -u GIT_CONFIG_PARAMETERS -u GIT_CONFIG_COUNT)
for command_scope_git_var in GIT_CONFIG_KEY_0 GIT_CONFIG_VALUE_0 GIT_CONFIG_KEY_1 GIT_CONFIG_VALUE_1; do
    command_scope_git_args+=(-u "$command_scope_git_var")
done
if env GIT_CONFIG_COUNT=2 \
    GIT_CONFIG_KEY_0=credential.helper GIT_CONFIG_VALUE_0=test-helper \
    GIT_CONFIG_KEY_1=http.https://github.com/.extraheader GIT_CONFIG_VALUE_1='Authorization: Basic secret' \
    "${command_scope_git_args[@]}" git -c "include.path=$triage_config" config --get credential.helper; then
        echo 'Expected command-scope credential helper to be removed' >&2
        exit 1
    fi
if env GIT_CONFIG_COUNT=2 \
    GIT_CONFIG_KEY_0=credential.helper GIT_CONFIG_VALUE_0=test-helper \
    GIT_CONFIG_KEY_1=http.https://github.com/.extraheader GIT_CONFIG_VALUE_1='Authorization: Basic secret' \
    "${command_scope_git_args[@]}" git -c "include.path=$triage_config" config --get http.https://github.com/.extraheader; then
        echo 'Expected command-scope HTTP header to be removed' >&2
        exit 1
    fi

# Older Git versions use `GIT_CONFIG_PARAMETERS` rather than numbered
# variables for command-scope configuration. It must be scrubbed as well.
if env GIT_CONFIG_PARAMETERS="'credential.helper'='test-helper' 'http.https://github.com/.extraheader'='Authorization: Basic secret'" \
    "${command_scope_git_args[@]}" git -c "include.path=$triage_config" config --get credential.helper; then
        echo 'Expected GIT_CONFIG_PARAMETERS credential helper to be removed' >&2
        exit 1
    fi
if env GIT_CONFIG_PARAMETERS="'credential.helper'='test-helper' 'http.https://github.com/.extraheader'='Authorization: Basic secret'" \
    "${command_scope_git_args[@]}" git -c "include.path=$triage_config" config --get http.https://github.com/.extraheader; then
        echo 'Expected GIT_CONFIG_PARAMETERS HTTP header to be removed' >&2
        exit 1
    fi

# Triage uses a private clone. After the PR head is checked out, it must still
# initialize submodules so it can build and inspect the complete source tree.
source <(sed -n '/^setup_triage_submodules()/,/^}/p' "$repo/utils/continue-all-prs.sh")
run_with_deadline()
{
    shift
    "$@"
}
triage_submodule_source="$scratch/triage-submodule-source"
triage_superproject="$scratch/triage-superproject"
triage_clone="$scratch/triage-clone"
git init -q "$triage_submodule_source"
git -C "$triage_submodule_source" config user.email test@example.com
git -C "$triage_submodule_source" config user.name test
printf 'triage submodule\n' > "$triage_submodule_source/content"
git -C "$triage_submodule_source" add content
git -C "$triage_submodule_source" commit -qm 'Add test content'
git init -q "$triage_superproject"
git -C "$triage_superproject" config user.email test@example.com
git -C "$triage_superproject" config user.name test
git -C "$triage_superproject" -c protocol.file.allow=always submodule add -q "$triage_submodule_source" dependency
git -C "$triage_superproject" commit -qam 'Add test submodule'
git clone -q --shared --no-checkout "$triage_superproject" "$triage_clone"
git -C "$triage_clone" checkout -q --detach HEAD
SKIP_SUBMODULES=0
GIT_CONFIG_COUNT=1
GIT_CONFIG_KEY_0=protocol.file.allow
GIT_CONFIG_VALUE_0=always
export GIT_CONFIG_COUNT GIT_CONFIG_KEY_0 GIT_CONFIG_VALUE_0
setup_triage_submodules "$triage_clone" $(( $(date +%s) + 30 ))
unset GIT_CONFIG_COUNT GIT_CONFIG_KEY_0 GIT_CONFIG_VALUE_0
[[ "$(<"$triage_clone/dependency/content")" == 'triage submodule' ]]

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

# SSH transport command overrides can authenticate an explicit SSH push. The
# triage environment must clear both supported environment carriers before
# entering the sandbox.
ssh_helper="$scratch/fake-ssh"
ssh_invocations="$scratch/fake-ssh-invocations"
printf '%s\n' '#!/usr/bin/env bash' "printf invoked >> '$ssh_invocations'" 'exit 1' > "$ssh_helper"
chmod +x "$ssh_helper"
triage_ssh_env=(env -u GIT_SSH_COMMAND -u GIT_SSH)
timeout 3 env GIT_SSH_COMMAND="$ssh_helper" "${triage_ssh_env[@]}" \
    git ls-remote ssh://git@example.com/ClickHouse/ClickHouse.git >/dev/null 2>&1 || true
[[ ! -e "$ssh_invocations" ]]
timeout 3 env GIT_SSH="$ssh_helper" "${triage_ssh_env[@]}" \
    git ls-remote ssh://git@example.com/ClickHouse/ClickHouse.git >/dev/null 2>&1 || true
[[ ! -e "$ssh_invocations" ]]
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
