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
[[ ! -e "${worktree_base}-0/tmp/continue-all-prs/codex-home" ]]

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
# The worker-local `CODEX_HOME` must not survive as a whole: `config.toml`,
# plugin, and MCP state written by one pull request would otherwise become the
# starting state of the next one on the same worker.
: > "$cleanup_wt/tmp/continue-all-prs/codex-home/config.toml"
: > "$cleanup_wt/tmp/continue-all-prs/triage-repository/.triage-codex-home/auth.json"
AGENT=codex
CUSTOM_KEY=1
WT=("$cleanup_wt")
cleanup_worker_codex_auth
[[ ! -e "$cleanup_wt/tmp/continue-all-prs/codex-home" ]]
[[ ! -e "$cleanup_wt/tmp/continue-all-prs/triage-repository/.triage-codex-home" ]]

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

# The triage sandbox must replace the host `/proc` with a private one. With the
# host procfs mounted, a triage turn can read `/proc/<pid>/environ` of the
# orchestrator or of any other same-UID process and recover exactly the
# credentials that were scrubbed from its own environment.
triage_sandbox_flags=$(sed -n '/^            bwrap$/,/^        )$/p' "$repo/utils/continue-all-prs.sh")
grep -q -- '--unshare-pid' <<< "$triage_sandbox_flags"
grep -q -- '--proc /proc' <<< "$triage_sandbox_flags"

# `gh` resolves `GH_CONFIG_DIR`, then `$XDG_CONFIG_HOME/gh`, and only then
# `$HOME/.config/gh`. Scrubbing `GH_CONFIG_DIR` alone therefore leaves the host
# token readable on any machine that uses an XDG configuration layout, so the
# sandbox must redirect `XDG_CONFIG_HOME` into the private home as well.
grep -q -- '--setenv XDG_CONFIG_HOME "$triage_home/.config"' <<< "$triage_sandbox_flags"
grep -q -- '--tmpfs "$XDG_CONFIG_HOME/gh"' "$repo/utils/continue-all-prs.sh"
grep -q -- '--tmpfs "$XDG_CONFIG_HOME/git"' "$repo/utils/continue-all-prs.sh"

if command -v bwrap >/dev/null 2>&1 && bwrap --ro-bind / / --dev /dev true 2>/dev/null; then
    xdg_host="$scratch/xdg-config"
    xdg_home="$scratch/xdg-triage-home"
    mkdir -p "$xdg_host/gh" "$xdg_home"
    printf 'github.com:\n  oauth_token: SECRET_XDG_TOKEN\n' > "$xdg_host/gh/hosts.yml"
    # Sanity check: the token is readable through the XDG path outside the
    # sandbox, so the assertions below are not vacuous.
    grep -q SECRET_XDG_TOKEN "$xdg_host/gh/hosts.yml"
    if env XDG_CONFIG_HOME="$xdg_host" bwrap --ro-bind / / --dev /dev \
        --tmpfs "$xdg_home" --setenv HOME "$xdg_home" \
        --setenv XDG_CONFIG_HOME "$xdg_home/.config" --tmpfs "$xdg_host/gh" \
        sh -c 'cat "$XDG_CONFIG_HOME/gh/hosts.yml" 2>/dev/null' | grep -q SECRET_XDG_TOKEN; then
        echo 'Expected the triage sandbox to hide the XDG gh configuration' >&2
        exit 1
    fi
    if env XDG_CONFIG_HOME="$xdg_host" bwrap --ro-bind / / --dev /dev \
        --tmpfs "$xdg_home" --setenv HOME "$xdg_home" \
        --setenv XDG_CONFIG_HOME "$xdg_home/.config" --tmpfs "$xdg_host/gh" \
        sh -c "cat '$xdg_host/gh/hosts.yml' 2>/dev/null" | grep -q SECRET_XDG_TOKEN; then
        echo 'Expected the triage sandbox to mask the host XDG gh directory' >&2
        exit 1
    fi
fi

# `--ro-bind / /` keeps the outer checkout readable by absolute path, and a
# standard `actions/checkout` run stores an `http.*.extraheader`
# authentication token in its real Git configuration. The sandbox must mask
# the worker's common and per-worktree config files, because only the triage
# clone's own config is bind-replaced with a sanitized copy.
grep -q -- '--ro-bind /dev/null "$host_git_config"' "$repo/utils/continue-all-prs.sh"
grep -q -- '--ro-bind /dev/null "$host_worktree_config"' "$repo/utils/continue-all-prs.sh"

if command -v bwrap >/dev/null 2>&1 && bwrap --ro-bind / / --dev /dev true 2>/dev/null; then
    host_checkout="$scratch/host-checkout"
    host_worker="$scratch/host-worker"
    git init -q "$host_checkout"
    git -C "$host_checkout" config user.email test@example.com
    git -C "$host_checkout" config user.name test
    git -C "$host_checkout" commit -q --allow-empty -m 'Empty'
    git -C "$host_checkout" worktree add -q --detach "$host_worker" HEAD
    git -C "$host_checkout" config http.https://github.com/.extraheader 'Authorization: Basic SECRET_HOST_HEADER'
    host_git_config=$(git -C "$host_worker" rev-parse --path-format=absolute --git-path config)
    host_worktree_config=$(git -C "$host_worker" rev-parse --path-format=absolute --git-path config.worktree)
    # Sanity check outside the sandbox: the worker resolves the shared common
    # config, and the header is readable through it, so the assertions below
    # are not vacuous.
    grep -q SECRET_HOST_HEADER "$host_git_config"
    host_mask_args=(--ro-bind /dev/null "$host_git_config")
    [[ ! -e "$host_worktree_config" ]] || host_mask_args+=(--ro-bind /dev/null "$host_worktree_config")
    if bwrap --ro-bind / / --dev /dev "${host_mask_args[@]}" \
        sh -c "cat '$host_git_config' 2>/dev/null" | grep -q SECRET_HOST_HEADER; then
        echo 'Expected the triage sandbox to mask the host checkout Git config' >&2
        exit 1
    fi
    if bwrap --ro-bind / / --dev /dev "${host_mask_args[@]}" \
        git -C "$host_checkout" config --get http.https://github.com/.extraheader 2>/dev/null \
        | grep -q SECRET_HOST_HEADER; then
        echo 'Expected the masked host config to hide the HTTP extra header from Git' >&2
        exit 1
    fi
    git -C "$host_checkout" worktree remove --force "$host_worker"
fi

if command -v bwrap >/dev/null 2>&1 && bwrap --ro-bind / / --unshare-pid --proc /proc --dev /dev true 2>/dev/null; then
    env CONTINUE_ALL_PRS_TEST_SECRET=SECRET_ENVIRON_TOKEN sleep 30 &
    secret_pid=$!
    # Sanity check: without the sandbox the secret is readable from the host
    # procfs, so the assertion below is not vacuous.
    secret_visible=0
    for _ in {1..200}; do
        if tr '\0' '\n' < "/proc/$secret_pid/environ" 2>/dev/null | grep -q SECRET_ENVIRON_TOKEN; then
            secret_visible=1
            break
        fi
        sleep 0.05
    done
    if (( ! secret_visible )); then
        echo 'Expected the host /proc to expose the secret outside the sandbox' >&2
        kill "$secret_pid" 2>/dev/null || true
        exit 1
    fi
    if bwrap --ro-bind / / --unshare-pid --proc /proc --die-with-parent --dev /dev \
        sh -c "cat /proc/$secret_pid/environ 2>/dev/null" | grep -q SECRET_ENVIRON_TOKEN; then
        echo 'Expected the triage sandbox to hide the host /proc' >&2
        kill "$secret_pid" 2>/dev/null || true
        exit 1
    fi
    kill "$secret_pid" 2>/dev/null || true
    wait "$secret_pid" 2>/dev/null || true
fi

# Triage uses a private clone. After the PR head is checked out, it must still
# initialize submodules so it can build and inspect the complete source tree.
source <(sed -n '/^setup_triage_submodules()/,/^}/p' "$repo/utils/continue-all-prs.sh")
source <(sed -n '/^materialize_trusted_submodules()/,/^}/p' "$repo/utils/continue-all-prs.sh")
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
# The worker's own checkout is the trusted source of submodule URLs.
setup_triage_submodules "$triage_clone" "$triage_superproject" $(( $(date +%s) + 30 ))
[[ "$(<"$triage_clone/dependency/content")" == 'triage submodule' ]]

# `.gitmodules` is pull-request content, and submodules are materialized in the
# orchestrator, before the triage sandbox exists. A URL taken from the pull
# request would make the worker authenticate to an author-chosen endpoint with
# its own SSH and credential configuration; every URL must come from the
# trusted worker checkout instead, and a submodule the worker does not know
# must not be fetched at all.
hostile_clone="$scratch/hostile-triage-clone"
hostile_super="$scratch/hostile-superproject"
hostile_ssh_marker="$scratch/hostile-ssh-invoked"
cp -a "$triage_superproject" "$hostile_super"
git -C "$hostile_super" config -f .gitmodules submodule.dependency.url ssh://git@example.com/evil/dependency.git
git -C "$hostile_super" config -f .gitmodules submodule.evil.path evil
git -C "$hostile_super" config -f .gitmodules submodule.evil.url ssh://git@example.com/evil/evil.git
git -C "$hostile_super" update-index --add --cacheinfo \
    "160000,$(git -C "$triage_submodule_source" rev-parse HEAD),evil"
git -C "$hostile_super" add .gitmodules
git -C "$hostile_super" commit -qm 'Point the submodules at an attacker endpoint'
git clone -q --shared --no-checkout "$hostile_super" "$hostile_clone"
git -C "$hostile_clone" checkout -q --detach HEAD
printf '%s\n' '#!/usr/bin/env bash' 'printf invoked > "$HOSTILE_SSH_MARKER"' 'exit 1' > "$bin/hostile-ssh"
chmod +x "$bin/hostile-ssh"
HOSTILE_SSH_MARKER="$hostile_ssh_marker" GIT_SSH_COMMAND="$bin/hostile-ssh" \
    setup_triage_submodules "$hostile_clone" "$triage_superproject" $(( $(date +%s) + 30 ))
[[ ! -e "$hostile_ssh_marker" ]]
# The known submodule is materialized from the trusted URL, and the one the
# pull request invented is left alone.
[[ "$(<"$hostile_clone/dependency/content")" == 'triage submodule' ]]
[[ "$(git -C "$hostile_clone" config --get submodule.dependency.url)" == "$triage_submodule_source" ]]
[[ -z "$(git -C "$hostile_clone" config --get submodule.evil.url || true)" ]]
[[ -z "$(ls -A "$hostile_clone/evil" 2>/dev/null)" ]]

# `headRefName` is chosen by the fork, and a short refname is ambiguous: when a
# remote carries both `refs/heads/<name>` and `refs/tags/<name>`, `FETCH_HEAD`
# resolves to the tag. Triage would then validate a tree that is not the PR
# head and could report `NO-CHANGE` for an unrelated revision.
source <(sed -n '/^prepare_triage_worktree()/,/^}/p' "$repo/utils/continue-all-prs.sh")
collision_base="$scratch/collision-base"
collision_head="$scratch/collision-head"
collision_wt="$scratch/collision-worktree"
git init -q -b master "$collision_base"
git -C "$collision_base" config user.email test@example.com
git -C "$collision_base" config user.name test
printf 'base\n' > "$collision_base/content"
git -C "$collision_base" add content
git -C "$collision_base" commit -qm 'Add base content'
git clone -q "$collision_base" "$collision_head"
git -C "$collision_head" config user.email test@example.com
git -C "$collision_head" config user.name test
# The tag shares the branch name but points at a different, older commit.
git -C "$collision_head" tag pr-head HEAD
git -C "$collision_head" checkout -q -b pr-head
printf 'head\n' > "$collision_head/content"
git -C "$collision_head" commit -qam 'Add head content'
git clone -q "$collision_base" "$collision_wt"

collision_branch_oid=$(git -C "$collision_head" rev-parse refs/heads/pr-head)
collision_tag_oid=$(git -C "$collision_head" rev-parse refs/tags/pr-head)
[[ "$collision_branch_oid" != "$collision_tag_oid" ]]

gh()
{
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' master pr-head "$collision_head" ClickHouse test-user false true
}
GH_USER=test-user
REPO=ClickHouse/ClickHouse
# The shipped helper always derives the base repository from `REPO`; the tests
# redirect it to a local path so no network access is needed.
source <(sed -n '/^repo_clone_url()/,/^}/p' "$repo/utils/continue-all-prs.sh")
[[ "$(repo_clone_url)" == 'https://github.com/ClickHouse/ClickHouse.git' ]]
repo_clone_url()
{
    printf '%s\n' "$collision_base"
}
collision_result=$(prepare_triage_worktree "$collision_wt" 1 $(( $(date +%s) + 30 )))
unset -f gh

IFS=$'\t' read -r collision_head_oid collision_base_oid collision_head_ref _ collision_pushable <<< "$collision_result"
[[ "$collision_head_oid" == "$collision_branch_oid" ]]
[[ "$(git -C "$collision_wt" rev-parse HEAD)" == "$collision_branch_oid" ]]
[[ "$collision_base_oid" == "$(git -C "$collision_base" rev-parse refs/heads/master)" ]]
[[ "$collision_head_ref" == pr-head ]]
[[ "$collision_pushable" == 1 ]]

# The triage clone inherits `origin` from the operator's checkout, which can be
# a fork with a divergent base branch. The validated merge must always use the
# base branch of `REPO`, so `prepare_triage_worktree` has to fetch it by
# explicit upstream URL rather than through the inherited `origin`.
fork_upstream="$scratch/fork-upstream"
fork_remote="$scratch/fork-remote"
fork_worktree="$scratch/fork-worktree"
git init -q -b master "$fork_upstream"
git -C "$fork_upstream" config user.email test@example.com
git -C "$fork_upstream" config user.name test
printf 'upstream base\n' > "$fork_upstream/content"
git -C "$fork_upstream" add content
git -C "$fork_upstream" commit -qm 'Add upstream base'
git clone -q "$fork_upstream" "$fork_remote"
git -C "$fork_remote" config user.email test@example.com
git -C "$fork_remote" config user.name test
# The fork's `master` diverges from the upstream one.
printf 'fork base\n' > "$fork_remote/content"
git -C "$fork_remote" commit -qam 'Diverge the fork base'
git -C "$fork_remote" checkout -q -b pr-head
printf 'fork head\n' > "$fork_remote/content"
git -C "$fork_remote" commit -qam 'Add fork head'
# The worker, and therefore the triage clone, has `origin` pointing at the fork.
git clone -q "$fork_remote" "$fork_worktree"
# The upstream base moves on after the fork was created.
printf 'upstream base 2\n' > "$fork_upstream/content"
git -C "$fork_upstream" commit -qam 'Advance the upstream base'

gh()
{
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' master pr-head "$fork_remote" ClickHouse test-user false true
}
repo_clone_url()
{
    printf '%s\n' "$fork_upstream"
}
fork_result=$(prepare_triage_worktree "$fork_worktree" 1 $(( $(date +%s) + 30 )))
unset -f gh

IFS=$'\t' read -r fork_head_oid fork_base_oid _ <<< "$fork_result"
[[ "$fork_head_oid" == "$(git -C "$fork_remote" rev-parse refs/heads/pr-head)" ]]
[[ "$fork_base_oid" == "$(git -C "$fork_upstream" rev-parse refs/heads/master)" ]]
[[ "$fork_base_oid" != "$(git -C "$fork_remote" rev-parse refs/heads/master)" ]]

# Triage works in a `--shared` clone: the objects it fetches stay in the
# clone's object store and never appear in the worker. Recreating the
# validated merge in the worker must import them first, otherwise the
# detached checkout fails with `reference is not a tree`.
source <(sed -n '/^import_triage_objects()/,/^}/p' "$repo/utils/continue-all-prs.sh")
source <(sed -n '/^recreate_validated_triage_merge()/,/^}/p' "$repo/utils/continue-all-prs.sh")
import_source="$scratch/import-source"
import_worker="$scratch/import-worker"
import_triage="$scratch/import-triage"
git init -q -b master "$import_source"
git -C "$import_source" config user.email test@example.com
git -C "$import_source" config user.name test
printf 'base\n' > "$import_source/base"
git -C "$import_source" add base
git -C "$import_source" commit -qm 'Add base'
git clone -q "$import_source" "$import_worker"
git -C "$import_worker" config user.email test@example.com
git -C "$import_worker" config user.name test
# The pull-request head and the newer base branch appear only after the worker
# was cloned, exactly as they do when triage fetches them.
git -C "$import_source" checkout -q -b pr-head
printf 'head\n' > "$import_source/head"
git -C "$import_source" add head
git -C "$import_source" commit -qm 'Add head'
import_head_oid=$(git -C "$import_source" rev-parse refs/heads/pr-head)
git -C "$import_source" checkout -q master
printf 'newer base\n' > "$import_source/newer-base"
git -C "$import_source" add newer-base
git -C "$import_source" commit -qm 'Advance the base'
import_base_oid=$(git -C "$import_source" rev-parse refs/heads/master)

git clone -q --shared --no-checkout "$import_worker" "$import_triage"
git -C "$import_triage" fetch -q "$import_source" \
    "+refs/heads/pr-head:refs/remotes/origin/pr-head" "+refs/heads/master:refs/remotes/origin/master"
# Sanity check: the worker does not have either object, so the assertions
# below are not vacuous.
! git -C "$import_worker" cat-file -e "$import_head_oid^{commit}" 2>/dev/null
! git -C "$import_worker" cat-file -e "$import_base_oid^{commit}" 2>/dev/null

recreate_validated_triage_merge "$import_worker" "$import_head_oid" "$import_base_oid" "$import_triage" >/dev/null
import_merge_oid=$(git -C "$import_worker" rev-parse HEAD)
[[ "$(git -C "$import_worker" show -s --format='%P' "$import_merge_oid")" == "$import_head_oid $import_base_oid" ]]
[[ "$(<"$import_worker/head")" == 'head' ]]
[[ "$(<"$import_worker/newer-base")" == 'newer base' ]]

# The synthetic merge is a mechanical handoff step. It must not depend on the
# operator's commit signing setup: with `commit.gpgSign` enabled, a clean
# validated merge would otherwise fail before the handoff.
sign_source="$scratch/sign-source"
sign_worker="$scratch/sign-worker"
sign_triage="$scratch/sign-triage"
git init -q -b master "$sign_source"
git -C "$sign_source" config user.email test@example.com
git -C "$sign_source" config user.name test
printf 'base\n' > "$sign_source/base"
git -C "$sign_source" add base
git -C "$sign_source" commit -qm 'Add base'
git clone -q "$sign_source" "$sign_worker"
git -C "$sign_worker" config user.email test@example.com
git -C "$sign_worker" config user.name test
git -C "$sign_worker" config commit.gpgSign true
git -C "$sign_worker" config gpg.program /bin/false
git -C "$sign_source" checkout -q -b pr-head
printf 'head\n' > "$sign_source/head"
git -C "$sign_source" add head
git -C "$sign_source" commit -qm 'Add head'
sign_head_oid=$(git -C "$sign_source" rev-parse refs/heads/pr-head)
git -C "$sign_source" checkout -q master
printf 'newer base\n' > "$sign_source/newer-base"
git -C "$sign_source" add newer-base
git -C "$sign_source" commit -qm 'Advance the base'
sign_base_oid=$(git -C "$sign_source" rev-parse refs/heads/master)
git clone -q --shared --no-checkout "$sign_worker" "$sign_triage"
git -C "$sign_triage" fetch -q "$sign_source" \
    "+refs/heads/pr-head:refs/remotes/origin/pr-head" "+refs/heads/master:refs/remotes/origin/master"
recreate_validated_triage_merge "$sign_worker" "$sign_head_oid" "$sign_base_oid" "$sign_triage" >/dev/null
[[ "$(git -C "$sign_worker" show -s --format='%P' HEAD)" == "$sign_head_oid $sign_base_oid" ]]

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
# A relative `--worktree-base` must survive the guards that compare the worker
# path with `git worktree list`, which reports absolute paths only. Without a
# canonicalized `WORKTREE_BASE`, the round dies in `prepare_worktree_for_task`
# before the agent starts.
! grep -q 'Refusing to clean an unexpected worktree' "$scratch/relative-reuse.log" \
    "$repo/tmp/continue-all-prs/pr-1.log"
! grep -q 'Worktree cleanup failed' "$scratch/relative-reuse.log" \
    "$repo/tmp/continue-all-prs/pr-1.log"
grep -q 'FINISHED  PR #1' "$scratch/relative-reuse.log"

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

[[ ! -e "${worktree_base}-0/tmp/continue-all-prs/codex-home" ]]
