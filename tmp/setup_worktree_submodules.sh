#!/usr/bin/env bash
set -u

MAIN_REPO=/home/ubuntu/ClickHouse-wmt_functions
WORKTREE_PATH=/home/ubuntu/ClickHouse-wmt_functions-harry-scatter

GIT_COMMON_DIR=$(git -C "$MAIN_REPO" rev-parse --git-common-dir)
case "$GIT_COMMON_DIR" in
    /*) GIT_DIR=$GIT_COMMON_DIR ;;
    *) GIT_DIR=$MAIN_REPO/$GIT_COMMON_DIR ;;
esac
WORKTREE_ENTRY=$(basename "$WORKTREE_PATH")

# Hardlink-copy the modules directory from the main repo while checking out the
# parent worktree files.
( cp -al "$GIT_DIR/modules" \
         "$GIT_DIR/worktrees/$WORKTREE_ENTRY/modules" ) &
cp_pid=$!

parent_checkout_status=0
git -C "$WORKTREE_PATH" \
    -c checkout.workers=0 \
    -c core.fsync=none \
    -c gc.auto=0 \
    checkout -q -f HEAD -- . || parent_checkout_status=$?

modules_copy_status=0
wait "$cp_pid" || modules_copy_status=$?

if [ "$parent_checkout_status" -ne 0 ]; then
    printf "FAILED: parent checkout\n" >&2
    exit "$parent_checkout_status"
fi
if [ "$modules_copy_status" -ne 0 ]; then
    printf "FAILED: cp -al modules\n" >&2
    exit "$modules_copy_status"
fi

# Register submodules in the worktree config while rewriting module configs.
git -C "$WORKTREE_PATH" submodule init &
init_pid=$!

find "$GIT_DIR/worktrees/$WORKTREE_ENTRY/modules" \
    \( -name config -o -name config.worktree \) -exec \
    sed -i "s|worktree = .*/contrib/|worktree = $WORKTREE_PATH/contrib/|" {} +

if ! wait "$init_pid"; then
    printf "FAILED: submodule init\n" >&2
    exit 1
fi

CPU_COUNT=$(nproc)
DEFAULT_SUBMODULE_CHECKOUT_WORKERS=8
if [ "$DEFAULT_SUBMODULE_CHECKOUT_WORKERS" -gt "$CPU_COUNT" ]; then
    DEFAULT_SUBMODULE_CHECKOUT_WORKERS=$CPU_COUNT
fi
SUBMODULE_CHECKOUT_WORKERS=${SUBMODULE_CHECKOUT_WORKERS:-$DEFAULT_SUBMODULE_CHECKOUT_WORKERS}
if [ "$SUBMODULE_CHECKOUT_WORKERS" -lt 1 ]; then
    printf "FAILED: SUBMODULE_CHECKOUT_WORKERS must be positive\n" >&2
    exit 1
fi
SUBMODULE_JOBS=${SUBMODULE_JOBS:-$(( CPU_COUNT / SUBMODULE_CHECKOUT_WORKERS ))}
if [ "$SUBMODULE_JOBS" -lt 1 ]; then
    SUBMODULE_JOBS=1
fi

# Refuse custom update commands.
( git -C "$WORKTREE_PATH" config --file .gitmodules --get-regexp "^submodule\\..*\\.update$" 2>/dev/null || true ) |
    while IFS=" " read -r config_key update_command; do
        case "$update_command" in
            "!"*)
                printf "FAILED: custom submodule update command is unsupported on local hardlink path: %s\n" "$config_key" >&2
                exit 1
                ;;
        esac
    done || exit 1

GITLINKS=$(git -C "$WORKTREE_PATH" ls-files -s |
    sed -n "s/^160000 \([0-9a-f][0-9a-f]*\) 0[[:space:]]\(.*\)$/\1 \2/p")
GITLINK_COUNT=$(printf "%s\n" "$GITLINKS" | sed -n '$=')
SUBMODULE_COUNT=$(git -C "$WORKTREE_PATH" config --file .gitmodules --get-regexp "^submodule\\..*\\.path$" |
    sed -n '$=')
if [ "${GITLINK_COUNT:-0}" != "${SUBMODULE_COUNT:-0}" ]; then
    printf "FAILED: gitlink count %s does not match .gitmodules count %s\n" "${GITLINK_COUNT:-0}" "${SUBMODULE_COUNT:-0}" >&2
    exit 1
fi

{
    for submodule_path in \
        contrib/llvm-project \
        contrib/google-cloud-cpp \
        contrib/aws \
        contrib/openssl \
        contrib/icu \
        contrib/boost \
        contrib/rust_vendor \
        contrib/sysroot \
        contrib/grpc \
        contrib/arrow \
        contrib/curl \
        contrib/rocksdb \
        contrib/postgres \
        contrib/wasmtime
    do
        printf "%s\n" "$GITLINKS" |
            awk -v p="$submodule_path" '$2 == p { print; exit }'
    done
    printf "%s\n" "$GITLINKS"
} |
    awk '!seen[$2]++ { print }' |
    while IFS=" " read -r expected_commit submodule_path; do
        printf "%s\0%s\0" "$expected_commit" "$submodule_path"
    done |
    xargs -0 -r -n2 -P "$SUBMODULE_JOBS" sh -c '
        worktree_path=$1
        git_dir=$2
        worktree_entry=$3
        checkout_workers=$4
        expected_commit=$5
        submodule_path=$6

        if [ -z "$expected_commit" ] || [ -z "$submodule_path" ]; then
            printf "FAILED: empty submodule checkout tuple\n" >&2
            exit 1
        fi

        module_git_dir="$git_dir/worktrees/$worktree_entry/modules/$submodule_path"
        module_worktree="$worktree_path/$submodule_path"

        mkdir -p "$module_worktree" || exit 1
        printf "gitdir: %s\n" "$module_git_dir" > "$module_worktree/.git" || exit 1

        if ! git --git-dir="$module_git_dir" --work-tree="$module_worktree" \
            -c advice.detachedHead=false \
            -c checkout.workers="$checkout_workers" \
            -c checkout.thresholdForParallelism=100 \
            -c index.threads=true \
            -c core.fsync=none \
            -c gc.auto=0 \
            checkout -q -f --detach "$expected_commit"
        then
            printf "FAILED: %s: commit %s is missing from the local mirror\n" "$submodule_path" "$expected_commit" >&2
            exit 1
        fi
    ' sh "$WORKTREE_PATH" "$GIT_DIR" "$WORKTREE_ENTRY" "$SUBMODULE_CHECKOUT_WORKERS"

submodule_checkout_status=$?
if [ "$submodule_checkout_status" -ne 0 ]; then
    printf "ERROR: a submodule could not be checked out at the recorded commit (see FAILED above).\n" >&2
    printf "Fix: git -C %s submodule update --init\n" "$MAIN_REPO" >&2
    exit "$submodule_checkout_status"
fi

printf "SUBMODULE SETUP OK\n"
