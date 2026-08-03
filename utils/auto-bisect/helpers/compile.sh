#!/bin/bash

set -e
export SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source ${SCRIPT_DIR}/lib.sh

if [[ -z "$GIT_WORK_TREE" || -z "$BUILD_DIR" ]]; then
    echo "Either GIT_WORK_TREE or BUILD_DIR is empty" >&2
    exit 1
fi

if [ -z "$1" ]; then
  echo "COMMIT_SHA is not provided. Correct usage: $0 <COMMIT_SHA>"
  exit 1
fi

COMMIT_SHA="$1"

# First try to find this in cache.
if $SCRIPT_DIR/cache.sh has $COMMIT_SHA; then
  echo "Found binary in cache for commit ${COMMIT_SHA}"
  $SCRIPT_DIR/cache.sh get $CH_PATH $COMMIT_SHA;
  exit 0;
fi

echo "Will compile the binary from ${COMMIT_SHA}."

# Compile in a temporary worktree so the main work tree is never modified.
WORKTREE_DIR=$(mktemp -d "$SCRIPT_DIR/data/compile_XXXXXX")
trap 'git -C "$GIT_WORK_TREE" worktree remove --force "$WORKTREE_DIR" 2>/dev/null || rm -rf "$WORKTREE_DIR"' EXIT

git -C "$GIT_WORK_TREE" worktree add --detach "$WORKTREE_DIR" "$COMMIT_SHA"
(cd "$WORKTREE_DIR" && git submodule sync --recursive && git submodule update --init --recursive) > /dev/null 2>&1

cd $BUILD_DIR
# Disabling RUST as it is very unstable.
(cmake -DCMAKE_C_COMPILER=/usr/bin/clang-19 -DCMAKE_CXX_COMPILER=/usr/bin/clang++-19 -DCMAKE_BUILD_TYPE=Debug -DENABLE_THINLTO=0 -DENABLE_RUST=0 -DENABLE_TESTS=0 -DENABLE_EXAMPLES=0 "$WORKTREE_DIR") > /dev/null 2>&1

# Sometimes the build may fail. In this case we stop the process,
# ask the user to fix the source code and retry.
run_with_retry "ninja"

strip --strip-unneeded $BUILD_DIR/programs/clickhouse
$SCRIPT_DIR/cache.sh add $BUILD_DIR/programs/clickhouse $COMMIT_SHA;
$SCRIPT_DIR/cache.sh get $CH_PATH $COMMIT_SHA;

chmod +x $CH_PATH
$CH_PATH --query 'SELECT 1'

exit 0
