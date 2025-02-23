#!/usr/bin/env bash
ROOT_PATH=$(git rev-parse --show-toplevel)
NPROC=$(($(nproc) + 3))
# Check sh tests with Shellcheck
find "$ROOT_PATH/tests/queries/"{0_stateless,1_stateful} -name '*.sh' -print0 | \
  xargs -0 -P "$NPROC" -n 20 shellcheck --check-sourced --external-sources --source-path=SCRIPTDIR \
  --severity info --exclude SC1071,SC2086,SC2016

# Check docker scripts with shellcheck
# Do not check sourced files, since it causes broken --source-path=SCRIPTDIR
find "$ROOT_PATH/docker" -type f -exec file -F' ' --mime-type {} + | \
  awk '$2=="text/x-shellscript" {print $1}' | \
  grep -v "compare.sh" | \
  xargs -P "$NPROC" -n 20 shellcheck --external-sources --source-path=SCRIPTDIR
