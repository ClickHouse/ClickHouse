#!/usr/bin/env bash
ROOT_PATH=$(git rev-parse --show-toplevel)
NPROC=$(($(nproc) + 3))
# Check sh tests with Shellcheck
( cd "$ROOT_PATH/tests/queries/0_stateless/" && \
  find "$ROOT_PATH/tests/queries/"{0_stateless,1_stateful} -name '*.sh' -print0 | \
    xargs -0 -P "$NPROC" -n 20 shellcheck --check-sourced --external-sources --severity info --exclude SC1071,SC2086,SC2016
)

# Check docker scripts with shellcheck
find "$ROOT_PATH/docker" -executable -type f -exec file -F'	' --mime-type {} \; | \
  awk -F'	' '$2==" text/x-shellscript" {print $1}' | \
  grep -v "compare.sh" | \
  xargs -P "$NPROC" -n 20 shellcheck
