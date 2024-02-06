#!/usr/bin/env bash
ROOT_PATH=$(git rev-parse --show-toplevel)
EXCLUDE_DIRS='build/|integration/|widechar_width/|glibc-compatibility/|memcpy/|consistent-hashing/|Parsers/New'
# Check sh tests with Shellcheck
(cd $ROOT_PATH/tests/queries/0_stateless/ && shellcheck --check-sourced --external-sources --severity info --exclude SC1071,SC2086,SC2016 *.sh ../1_stateful/*.sh)

# Check docker scripts with shellcheck
find "$ROOT_PATH/docker" -executable -type f -exec file -F'	' --mime-type {} \; | awk -F'	' '$2==" text/x-shellscript" {print $1}' | grep -v "entrypoint.alpine.sh" | grep -v "compare.sh"| xargs shellcheck

