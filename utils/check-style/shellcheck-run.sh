#!/usr/bin/env bash

DIR=$(readlink -f $(dirname "$0"))
if [ -z "$1" ]; then
    . "$DIR/functions.sh"
    FILES=$(get_files_to_check)
else
    FILES="$1"
fi

ROOT_PATH=$(git rev-parse --show-toplevel)
NPROC=$(($(nproc) + 3))
cd "$ROOT_PATH" || exit

# Check sh tests with Shellcheck
FILES_TO_CHECK=$(echo "$FILES" | grep -E "tests/queries.*sh" )
if [ -n "$FILES_TO_CHECK" ]; then
  echo "$FILES_TO_CHECK" | \
    xargs -P "$NPROC" -n 20 shellcheck --check-sourced --external-sources --severity info --exclude SC1071,SC2086,SC2016
fi

# Check docker scripts with shellcheck
FILES_TO_CHECK=$(echo "$FILES" | grep -E ^docker | xargs file -i | grep -E 'text/x-shellscript|application/x-shellscript' | cut -d: -f1 | grep -v "compare.sh" )
if [ -n "$FILES_TO_CHECK" ]; then
      echo "$FILES_TO_CHECK" | \
        xargs -P "$NPROC" -n 20 shellcheck
fi

# Check docker scripts with shellcheck
# FILES_TO_CHECK=$(echo "$FILES" | grep -E ^utils | xargs file -i | grep -E 'text/x-shellscript|application/x-shellscript' | cut -d: -f1 | grep -v "compare.sh" )
# if [ -n "$FILES_TO_CHECK" ]; then
#       echo "$FILES_TO_CHECK" | \
#         xargs -P "$NPROC" -n 20 sh -c "for file; do shellcheck -x --source-path="$(dirname "$file")" "$file"; done" sh
# fi