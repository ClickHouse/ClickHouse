#!/usr/bin/env bash

# Find duplicate include directives

if [ -z "$1" ]; then
    DIR=$(readlink -f $(dirname $0))
    . "$DIR/functions.sh"
    CHANGED_FILES=$(get_files_to_check)
else
    CHANGED_FILES="$1"
fi

ROOT_PATH=$(git rev-parse --show-toplevel)
cd $ROOT_PATH

echo "$CHANGED_FILES" | while read file; do grep -P '^#include ' $file | sort | uniq -c | grep -v -P '^\s+1\s' && echo $file; done | sed '/^[[:space:]]*$/d'
