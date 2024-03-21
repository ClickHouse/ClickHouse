#!/usr/bin/env bash

ROOT_PATH=$(git rev-parse --show-toplevel)
NPROC=$(($(nproc) + 3))

# Find duplicate include directives
find "$ROOT_PATH"/{src,base,programs,utils} -type f '(' -name '*.h' -or -name '*.cpp' ')' -print0 | \
  xargs -0 -I {} -P "$NPROC" bash -c 'grep -P "^#include " "{}" | sort | uniq -c | grep -v -P "^\s+1\s" && echo "{}" '| \
  sed '/^\s*$/d'
