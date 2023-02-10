#!/usr/bin/env bash

ROOT_PATH=$(git rev-parse --show-toplevel)

# Find duplicate include directives
find $ROOT_PATH/{src,base/base,base/glibc-compatibility,base/harmful,base/pcg-random,base/readpassphrase,base/widechar_width,programs,utils} -name '*.h' -or -name '*.cpp' | while read file; do grep -P '^#include ' $file | sort | uniq -c | grep -v -P '^\s+1\s' && echo $file; done | sed '/^[[:space:]]*$/d' # all in base/ except poco
