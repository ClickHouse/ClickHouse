#!/usr/bin/env bash

ROOT_PATH=$(git rev-parse --show-toplevel)

# Find duplicate include directives
find $ROOT_PATH/{src,base,programs,utils} -name '*.h' -or -name '*.cpp' | while read file; do grep -P '^#include ' $file | sort | uniq -c | grep -v -P '^\s+1\s' && echo $file; done
