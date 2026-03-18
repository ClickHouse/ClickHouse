#!/usr/bin/env bash

ROOT_PATH=$(git rev-parse --show-toplevel)

# Find files with includes not grouped together by first component of path
find $ROOT_PATH/src -name '*.h' -or -name '*.cpp' | while read file; do 
    [[ $(grep -oP '^#include <\w+' $file | uniq -c | wc -l) > $(grep -oP '^#include <\w+' $file | sort | uniq -c | wc -l) ]] && echo $file && grep -oP '^#include <\w+' $file | uniq -c;
done
