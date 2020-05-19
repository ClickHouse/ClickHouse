#!/usr/bin/env bash

# Format almost all code with current clang-format settings

cd `readlink -f $(dirname $0)`/../..

clang_format=`bash -c "compgen -c clang-format | grep 'clang-format-[[:digit:]]' | sort --version-sort --reverse | head -n1"`
if [ ! -z $clang_format ]; then
    find base dbms utils -name *.cpp -or -name *.h -exec $clang_format -i {} + ;
else
    echo clang-format missing. try to install:
    echo sudo apt install clang-format
    echo or
    echo sudo apt install clang-format-3.9
fi
