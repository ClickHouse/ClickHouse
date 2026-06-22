#!/bin/bash
ROOT=/home/ubuntu/ClickHouse-wmt_functions
cd "$ROOT"

# Already linted top-level dirs (skip them and their subdirs)
ALREADY="src/AggregateFunctions src/BridgeHelper src/Columns src/Compression src/Daemon src/Dictionaries src/Examples src/Functions src/IO src/Loggers src/QueryPipeline src/TableFunctions"

# All subdirs at depth 1 and 2 under src/
find src -mindepth 1 -maxdepth 2 -type d | sort | while read -r dir; do
    skip=0
    for a in $ALREADY; do
        if [[ "$dir" == "$a"* ]]; then skip=1; break; fi
    done
    [[ $skip -eq 1 ]] && continue

    # Count direct files (not recursive — we want files immediately in this dir or recursive?)
    # The current script uses path matching "/$dir/" so it's recursive.
    # We want: subdir d such that d (recursively) has zero hits AND has at least 1 cpp/h file
    files=$(find "$dir" -type f \( -name '*.cpp' -o -name '*.h' -o -name '*.hpp' \) | grep -vE '/(tests|examples)/' || true)
    [[ -z "$files" ]] && continue
    nfiles=$(echo "$files" | wc -l)

    hits=$(echo "$files" | xargs -r rg -Hn 'std::(deque|list|map|multimap|multiset|queue|set|unordered_map|unordered_multimap|unordered_multiset|unordered_set|vector)<' 2>/dev/null \
        | grep -vE '^[^:]+:[0-9]+:[[:space:]]*(\*|//|/\*)' \
        | grep -v 'STYLE_CHECK_ALLOW_STD_CONTAINERS' \
        | wc -l)
    if [[ "$hits" == "0" ]]; then
        printf "clean (%d files): %s\n" "$nfiles" "$dir"
    fi
done
