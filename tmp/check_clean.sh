#!/bin/bash
ROOT=/home/ubuntu/ClickHouse-wmt_functions

# Same EXCLUDE as check_cpp.sh, applied to src/** file list.
# Strategy: emulate the std-containers lint pattern for a given path.
check_dir() {
    local dir="$1"
    # All cpp/h files under dir, excluding tests/ subdirs
    local matches
    matches=$(find "$ROOT/$dir" -type f \( -name '*.cpp' -o -name '*.h' -o -name '*.hpp' -o -name '*.cc' -o -name '*.c' \) 2>/dev/null \
        | grep -vE '/(tests|examples)/' \
        | xargs -r rg -Hn 'std::(deque|list|map|multimap|multiset|queue|set|unordered_map|unordered_multimap|unordered_multiset|unordered_set|vector)<' 2>/dev/null \
        | grep -vE '^[^:]+:[0-9]+:[[:space:]]*(\*|//|/\*)' \
        | grep -v 'STYLE_CHECK_ALLOW_STD_CONTAINERS' || true)
    local count
    count=$(echo -n "$matches" | grep -c . || true)
    printf "%6d  %s\n" "$count" "$dir"
}

for d in src/Access src/Analyzer src/Backups src/Client src/Common src/Coordination src/Core src/Databases src/DataTypes src/Disks src/Formats src/Interpreters src/Parsers src/Planner src/Processors src/Server src/Storages; do
    check_dir "$d"
done
