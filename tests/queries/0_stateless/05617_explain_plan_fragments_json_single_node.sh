#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `EXPLAIN PLAN fragments = 1, json = 1` of a plan that is not cut is the document of `json = 1` with the keys in
# camelCase, plus the reason when the query fell back to local execution. Each document is taken in its own query: the
# step ids (`Node Id`) are numbered per query, so two `EXPLAIN` in one query would number their steps differently.

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_fragments_json;
    CREATE TABLE t_fragments_json (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t_fragments_json SELECT number, number FROM numbers(1000);
"

# In ClickHouse Cloud a scan on shared storage is replaced by `ReadFromMergeTreeAtWorker`, which prints no table name.
SETTINGS="explain_query_plan_default = 'legacy', enable_parallel_replicas = 0, max_rows_to_group_by = 0, optimize_aggregation_in_order = 0, distributed_plan_prefer_replicas_over_workers = 1"
QUERY="SELECT k % 10 AS g, sum(v) FROM t_fragments_json GROUP BY g"

explain()
{
    $CLICKHOUSE_CLIENT -q "EXPLAIN $1 $QUERY $2 SETTINGS $SETTINGS, $3 FORMAT TSVRaw"
}

# The document re-printed with one key per line, with the keys of `json = 1` converted to camelCase as `fragments = 1`
# writes them: split at every character that is not a letter or a digit, the first word lowercased, the next words
# capitalized, a word in capitals only lowercased first.
normalize()
{
    python3 -c '
import json, re, sys
def camel(key):
    words = []
    for i, word in enumerate(re.findall(r"[A-Za-z0-9]+", key)):
        if not any(c.islower() for c in word):
            word = word.lower()
        words.append((word[0].lower() if i == 0 else word[0].upper()) + word[1:])
    return "".join(words)
def convert(item):
    if isinstance(item, dict):
        return {camel(k): convert(v) for k, v in item.items()}
    if isinstance(item, list):
        return [convert(v) for v in item]
    return item
document = json.load(sys.stdin)
if sys.argv[1] == "1":
    document = convert(document)
print(json.dumps(document, indent=1))
' "$1"
}

compare()
{
    local label=$1
    shift
    echo "$label"
    # Only the lines that differ, without the line numbers, which follow the length of the plan.
    # Plain `diff` output filtered with `grep`: the GNU line format options are not available in BSD `diff` (macOS).
    local old new changed
    old=$(explain "$1" "$2" "$3" | normalize "$7")
    new=$(explain "$4" "$5" "$6" | normalize 0)
    changed=$(diff <(echo "$old") <(echo "$new") | grep '^[<>]')
    if [ -z "$changed" ]; then echo "same document"; else echo "$changed"; fi
}

compare "-- make_distributed_plan = 0" \
    "json = 1" "" "make_distributed_plan = 0" \
    "fragments = 1, json = 1" "" "make_distributed_plan = 0" 1

compare "-- make_distributed_plan = 0, pretty = 1" \
    "fragments = 1, json = 1, pretty = 0" "" "make_distributed_plan = 0" \
    "fragments = 1, json = 1, pretty = 1" "" "make_distributed_plan = 0" 0

# The query falls back to local execution: the only difference is the reason after the plan.
compare "-- fallback to local execution" \
    "json = 1" "WITH TOTALS" "make_distributed_plan = 1, distributed_plan_fallback_to_local_execution = 1" \
    "fragments = 1, json = 1" "WITH TOTALS" "make_distributed_plan = 1, distributed_plan_fallback_to_local_execution = 1" 1

$CLICKHOUSE_CLIENT -q "DROP TABLE t_fragments_json"
