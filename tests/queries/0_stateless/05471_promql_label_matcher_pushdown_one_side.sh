#!/usr/bin/env bash
# Tags: no-fasttest, long, no-msan
# Tag long: 13 PromQL checks run twice each plus 8 EXPLAINs, each in its own clickhouse-client process, which takes too long in the flaky check.
# Tag no-fasttest: PromQL needs ANTLR4. Tag no-msan: under MSan each PromQL query takes many seconds to plan, as in 05271.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1 --session_timezone=UTC"

# Samples every 10s from 0 to 1800 (or only in the given range), the queries run from 600 to 1800.
$CLIENT -n -q "
DROP TABLE IF EXISTS prometheus;
CREATE TABLE prometheus ENGINE = TimeSeries;
INSERT INTO prometheus (metric_name, tags, samples)
SELECT name, tags, arrayMap(t -> (toDateTime64(t, 3), toFloat64(if(k = 0, 1, k * t + intDiv(t, 70) * k))), arrayFilter(t -> t BETWEEN lo AND hi, range(0, 1801, 10)))
FROM values('name String, tags Map(String, String), lo UInt32, hi UInt32, k UInt32',
    ('req', map('job', 'api', 'instance', 'i1'), 0, 1800, 1),
    ('req', map('job', 'api', 'instance', 'i2'), 0, 1800, 2),
    ('req', map('job', 'db', 'instance', 'i3'), 0, 1800, 3),
    ('req', map('job', 'db', 'instance', 'i4'), 0, 600, 4),
    ('lim', map('job', 'api', 'instance', 'i1', 'team', 't1'), 0, 1800, 5),
    ('lim', map('job', 'api', 'instance', 'i2', 'team', 't2'), 0, 1800, 6),
    ('lim', map('job', 'api', 'instance', 'i3', 'team', 't3'), 0, 1800, 7),
    ('lim', map('job', 'db', 'instance', 'i3', 'team', 't1'), 0, 1800, 8),
    ('lim', map('job', 'db', 'instance', 'i4', 'team', 't4'), 0, 1800, 9),
    ('lim', map('job', 'db', 'instance', 'i4', 'team', 't5'), 1200, 1800, 10),
    ('info', map('job', 'api', 'owner', 'alice'), 0, 1800, 0),
    ('info', map('job', 'db', 'owner', 'bob'), 0, 1800, 0),
    ('info', map('job', 'db', 'owner', 'carol'), 1500, 1800, 0),
    ('m1', map('a', '1'), 0, 1800, 13),
    ('m1', map('a', '2'), 0, 1800, 14),
    ('m2', map('a', '2'), 0, 1800, 15),
    ('m3', map('a', '3'), 0, 1800, 16));
"

# Prints the result of the query, or "error".
function run()
{
    local out
    out=$($CLIENT --promql_push_down_label_matchers="$1" -q "SELECT tags, arrayMap(s -> (s.1, round(s.2, 9)), samples) FROM prometheusQueryRange('prometheus', '$2', 600, 1800, 60) ORDER BY tags" 2>&1)
    if [[ "$out" == *CANNOT_EXECUTE_PROMQL_QUERY* ]]; then echo "error"; else echo "$out"; fi
}

# Prints whether the query gives the same result or error with and without the pushdown, and how many series it returns.
function check()
{
    local on off
    on=$(run 1 "$1")
    off=$(run 0 "$1")
    if [ "$on" != "$off" ]; then echo "$1: different"; elif [ "$on" = "error" ]; then echo "$1: same, error"; else echo "$1: same, $(echo "$on" | grep -c .) series"; fi
}

echo "-- duplicates on the one side outside the matched groups, then in a matched group"
check 'rate(req{instance="i1"}[1m]) / on(instance) rate(lim[1m])'
check 'req{instance=~"i1|i4"} / on(instance) lim'
check 'req{instance="i3"} / on(instance) lim'
check 'req{instance="i1"} / ignoring(team) lim'
check 'req{job="api"} * on(job) group_left(owner) info'
check 'req{job="db"} * on(job) group_left(owner) info'
check 'info * on(job) group_right(owner) req{job="api"}'
check 'req{instance="i1"} / ignoring(team) group_left sum without (team) (lim)'

echo "-- a side which fails by itself is not skipped because the other side became empty"
check 'rate({__name__=~"m1|m2", a="2"}[1m]) / on(a) m3'
check 'rate({__name__=~"m1|m2", a="2"}[1m]) / on(a) sum by (a) (m3)'
check 'rate({__name__=~"m1|m2", a="2"}[1m]) and on(a) m3'
check 'rate({__name__=~"m1|m2", a="2"}[1m]) * on(a) group_right m3'
check '(rate({__name__=~"m1|m2"}[1m]) and on(a) m3) / on(a) m1{a="2"}'
CLIENT="$CLIENT --query_plan_join_swap_table=1" check 'm3 / on(a) rate({__name__=~"m1|m2", a="2"}[1m])'

# Prints the label values each selector filters by, as in 05271_promql_label_matcher_pushdown.
function selectors()
{
    echo "$1"
    $CLIENT -q "
        SELECT DISTINCT arrayStringConcat(extractAll(explain, '= \'([a-z0-9]+)\''), ' ') AS filter
        FROM (EXPLAIN SELECT * FROM prometheusQueryRange('prometheus_empty', '$1', 600, 1800, 60))
        WHERE explain LIKE '%Filter column:%metric_name%'
        ORDER BY filter"
}

$CLIENT -n -q "DROP TABLE IF EXISTS prometheus_empty; CREATE TABLE prometheus_empty ENGINE = TimeSeries;"

echo "-- the selectors read"
selectors 'rate(req{instance="i1"}[1m]) / on(instance) rate(lim[1m])'
selectors 'req{job="api"} * on(job) group_left(owner) info'
selectors 'info * on(job) group_right(owner) req{job="api"}'
selectors 'req{instance="i1"} / ignoring(team) group_left sum without (team) (lim)'
selectors '(req{instance="i1"} - ignoring(team) lim) / on(instance) lim'
selectors 'req{instance="i1"} / on(instance) label_replace(lim, "x", "y", "", "")'
selectors 'rate({__name__=~"m1|m2", a="2"}[1m]) / on(a) m3'
selectors 'm3 / on(a) rate({__name__=~"m1|m2", a="2"}[1m])'

$CLIENT -q "DROP TABLE prometheus_empty"
$CLIENT -q "DROP TABLE prometheus"
