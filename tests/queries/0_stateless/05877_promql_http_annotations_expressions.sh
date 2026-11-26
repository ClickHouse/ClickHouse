#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: ANTLR4 support is disabled in the fast-test build, and the PromQL grammar requires it.

# The `warnings` and `infos` arrays of /api/v1/query and /api/v1/query_range: larger expressions, label matcher pushdown,
# subqueries, the cap of 10 annotations of each kind, range queries and errors. The expected strings are what Prometheus v3.5.0 returns
# for the same data, except that it returns them in a random order.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./05616_promql_http_annotations.lib
. "$CUR_DIR"/05616_promql_http_annotations.lib

create_annotations_table

echo "--- rate inside a larger expression: the position of its selector"
instant $T 'rate(foo[5m]) > 1e9'
instant $T 'rate(foo[5m]) + rate(foo[10m])'
instant $T 'sum(rate(foo[5m])) / count(rate(foo[5m]))'
instant $T 'label_replace(rate(foo[5m]), "x", "y", "", "")'
instant $T 'sum(
  rate(
    foo[5m]))'

echo "--- annotations are collected from the original tree, before the pushdown of label matchers"
instant $T 'rate(foo[5m]) and on(job) bar_total{job="missing"}'
instant $T 'rate(foo[5m]) and on(job) bar_total{job="missing"}' --data-urlencode 'promql_push_down_label_matchers=0'

echo "--- a subquery argument is reported at 1:1, as Prometheus does"
instant $T 'rate(foo[5m:1m])'
instant $T 'rate((foo + 1)[5m:1m])'
instant $T 'rate(bar_total[5m:1m])'
instant $T 'rate({__name__=~"foo|bar_total"}[5m:1m])'
instant $T 'rate((foo offset 1m)[5m:1m])'

echo "--- at most 10 of each kind"
instant $T 'rate({__name__=~"m.+"}[5m])'
instant $T 'quantile(2, rate({__name__=~"m.+"}[5m])) + quantile(3, foo)'

echo "--- range queries"
range 'quantile(2, foo)'
range 'quantile_over_time(2, baz[5m])'
range 'rate(foo[5m])'

echo "--- no annotations in an error"
instant $T 'quantile(2, foo) + rate(foo)'

$CLICKHOUSE_CLIENT -q "DROP TABLE ts"
