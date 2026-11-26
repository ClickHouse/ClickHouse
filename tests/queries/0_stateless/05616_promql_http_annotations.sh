#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: ANTLR4 support is disabled in the fast-test build, and the PromQL grammar requires it.

# The `warnings` and `infos` arrays of /api/v1/query and /api/v1/query_range. The expected strings are
# what Prometheus v3.5.0 returns for the same data, except that it returns them in a random order.
# This file has the warnings of the quantile functions. The other cases are in 05876_promql_http_annotations_counter_name,
# 05877_promql_http_annotations_expressions and 05878_promql_http_annotations_probe.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./05616_promql_http_annotations.lib
. "$CUR_DIR"/05616_promql_http_annotations.lib

create_annotations_table

echo "--- quantile: a constant phi outside [0, 1], even without data"
instant $T 'quantile(2, foo)'
instant $T 'quantile((2), foo)'
instant $T 'quantile(-(2), foo)'
instant $T 'quantile(NaN, foo)'
instant $T 'quantile(Inf, foo)'
instant $T 'quantile(1e6, foo)'
instant $T 'quantile(1234567, foo)'
instant $T 'quantile(-0.00001, foo)'
instant $T 'quantile(1.5e-7 - 1, foo)'
instant $T 'quantile(2, nonexistent)'
instant $T 'quantile(0.5, foo)'
instant $T 'quantile(2, foo) + quantile(2, foo)'

echo "--- histogram_quantile: even without data"
instant $T 'histogram_quantile(2, req_seconds_bucket)'
instant $T 'histogram_quantile(2, nonexistent)'

echo "--- quantile_over_time: only if a window has a sample"
instant $T 'quantile_over_time(2, foo[5m])'
instant $T 'quantile_over_time(2, baz[5m])'
instant 1700000100 'quantile_over_time(2, baz[5m])'
instant $T 'quantile_over_time(2, foo[5m:1m])'
instant $T 'sum(quantile_over_time(2, foo[5m])) - avg(quantile_over_time(2, foo[5m]))'

$CLICKHOUSE_CLIENT -q "DROP TABLE ts"
