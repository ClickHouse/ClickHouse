#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: ANTLR4 support is disabled in the fast-test build, and the PromQL grammar requires it.

# The `infos` array of /api/v1/query for `rate` and `increase` over a metric whose name has no counter suffix.
# The expected strings are what Prometheus v3.5.0 returns for the same data, except that it returns them in a random order.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./05616_promql_http_annotations.lib
. "$CUR_DIR"/05616_promql_http_annotations.lib

create_annotations_table

echo "--- rate and increase: each metric name without a counter suffix, only if there is a result"
instant $T 'rate(foo[5m])'
instant $T 'increase(foo[5m])'
instant $T 'irate(foo[5m])'
instant $T 'rate(bar_total[5m])'
instant $T 'rate(total_requests[5m])'
instant $T 'rate(req_seconds_bucket[5m])'
instant $T 'rate(baz[5m])'
instant $T 'rate(nonexistent[5m])'
instant $T 'rate({job="a"}[5m])'
instant 1700000100 'rate({job="a"}[5m])'
instant $T 'rate({__name__=~".+_total"}[5m])'
instant $T 'rate({"http.requests"}[5m])'
instant $T 'rate(foo[5m] offset 1m)'
instant $T 'rate(foo[5m] @ 1700000600)'

$CLICKHOUSE_CLIENT -q "DROP TABLE ts"
