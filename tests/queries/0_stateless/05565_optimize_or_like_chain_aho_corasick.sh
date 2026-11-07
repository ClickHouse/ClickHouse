#!/usr/bin/env bash
# Tags: no-fasttest
# With Aho-Corasick support, `multiSearchAny` accepts more than 255 constant needles, so
# `optimize_or_like_chain` rewrites a chain of 256 pure-substring LIKE predicates into it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_or_like_aho (s String) ENGINE = Memory;
    INSERT INTO t_or_like_aho VALUES ('xxneedle005xx'), ('nothing matches here'), ('endneedle255');
"

predicate=""
for i in $(seq -w 0 255); do
    if [ -n "$predicate" ]; then
        predicate="$predicate OR "
    fi
    predicate="${predicate}s LIKE '%needle${i}%'"
done

query="SELECT count() FROM t_or_like_aho WHERE ${predicate}"

${CLICKHOUSE_CLIENT} -q "${query} SETTINGS optimize_or_like_chain = 0"
${CLICKHOUSE_CLIENT} -q "${query} SETTINGS optimize_or_like_chain = 1"
${CLICKHOUSE_CLIENT} -q "EXPLAIN QUERY TREE ${query} SETTINGS optimize_or_like_chain = 1" | grep -c 'function_name: multiSearchAny,'

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_or_like_aho"
