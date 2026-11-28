#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A parameterized function name is accepted only for the table function of a table expression.
# It must be rejected while the JSON is read, also in expressions that an AST node owns outside `children`.
PARAM='"name":"","query_parameter_name":{"type":"Identifier","name":"","children":[{"type":"QueryParameter","name":"f","param_type":"Identifier"}]}'

# TTL `GROUP BY ... SET` assignments.
$CLICKHOUSE_CLIENT -q "SELECT formatQueryFromJSON(replaceOne(parseQueryToJSON('CREATE TABLE t (ts DateTime, k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k TTL ts GROUP BY k SET v = max(v)'), '\"name\":\"max\"', '$PARAM'))" 2>&1 | grep -q BAD_ARGUMENTS && echo rejected

# TTL `GROUP BY` key.
$CLICKHOUSE_CLIENT -q "SELECT formatQueryFromJSON(replaceOne(parseQueryToJSON('CREATE TABLE t (ts DateTime, k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k TTL ts GROUP BY abs(k) SET v = max(v)'), '\"name\":\"abs\"', '$PARAM'))" 2>&1 | grep -q BAD_ARGUMENTS && echo rejected

# `BACKUP` partitions.
$CLICKHOUSE_CLIENT -q "SELECT formatQueryFromJSON(replaceOne(parseQueryToJSON('BACKUP TABLE t PARTITION tuple(abs(1)) TO Null'), '\"name\":\"abs\"', '$PARAM'))" 2>&1 | grep -q BAD_ARGUMENTS && echo rejected

# The table function of a table expression still accepts it.
$CLICKHOUSE_CLIENT -q "SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM {f:Identifier}(1)'))"
