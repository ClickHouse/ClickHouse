#!/usr/bin/env bash
# Tags: no-old-analyzer
# no-old-analyzer: make_distributed_plan requires the analyzer.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# clickhouse-local has no `stateless_worker_client` configured, so it is disabled by default -
# the same as a server with `stateless_worker_client.enabled = 0`. A plan that wants workers must
# be rejected instead of silently planning zero nodes and running on the initiator alone.
# `distributed_plan_execute_locally` is the way to run a plan in-process without workers; this
# query does not set it, so sizing the read falls to the (disabled) worker client. sum() and not
# a bare count(): the trivial-count optimization would otherwise fold the read away before the
# read's bucket count - and with it the worker sizing - is ever chosen.
${CLICKHOUSE_LOCAL} --multiquery --query "
CREATE TABLE t (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t SELECT number FROM numbers(10);
SELECT sum(k) FROM t SETTINGS make_distributed_plan = 1;
" 2>&1 | grep -o "Stateless worker client is not enabled"
