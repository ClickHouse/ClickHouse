#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-parallel: the `database_remove_detached_permanently_flag_fail` failpoint is process-global and would
# make the first `CREATE TABLE` of any concurrently running test fail.
# no-replicated-database: `SYSTEM ENABLE FAILPOINT` is process-local, but the test cluster executes
# the `CREATE TABLE` on all replicas.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `CREATE TABLE` can throw after the table was already published: `DatabaseOnDisk::createTable` commits
# the table and only then removes the permanent-detach flag. The table stays in the database with its
# metadata, so it must keep its named collection dependency, and `DROP NAMED COLLECTION` must be refused.

NC="nc_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:11111/test.csv', format = 'CSV'"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT database_remove_detached_permanently_flag_fail"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t (x UInt32) ENGINE = URL(${NC})" 2>&1 | grep -o -m1 "FAULT_INJECTED"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT database_remove_detached_permanently_flag_fail"

${CLICKHOUSE_CLIENT} -q "EXISTS TABLE t"
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION ${NC}" 2>&1 | grep -o -m1 "NAMED_COLLECTION_IS_USED"

${CLICKHOUSE_CLIENT} -m -q "
DROP TABLE t;
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"
