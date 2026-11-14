#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database, no-ordinary-database
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-parallel: the `database_atomic_drop_table_fail_before_replace` failpoint is process-global and would
# make the `DROP TABLE` of any concurrently running test fail.
# no-replicated-database: `SYSTEM ENABLE FAILPOINT` is process-local, but the test cluster executes
# the `DROP TABLE` on all replicas.
# no-ordinary-database: the failpoint is in `DatabaseAtomic`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `DROP TABLE` that throws before the metadata of the table is removed leaves the table in place, so
# it must keep its named collection dependency - including after `DETACH` and `ATTACH`, which leave a
# detached entry behind - and `DROP NAMED COLLECTION` must be refused.

NC="nc_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:11111/test.csv', format = 'CSV';
CREATE TABLE t (x UInt32) ENGINE = URL(${NC});
DETACH TABLE t;
ATTACH TABLE t;
"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT database_atomic_drop_table_fail_before_replace"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t" 2>&1 | grep -o -m1 "FAULT_INJECTED"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT database_atomic_drop_table_fail_before_replace"

${CLICKHOUSE_CLIENT} -q "EXISTS TABLE t"
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION ${NC}" 2>&1 | grep -o -m1 "NAMED_COLLECTION_IS_USED"

${CLICKHOUSE_CLIENT} -m -q "
DROP TABLE t;
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"
