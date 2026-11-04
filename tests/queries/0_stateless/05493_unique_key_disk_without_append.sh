#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree
# UNIQUE KEY is refused on a disk that does not support writing with append: at CREATE, and at INSERT for an attached table.
# no-fasttest: needs the object storage disks and RocksDB.
# no-ordinary-database: ATTACH ... UUID needs an Atomic database.
# no-replicated-database: a plain_rewritable disk must not be shared between replicas.
# no-shared-merge-tree: UNIQUE KEY is supported only by plain MergeTree.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

UUID=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

$CLICKHOUSE_CLIENT --enable_unique_key 1 -n -q "
CREATE TABLE t_local (c0 Int32) ENGINE = MergeTree ORDER BY c0 UNIQUE KEY (c0) SETTINGS disk = 'local_plain_rewritable'; -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE t_enc (c0 Int32) ENGINE = MergeTree ORDER BY c0 UNIQUE KEY (c0) SETTINGS disk = 'encrypted_s3_plain_rewritable_cache'; -- { serverError SUPPORT_IS_DISABLED }
CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY c0 UNIQUE KEY (c0) SETTINGS disk = 'local_plain_rewritable' AS SELECT 1::Int32 AS c0; -- { serverError SUPPORT_IS_DISABLED }
CREATE MATERIALIZED VIEW rmv REFRESH EVERY 1 HOUR APPEND ENGINE = MergeTree ORDER BY c0 UNIQUE KEY (c0) SETTINGS disk = 'local_plain_rewritable' EMPTY AS SELECT 1::Int32 AS c0; -- { serverError SUPPORT_IS_DISABLED }

ATTACH TABLE t_attached UUID '$UUID' (c0 Int32) ENGINE = MergeTree ORDER BY c0 UNIQUE KEY (c0) SETTINGS disk = 'local_plain_rewritable';
INSERT INTO t_attached VALUES (1); -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM t_attached;
DROP TABLE t_attached SYNC;

CREATE TABLE t_s3 (c0 Int32) ENGINE = MergeTree ORDER BY c0 UNIQUE KEY (c0) SETTINGS disk = 's3_disk';
INSERT INTO t_s3 VALUES (1), (2);
INSERT INTO t_s3 VALUES (2);
SELECT count() FROM t_s3;
DROP TABLE t_s3;
"
