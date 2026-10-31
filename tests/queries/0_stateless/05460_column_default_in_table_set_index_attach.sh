#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

UUID=$(${CLICKHOUSE_CLIENT} --query "SELECT generateUUIDv4()")

${CLICKHOUSE_CLIENT} --multiquery <<EOF
-- A skip index over an ALIAS column that expands to IN <table> is rejected for a new definition,
-- but a definition accepted before that check existed must still load. Such a definition works with
-- a Set table on the right-hand side, which is a ready set. A full ATTACH TABLE loads the
-- definition the same way as the server does on startup.

DROP TABLE IF EXISTS t_index_attach;
DROP TABLE IF EXISTS t_index_attach_keys;

CREATE TABLE t_index_attach_keys (p UInt8) ENGINE = Set;
INSERT INTO t_index_attach_keys VALUES (1);

CREATE TABLE t_index_attach (p UInt8, flag UInt8 ALIAS p IN t_index_attach_keys, INDEX i flag TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY p; -- { serverError BAD_ARGUMENTS }

ATTACH TABLE t_index_attach UUID '$UUID' (p UInt8, flag UInt8 ALIAS p IN t_index_attach_keys, INDEX i flag TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY p;

INSERT INTO t_index_attach VALUES (1), (2);
SELECT p, flag FROM t_index_attach ORDER BY p;

DETACH TABLE t_index_attach;
ATTACH TABLE t_index_attach;
SELECT p, flag FROM t_index_attach ORDER BY p;

-- An unrelated ALTER does not break the existing index.
ALTER TABLE t_index_attach ADD COLUMN x UInt8;
SELECT p, flag, x FROM t_index_attach ORDER BY p;

-- A new index over the same column is rejected.
ALTER TABLE t_index_attach ADD INDEX j flag TYPE minmax GRANULARITY 1; -- { serverError BAD_ARGUMENTS }

-- So is an ALTER that makes an existing index read a table.
ALTER TABLE t_index_attach ADD COLUMN y UInt8 ALIAS x, ADD INDEX k y TYPE minmax GRANULARITY 1;
ALTER TABLE t_index_attach MODIFY COLUMN y UInt8 ALIAS x IN t_index_attach_keys; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_index_attach;
DROP TABLE t_index_attach_keys;
EOF
