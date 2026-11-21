#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Raw format data can contain unmatched brackets and semicolons.
${CLICKHOUSE_LOCAL} --multiquery <<'SQL'
CREATE TABLE t (s String) ENGINE = Memory;
INSERT INTO t FORMAT TSV
);(

SELECT s FROM t;
SELECT (2);
SQL
