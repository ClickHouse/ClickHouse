#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the `DataLakeCatalog` database engine needs the Avro and Parquet libraries.

# A temporary table created while a `DataLakeCatalog` database is current does not belong to that database,
# so `CREATE TEMPORARY TABLE ... AS` omits the skipping indices and projections of the source as for any `Memory` table.
# `ATTACH` does not connect to the catalog, so the endpoint does not have to exist.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DL_DATABASE="${CLICKHOUSE_DATABASE}_dl"

$CLICKHOUSE_CLIENT -m -q "
DROP DATABASE IF EXISTS ${DL_DATABASE};
ATTACH DATABASE ${DL_DATABASE}
ENGINE = DataLakeCatalog('http://localhost:18181/v1')
SETTINGS catalog_type = 'rest', warehouse = 'demo', auth_header = 'Authorization: Bearer a';

CREATE TABLE ${CLICKHOUSE_DATABASE}.src
(
    x UInt64,
    INDEX idx x TYPE minmax,
    PROJECTION p (SELECT x ORDER BY x)
)
ENGINE = MergeTree ORDER BY tuple();

USE ${DL_DATABASE};
CREATE TEMPORARY TABLE tmp AS ${CLICKHOUSE_DATABASE}.src;
INSERT INTO tmp VALUES (1);
SELECT x FROM tmp;

USE ${CLICKHOUSE_DATABASE};
DROP DATABASE ${DL_DATABASE};
"
