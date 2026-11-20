#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A row policy whose filter is a SQL UDF that returns its argument unchanged resolves to the input column itself.
# Reading through `Merge` must still request the physical column, not the expression name.
# SQL UDFs are global, so the function name is made unique per test database.

UDF="identity_udf_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS r_identity_05397;
    DROP FUNCTION IF EXISTS ${UDF};

    CREATE TABLE r_identity_05397 (flag UInt8, v UInt8) ENGINE = MergeTree ORDER BY v;
    INSERT INTO r_identity_05397 VALUES (0, 0), (1, 1), (0, 2), (1, 3);

    CREATE FUNCTION ${UDF} AS x -> x;
    CREATE ROW POLICY policy_05397 ON r_identity_05397 USING ${UDF}(flag) TO ALL;

    SELECT v FROM r_identity_05397 ORDER BY v;
    SELECT v FROM merge(currentDatabase(), '^r_identity_05397\$') ORDER BY v;
    SELECT flag, v FROM merge(currentDatabase(), '^r_identity_05397\$') ORDER BY v;

    DROP ROW POLICY policy_05397 ON r_identity_05397;
    DROP TABLE r_identity_05397;
    DROP FUNCTION ${UDF};
"
