#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A lambda declared in the query shadows a SQL UDF of the same name called from the body of another UDF in `APPLY`.

# SQL user-defined functions are server-global rather than per-database, so the names have to embed the test database.
g="${CLICKHOUSE_DATABASE}_05617_g"
f="${CLICKHOUSE_DATABASE}_05617_f"

$CLICKHOUSE_CLIENT "
DROP FUNCTION IF EXISTS $f;
DROP FUNCTION IF EXISTS $g;
CREATE FUNCTION $g AS x -> x + 1;
CREATE FUNCTION $f AS x -> tuple($g(x), CAST('00000000-0000-0000-0000-000000000001', 'UUID'));

SET uuid_type_version = 2;

WITH (x -> x * 3) AS $g
SELECT * APPLY $f FROM values('a UInt8', 1);

DROP TABLE IF EXISTS t_05617;
CREATE TABLE t_05617 ENGINE = Memory AS
WITH (x -> x * 3) AS $g
SELECT * APPLY $f FROM values('a UInt8', 1);
SELECT * FROM t_05617;
DROP TABLE t_05617;

-- Without a lambda of that name, the UDF applies.
SELECT * APPLY $f FROM values('a UInt8', 1);

DROP FUNCTION $f;
DROP FUNCTION $g;
"

# An explicit `UUID1` keeps the historical type in `ALTER` with `data_type_default_nullable`.
$CLICKHOUSE_CLIENT "
SET uuid_type_version = 1;
DROP TABLE IF EXISTS t_05617_alter;
CREATE TABLE t_05617_alter (x UInt8) ENGINE = Memory;
SET uuid_type_version = 2, data_type_default_nullable = 1;
ALTER TABLE t_05617_alter ADD COLUMN u UUID1;
ALTER TABLE t_05617_alter ADD COLUMN v UUID;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_05617_alter' ORDER BY name;
ALTER TABLE t_05617_alter MODIFY COLUMN u UUID1;
ALTER TABLE t_05617_alter MODIFY COLUMN v UUID1;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_05617_alter' ORDER BY name;
DROP TABLE t_05617_alter;
"
