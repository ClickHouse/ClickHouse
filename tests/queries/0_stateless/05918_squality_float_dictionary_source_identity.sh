#!/usr/bin/env bash

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

database=$(${CLICKHOUSE_CLIENT} --query "SELECT currentDatabase()")

${CLICKHOUSE_CLIENT} --multiquery <<SQL
DROP DICTIONARY IF EXISTS squality_float_identity_dict;
DROP TABLE IF EXISTS squality_float_identity_source;

CREATE TABLE squality_float_identity_source
(
    k Float64,
    part UInt8,
    v String
) ENGINE = Memory;
INSERT INTO squality_float_identity_source
SELECT toFloat64(0), toUInt8(1), 'z-positive'
UNION ALL
SELECT reinterpretAsFloat64(toUInt64(9223372036854775808)), toUInt8(1), 'a-negative';

CREATE DICTIONARY squality_float_identity_dict
(
    k Float64,
    part UInt8,
    v String DEFAULT 'missing'
)
PRIMARY KEY k, part
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' PASSWORD '' DB '$database'
    QUERY 'SELECT k, part, v FROM \`$database\`.squality_float_identity_source WHERE {condition} ORDER BY v LIMIT 1'))
LAYOUT(COMPLEX_KEY_DIRECT());

SELECT dictHas('squality_float_identity_dict', (toFloat64(0), toUInt8(1))),
    dictGet('squality_float_identity_dict', 'v', (toFloat64(0), toUInt8(1)));
SELECT dictHas('squality_float_identity_dict',
    (reinterpretAsFloat64(toUInt64(9223372036854775808)), toUInt8(1))),
    dictGet('squality_float_identity_dict', 'v',
    (reinterpretAsFloat64(toUInt64(9223372036854775808)), toUInt8(1)));

DROP DICTIONARY squality_float_identity_dict;
DROP TABLE squality_float_identity_source;
SQL
