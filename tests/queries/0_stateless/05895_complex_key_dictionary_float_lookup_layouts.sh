#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE src_float_keys (k Float64, v UInt8, lo UInt64, hi UInt64) ENGINE = Memory;
    INSERT INTO src_float_keys VALUES (0, 1, 0, 10), (nan, 2, 0, 10);
"

for layout in COMPLEX_KEY_DIRECT COMPLEX_KEY_HASHED COMPLEX_KEY_SPARSE_HASHED COMPLEX_KEY_HASHED_ARRAY COMPLEX_KEY_CACHE COMPLEX_KEY_SSD_CACHE COMPLEX_KEY_RANGE_HASHED; do
    options=""
    lifetime="LIFETIME(0)"
    range_columns=""
    range_clause=""
    range_argument=""
    case "$layout" in
        COMPLEX_KEY_DIRECT)
            lifetime=""
            ;;
        COMPLEX_KEY_CACHE)
            options="SIZE_IN_CELLS 128"
            lifetime="LIFETIME(3600)"
            ;;
        COMPLEX_KEY_SSD_CACHE)
            options="BLOCK_SIZE 4096 FILE_SIZE 65536 WRITE_BUFFER_SIZE 4096 PATH '$USER_FILES_PATH/${CLICKHOUSE_DATABASE}_float_keys'"
            lifetime="LIFETIME(3600)"
            ;;
        COMPLEX_KEY_RANGE_HASHED)
            range_columns=", lo UInt64, hi UInt64"
            range_clause="RANGE(MIN lo MAX hi)"
            range_argument=", toUInt64(5)"
            ;;
    esac

    echo "$layout"
    $CLICKHOUSE_CLIENT --query "
        CREATE DICTIONARY dict_float_keys (k Float64, v UInt8 $range_columns)
        PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'src_float_keys'))
        LAYOUT($layout($options)) $lifetime $range_clause;

        SELECT
            dictHas('dict_float_keys', tuple(0.) $range_argument),
            dictHas('dict_float_keys', tuple(-0.) $range_argument),
            dictHas('dict_float_keys', tuple(nan) $range_argument),
            dictHas('dict_float_keys', tuple(reinterpretAsFloat64(toUInt64(9221120237041090561))) $range_argument);
        SELECT
            dictGetOrDefault('dict_float_keys', 'v', tuple(0.) $range_argument, 42),
            dictGetOrDefault('dict_float_keys', 'v', tuple(-0.) $range_argument, 42),
            dictGetOrDefault('dict_float_keys', 'v', tuple(nan) $range_argument, 42),
            dictGetOrDefault('dict_float_keys', 'v', tuple(reinterpretAsFloat64(toUInt64(9221120237041090561))) $range_argument, 42);
        SELECT groupArray(dictHas('dict_float_keys', tuple(k) $range_argument)),
               groupArray(dictGetOrDefault('dict_float_keys', 'v', tuple(k) $range_argument, 42))
        FROM (SELECT arrayJoin([-0., reinterpretAsFloat64(toUInt64(9221120237041090561))]) AS k);

        DROP DICTIONARY dict_float_keys;
    "
done

$CLICKHOUSE_CLIENT --query "DROP TABLE src_float_keys"
