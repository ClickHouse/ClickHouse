#!/usr/bin/env bash
# Tags: no-fasttest
# Each key of a disk definition can be specified only once, except the keys that a disk reads as a list.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS t_dynamic_disk_repeated_key;

CREATE TABLE t_dynamic_disk_repeated_key (x UInt8) ENGINE = MergeTree ORDER BY tuple()
SETTINGS disk = disk(name = 'd_dynamic_disk_repeated_key', type = s3, type = local, endpoint = 'http://127.0.0.1:1/b/',
    no_sign_request = 1); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_dynamic_disk_repeated_key (x UInt8) ENGINE = MergeTree ORDER BY tuple()
SETTINGS disk = disk(name = 'd_dynamic_disk_repeated_key', type = s3, endpoint = 'http://127.0.0.1:1/b/',
    endpoint = 'http://127.0.0.1:2/b/', no_sign_request = 1); -- { serverError BAD_ARGUMENTS }

-- A repeated key in a nested disk.
CREATE TABLE t_dynamic_disk_repeated_key (x UInt8) ENGINE = MergeTree ORDER BY tuple()
SETTINGS disk = disk(name = 'd_dynamic_disk_repeated_key_encrypted', type = encrypted, key = '1234567812345678',
    disk = disk(name = 'd_dynamic_disk_repeated_key', type = s3, endpoint = 'http://127.0.0.1:1/b/',
        no_sign_request = 1, no_sign_request = 0)); -- { serverError BAD_ARGUMENTS }

-- The keys of an encrypted disk are read as a list, so they can be repeated.
CREATE TABLE t_dynamic_disk_repeated_key (x UInt8) ENGINE = MergeTree ORDER BY tuple()
SETTINGS disk = disk(type = encrypted, key = '1234567812345678', key = '8765432187654321', current_key = '8765432187654321',
    disk = disk(type = local, path = '${CLICKHOUSE_DISKS_FILES}/${CLICKHOUSE_DATABASE}_repeated_key/'));

INSERT INTO t_dynamic_disk_repeated_key VALUES (1), (2);
SELECT sum(x) FROM t_dynamic_disk_repeated_key;
DROP TABLE t_dynamic_disk_repeated_key SYNC;
"
