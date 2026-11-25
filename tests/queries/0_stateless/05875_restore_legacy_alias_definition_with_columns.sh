#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Servers before 26.10 wrote the resolved columns of an `Alias` table into its definition, also in backups.
# Such a backup is emulated by adding the columns to the definition in a new backup, and it is still restored.

dir="${CLICKHOUSE_TMP:?}/05875"
rm -rf "${dir:?}"
mkdir -p "${dir}"
local_args=(--path="${dir}/data" -- --backups.allowed_path="${dir}/backup")

$CLICKHOUSE_LOCAL -q "
    CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t VALUES (1);
    CREATE TABLE a ENGINE = Alias(t);
    BACKUP TABLE a TO File('${dir}/backup/') FORMAT Null;
    DROP TABLE a" "${local_args[@]}"

$CLICKHOUSE_LOCAL --param_metadata="${dir}/backup/metadata/default/a.sql" --param_copy="${dir}/a.sql" -q "
    SET engine_file_truncate_on_insert = 1;
    INSERT INTO FUNCTION file({copy:String}, 'RawBLOB')
        SELECT replaceOne(raw_blob, ' ENGINE = Alias', ' (\`x\` UInt64) ENGINE = Alias') FROM file({metadata:String}, 'RawBLOB');
    INSERT INTO FUNCTION file({metadata:String}, 'RawBLOB') SELECT * FROM file({copy:String}, 'RawBLOB')"
grep -c 'UInt64' "${dir}/backup/metadata/default/a.sql"

$CLICKHOUSE_LOCAL -q "
    RESTORE TABLE a FROM File('${dir}/backup/') FORMAT Null;
    SELECT x FROM a" "${local_args[@]}"

rm -rf "${dir:?}"
