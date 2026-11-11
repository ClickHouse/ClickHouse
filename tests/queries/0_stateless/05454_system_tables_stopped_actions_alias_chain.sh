#!/usr/bin/env bash
# Tags: no-replicated-database

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

source_database="${CLICKHOUSE_DATABASE}_stopped_actions_chain_src"
overlay_database="${CLICKHOUSE_DATABASE}_stopped_actions_chain_ov"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --multiquery --query "
        DROP DATABASE IF EXISTS ${overlay_database} SYNC;
        DROP DATABASE IF EXISTS ${source_database} SYNC;
    "
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE DATABASE ${source_database};
    CREATE DATABASE ${overlay_database} ENGINE = Overlay(${source_database});

    -- The aliases name their targets before the targets exist, so every hop is a lazy stand-in.
    CREATE TABLE ${source_database}.a ENGINE = Alias('${overlay_database}', 'b');
    CREATE TABLE ${source_database}.b ENGINE = Alias('${overlay_database}', 't');
    CREATE TABLE ${source_database}.t (x UInt64) ENGINE = MergeTree ORDER BY x;
"

# Stopping through the outermost alias reaches `t` through `ov.b` and `ov.t`.
${CLICKHOUSE_CLIENT} --multiquery --query "
    SYSTEM STOP MERGES ${source_database}.a;
    SELECT stopped_actions FROM system.tables WHERE database = '${source_database}' AND name = 't';

    -- So the same entry is released by naming t directly.
    SYSTEM START MERGES ${source_database}.t;
    SELECT stopped_actions FROM system.tables WHERE database = '${source_database}' AND name = 't';

    -- Merges really run again: this throws ABORTED if the stop was left behind.
    INSERT INTO ${source_database}.t VALUES (1);
    INSERT INTO ${source_database}.t VALUES (2);
    OPTIMIZE TABLE ${source_database}.t FINAL;
    SELECT count() FROM ${source_database}.t;
"
