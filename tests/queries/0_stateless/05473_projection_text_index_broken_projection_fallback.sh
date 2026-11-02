#!/usr/bin/env bash

# A part whose projection text index is broken must evaluate the text predicate on its own columns.
# The virtual column of the direct read from the text index used to be considered materialized in such
# a part, so its source column was not read and the predicate saw default values, losing matching rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP}/05473_projection_text_index_broken_projection_fallback"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"

${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" --multiquery -q "
    SET allow_experimental_projection_text_index = 1;
    CREATE TABLE t (id UInt64, s String, PROJECTION idx INDEX s TYPE text(tokenizer = 'splitByNonAlpha'))
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
    INSERT INTO t SELECT number, if(number % 10 = 0, 'hello world', 'foo bar') FROM numbers(1000);
    INSERT INTO t SELECT number + 1000, if(number % 10 = 0, 'hello world', 'foo bar') FROM numbers(1000);
" </dev/null

PART_PATH=$(${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" -q "
    SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't' ORDER BY name LIMIT 1" </dev/null)

# The marks of the projection of the first part are gone, so the projection is loaded as broken.
truncate -s 0 "${PART_PATH}/idx.proj/data.cmrk3"

${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" --multiquery -q "
    SELECT 'broken projections', countIf(is_broken) FROM system.projection_parts WHERE database = currentDatabase() AND table = 't';
    SELECT count() FROM t WHERE hasToken(s, 'hello');
    SELECT count() FROM t WHERE hasAllTokens(s, ['hello']);
    SELECT count() FROM t WHERE hasToken(s, 'hello') SETTINGS query_plan_direct_read_from_text_index = 0;
" </dev/null

rm -rf "${WORKING_DIR}"
