#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Parquet is not available in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Parquet files with data pages that have no values, https://github.com/ClickHouse/ClickHouse/issues/128300

# x Array(Int32) = [1], [2], [3] written by pyarrow with data_page_size=1, write_batch_size=1, data page v1 and v2:
# pages with 1, 1, 0, 1 values, the offset index gives the empty page and the next one the same first row.
SIMPLE_V1='UEFSMRUEFRgVHEwVBhUAEgAADCwBAAAAAgAAAAMAAAAVABUeFSIsFQIVEBUGFQYAAA84AgAAAAIAAgAAAAIDAQIAFQAVHhUiLBUCFRAVBhUGAAAPOAIAAAACAAIAAAACAwECARUAFRIVFiwVABUQFQYVBgAACSAAAAAAAAAAAAEVABUeFSIsFQIVEBUGFQYAAA84AgAAAAIAAgAAAAIDAgICGUwWQBVEFgAAFoQBFUQWAgAWyAEVOBYEABaAAhVEFgQAABUEGUw1ABgGc2NoZW1hFQIANQIYAXgVAhUGTDwAAAA1BBgEbGlzdBUCABUCJQIYB2VsZW1lbnQAFgYZHBkcJgAcFQIZNQAGEBk4AXgEbGlzdAdlbGVtZW50FQIWBhaoAha8AiZAJggpLBUEFQAVAgAVABUQFQgAPCkmBgAZRgAAAAYAABbEAhVEABaoAhYGJggWvAIAKCBwYXJxdWV0LWNwcC1hcnJvdyB2ZXJzaW9uIDI0LjAuMBkcHAAAAL0AAABQQVIx'
SIMPLE_V2='UEFSMRUEFRgVHEwVBhUAEgAADCwBAAAAAgAAAAMAAAAVBhUOFQ5cFQIVABUCFRAVBBUEEgAAAgACAwECABUGFQ4VDlwVAhUAFQIVEBUEFQQSAAACAAIDAQIBFQYVAhUCXBUAFQAVABUQFQAVABIAAAEVBhUOFQ5cFQIVABUCFRAVBBUEEgAAAgACAwICAhlMFkAVOhYAABZ6FToWAgAWtAEVLhYEABbiARU6FgQAABUEGUw1ABgGc2NoZW1hFQIANQIYAXgVAhUGTDwAAAA1BBgEbGlzdBUCABUCJQIYB2VsZW1lbnQAFgYZHBkcJgAcFQIZNQAGEBk4AXgEbGlzdAdlbGVtZW50FQIWBhaQAhaUAiZAJggpLBUEFQAVAgAVABUQFQgAPCkmBgAZRgAAAAYAABacAhVCABaQAhYGJggWlAIAKCBwYXJxdWV0LWNwcC1hcnJvdyB2ZXJzaW9uIDI0LjAuMBkcHAAAAL0AAABQQVIx'
# m Map(String, Int32), aa Array(Array(Int32)), 5 rows, data page v2, data_page_size=64: an empty page in every leaf.
NESTED_V2='UEFSMRUEFTIVNEwVChUAEgAAGRABAAAAMAEFPDEBAAAAMgEAAAAzAQAAADQVBhUQFRBcFQgVABUIFRAVBBUEEgAACAAIAgID5AAVBhUCFQJcFQAVABUAFRAVABUAEgAAAhUGFQ4VDlwVAhUAFQIVEBUEFQQSAAACAAICAwIEFQQVKBUsTBUKFQASAAAUTAAAAAABAAAAAgAAAAMAAAAEAAAAFQYVEBUQXBUIFQAVCBUQFQQVBBIAAAgACAMCA+QAFQYVAhUCXBUAFQAVABUQFQAVABIAAAIVBhUOFQ5cFQIVABUCFRAVBBUEEgAAAgACAwMCBBUEFTAVNEwVDBUAEgAAGFwAAAAAAQAAAAIAAAADAAAABAAAAAUAAAAVBhUSFRJcFQgVABUEFRAVBBUGEgAAA0QACAUCA5QAFQYVFBUUXBUIFQAVBBUQFQQVBhIAAANEAAgFAwPaCAAVBhUCFQJcFQAVABUAFRAVABUAEgAAAxUGFRQVFFwVBBUAFQIVEBUEFQYSAAADBAAEBQMDLAAAGTwWWBU8FgAAFpQBFS4WCAAWwgEVOhYIABk2CAACABk8FsQCFTwWAAAWgAMVLhYIABauAxU6FggAABlMFrgEFT4WAAAW9gQVQBYEABa2BRUuFggAFuQFFUAWCAAAFQQZrDUAGAZzY2hlbWEVBAA1AhgBbRUCFQJMLAAAADUEGAlrZXlfdmFsdWUVBAAVDCUAGANrZXklAEwcAAAAFQIlAhgFdmFsdWUANQIYAmFhFQIVBkw8AAAANQQYBGxpc3QVAgA1AhgHZWxlbWVudBUCFQZMPAAAADUEGARsaXN0FQIAFQIlAhgHZWxlbWVudAAWChkcGTwmABwVDBk1AAYQGTgBbQlrZXlfdmFsdWUDa2V5FQIWChbyARb0ASZYJggpLBUEFQAVAgAVABUQFQYAPBYKGSYKABk2AAAKAAAWpAYVPgAmABwVAhk1AAYQGTgBbQlrZXlfdmFsdWUFdmFsdWUVAhYKFugBFuwBJsQCJvwBKSwVBBUAFQIAFQAVEBUGADwpJgoAGUYAAAAKAAAW4gYVNgAmABwVAhk1AAYQGVgCYWEEbGlzdAdlbGVtZW50BGxpc3QHZWxlbWVudBUCFhQWuAIWvAImuAQm6AMpLBUEFQAVAgAVABUQFQgAPCk2CgoAGWYAAAAAABQAABaYBxVGABaSBhYKJggWnAYAKCBwYXJxdWV0LWNwcC1hcnJvdyB2ZXJzaW9uIDI0LjAuMBk8HAAAHAAAHAAAAM8BAABQQVIx'
# x Nullable(Int32) = 0, 1, 4, 5, 6, 7 in four v1 pages with 2, 0, 2, 2 values, offset index first rows 0, 2, 2, 4,
# column index min/max per page [0, 1], [2, 3], [4, 5], [6, 7].
FLAT='UEFSMRUAFRwVHCwVBBUAFQYVBgAAAgAAAAQBAAAAAAEAAAAVABUcFRwsFQAVABUGFQYAAAIAAAAEAQIAAAADAAAAFQAVHBUcLBUEFQAVBhUGAAACAAAABAEEAAAABQAAABUAFRwVHCwVBBUAFQYVBgAAAgAAAAQBBgAAAAcAAAAZQQICAgIZSAQAAAAABAIAAAAEBAAAAAQGAAAAGUgEAQAAAAQDAAAABAUAAAAEBwAAABUCGUYAAAAAKYYABAAEAAQABAAZTBYIFT4WAAAWRhU+FgQAFoQBFT4WBAAWwgEVPhYIAAAVBBksNQAYBnNjaGVtYRUCABUCJQIYAXgAFgwZHBkcJgAcFQIZJQYAGRgBeBUAFgwW+AEW+AEmCDwYBAcAAAAYBAAAAAAWACgEBwAAABgEAAAAABERABkcFQAVABUIADwpBhkmABAAABaKAxVCFoACFYoBABb4ARYMJggW+AEAKCBwYXJxdWV0LWNwcC1hcnJvdyB2ZXJzaW9uIDI0LjAuMBkcHAAAAKcAAABQQVIx'

D=$(cd "$CLICKHOUSE_TMP" && pwd)
echo "$SIMPLE_V1" | base64 --decode > "$D/simple_v1.parquet"
echo "$SIMPLE_V2" | base64 --decode > "$D/simple_v2.parquet"
echo "$NESTED_V2" | base64 --decode > "$D/nested_v2.parquet"
echo "$FLAT" | base64 --decode > "$D/flat.parquet"
# The same, but the second page has its 2 values (2, 3) while the offset index still gives it no rows.
${CLICKHOUSE_LOCAL} -q "SELECT overlay(base64Decode('$FLAT'), '\\x04', 44) FORMAT RawBLOB" > "$D/flat_hidden.parquet"

for file in simple_v1 simple_v2; do
    for use_offset_index in 1 0; do
        for max_block_size in 1000 1; do
            ${CLICKHOUSE_LOCAL} -q "SELECT '$file', $use_offset_index, $max_block_size, groupArray(x) FROM (SELECT x FROM file('$D/$file.parquet') ORDER BY x)
                SETTINGS input_format_parquet_use_offset_index = $use_offset_index, input_format_parquet_max_block_size = $max_block_size"
        done
    done
done

${CLICKHOUSE_LOCAL} -q "SELECT 'simple_v2 has(x, 3)', groupArray(x) FROM (SELECT x FROM file('$D/simple_v2.parquet') WHERE has(x, 3) ORDER BY x)
    SETTINGS input_format_parquet_use_offset_index = 1"

for use_offset_index in 1 0; do
    ${CLICKHOUSE_LOCAL} -q "SELECT 'nested_v2', $use_offset_index, m, aa FROM file('$D/nested_v2.parquet') ORDER BY aa
        SETTINGS input_format_parquet_use_offset_index = $use_offset_index"
done

for use_offset_index in 1 0; do
    ${CLICKHOUSE_LOCAL} -q "SELECT 'flat', $use_offset_index, groupArray(x) FROM (SELECT x FROM file('$D/flat.parquet') ORDER BY x)
        SETTINGS input_format_parquet_use_offset_index = $use_offset_index"
done

${CLICKHOUSE_LOCAL} -q "SELECT 'flat filtered', groupArray(x) FROM (SELECT x FROM file('$D/flat.parquet') WHERE x < 2 OR (x > 3 AND x < 6) ORDER BY x)
    SETTINGS input_format_parquet_page_filter_push_down = 1, input_format_parquet_use_offset_index = 1"

${CLICKHOUSE_LOCAL} -q "SELECT 'flat_hidden', groupArray(x) FROM (SELECT x FROM file('$D/flat_hidden.parquet') ORDER BY x)
    SETTINGS input_format_parquet_use_offset_index = 1"
${CLICKHOUSE_LOCAL} -q "SELECT 'flat_hidden x = 2', groupArray(x) FROM (SELECT x FROM file('$D/flat_hidden.parquet') WHERE x = 2 ORDER BY x)
    SETTINGS input_format_parquet_page_filter_push_down = 1, input_format_parquet_use_offset_index = 1"
