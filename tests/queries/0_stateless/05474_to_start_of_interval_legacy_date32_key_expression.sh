#!/usr/bin/env bash
# https://github.com/ClickHouse/ClickHouse/issues/116945
# With the legacy result type, a sorting or partition key `toStartOfInterval(d, ...)` over a `Date32` column stores wrapped values,
# so a condition on `d` must not be pushed through it. Key expressions use the server settings, hence `clickhouse-local`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --to_start_of_interval_preserves_argument_type=0 --query "
SET session_timezone = 'UTC';
CREATE TABLE ko (d Date32) ENGINE = MergeTree ORDER BY toStartOfInterval(d, INTERVAL 1 YEAR)
    SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, auto_statistics_types = '';
CREATE TABLE kp (d Date32) ENGINE = MergeTree PARTITION BY toStartOfInterval(d, INTERVAL 1 YEAR) ORDER BY tuple()
    SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, auto_statistics_types = '';
INSERT INTO ko VALUES ('2000-01-01'), ('2200-01-01'), ('2050-01-01');
INSERT INTO ko VALUES ('2000-01-01'), ('2200-01-01'), ('2050-01-01'), ('2060-01-01'), ('2070-01-01');
INSERT INTO kp VALUES ('2000-01-01'), ('2200-01-01'), ('2050-01-01');
SELECT toTypeName(toStartOfInterval(d, INTERVAL 1 YEAR)) FROM ko LIMIT 1;
SELECT count() FROM ko WHERE d >= toDate32('2100-01-01');
SELECT countIf(d >= toDate32('2100-01-01')) FROM ko;
SELECT count() FROM kp WHERE d >= toDate32('2100-01-01');
SELECT countIf(d >= toDate32('2100-01-01')) FROM kp;
"
