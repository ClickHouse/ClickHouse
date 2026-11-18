#!/usr/bin/env bash

# A large `DateTime64` / `Time64` query parameter fits into the default `max_ast_elements`, also in a stored row policy.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ARR="[-1.5,$(seq -s, 10000)]"
MAP="{$(seq 10000 | awk '{ printf "%s%d:%d", (NR > 1 ? "," : ""), $1, $1 }')}"

${CLICKHOUSE_CLIENT} <<EOF_QUERY
SET cast_string_to_date_time_mode = 'best_effort';
SET param_arr = '${ARR}', param_map = '${MAP}';

DROP TABLE IF EXISTS t_large_time_params;
CREATE TABLE t_large_time_params (ts DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY ts;
INSERT INTO t_large_time_params SELECT toDateTime64(number, 3, 'UTC') FROM numbers(20);
INSERT INTO t_large_time_params VALUES ('1969-12-31 23:59:58.500');
CREATE ROW POLICY p_large_time_params ON t_large_time_params USING ts IN {arr:Array(DateTime64(3, 'UTC'))} TO ALL;
SELECT count(), min(ts) FROM t_large_time_params;
DROP ROW POLICY p_large_time_params ON t_large_time_params;
DROP TABLE t_large_time_params;

SELECT length({arr:Array(DateTime64(3, 'UTC'))}), arrayMin({arr:Array(DateTime64(3, 'UTC'))}), arrayMax({arr:Array(DateTime64(3, 'UTC'))});
SELECT length({map:Map(UInt16, Time64(3))}), {map:Map(UInt16, Time64(3))}[10000];
EOF_QUERY
