#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_config.xml"

cat > "$config" <<'EOF'
<clickhouse>
    <max_table_num_to_throw>1</max_table_num_to_throw>
</clickhouse>
EOF

# A non-APPEND refresh replaces the target table, so it must not hit `max_table_num_to_throw`.
${CLICKHOUSE_LOCAL} --config-file="$config" --query "
CREATE DATABASE d ENGINE = Atomic;
CREATE MATERIALIZED VIEW d.mv REFRESH EVERY 1 YEAR (x UInt64) ENGINE = MergeTree ORDER BY x EMPTY AS SELECT 1 AS x;
SYSTEM REFRESH VIEW d.mv;
SYSTEM WAIT VIEW d.mv;
SELECT * FROM d.mv;
"

rm -f "$config"
