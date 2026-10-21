#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings

# A table created before the implicit min-max indices were enabled by default may declare an index of
# its own that happens to be named `auto_minmax_index_<column>`. On load such an index, when it is
# identical to the generated one, is treated as implicitly created only when the table's stored
# `SETTINGS` clause enables the implicit-index policy of the indexed column's own category. A stored
# policy of another category says nothing about the origin of the index: treating it as implicit then
# made it disappear from the definition on the next metadata rewrite, and from the table once the
# numeric policy was turned off.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --query "
ATTACH TABLE t_string_disabled UUID '10000000-0000-0000-0000-000000000001' (x UInt64, INDEX auto_minmax_index_x x TYPE minmax() GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1, add_minmax_index_for_string_columns = 0;

ATTACH TABLE t_other_category UUID '10000000-0000-0000-0000-000000000002' (x UInt64, INDEX auto_minmax_index_x x TYPE minmax() GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_string_columns = 0;

ATTACH TABLE t_other_category_enabled UUID '10000000-0000-0000-0000-000000000003' (x UInt64, INDEX auto_minmax_index_x x TYPE minmax() GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_string_columns = 1;

ATTACH TABLE t_numeric_stored UUID '10000000-0000-0000-0000-000000000004' (x UInt64, INDEX auto_minmax_index_x x TYPE minmax() GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1;

SELECT table, name, creation, granularity FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table LIKE 't\\_%' ORDER BY table, name;
"
