#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# Row lineage (`_row_id`, `_last_updated_sequence_number`) of format version 3 tables written by the Apache Iceberg 1.11.0 Java API.
# The tables come from the parquetry test kit, see `data_iceberg/tileverse/README.md`.
# The metadata is rooted at `file:///iceberg-<corpus>/<table>`, which is resolved relative to the table directory;
# the queries run with `data_iceberg/tileverse` as the current directory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cd "$CUR_DIR/data_iceberg/tileverse" || exit 1


$CLICKHOUSE_LOCAL -q "
-- Three data files in two commits; no file materializes \`_row_id\`. Files A (id 100..104) and B (id 105..109) are in
-- one manifest with \`first_row_id\` 0 and NULL \`first_row_id\` of the data files, so file B inherits 0 + 5 (the record
-- count of file A). File C (id 110..114) is in the second manifest with \`first_row_id\` 10.
SELECT 'fresh';
SELECT id, value, _row_id, _last_updated_sequence_number FROM icebergLocal('iceberg-row-lineage/fresh') ORDER BY id;

-- File 0 (id 0..4) has synthesized lineage. File 1 (id 5..9, \`first_row_id\` 5) materializes \`_row_id\`
-- as [NULL, 1001, NULL, 1003, NULL]: a stored value wins, NULL resolves to \`first_row_id\` + position.
SELECT 'materialized';
SELECT id, value, _row_id, _last_updated_sequence_number FROM icebergLocal('iceberg-row-lineage/materialized') ORDER BY id;
-- A filter on \`_row_id\` must not prune file 1 by its \`first_row_id\` range [5, 10): its materialized values are outside of it.
-- Bug: today the file is pruned and nothing is returned (with \`use_iceberg_partition_pruning = 0\` the rows are found),
-- https://github.com/ClickHouse/ClickHouse/issues/130022.
SELECT id FROM icebergLocal('iceberg-row-lineage/materialized') WHERE _row_id > 1000 ORDER BY id;
"
