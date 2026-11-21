#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# Merge-on-read deletes in Iceberg tables written by Apache Iceberg (Spark 3.4.1 + Iceberg 1.7.1, Iceberg 1.11.0 Java API).
# The tables come from the parquetry test kit, see `data_iceberg/tileverse/README.md`.
# The metadata is rooted at `file:///iceberg-<corpus>/<table>`, which is resolved relative to the table directory;
# the queries run with `data_iceberg/tileverse` as the current directory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cd "$CUR_DIR/data_iceberg/tileverse" || exit 1

$CLICKHOUSE_LOCAL -q "
-- Format version 2, one data file with id 0..999 (value = id * 1.5) and one positional delete file
-- with 110 positions (300..399 and 595..604). When the table was vendored, the paths were rewritten,
-- but not the \`lower_bounds\`/\`upper_bounds\` of \`file_path\` (field id 2147483546) of the delete file,
-- which still name the data file at its original path. By these bounds the delete file does not
-- reference the data file, so Apache Iceberg (\`DeleteFileIndex\`) and PyIceberg do not apply it and
-- read 1000 rows; parquetry ignores the bounds and expects 890.
SELECT 'positional';
SELECT count(), sum(id), countIf(id BETWEEN 300 AND 399), countIf(id BETWEEN 595 AND 604), countIf(value != id * 1.5) FROM icebergLocal('iceberg-deletes/positional');

-- Format version 2, an equality delete keyed on (category, flag) with the tuples ('a', 'x') and (NULL, 'y'):
-- only ids 1 and 2 match, NULL matches NULL.
SELECT 'equality';
SELECT * FROM icebergLocal('iceberg-deletes/equality') ORDER BY id;
SELECT count() FROM icebergLocal('iceberg-deletes/equality');

-- Partitioned by identity(category); \`category\` is dropped from the data files and comes from the partition
-- value. An equality delete keyed on \`category\` = 'a' removes ids 0..4.
SELECT 'equality-evolved';
SELECT * FROM icebergLocal('iceberg-deletes/equality-evolved') ORDER BY id;
SELECT count() FROM icebergLocal('iceberg-deletes/equality-evolved') WHERE category = 'a';

-- Format version 3, one data file with id 0..99 and a deletion vector removing positions 10..14, 50, 73, 97..99.
SELECT 'deletion-vectors';
SELECT count(), sum(id), countIf(value != id * 1.5), countIf(category != ['a', 'b', 'c'][id % 3 + 1]) FROM icebergLocal('iceberg-deletes/deletion-vectors');
SELECT arrayFilter(x -> x NOT IN (SELECT id FROM icebergLocal('iceberg-deletes/deletion-vectors')), range(100));
SELECT id, _row_number FROM icebergLocal('iceberg-deletes/deletion-vectors') WHERE id BETWEEN 9 AND 15 ORDER BY id;
"
