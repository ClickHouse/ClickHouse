#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# Identity-partitioned tables written by PyIceberg, and a migrated table whose data files have no Parquet field ids.
# The tables come from the parquetry test kit, see `data_iceberg/tileverse/README.md`.
# The metadata is rooted at `file:///iceberg-<corpus>/<table>`, which is resolved relative to the table directory;
# the queries run with `data_iceberg/tileverse` as the current directory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cd "$CUR_DIR/data_iceberg/tileverse" || exit 1


$CLICKHOUSE_LOCAL -q "
-- Partitioned by identity(category) over 'a', 'b', 'c', 100 rows each: id 0..99, 1000..1099, 2000..2099; amount = id * 2.
-- In \`by_category_omitted\` the \`category\` column is dropped from the data files and comes from the partition value.
SELECT 'by_category';
SELECT category, count(), min(id), max(id), sum(amount), countIf(amount != id * 2) FROM icebergLocal('iceberg-partitioned/by_category') GROUP BY category ORDER BY category;
SELECT count(), min(id), max(id) FROM icebergLocal('iceberg-partitioned/by_category') WHERE category = 'b';
SELECT * FROM icebergLocal('iceberg-partitioned/by_category') WHERE id IN (99, 1000, 2099) ORDER BY id;
SELECT 'by_category_omitted';
SELECT category, count(), min(id), max(id), sum(amount), countIf(amount != id * 2) FROM icebergLocal('iceberg-partitioned/by_category_omitted') GROUP BY category ORDER BY category;
SELECT count(), min(id), max(id) FROM icebergLocal('iceberg-partitioned/by_category_omitted') WHERE category = 'b';
SELECT count() FROM icebergLocal('iceberg-partitioned/by_category_omitted') WHERE category IN ('a', 'c') AND id >= 1000;
SELECT * FROM icebergLocal('iceberg-partitioned/by_category_omitted') WHERE id IN (99, 1000, 2099) ORDER BY id;

-- The \`add_files\` migration shape: three data files of 1000 rows (\`alpha-N\`, \`bravo-N\`, \`charlie-N\`) without
-- Parquet field ids, read through \`schema.name-mapping.default\`. The values of \`n\` are pseudo-random; the
-- aggregates are the same as read by PyIceberg and from the Parquet files directly.
SELECT 'name-mapping/migrated';
SELECT count(), uniqExact(id), sum(n), countIf(n IS NULL) FROM icebergLocal('iceberg-name-mapping/migrated');
SELECT splitByChar('-', id)[1] AS part, count(), sum(n) FROM icebergLocal('iceberg-name-mapping/migrated') GROUP BY part ORDER BY part;
SELECT * FROM icebergLocal('iceberg-name-mapping/migrated') WHERE id IN ('alpha-0', 'alpha-1', 'charlie-999') ORDER BY id;
SELECT count() FROM icebergLocal('iceberg-name-mapping/migrated') AS t
    INNER JOIN file('iceberg-name-mapping/migrated/data/*.parquet', Parquet) AS f ON t.id = f.id AND t.n = f.n;
"
