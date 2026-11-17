#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no Parquet support.

# The second half of the round trip of the corpus datasets through the ClickHouse Parquet writer,
# split from `05478_parquet_corpus_clickhouse_writer` to keep each test under the time limit of the
# flaky check. `05478` runs the variants that change the layout of values, pages and row groups on
# every dataset and the variants below on the flat and the nested dataset; this test runs the
# variants below on the remaining datasets, and the default format version on every dataset.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CORPUS="$CUR_DIR/data_lake_corpus/parquet/pyarrow-26.0.0"
OUT="$CLICKHOUSE_TMP/parquet_corpus_clickhouse_writer_codecs_$CLICKHOUSE_DATABASE"
rm -rf "$OUT"
mkdir -p "$OUT"

VARIANTS=(
    "v1.0:output_format_parquet_version = '1.0'"
    "v2.4:output_format_parquet_version = '2.4'"
    "none:output_format_parquet_compression_method = 'none'"
    "snappy:output_format_parquet_compression_method = 'snappy'"
    "gzip:output_format_parquet_compression_method = 'gzip'"
    "brotli:output_format_parquet_compression_method = 'brotli'"
    "lz4:output_format_parquet_compression_method = 'lz4'"
    "no_index_bloom_checksums:output_format_parquet_write_page_index = 0, output_format_parquet_write_bloom_filter = 0, output_format_parquet_write_checksums = 0"
    "no_parallel:output_format_parquet_parallel_encoding = 0, max_threads = 1"
)
LATEST="v2.latest:output_format_parquet_version = '2.latest'"

for dataset in flat.v2.6.dpv1 nested.v2.6.dpv1 strings.dict.dpv1 numbers.plain.dpv1 decimals.flba logical all_nulls empty names
do
    # The Parquet writer does not support `Time64`, which the reader produces for TIME columns.
    except=""
    [ "$dataset" = logical ] && except="EXCEPT (t32_ms, t64_us)"
    case "$dataset" in
        flat.v2.6.dpv1|nested.v2.6.dpv1) variants=("$LATEST") ;;
        *) variants=("${VARIANTS[@]}" "$LATEST") ;;
    esac
    {
        echo "CREATE TABLE src ENGINE = Memory AS SELECT * $except FROM file('$CORPUS/$dataset.parquet', Parquet);"
        echo "CREATE TABLE back AS src ENGINE = Memory;"
        for variant in "${variants[@]}"
        do
            name=${variant%%:*}
            settings=${variant#*:}
            out="$OUT/$dataset.$name.parquet"
            echo "INSERT INTO FUNCTION file('$out', Parquet) SELECT * FROM src SETTINGS $settings;"
            echo "TRUNCATE TABLE back;"
            echo "INSERT INTO back SELECT * FROM file('$out', Parquet);"
            echo "SELECT '$dataset $name', if((SELECT count(), sum(cityHash64(tuple(*))) FROM src) = (SELECT count(), sum(cityHash64(tuple(*))) FROM back), 'ok', 'MISMATCH');"
        done
    } > "$OUT/$dataset.sql"
    $CLICKHOUSE_LOCAL --session_timezone=UTC --queries-file "$OUT/$dataset.sql"
done

rm -rf "$OUT"
