#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no Parquet support.

# Round trip of the corpus datasets through the ClickHouse Parquet writer: every dataset of
# `data_lake_corpus` (written by pyarrow, see `data_lake_corpus/manifest.jsonl`) is read, written
# back with every variant of the `output_format_parquet_*` settings, read again into the original
# structure and compared with the original. The same files are read by pyarrow, DuckDB, polars
# and fastparquet in `utils/data-lake-corpus/generate.py roundtrip`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CORPUS="$CUR_DIR/data_lake_corpus/parquet/pyarrow-26.0.0"
OUT="$CLICKHOUSE_TMP/parquet_corpus_clickhouse_writer_$CLICKHOUSE_DATABASE"
rm -rf "$OUT"
mkdir -p "$OUT"

# Variants that change how values, pages and row groups are laid out run on every dataset.
VARIANTS=(
    "default:output_format_parquet_row_group_size = 1000000"
    "small_groups_pages:output_format_parquet_row_group_size = 7, output_format_parquet_data_page_size = 64, output_format_parquet_batch_size = 3"
    "no_dictionary:output_format_parquet_max_dictionary_size = 0"
    "dictionary_fallback:output_format_parquet_max_dictionary_size = 64, output_format_parquet_batch_size = 4"
    "binary_strings:output_format_parquet_string_as_string = 0, output_format_parquet_fixed_string_as_fixed_byte_array = 0"
    "non_compliant_nested:output_format_parquet_compliant_nested_types = 0"
    "alp:output_format_parquet_use_alp_encoding = 1"
)

# Variants that only change the format version, the codec or the file trailer do not depend on
# the column types, so they run on the flat and the nested dataset only. Every query costs about
# the same regardless of the variant, and the full matrix ran over 180s in the flaky check.
EXTRA_VARIANTS=(
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

for dataset in flat.v2.6.dpv1 nested.v2.6.dpv1 strings.dict.dpv1 numbers.plain.dpv1 decimals.flba logical all_nulls empty names
do
    # The Parquet writer does not support `Time64`, which the reader produces for TIME columns.
    except=""
    [ "$dataset" = logical ] && except="EXCEPT (t32_ms, t64_us)"
    variants=("${VARIANTS[@]}")
    case "$dataset" in
        flat.v2.6.dpv1|nested.v2.6.dpv1) variants+=("${EXTRA_VARIANTS[@]}") ;;
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
