"""Job lists of the non-pyarrow Parquet writers (see `WRITERS` in `generate.py`)."""

import pyarrow as pa


def enc_for(table, kinds):
    """Column encoding map: `kinds` maps a type class to a Parquet encoding."""
    out = {}
    for f in table.schema:
        t = f.type
        if pa.types.is_boolean(t):
            k = "bool"
        elif pa.types.is_integer(t) or pa.types.is_date32(t) or pa.types.is_timestamp(t) or pa.types.is_time(t):
            k = "int"
        elif pa.types.is_floating(t):
            k = "float"
        elif pa.types.is_string(t) or pa.types.is_binary(t) or pa.types.is_large_string(t):
            k = "bytes"
        elif pa.types.is_fixed_size_binary(t):
            k = "fixed"
        elif pa.types.is_decimal(t):
            k = "decimal"
        else:
            continue
        if k in kinds:
            out[f.name] = kinds[k]
    return out


def _jobs(spec):
    """Build a job list from (name, dataset, options) tuples."""
    return [{"name": n, "dataset": d, "options": o} for n, d, o in spec]


def arrow_go_jobs(ds):
    spec = [
        ("flat", "flat", {}),
        ("flat.v1.0.dpv1", "flat", {"version": "1.0"}),
        ("flat.v2.6.dpv2", "flat", {"version": "2.6", "data_page_version": "2.0"}),
        ("flat.no_dict.no_stats", "flat", {"dictionary": False, "stats": False}),
        ("flat.tiny_pages.page_index", "flat", {"data_page_size": 128, "batch_size": 16, "page_index": True}),
        ("flat.row_group_23", "flat", {"row_group_size": 23, "page_index": True}),
        ("flat.bloom", "flat", {"bloom_filter": ["id", "s", "s_dict", "i64", "u64"], "bloom_ndv": 200}),
        ("flat.int96", "flat", {"int96": True}),
        ("flat.decimal_as_int", "flat", {"decimal_as_integer": True}),
        ("flat.dict_fallback.dpv2", "flat", {"dictionary_page_size": 64, "batch_size": 8, "data_page_version": "2.0"}),
        ("nested", "nested", {}),
        # With `data_page_size` 64 and `batch_size` 4 arrow-go v18.4.1 never finishes writing this file (memory grows without bound).
        ("nested.dpv2.small_pages", "nested", {"data_page_version": "2.0", "data_page_size": 512, "batch_size": 16, "page_index": True}),
        ("strings.plain", "strings", {"dictionary": False}),
        ("strings.delta_length.dpv2", "strings", {"data_page_version": "2.0", "encoding": enc_for(ds["strings"], {"bytes": "DELTA_LENGTH_BYTE_ARRAY"})}),
        ("strings.delta_byte_array", "strings", {"encoding": enc_for(ds["strings"], {"bytes": "DELTA_BYTE_ARRAY"})}),
        ("numbers.delta.dpv2", "numbers", {"data_page_version": "2.0", "encoding": enc_for(ds["numbers"], {"int": "DELTA_BINARY_PACKED"})}),
        ("numbers.bss", "numbers", {"encoding": enc_for(ds["numbers"], {"float": "BYTE_STREAM_SPLIT"})}),
        ("decimals", "decimals", {}),
        ("decimals.as_int", "decimals", {"decimal_as_integer": True}),
        ("all_nulls", "all_nulls", {}),
        ("empty", "empty", {}),
        ("logical", "logical", {}),
        ("names", "names", {}),
    ]
    spec += [(f"flat.{c}", "flat", {"compression": c}) for c in ["none", "snappy", "gzip", "brotli", "zstd", "lz4_raw"]]
    jobs = _jobs(spec)
    for j in jobs:
        j["library"] = "arrow-go"
        if j["name"] == "nested.dpv2.small_pages":
            j["out_of_spec"] = ("arrow-go v18.4.1 splits a row across data pages v2 (a page with `num_rows` 0 continuing the previous row), "
                                "which the format forbids; arrow-go and Arrow C++ read it, ClickHouse rejects it")
    return jobs


def parquet_go_jobs(ds):
    spec = [
        ("flat", "flat", {}),
        ("flat.dpv1", "flat", {"data_page_version": 1}),
        ("flat.plain.no_stats", "flat", {"encoding": {"int32": "PLAIN", "int64": "PLAIN", "bytes": "PLAIN"}, "statistics": False}),
        ("flat.dict", "flat", {"encoding": {"int32": "RLE_DICTIONARY", "int64": "RLE_DICTIONARY", "bytes": "RLE_DICTIONARY", "double": "RLE_DICTIONARY"}}),
        ("flat.delta", "flat", {"encoding": {"int32": "DELTA_BINARY_PACKED", "int64": "DELTA_BINARY_PACKED", "bytes": "DELTA_LENGTH_BYTE_ARRAY"}}),
        ("flat.delta_byte_array.dpv1", "flat", {"data_page_version": 1, "encoding": {"bytes": "DELTA_BYTE_ARRAY"}}),
        ("flat.bss", "flat", {"encoding": {"float": "BYTE_STREAM_SPLIT", "double": "BYTE_STREAM_SPLIT"}}),
        ("flat.rle_bool", "flat", {"encoding": {"bool": "RLE"}}),
        ("flat.small_pages.row_group_37", "flat", {"page_buffer_size": 256, "row_group_size": 37}),
        ("flat.bloom", "flat", {"bloom_filter": ["id", "s", "s_dict"]}),
    ]
    spec += [(f"flat.{c}", "flat", {"compression": c}) for c in ["none", "snappy", "gzip", "brotli", "zstd", "lz4_raw"]]
    jobs = _jobs(spec)
    for j in jobs:
        j["library"] = "parquet-go"
        # parquet-go v0.25.1 writes data page v1 with fewer definition levels than values: neither
        # parquet-go itself nor pyarrow can read the file, so it is kept as a corrupt file.
        if j["options"].get("data_page_version") == 1:
            j["corrupt"] = True
    return jobs


def polars_jobs(ds, old=False):
    spec = [
        ("flat", "flat", {}),
        ("flat.no_stats", "flat", {"statistics": False}),
        ("flat.row_group_29.page_200", "flat", {"row_group_size": 29, "data_page_size": 200}),
        ("nested", "nested", {}),
        ("nested.small_pages", "nested", {"data_page_size": 128}),
        ("strings", "strings", {}),
        ("numbers", "numbers", {}),
        ("decimals", "decimals", {}),
        ("all_nulls", "all_nulls", {}),
        ("empty", "empty", {}),
    ]
    spec += [(f"flat.{c}", "flat", {"compression": c}) for c in ["uncompressed", "snappy", "gzip", "lz4", "zstd", "brotli"]]
    if not old:
        spec += [("flat.full_stats", "flat", {"statistics": "full"}), ("logical", "logical", {}), ("names", "names", {}), ("wide", "wide", {})]
    return _jobs(spec)


# DuckDB-specific types; every value is deterministic.
DUCKDB_TYPES_SQL = """
SELECT
    i::BIGINT AS id,
    CASE WHEN i % 7 = 0 THEN NULL ELSE ('00000000-0000-4000-8000-' || lpad(printf('%x', i * 7919), 12, '0'))::UUID END AS u,
    CASE WHEN i % 5 = 0 THEN NULL ELSE (i * 1234567890123)::HUGEINT * 1000000007 END AS huge,
    CASE WHEN i % 4 = 0 THEN NULL ELSE (CASE i % 3 WHEN 0 THEN 'red' WHEN 1 THEN 'green' ELSE 'blue' END)::ENUM('red', 'green', 'blue') END AS color,
    CASE WHEN i % 8 = 0 THEN NULL ELSE TIME '00:00:00' + INTERVAL (i * 3601) SECOND END AS t,
    CASE WHEN i % 9 = 0 THEN NULL ELSE make_timestamp(2000 + i % 300, i % 12 + 1, i % 28 + 1, i % 24, i % 60, i % 60 + 0.123456)::TIMESTAMP_NS END AS ts_ns,
    CASE WHEN i % 10 = 0 THEN NULL ELSE make_timestamp(1950 + i, 1, 1, 0, 0, 0)::TIMESTAMPTZ END AS ts_tz,
    CASE WHEN i % 11 = 0 THEN NULL ELSE make_timestamp(1990 + i, 6, 15, 12, 30, 0)::TIMESTAMP_S END AS ts_s,
    CASE WHEN i % 12 = 0 THEN NULL ELSE make_timestamp(1990 + i, 6, 15, 12, 30, 0.5)::TIMESTAMP_MS END AS ts_ms,
    CASE WHEN i % 3 = 0 THEN NULL ELSE ('blob' || i)::BLOB END AS bl,
    {'k': i, 'v': [i::VARCHAR, NULL], 'm': MAP {i: 'x' || i}} AS st,
    CASE WHEN i % 2 = 0 THEN NULL ELSE MAP {'a': [i], 'b': []} END AS mp,
    (i * 1.5)::FLOAT AS fl,
    (i * 0.25)::DECIMAL(4, 2) AS d4,
    (i * 12345.6789)::DECIMAL(15, 4) AS d15,
    i % 2 = 0 AS bo,
    (i - 50)::TINYINT AS ti, (i * 200)::USMALLINT AS us, (i * 100000)::UINTEGER AS ui, (i * 1000000000000)::UBIGINT AS ub,
    DATE '1900-01-01' + INTERVAL (i * 1000) DAY AS dt
FROM range(100) t(i)
"""

DUCKDB_TYPES_NEW_SQL = """
SELECT i::BIGINT AS id, [i, i + 1, NULL]::INTEGER[3] AS arr3, [[i], [], NULL]::INTEGER[][3] AS arr3_nested
FROM range(50) t(i)
"""

# Types that ClickHouse reads as something else than DuckDB does (INTERVAL as raw `FixedString(12)`, UNION
# as a `Tuple` with the tag, UHUGEINT as `Float64`), so there is no common reference: read them only.
DUCKDB_TYPES_SMOKE_SQL = """
SELECT
    i::BIGINT AS id,
    CASE WHEN i % 6 = 0 THEN NULL ELSE (i * 98765432109)::UHUGEINT END AS uhuge,
    CASE WHEN i % 5 = 0 THEN NULL ELSE INTERVAL (i) MONTH + INTERVAL (i * 2) DAY + INTERVAL (i * 3) MILLISECOND END AS iv,
    CASE WHEN i % 4 = 0 THEN NULL ELSE union_value(num := i)::UNION(num INTEGER, str VARCHAR) END AS un
FROM range(50) t(i)
"""


def duckdb_jobs(ds, version):
    major, minor = (int(x) for x in version.split(".")[:2])
    spec = [
        ("flat", "flat", {}),
        ("flat.row_group_31", "flat", {"ROW_GROUP_SIZE": 31}),
        ("nested", "nested", {}),
        ("strings", "strings", {}),
        ("numbers", "numbers", {}),
        ("decimals", "decimals", {}),
        ("all_nulls", "all_nulls", {}),
        ("empty", "empty", {}),
        ("names", "names", {}),
    ]
    codecs = ["uncompressed", "snappy", "gzip", "zstd"] + (["brotli", "lz4", "lz4_raw"] if (major, minor) >= (1, 1) else [])
    spec += [(f"flat.{c}", "flat", {"COMPRESSION": c}) for c in codecs]
    if (major, minor) >= (1, 2):
        spec += [
            ("flat.v2", "flat", {"PARQUET_VERSION": "V2"}),
            ("nested.v2", "nested", {"PARQUET_VERSION": "V2"}),
            ("strings.v2", "strings", {"PARQUET_VERSION": "V2"}),
            ("numbers.v2", "numbers", {"PARQUET_VERSION": "V2"}),
            ("flat.dict_limit_10", "flat", {"DICTIONARY_SIZE_LIMIT": 10}),
            ("flat.bloom_fpp_0.001", "flat", {"BLOOM_FILTER_FALSE_POSITIVE_RATIO": 0.001}),
        ]
    jobs = _jobs(spec)
    jobs.append({"name": "duckdb_types", "dataset": None, "sql": DUCKDB_TYPES_SQL, "options": {}})
    if (major, minor) >= (1, 2):
        jobs.append({"name": "duckdb_types.v2", "dataset": None, "sql": DUCKDB_TYPES_SQL, "options": {"PARQUET_VERSION": "V2"}})
        jobs.append({"name": "duckdb_types_new", "dataset": None, "sql": DUCKDB_TYPES_NEW_SQL, "options": {}})
        jobs.append({"name": "duckdb_types_smoke", "dataset": None, "sql": DUCKDB_TYPES_SMOKE_SQL, "options": {}, "smoke": True})
    return jobs


def fastparquet_jobs(ds):
    spec = [
        ("flat", "flat", {}),
        ("flat.int96", "flat", {"times": "int96"}),
        ("flat.no_stats", "flat", {"stats": False}),
        ("flat.row_groups", "flat", {"row_group_offsets": 50}),
        ("flat.utf8", "flat", {"object_encoding": {"s": "utf8", "s_dict": "utf8", "bin": "bytes"}}),
        ("strings", "strings", {}),
        ("numbers", "numbers", {}),
        ("all_nulls", "all_nulls", {}),
        ("empty", "empty", {}),
        ("names", "names", {}),
        ("wide", "wide", {}),
    ]
    spec += [(f"flat.{c.lower()}", "flat", {"compression": c}) for c in ["SNAPPY", "GZIP", "ZSTD", "LZ4", "BROTLI", "LZ4_RAW"]]
    return _jobs(spec)


def spark_jobs(ds):
    v2 = {"parquet.writer.version": "PARQUET_2_0"}
    legacy = {"spark.sql.parquet.writeLegacyFormat": "true"}
    spec = [
        ("flat", "flat", {}, {}),
        ("flat.micros", "flat", {"spark.sql.parquet.outputTimestampType": "TIMESTAMP_MICROS"}, {}),
        ("flat.millis", "flat", {"spark.sql.parquet.outputTimestampType": "TIMESTAMP_MILLIS"}, {}),
        ("flat.legacy", "flat", legacy, {}),
        ("flat.v2", "flat", {}, v2),
        ("flat.no_dict", "flat", {}, {"parquet.enable.dictionary": "false"}),
        ("flat.bloom", "flat", {}, {**{f"parquet.bloom.filter.enabled#{c}": "true" for c in ["id", "s", "s_dict", "i64"]},
                                    **{f"parquet.bloom.filter.expected.ndv#{c}": "200" for c in ["id", "s", "s_dict", "i64"]}}),
        ("flat.small_pages", "flat", {}, {"parquet.page.size": "256", "parquet.page.row.count.limit": "16", "parquet.page.write-checksum.enabled": "true"}),
        ("flat.truncated_index", "flat", {}, {"parquet.columnindex.truncate.length": "3", "parquet.statistics.truncate.length": "3"}),
        ("flat.row_groups", "flat", {}, {"parquet.block.size": "4096", "parquet.page.size.row.check.min": "10",
                                         "parquet.block.size.row.check.min": "10", "parquet.block.size.row.check.max": "20"}),
        ("nested", "nested", {}, {}),
        ("nested.legacy", "nested", legacy, {}),
        ("nested.v2", "nested", {}, v2),
        ("nested.small_pages", "nested", {}, {"parquet.page.size": "128", "parquet.page.row.count.limit": "4"}),
        ("strings", "strings", {}, {}),
        ("strings.v2", "strings", {}, v2),
        ("strings.v2.no_dict", "strings", {}, {**v2, "parquet.enable.dictionary": "false"}),
        ("numbers.v2", "numbers", {}, v2),
        ("numbers.v2.no_dict", "numbers", {}, {**v2, "parquet.enable.dictionary": "false"}),
        ("decimals", "decimals", {}, {}),
        ("decimals.legacy", "decimals", legacy, {}),
        ("all_nulls", "all_nulls", {}, {}),
        ("empty", "empty", {}, {}),
        ("wide", "wide", {}, {}),
    ]
    spec += [(f"flat.{c}", "flat", {}, {"compression": c}) for c in ["none", "gzip", "lz4", "lz4_raw", "zstd"]]
    return [{"name": n, "dataset": d, "conf": c, "options": o} for n, d, c, o in spec]


def deltalake_jobs(ds):
    """`_default` and `_columns` are `ColumnProperties` of delta-rs (arrow-rs `WriterProperties`)."""
    def cols(dataset, kinds):
        return {c: {"encoding": e, "dictionary_enabled": False} for c, e in enc_for(ds[dataset], kinds).items()}

    spec = [
        ("flat", "flat", {}),
        ("flat.page_stats.small_pages", "flat", {"data_page_size_limit": 128, "write_batch_size": 16, "_default": {"statistics_enabled": "PAGE"}}),
        ("flat.no_stats.no_dict", "flat", {"_default": {"statistics_enabled": "NONE", "dictionary_enabled": False}}),
        ("flat.row_group_19", "flat", {"max_row_group_size": 19}),
        ("flat.rows_per_page_7", "flat", {"data_page_row_count_limit": 7, "write_batch_size": 7}),
        ("flat.bloom", "flat", {"_bloom": ["id", "s", "s_dict", "i64"]}),
        ("flat.rle_bool", "flat", {"_columns": {"b": {"encoding": "RLE", "dictionary_enabled": False}}}),
        ("nested", "nested", {}),
        ("nested.small_pages", "nested", {"data_page_size_limit": 64, "write_batch_size": 4, "data_page_row_count_limit": 3}),
        ("strings", "strings", {}),
        ("strings.plain", "strings", {"_default": {"dictionary_enabled": False}}),
        ("strings.delta_length", "strings", {"_columns": cols("strings", {"bytes": "DELTA_LENGTH_BYTE_ARRAY"})}),
        ("strings.delta_byte_array", "strings", {"_columns": cols("strings", {"bytes": "DELTA_BYTE_ARRAY", "fixed": "DELTA_BYTE_ARRAY"})}),
        ("numbers", "numbers", {}),
        ("numbers.delta", "numbers", {"_columns": cols("numbers", {"int": "DELTA_BINARY_PACKED"})}),
        ("numbers.bss", "numbers", {"_columns": cols("numbers", {"int": "BYTE_STREAM_SPLIT", "float": "BYTE_STREAM_SPLIT"})}),
        ("decimals", "decimals", {}),
        ("all_nulls", "all_nulls", {}),
    ]
    spec += [(f"flat.{c.lower()}", "flat", {"compression": c}) for c in ["UNCOMPRESSED", "SNAPPY", "GZIP", "BROTLI", "LZ4", "ZSTD", "LZ4_RAW"]]
    return _jobs(spec)
