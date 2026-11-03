"""Public test corpora (both under the Apache License 2.0, like ClickHouse).

* apache/parquet-testing is the `contrib/parquet-testing` submodule. `05359_parquet_testing_corpus`
  already reads every file of it and records a snapshot of what ClickHouse returns; here every
  file that pyarrow can read gets a reference produced by pyarrow instead, so the values are
  checked against an independent implementation. The files are not copied: the test reads them
  from the submodule, pinned below to the submodule commit.
* apache/arrow-testing is not a submodule, so its few Parquet files are copied (with the
  provenance in the manifest): the files written by parquet-java and protobuf, and the small
  fuzzer reproducers, which must fail cleanly or be read.
"""

import hashlib
import subprocess
from pathlib import Path

SOURCES = {
    "parquet-testing": ("https://github.com/apache/parquet-testing", "56653c437c8092f704a092d0d1d4e600124cd49f"),
    "arrow-testing": ("https://github.com/apache/arrow-testing", "5a26d35785ff8761e56d79eebff971f6f737e18c"),
}

# Files read from the submodule; `{PARQUET_TESTING}` in the test SQL is the submodule path.
PARQUET_TESTING_GLOB = "data/**/*.parquet"
# Files whose reference would be too large to check in (the JSON of a 1 GiB string, tens of thousands of rows).
PARQUET_TESTING_SKIP = {
    "data/large_string_map.brotli.parquet",
    "data/overflow_i16_page_cnt.parquet",
    "data/alltypes_tiny_pages_plain.parquet",
    "data/hadoop_lz4_compressed_larger.parquet",
    "data/lz4_raw_compressed_larger.parquet",
}
MAX_EXPECTED_BYTES = 256 * 1024
# Files that ClickHouse does not read like pyarrow; what ClickHouse does with them is recorded by
# `05359_parquet_testing_corpus`, so there is no point in a reference here.
PARQUET_TESTING_NO_REFERENCE = {
    "data/concatenated_gzip_members.parquet": "CANNOT_DECOMPRESS: only the first gzip member of a page is decompressed",
    "data/datapage_v2_empty_datapage.snappy.parquet": "CANNOT_DECOMPRESS: an empty compressed data page v2",
    "data/datapage_v1-corrupt-checksum.parquet": "INCORRECT_DATA: ClickHouse verifies page checksums, pyarrow does not by default",
    "data/rle-dict-uncompressed-corrupt-checksum.parquet": "INCORRECT_DATA: ClickHouse verifies page checksums, pyarrow does not by default",
    "data/floating_orders_nan_count.parquet": "NaN values with different bit patterns, which JSON cannot represent",
    "data/int32_with_uuid_logical_type.parquet": "CANNOT_EXTRACT_TABLE_STRUCTURE: the UUID logical type on INT32 is invalid; pyarrow ignores it",
    "data/int96_from_spark.parquet": "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE: INT96 values outside of the DateTime64(9) range",
    "data/json.parquet": "INCORRECT_DATA: a JSON array at the top level does not fit the ClickHouse JSON type",
    "data/map_no_value.parquet": "a MAP without the value column is read as a Tuple of an Array of key tuples",
    "data/unknown-logical-type.parquet": "CANNOT_EXTRACT_TABLE_STRUCTURE: ClickHouse rejects an unknown logical type instead of ignoring it",
}

ARROW_TESTING_VALID = ["data/parquet/alltypes-java.parquet", "data/parquet/generated_simple_numerics/blogs.parquet"]
ARROW_TESTING_SMOKE_GLOB = "data/parquet/fuzzing/*"
ARROW_TESTING_SMOKE_MAX_SIZE = 16 * 1024


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def checkout(work):
    """Shallow-fetch every source at its pinned commit; return {source: path}."""
    out = {}
    for name, (url, commit) in SOURCES.items():
        d = work / "external" / name
        if not (d / ".git").exists():
            d.mkdir(parents=True, exist_ok=True)
            subprocess.run(["git", "init", "-q", str(d)], check=True)
            subprocess.run(["git", "-C", str(d), "fetch", "-q", "--depth", "1", url, commit], check=True)
            subprocess.run(["git", "-C", str(d), "checkout", "-q", "FETCH_HEAD"], check=True)
        out[name] = d
    return out


def parquet_testing_files(root):
    return sorted(str(p.relative_to(root)) for p in root.glob(PARQUET_TESTING_GLOB) if str(p.relative_to(root)) not in PARQUET_TESTING_SKIP)


def arrow_testing_smoke_files(root):
    return sorted(str(p.relative_to(root)) for p in root.glob(ARROW_TESTING_SMOKE_GLOB)
                  if p.is_file() and p.suffix not in (".md", ".json") and p.stat().st_size <= ARROW_TESTING_SMOKE_MAX_SIZE)


def existing_hashes(repo):
    hashes = {}
    for p in (repo / "tests").rglob("*"):
        if p.is_file() and "data_lake_corpus" not in p.parts and p.stat().st_size > 64 and "parquet" in p.name.lower():
            hashes[sha256(p)] = str(p.relative_to(repo))
    return hashes
