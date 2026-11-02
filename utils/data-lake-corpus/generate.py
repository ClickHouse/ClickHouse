#!/usr/bin/env python3
"""Generator of the static Parquet / Iceberg corpus used by the stateless tests
`tests/queries/0_stateless/*_parquet_corpus_*.sh` and `*_iceberg_corpus*.sh`.

Every file in `tests/queries/0_stateless/data_lake_corpus/` is produced here by a
third-party writer pinned in `WRITERS` (or copied from a public corpus listed in
`external.py`), then read back with the same library. The readback is the reference:
it is normalized to JSON Lines (`expected/*.jsonl.zst`, deduplicated by content) that
the tests parse with `JSONEachRow` into the types ClickHouse inferred for the file and
compare with what ClickHouse read from the file itself.

Usage (from the repository root):

    uv venv --python 3.12 tmp/dlc-venv
    uv pip install --python tmp/dlc-venv/bin/python -r utils/data-lake-corpus/requirements.txt
    tmp/dlc-venv/bin/python utils/data-lake-corpus/generate.py parquet [--only pyarrow-26.0.0,...]
    tmp/dlc-venv/bin/python utils/data-lake-corpus/generate.py external
    tmp/dlc-venv/bin/python utils/data-lake-corpus/generate.py iceberg
    tmp/dlc-venv/bin/python utils/data-lake-corpus/generate.py checks
    tmp/dlc-venv/bin/python utils/data-lake-corpus/generate.py roundtrip --clickhouse <binary>

Writers run with `uv run --with <pinned package>` (the Rust writers arrow-rs and polars-parquet
come with the `deltalake` and `polars` packages), Spark with JDK 17 (`JAVA17_HOME`) and the
pinned Maven coordinates of the Iceberg runtime, the Go writers from the modules pinned in
`writers/go/go.mod`. Generated files are not byte-reproducible (writers embed timestamps,
UUIDs, and `created_by`), but their content and options are. After regenerating, run
`checks` and record the references of the tests with `tests/clickhouse-test --record`.
"""

import argparse
import decimal
import fnmatch
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pyarrow as pa

sys.path.insert(0, str(Path(__file__).parent))
import corpus_lib as cl  # noqa: E402
import iceberg_gen  # noqa: E402
from writer_jobs import (  # noqa: E402
    arrow_go_jobs, deltalake_jobs, duckdb_jobs, enc_for, fastparquet_jobs, parquet_go_jobs, polars_jobs, spark_jobs,
)

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
CORPUS = REPO / "tests" / "queries" / "0_stateless" / "data_lake_corpus"
WORK = REPO / "tmp" / "data-lake-corpus-work"
MANIFEST = CORPUS / "manifest.jsonl"

JAVA17 = os.environ.get("JAVA17_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")

# ---------------------------------------------------------------------------
# Writer profiles
# ---------------------------------------------------------------------------

# Columns that a writer cannot represent are dropped from its input. Types are matched by
# `drop_types` (predicate names over Arrow types) and columns by name.


def _is_ext(t):
    return isinstance(t, pa.BaseExtensionType)


TYPE_PREDICATES = {
    "unsigned": lambda t: pa.types.is_unsigned_integer(t),
    "float16": lambda t: pa.types.is_float16(t),
    "decimal256": lambda t: pa.types.is_decimal256(t),
    "extension": _is_ext,
    "time": lambda t: pa.types.is_time(t),
    "large": lambda t: pa.types.is_large_string(t) or pa.types.is_large_binary(t) or pa.types.is_large_list(t),
    "dictionary": lambda t: pa.types.is_dictionary(t),
    "fixed_size_list": lambda t: pa.types.is_fixed_size_list(t),
    "date64": lambda t: pa.types.is_date64(t),
    "fixed_binary": lambda t: pa.types.is_fixed_size_binary(t),
    "map": lambda t: pa.types.is_map(t),
    "ts_ns": lambda t: pa.types.is_timestamp(t) and t.unit == "ns",
    "ts_tz": lambda t: pa.types.is_timestamp(t) and t.tz is not None,
    "struct": lambda t: pa.types.is_struct(t),
    "list": lambda t: pa.types.is_list(t),
    "decimal": lambda t: pa.types.is_decimal(t),
}


def _contains(t, pred):
    if pred(t):
        return True
    if pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_fixed_size_list(t):
        return _contains(t.value_type, pred)
    if pa.types.is_map(t):
        return _contains(t.key_type, pred) or _contains(t.item_type, pred)
    if pa.types.is_struct(t):
        return any(_contains(t.field(i).type, pred) for i in range(t.num_fields))
    return False


def project(table, drop_types=(), drop_columns=()):
    keep = []
    for f in table.schema:
        if f.name in drop_columns:
            continue
        if any(_contains(f.type, TYPE_PREDICATES[p]) for p in drop_types):
            continue
        keep.append(f.name)
    return table.select(keep)


def pyarrow_jobs_full(ds):
    """Full option matrix for the newest pyarrow."""
    jobs = []

    def add(name, dataset, **options):
        jobs.append({"name": name, "dataset": dataset, "options": options})

    for version in ["1.0", "2.4", "2.6"]:
        for dpv in ["1.0", "2.0"]:
            for d in ["flat", "nested"]:
                extra = {} if version == "2.6" else {"coerce_timestamps": "us", "allow_truncated_timestamps": True}
                add(f"{d}.v{version}.dpv{dpv[0]}", d, version=version, data_page_version=dpv, **extra)
    for codec in ["none", "snappy", "gzip", "brotli", "lz4", "zstd"]:
        add(f"flat.{codec}", "flat", compression=codec)
    add("flat.zstd22.dpv2", "flat", compression="zstd", compression_level=22, data_page_version="2.0")
    add("flat.gzip9.dpv2", "flat", compression="gzip", compression_level=9, data_page_version="2.0")
    add("flat.no_dict", "flat", use_dictionary=False)
    add("flat.no_stats", "flat", write_statistics=False)
    add("flat.page_index_checksum", "flat", write_page_index=True, write_page_checksum=True)
    add("flat.tiny_pages", "flat", data_page_size=128, write_batch_size=16, write_page_index=True)
    add("flat.rows_per_page_7.dpv2", "flat", max_rows_per_page=7, write_page_index=True, data_page_version="2.0")
    add("flat.row_group_17", "flat", row_group_size=17, write_page_index=True)
    add("flat.dict_fallback", "flat", dictionary_pagesize_limit=64, write_batch_size=8)
    add("flat.dict_fallback.dpv2", "flat", dictionary_pagesize_limit=64, write_batch_size=8, data_page_version="2.0")
    add("flat.int96", "flat", use_deprecated_int96_timestamps=True)
    add("flat.coerce_ms", "flat", coerce_timestamps="ms", allow_truncated_timestamps=True)
    add("flat.no_arrow_schema", "flat", store_schema=False)
    add("flat.decimal_as_int", "flat", store_decimal_as_integer=True)
    add("flat.adjusted_utc", "flat", write_time_adjusted_to_utc=True)
    add("flat.sorted", "flat", sorting_columns=[["id", False, False]])
    add("flat.rle_bool", "flat", use_dictionary=False, column_encoding={"b": "RLE"})
    add("flat.empty_row_groups", "flat", _special="empty_row_groups")
    add("flat.encrypted", "flat", _special="encrypt")
    add("nested.no_dict", "nested", use_dictionary=False)
    add("nested.tiny_pages.dpv2", "nested", data_page_size=64, write_batch_size=4, data_page_version="2.0", write_page_index=True)
    add("nested.rows_per_page_3", "nested", max_rows_per_page=3, write_page_index=True)
    add("nested.legacy_list_names", "nested", use_compliant_nested_type=False)
    add("nested.row_group_13", "nested", row_group_size=13)
    add("nested.no_arrow_schema", "nested", store_schema=False)
    for dpv in ["1.0", "2.0"]:
        s = f".dpv{dpv[0]}"
        add("strings.plain" + s, "strings", use_dictionary=False, data_page_version=dpv)
        add("strings.dict" + s, "strings", data_page_version=dpv)
        add("strings.dict_fallback" + s, "strings", dictionary_pagesize_limit=256, write_batch_size=16, data_page_version=dpv)
        add("strings.delta_length" + s, "strings", data_page_version=dpv, column_encoding=enc_for(ds["strings"], {"bytes": "DELTA_LENGTH_BYTE_ARRAY"}))
        add("strings.delta_byte_array" + s, "strings", data_page_version=dpv, column_encoding=enc_for(ds["strings"], {"bytes": "DELTA_BYTE_ARRAY", "fixed": "DELTA_BYTE_ARRAY"}))
        add("strings.bss_fixed" + s, "strings", data_page_version=dpv, column_encoding=enc_for(ds["strings"], {"fixed": "BYTE_STREAM_SPLIT"}))
        add("numbers.plain" + s, "numbers", use_dictionary=False, data_page_version=dpv)
        add("numbers.dict" + s, "numbers", data_page_version=dpv)
        add("numbers.delta" + s, "numbers", data_page_version=dpv, column_encoding=enc_for(ds["numbers"], {"int": "DELTA_BINARY_PACKED"}))
        add("numbers.bss" + s, "numbers", data_page_version=dpv, column_encoding=enc_for(ds["numbers"], {"int": "BYTE_STREAM_SPLIT", "float": "BYTE_STREAM_SPLIT", "decimal": "BYTE_STREAM_SPLIT"}))
    add("numbers.delta.tiny_pages", "numbers", data_page_size=100, write_batch_size=7, column_encoding=enc_for(ds["numbers"], {"int": "DELTA_BINARY_PACKED"}), write_page_index=True)
    add("numbers.delta.zstd_bss", "numbers", compression="zstd", column_encoding=enc_for(ds["numbers"], {"int": "DELTA_BINARY_PACKED", "float": "BYTE_STREAM_SPLIT"}))
    add("decimals.flba", "decimals")
    add("decimals.as_int", "decimals", store_decimal_as_integer=True)
    add("decimals.as_int.delta", "decimals", store_decimal_as_integer=True, use_dictionary=False,
        column_encoding={n: "DELTA_BINARY_PACKED" for n in ds["decimals"].column_names if n != "id" and int(n.split("_")[1]) <= 18})
    add("decimals.bss", "decimals", column_encoding=enc_for(ds["decimals"], {"decimal": "BYTE_STREAM_SPLIT"}))
    add("wide", "wide")
    add("wide.dpv2.zstd", "wide", data_page_version="2.0", compression="zstd")
    add("all_nulls", "all_nulls")
    add("all_nulls.dpv2.no_dict", "all_nulls", data_page_version="2.0", use_dictionary=False)
    add("empty", "empty")
    add("empty.no_row_groups", "empty", _special="no_row_groups")
    add("names", "names")
    add("names.row_group_1", "names", row_group_size=1)
    add("logical", "logical")
    add("logical.dpv2.no_dict", "logical", data_page_version="2.0", use_dictionary=False)
    return jobs


def pyarrow_jobs_old(ds, version):
    """Smaller matrix for older pyarrow versions; options limited to what they support."""
    jobs = []
    major = int(version.split(".")[0])

    def add(name, dataset, **options):
        # Before pyarrow 13 the default format version was 2.4 at most, which has no nanosecond timestamps.
        if "use_deprecated_int96_timestamps" not in options and options.get("version") != "2.6":
            options.update(coerce_timestamps="us", allow_truncated_timestamps=True)
        jobs.append({"name": name, "dataset": dataset, "options": options})

    add("flat", "flat")
    add("flat.v1.0", "flat", version="1.0")
    add("flat.dpv2", "flat", version="2.0" if major < 6 else "2.6", data_page_version="2.0")
    add("flat.no_dict.gzip", "flat", use_dictionary=False, compression="gzip")
    add("flat.int96", "flat", use_deprecated_int96_timestamps=True)
    add("nested", "nested")
    add("nested.dpv2", "nested", data_page_version="2.0")
    if major >= 6:
        add("nested.legacy_list_names", "nested", use_compliant_nested_type=False)
    add("strings", "strings")
    add("strings.plain.dpv2", "strings", use_dictionary=False, data_page_version="2.0")
    add("numbers.bss", "numbers", use_byte_stream_split=["f32", "f64"], use_dictionary=False)
    if major >= 12:
        add("numbers.delta", "numbers", column_encoding=enc_for(ds["numbers"], {"int": "DELTA_BINARY_PACKED"}))
    add("decimals", "decimals")
    add("all_nulls", "all_nulls")
    add("empty", "empty")
    return jobs


# Each profile: how to run it, what it cannot write, and its job list.
WRITERS = {
    "pyarrow-26.0.0": {
        "kind": "pyarrow", "group": "pyarrow", "python": "3.12", "with": ["pyarrow==26.0.0", "numpy==2.3.5"],
        "drop_types": [], "jobs": pyarrow_jobs_full,
    },
    "pyarrow-17.0.0": {
        "kind": "pyarrow", "group": "pyarrow", "python": "3.12", "with": ["pyarrow==17.0.0", "numpy==1.26.4"],
        "drop_types": ["extension"], "jobs": lambda ds: pyarrow_jobs_old(ds, "17.0.0"),
    },
    "pyarrow-12.0.1": {
        "kind": "pyarrow", "group": "pyarrow", "python": "3.11", "with": ["pyarrow==12.0.1", "numpy==1.26.4"],
        "drop_types": ["extension"], "jobs": lambda ds: pyarrow_jobs_old(ds, "12.0.1"),
    },
    "pyarrow-8.0.0": {
        "kind": "pyarrow", "group": "pyarrow", "python": "3.10", "with": ["pyarrow==8.0.0", "numpy==1.23.5"],
        "drop_types": ["extension"], "jobs": lambda ds: pyarrow_jobs_old(ds, "8.0.0"),
    },
    "pyarrow-2.0.0": {
        "kind": "pyarrow", "group": "pyarrow", "python": "3.8", "with": ["pyarrow==2.0.0", "numpy==1.19.5"],
        "drop_types": ["extension"], "jobs": lambda ds: pyarrow_jobs_old(ds, "2.0.0"),
    },
    "arrow-go-v18.4.1": {"kind": "go", "library": "arrow-go", "drop_types": ["large", "fixed_size_list"], "jobs": lambda ds: arrow_go_jobs(ds)},
    "parquet-go-v0.25.1": {"kind": "go", "library": "parquet-go", "drop_types": [], "jobs": lambda ds: parquet_go_jobs(ds)},
    "polars-2.1.0": {
        "kind": "polars", "python": "3.12", "with": ["polars==2.1.0", "pyarrow==26.0.0"],
        "drop_types": ["decimal256", "extension"], "jobs": lambda ds: polars_jobs(ds),
    },
    "polars-0.20.31": {
        "kind": "polars", "python": "3.11", "with": ["polars==0.20.31", "pyarrow==17.0.0", "numpy==1.26.4"],
        "drop_types": ["extension", "map"], "jobs": lambda ds: polars_jobs(ds, old=True),
    },
    "duckdb-2.0.0": {
        "kind": "duckdb", "python": "3.12", "with": ["duckdb==2.0.0", "pyarrow==26.0.0"],
        "drop_types": ["float16"], "jobs": lambda ds: duckdb_jobs(ds, "2.0.0"),
    },
    "duckdb-1.1.3": {
        "kind": "duckdb", "python": "3.12", "with": ["duckdb==1.1.3", "pyarrow==17.0.0", "numpy==1.26.4"],
        "drop_types": ["float16", "extension"], "jobs": lambda ds: duckdb_jobs(ds, "1.1.3"),
    },
    "duckdb-0.9.2": {
        "kind": "duckdb", "python": "3.11", "with": ["duckdb==0.9.2", "pyarrow==14.0.2", "numpy==1.26.4"],
        "drop_types": ["float16", "extension"], "jobs": lambda ds: duckdb_jobs(ds, "0.9.2"),
    },
    "fastparquet-2026.9.0": {
        "kind": "fastparquet", "python": "3.12", "with": ["fastparquet==2026.9.0", "pyarrow==26.0.0", "pandas==2.3.3"],
        # pandas has no date dtype, and fastparquet cannot write `datetime.date` objects.
        "drop_types": ["fixed_binary", "decimal", "list", "struct", "map"], "drop_columns": ["d"], "jobs": lambda ds: fastparquet_jobs(ds),
    },
    "spark-3.5.5": {
        "kind": "spark", "python": "3.11", "with": ["pyspark==3.5.5", "pyarrow==17.0.0", "pandas==2.2.3", "numpy==1.26.4"],
        "input_format": "parquet", "drop_types": ["ts_ns"], "jobs": lambda ds: spark_jobs(ds),
    },
    "deltalake-1.6.6": {
        "kind": "deltalake", "python": "3.12", "with": ["deltalake==1.6.6", "pyarrow==26.0.0"],
        # Delta Lake has no unsigned, fixed-size binary, or nanosecond timestamp types.
        "drop_types": ["unsigned", "fixed_binary", "ts_ns"], "jobs": lambda ds: deltalake_jobs(ds),
    },
}


# ---------------------------------------------------------------------------
# Running writers and collecting results
# ---------------------------------------------------------------------------


def build_datasets():
    return {name: fn() for name, fn in cl.DATASETS.items()}


def run_writer(profile_name, profile, ds):
    work = WORK / "parquet" / profile_name
    if work.exists():
        shutil.rmtree(work)
    (work / "in").mkdir(parents=True)
    (work / "out").mkdir()
    jobs = profile["jobs"](ds)
    for j in jobs:
        if j["dataset"] is None:
            continue
        table = project(ds[j["dataset"]], profile.get("drop_types", ()), profile.get("drop_columns", ()))
        if profile.get("input_format") == "parquet":
            j["input"] = j["dataset"] + ".parquet"
            path = work / "in" / j["input"]
            if not path.exists():
                import pyarrow.parquet as pq
                pq.write_table(table, path, coerce_timestamps="us", allow_truncated_timestamps=True)
        else:
            j["input"] = j["dataset"] + ".arrow"
            path = work / "in" / j["input"]
            if not path.exists():
                cl.write_ipc(table, path)
    job_file = work / "job.json"
    result_file = work / "result.json"
    json.dump({"input_dir": str(work / "in"), "output_dir": str(work / "out"), "result": str(result_file), "jobs": jobs},
              open(job_file, "w"), indent=1)
    cmd = profile_command(profile, job_file)
    print("+", " ".join(cmd), flush=True)
    env = dict(os.environ)
    env.pop("JAVA_TOOL_OPTIONS", None)
    if profile["kind"] == "spark":
        env.pop("SPARK_HOME", None)
        env["JAVA_HOME"] = JAVA17
        env["PATH"] = f"{JAVA17}/bin:" + env["PATH"]
    subprocess.run(cmd, check=True, env=env, cwd=str(work), timeout=1800)
    result = json.load(open(result_file))
    return jobs, result, work / "out"


def profile_command(profile, job_file):
    kind = profile["kind"]
    if kind in ("pyarrow", "polars", "duckdb", "fastparquet", "deltalake", "spark"):
        cmd = ["uv", "run", "--isolated", "--no-project", "--managed-python", "--python", profile["python"]]
        for w in profile["with"]:
            cmd += ["--with", w]
        return cmd + ["python", "-I", str(HERE / "writers" / f"{kind}_writer.py"), str(job_file)]
    if kind == "go":
        return [str(build_go()), str(job_file)]
    if kind == "rust":
        return [str(build_rust()), str(job_file)]
    raise ValueError(kind)


def build_go():
    out = WORK / "bin" / "go_writer"
    out.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(["go", "build", "-o", str(out), "."], check=True, cwd=str(HERE / "writers" / "go"))
    return out


def build_rust():
    target = WORK / "rust-target"
    subprocess.run(["cargo", "build", "--release", "--locked", "--target-dir", str(target)], check=True, cwd=str(HERE / "writers" / "rust"))
    return target / "release" / "rust_writer"


PREDICATES = {
    "flat": [("id", ">=", 84), ("id", "<", 13), ("id", "=", 17), ("id", "IN", [3, 100, 9999]), ("s", ">=", "m"),
             ("s", "=", "alpha"), ("s", "=", "zz-not-present"), ("s", "IS NULL", None), ("s_dict", "=", "beta"),
             ("i64", "<", 0), ("u64", ">", 9223372036854775807), ("d", ">=", "2000-01-01"), ("f64", ">", 0.0)],
    "nested": [("id", ">=", 70), ("id", "=", 7)],
    "strings": [("id", "<", 80), ("s_prefix", ">=", "https://github"), ("s_prefix", "=", "zz-not-present"),
                ("s_low_card", "=", "alpha"), ("s_nulls", "IS NULL", None), ("s", "=", "omega")],
    "numbers": [("id", ">=", 400), ("i64_mono", "<", -1099511000000), ("i32_small", "=", 3), ("f64", ">", 0.0),
                ("u64_rand", ">", 9223372036854775807), ("d", "<", "1990-01-01"), ("ts_us", ">=", "2020-09-13 15:00:00.000000")],
    "decimals": [("id", "<", 30)],
    "all_nulls": [("s", "IS NULL", None), ("i32", "=", 1)],
    "names": [("id", "<", 5)],
}


def _eval_pred(rows, col, op, val):
    n = 0
    for r in rows:
        v = r.get(col)
        if op == "IS NULL":
            n += v is None
            continue
        if isinstance(v, str) and isinstance(val, (int, float)):
            # Floats that JSON cannot hold and decimals are strings in the normalized rows.
            v = float(v) if v in ("inf", "-inf", "nan") else decimal.Decimal(v)
        if v is None or isinstance(v, str) and isinstance(val, (int, float)):
            continue
        if op == ">=":
            n += v >= val
        elif op == "<":
            n += v < val
        elif op == ">":
            n += v > val
        elif op == "=":
            n += v == val
        elif op == "IN":
            n += v in val
    return n


def _sql_value(v):
    if isinstance(v, str):
        return "'" + v.replace("\\", "\\\\").replace("'", "\\'") + "'"
    if isinstance(v, list):
        return "(" + ", ".join(_sql_value(x) for x in v) + ")"
    return repr(v)


def predicates_for(dataset, rows, columns):
    out = []
    for col, op, val in PREDICATES.get(dataset, []):
        if col not in columns:
            continue
        sql = f"{ident(col)} {op}" if op == "IS NULL" else f"{ident(col)} {op} {_sql_value(val)}"
        out.append({"where": sql, "count": _eval_pred(rows, col, op, val)})
    return out


def load_manifest():
    if not MANIFEST.exists():
        return []
    return [json.loads(line) for line in open(MANIFEST) if line.strip()]


def save_manifest(entries):
    entries = sorted(entries, key=lambda e: e["file"])
    with open(MANIFEST, "w") as f:
        for e in entries:
            f.write(json.dumps(e, ensure_ascii=False, sort_keys=True) + "\n")


def store_expected(table):
    rows = cl.normalize_table(table)
    compressed, raw = cl.rows_to_jsonl_zst(rows)
    name = f"expected/{cl.jsonl_digest(raw)}.jsonl.zst"
    path = CORPUS / name
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_bytes(compressed)
    return name, rows


def collect(profile_name, profile, jobs, result, out_dir, ds):
    entries = []
    by_name = {r["name"]: r for r in result["results"]}
    dest_dir = CORPUS / "parquet" / profile_name
    if dest_dir.exists():
        shutil.rmtree(dest_dir)
    dest_dir.mkdir(parents=True)
    for j in jobs:
        res = by_name[j["name"]]
        src = out_dir / (j["name"] + ".parquet")
        rel = f"parquet/{profile_name}/{j['name']}.parquet"
        shutil.copyfile(src, CORPUS / rel)
        version = result["version"]
        library = result["library"]
        if isinstance(version, dict):
            library = profile["library"]
            version = version[library]
        import pyarrow.parquet as pq
        try:
            created_by = pq.ParquetFile(src).metadata.created_by
        except OSError:
            created_by = res.get("created_by")
        entry = {
            "file": rel,
            "kind": "parquet",
            "group": profile.get("group", "writers"),
            "writer": library,
            "writer_version": version,
            "created_by": created_by,
            "dataset": j["dataset"],
            "options": {k: v for k, v in j.get("options", {}).items()},
            **({"spark_conf": j["conf"]} if j.get("conf") else {}),
            **({"sql": " ".join(j["sql"].split())} if j.get("sql") else {}),
            **({"parquet_java": result["parquet_java"]} if result.get("parquet_java") else {}),
            "size": src.stat().st_size,
        }
        special = j.get("options", {}).get("_special")
        if j.get("smoke"):
            entry["status"] = "smoke"
        elif j.get("out_of_spec"):
            entry["status"] = "error"
            entry["notes"] = j["out_of_spec"]
        elif j.get("corrupt"):
            entry["status"] = "error"
            entry["notes"] = "the writer produces a corrupt file that no reader accepts (including the writer itself); ClickHouse must fail cleanly"
        elif special == "encrypt":
            entry["status"] = "error"
            entry["notes"] = "Parquet modular encryption (AES-GCM, encrypted footer); ClickHouse does not support it and must fail cleanly"
        else:
            if "readback_error" in res:
                # The writer cannot read its own file back; record that and use the newest pyarrow as the reference.
                back = pq.read_table(src)
                entry["readback"] = f"pyarrow {pa.__version__}"
                entry["notes"] = f"{library} {version} fails to read its own file: {res['readback_error']}"
            else:
                back = cl.read_ipc(out_dir / (j["name"] + ".arrow"))
            entry["expected"], rows = store_expected(back)
            entry["rows"] = back.num_rows
            entry["columns"] = back.column_names
            entry["subcolumns"] = subcolumns_for(back.schema)
            entry["predicates"] = predicates_for(j["dataset"] or "", rows, back.column_names)
            entry["status"] = "ok"
        entries.append(entry)
    return entries


def subcolumns_for(schema):
    """Subcolumn expressions exercising partial reads of nested columns."""
    out = []
    for f in schema:
        t = f.type
        if pa.types.is_dictionary(t):
            continue
        q = ident(f.name)
        if pa.types.is_struct(t):
            out += [f"{q}.{ident(t.field(i).name)}" for i in range(t.num_fields)]
        elif pa.types.is_map(t):
            out += [f"{q}.keys", f"{q}.values"]
        elif pa.types.is_list(t) or pa.types.is_large_list(t):
            out.append(f"{q}.size0")
            if pa.types.is_struct(t.value_type):
                vt = t.value_type
                out += [f"{q}.{ident(vt.field(i).name)}" for i in range(vt.num_fields)]
    return out


def cmd_parquet(args):
    ds = build_datasets()
    manifest = load_manifest()
    only = set(args.only.split(",")) if args.only else None
    for name, profile in WRITERS.items():
        if only and name not in only:
            continue
        jobs, result, out_dir = run_writer(name, profile, ds)
        entries = collect(name, profile, jobs, result, out_dir, ds)
        manifest = [e for e in manifest if not e["file"].startswith(f"parquet/{name}/")] + entries
        save_manifest(manifest)
        print(f"{name}: {len(entries)} files", flush=True)
    gc_expected(manifest)


def gc_expected(manifest):
    used = {e["expected"] for e in manifest if e.get("expected")} | {s["expected"] for e in manifest for s in e.get("snapshots", [])}
    for p in (CORPUS / "expected").glob("*.jsonl.zst"):
        if f"expected/{p.name}" not in used:
            p.unlink()


# ---------------------------------------------------------------------------
# Test SQL generation
# ---------------------------------------------------------------------------

STRESS_SETTINGS = (
    "max_block_size = 7, input_format_parquet_max_block_size = 7, input_format_parquet_prefer_block_bytes = 1, "
    "input_format_parquet_use_offset_index = 0, input_format_parquet_filter_push_down = 0, "
    "input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0, "
    "input_format_parquet_enable_row_group_prefetch = 0, input_format_parquet_local_file_min_bytes_for_seek = 0, "
    "input_format_parquet_preserve_order = 1, max_threads = 1"
)


def ident(name):
    return "`" + name.replace("\\", "\\\\").replace("`", "\\`") + "`"


def _q(s):
    return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"


# ClickHouse bugs exposed by corpus files. The checks of such a file run with the
# workaround `settings`, and one extra check runs without them, so the reference records
# the current (wrong) behavior until the bug is fixed.
KNOWN_ISSUES = {
    # Arrow C++ writes an empty data page v2 (0 values, 0 rows) in nested columns when `data_page_size` is
    # tiny. With the offset index ClickHouse rejects the duplicate `first_row_index` of the empty page
    # (`INCORRECT_DATA`), without it decoding fails with `ATTEMPT_TO_READ_AFTER_EOF`; pyarrow reads it.
    "parquet/pyarrow-26.0.0/nested.tiny_pages.dpv2.parquet": {"issue": "empty data page v2 in a nested column", "expect_error": True},
    # DuckDB (`PARQUET_VERSION V2`) and arrow-go compute the DELTA_BINARY_PACKED deltas of INT32 columns in
    # 64 bits, so a column with a wide range of values has miniblocks of bit width 33. Both read their files
    # back; ClickHouse rejects them (`INCORRECT_DATA`), like Arrow C++. Before the check was added, ClickHouse
    # returned wrong values.
    "parquet/duckdb-2.0.0/flat.v2.parquet": {"issue": "33-bit DELTA_BINARY_PACKED miniblocks in an INT32 column", "expect_error": True},
    "parquet/duckdb-2.0.0/numbers.v2.parquet": {"issue": "33-bit DELTA_BINARY_PACKED miniblocks in an INT32 column", "expect_error": True},
    "parquet/arrow-go-v18.4.1/numbers.delta.dpv2.parquet": {"issue": "33-bit DELTA_BINARY_PACKED miniblocks in an INT32 column", "expect_error": True},
}


# Reading only the `keys` or only the `values` subcolumn of a `Map` column directly from a
# Parquet file returns empty arrays (ClickHouse 26.9 and 26.11 master, also for files written
# by ClickHouse itself), so these subcolumn checks are not generated until it is fixed.
SKIPPED_SUBCOLUMN_SUFFIXES = (".keys", ".values")


def checked_subcolumns(e):
    return [x for x in e.get("subcolumns", []) if not x.endswith(SKIPPED_SUBCOLUMN_SUFFIXES)]


def source_of(e):
    """Placeholder-based path of the file of a manifest entry, and its label in the test output."""
    if e.get("root") == "parquet-testing":
        return "{PARQUET_TESTING}/" + e["file"].removeprefix("parquet-testing/"), e["file"]
    return "{CORPUS}/" + e["file"], e["file"]


def _known_issue(e):
    for pattern, issue in KNOWN_ISSUES.items():
        if fnmatch.fnmatch(e["file"], pattern):
            return issue
    return None


def checks_sql(entries):
    """SQL script with one result row per check, run by the stateless test over the corpus.

    Every row is `<file> <check>` and the outcome: `ok`, `MISMATCH`, or a value. The test
    turns an exception into `<file> <check>` and the error code name.
    """
    lines = ["-- Generated by utils/data-lake-corpus/generate.py checks. Do not edit.", ""]
    for e in entries:
        f, name = source_of(e)
        src = f"file({_q(f)}, Parquet)"

        def label(check):
            # No quotes or backslashes: the test extracts the label of a failed check from the query text.
            return _q(f"{name} {check}".replace("\\", "/").replace("'", '"'))

        lines.append(f"SELECT {_q('@@ ' + name)};")
        issue = _known_issue(e)
        if e["status"] != "ok" or (issue and issue.get("expect_error")):
            # Read every column, so that a corrupt page is decoded. Must fail cleanly or return the data.
            check = "read" if e["status"] != "ok" else "known issue " + issue["issue"]
            lines.append(f"SELECT {label(check)}, count(), sum(sipHash64(toString(tuple(*)))) FROM {src};")
            lines.append("")
            continue
        ws = f" SETTINGS {issue['settings']}" if issue and issue.get("settings") else ""
        exp = "{CORPUS}/" + e["expected"]
        lines.append("DROP TABLE IF EXISTS t; DROP TABLE IF EXISTS e;")
        lines.append(f"CREATE TABLE t ENGINE = Memory AS SELECT * FROM {src}{ws};")
        lines.append("CREATE TABLE e AS t ENGINE = Memory;")
        lines.append(f"INSERT INTO e SELECT * FROM file({_q(exp)}, JSONEachRow);")
        wide = len(e["columns"]) > 40
        types = "concat(toString(count()), ' columns, types hash ', hex(sipHash64(groupArray(type))))" if wide else "arrayStringConcat(groupArray(type), ', ')"
        lines.append(f"SELECT {label('types')}, {types} FROM system.columns WHERE database = currentDatabase() AND table = 't';")
        lines.append(
            f"SELECT {label('content')}, (SELECT count() FROM t), "
            f"if((SELECT count(), sum(cityHash64(tuple(*))) FROM t) = (SELECT count(), sum(cityHash64(tuple(*))) FROM e), 'ok', 'MISMATCH');")
        stress = STRESS_SETTINGS + (", " + issue["settings"] if issue and issue.get("settings") else "")
        lines.append(
            f"SELECT {label('stress')}, if((SELECT count(), sum(cityHash64(tuple(*))) FROM {src} SETTINGS {stress}) = "
            f"(SELECT count(), sum(cityHash64(tuple(*))) FROM e), 'ok', 'MISMATCH');")
        # Reading each column alone exercises column pruning; nested datasets need it most, and for the
        # flat ones it is enough to do it for the file written with the default options of each writer.
        if not wide and (e.get("dataset") in (None, "nested", "logical", "names", "duckdb_types") or not e.get("options")):
            exprs = [ident(c) for c in e["columns"]] + checked_subcolumns(e)
            for x in exprs:
                lines.append(
                    f"SELECT {label(x)}, if((SELECT sum(cityHash64(tuple({x}))) FROM {src}{ws}) = (SELECT sum(cityHash64(tuple({x}))) FROM e), 'ok', 'MISMATCH');")
        for pr in e.get("predicates", []):
            lines.append(
                f"SELECT {label('WHERE ' + pr['where'])}, if((SELECT count() FROM {src} WHERE {pr['where']}{ws}) = {pr['count']}, 'ok', 'MISMATCH');")
        if issue:
            lines.append(f"SELECT {label('known issue ' + issue['issue'])}, {issue['query'].replace('{SRC}', src)};")
        lines.append("")
    lines.append("DROP TABLE IF EXISTS t; DROP TABLE IF EXISTS e;")
    return "\n".join(lines) + "\n"


def cmd_checks(args):
    manifest = load_manifest()
    groups = sorted({e["group"] for e in manifest if e["kind"] == "parquet"})
    for g in groups:
        entries = [e for e in manifest if e["kind"] == "parquet" and e["group"] == g]
        (CORPUS / f"checks_{g}.sql").write_text(checks_sql(entries))
        print(f"{g}: {len(entries)} files")
    entries = [e for e in manifest if e["kind"] == "iceberg"]
    (CORPUS / "checks_iceberg.sql").write_text(iceberg_gen.checks_sql(sys.modules[__name__], entries))
    print(f"iceberg: {len(entries)} tables")


def cmd_iceberg(args):
    iceberg_gen.run(sys.modules[__name__], args)


def cmd_external(args):
    import pyarrow.parquet as pq

    import external as ex

    roots = ex.checkout(WORK)
    have = ex.existing_hashes(REPO)
    manifest = [e for e in load_manifest() if e["group"] not in ("external", "smoke") or e["writer"] != "external"]
    dest_root = CORPUS / "parquet" / "external"
    if dest_root.exists():
        shutil.rmtree(dest_root)
    skipped = []

    def reference(entry, path):
        back = pq.read_table(path)
        entry["created_by"] = pq.ParquetFile(path).metadata.created_by
        entry["readback"] = f"pyarrow {pa.__version__}"
        rows = cl.normalize_table(back)
        compressed, raw = cl.rows_to_jsonl_zst(rows)
        if len(compressed) > ex.MAX_EXPECTED_BYTES:
            return f"reference of {len(compressed)} bytes is too large"
        entry["expected"], _ = store_expected(back)
        entry["rows"] = back.num_rows
        entry["columns"] = back.column_names
        entry["subcolumns"] = subcolumns_for(back.schema)
        return None

    pt_url, pt_commit = ex.SOURCES["parquet-testing"]
    for path in ex.parquet_testing_files(roots["parquet-testing"]):
        if path in ex.PARQUET_TESTING_NO_REFERENCE:
            skipped.append(f"parquet-testing/{path}: {ex.PARQUET_TESTING_NO_REFERENCE[path]}")
            continue
        src = roots["parquet-testing"] / path
        entry = {"file": f"parquet-testing/{path}", "root": "parquet-testing", "kind": "parquet", "group": "external",
                 "writer": "external", "source": f"{pt_url}/blob/{pt_commit}/{path}", "license": "Apache-2.0", "status": "ok"}
        try:
            why = reference(entry, src)
        except Exception as e:  # pyarrow cannot read it: no independent reference, `05359_parquet_testing_corpus` covers it
            why = f"pyarrow cannot read it: {e}"
        if why:
            skipped.append(f"parquet-testing/{path}: {why}")
            continue
        manifest.append(entry)

    at_url, at_commit = ex.SOURCES["arrow-testing"]
    items = [(p, "ok") for p in ex.ARROW_TESTING_VALID] + [(p, "smoke") for p in ex.arrow_testing_smoke_files(roots["arrow-testing"])]
    for path, status in items:
        src = roots["arrow-testing"] / path
        digest = ex.sha256(src)
        if digest in have:
            skipped.append(f"arrow-testing/{path}: already in the repository as {have[digest]}")
            continue
        rel = f"parquet/external/arrow-testing/{path.removeprefix('data/parquet/')}"
        (CORPUS / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, CORPUS / rel)
        entry = {"file": rel, "kind": "parquet", "group": "external" if status == "ok" else "smoke", "writer": "external",
                 "source": f"{at_url}/blob/{at_commit}/{path}", "license": "Apache-2.0", "sha256": digest,
                 "size": src.stat().st_size, "status": status}
        if status == "ok":
            why = reference(entry, src)
            assert why is None, why
        manifest.append(entry)
    save_manifest(manifest)
    gc_expected(manifest)
    print("skipped:")
    for x in skipped:
        print("  " + x)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("parquet")
    p.add_argument("--only", default="")
    sub.add_parser("checks")
    sub.add_parser("external")
    p = sub.add_parser("iceberg")
    p.add_argument("--only", default="")
    p = sub.add_parser("roundtrip")
    p.add_argument("--clickhouse", required=True, help="path to the clickhouse binary")
    args = ap.parse_args()
    if args.cmd == "roundtrip":
        import roundtrip
        roundtrip.run(sys.modules[__name__], args)
        return
    {"parquet": cmd_parquet, "checks": cmd_checks, "external": cmd_external, "iceberg": cmd_iceberg}[args.cmd](args)


if __name__ == "__main__":
    main()
