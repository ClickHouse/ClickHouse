"""ClickHouse as the writer: write every corpus dataset with every `output_format_parquet_*`
variant (the same list as `05478_parquet_corpus_clickhouse_writer.sh`), then read the files with
pyarrow, DuckDB, polars, fastparquet, and Spark (parquet-java), and compare the values with the
pyarrow readback of the source file. Also writes Iceberg tables with ClickHouse (`INSERT` into
`icebergLocal`) and reads them with PyIceberg.

The result is a report (`tmp/data-lake-corpus-work/roundtrip/report.json` and stdout); nothing
is checked in, because this direction needs the third-party readers, which the stateless tests
do not have.
"""

import collections
import json
import os
import shutil
import subprocess

import pyarrow.parquet as pq

import corpus_lib as cl

DATASETS = ["flat.v2.6.dpv1", "nested.v2.6.dpv1", "strings.dict.dpv1", "numbers.plain.dpv1", "decimals.flba", "logical", "all_nulls", "names"]
# The ClickHouse Parquet writer does not support `Time64`, which its reader produces for TIME columns.
EXCEPT = {"logical": ["t32_ms", "t64_us"]}

VARIANTS = {
    "default": "output_format_parquet_row_group_size = 1000000",
    "v1.0": "output_format_parquet_version = '1.0'",
    "v2.4": "output_format_parquet_version = '2.4'",
    "v2.latest": "output_format_parquet_version = '2.latest'",
    "none": "output_format_parquet_compression_method = 'none'",
    "snappy": "output_format_parquet_compression_method = 'snappy'",
    "gzip": "output_format_parquet_compression_method = 'gzip'",
    "brotli": "output_format_parquet_compression_method = 'brotli'",
    "lz4": "output_format_parquet_compression_method = 'lz4'",
    "small_groups_pages": "output_format_parquet_row_group_size = 7, output_format_parquet_data_page_size = 64, output_format_parquet_batch_size = 3",
    "no_index_bloom_checksums": "output_format_parquet_write_page_index = 0, output_format_parquet_write_bloom_filter = 0, output_format_parquet_write_checksums = 0",
    "no_dictionary": "output_format_parquet_max_dictionary_size = 0",
    "dictionary_fallback": "output_format_parquet_max_dictionary_size = 64, output_format_parquet_batch_size = 4",
    "binary_strings": "output_format_parquet_string_as_string = 0, output_format_parquet_fixed_string_as_fixed_byte_array = 0",
    "non_compliant_nested": "output_format_parquet_compliant_nested_types = 0",
    "alp": "output_format_parquet_use_alp_encoding = 1",
}

READERS = {
    "duckdb-2.0.0": ("3.12", ["duckdb==2.0.0", "pyarrow==26.0.0"], "duckdb"),
    "polars-2.1.0": ("3.12", ["polars==2.1.0", "pyarrow==26.0.0"], "polars"),
    "fastparquet-2026.9.0": ("3.12", ["fastparquet==2026.9.0", "pyarrow==26.0.0", "pandas==2.3.3"], "fastparquet"),
    "spark-3.5.5": ("3.11", ["pyspark==3.5.5", "pyarrow==17.0.0", "pandas==2.2.3", "numpy==1.26.4"], "spark"),
}


def canonical(rows):
    return collections.Counter(json.dumps(r, sort_keys=True, ensure_ascii=False) for r in rows)


def diff_columns(expected, actual):
    """Columns whose values differ (comparing sorted per-column values, since row order may differ)."""
    out = []
    for c in expected[0].keys() if expected else []:
        e = sorted(json.dumps(r.get(c), sort_keys=True) for r in expected)
        a = sorted(json.dumps(r.get(c), sort_keys=True) for r in actual)
        if e != a:
            sample = next(((x, y) for x, y in zip(e, a) if x != y), None)
            out.append({"column": c, "sample": sample})
    return out


def run(g, args):
    cl.BINARY_DECODE_ERRORS = "surrogateescape"
    work = g.WORK / "roundtrip"
    if work.exists():
        shutil.rmtree(work)
    (work / "files").mkdir(parents=True)
    src_dir = g.CORPUS / "parquet" / "pyarrow-26.0.0"
    references = {}
    files = []
    for ds in DATASETS:
        src = src_dir / f"{ds}.parquet"
        source = pq.read_table(src)
        references[ds] = cl.normalize_table(source.drop_columns(EXCEPT.get(ds, [])))
        select = f"SELECT * EXCEPT ({', '.join(EXCEPT[ds])})" if ds in EXCEPT else "SELECT *"
        sql = []
        for name, settings in VARIANTS.items():
            out = work / "files" / f"{ds}.{name}.parquet"
            sql.append(f"INSERT INTO FUNCTION file('{out}', Parquet) {select} FROM file('{src}', Parquet) SETTINGS {settings};")
            files.append((ds, name, out))
        subprocess.run([args.clickhouse, "local", "--session_timezone=UTC", "--query", "\n".join(sql)], check=True)

    report = {"clickhouse": subprocess.run([args.clickhouse, "local", "-q", "SELECT version()"], capture_output=True, text=True, check=True).stdout.strip(), "results": []}
    readbacks = {"pyarrow-26.0.0": {}}
    for ds, name, path in files:
        try:
            readbacks["pyarrow-26.0.0"][path.name] = cl.normalize_table(pq.read_table(path))
        except Exception as e:  # the outcome for this file is the error, recorded in the report
            readbacks["pyarrow-26.0.0"][path.name] = f"{type(e).__name__}: {e}"[:300]
    env = dict(os.environ)
    env.pop("JAVA_TOOL_OPTIONS", None)
    env.pop("SPARK_HOME", None)
    env["JAVA_HOME"] = g.JAVA17
    env["PATH"] = f"{g.JAVA17}/bin:" + env["PATH"]
    for reader, (python, packages, library) in READERS.items():
        out_dir = work / reader
        out_dir.mkdir()
        job = {"library": library, "files": [str(p) for _, _, p in files], "output_dir": str(out_dir), "result": str(out_dir / "result.json")}
        json.dump(job, open(out_dir / "job.json", "w"))
        cmd = ["uv", "run", "--isolated", "--no-project", "--managed-python", "--python", python]
        for p in packages:
            cmd += ["--with", p]
        subprocess.run(cmd + ["python", "-I", str(g.HERE / "writers" / "reader.py"), str(out_dir / "job.json")], check=True, env=env, timeout=3600)
        result = json.load(open(out_dir / "result.json"))
        readbacks[reader] = {}
        for _, _, p in files:
            r = result[p.name]
            readbacks[reader][p.name] = r["error"] if "error" in r else cl.normalize_table(cl.read_ipc(out_dir / (p.name + ".arrow")))
    for ds, name, path in files:
        for reader, values in readbacks.items():
            v = values[path.name]
            if isinstance(v, str):
                outcome = {"status": "error", "error": v}
            elif canonical(v) == canonical(references[ds]):
                outcome = {"status": "ok"}
            else:
                outcome = {"status": "mismatch", "columns": diff_columns(references[ds], v)}
            report["results"].append({"dataset": ds, "variant": name, "reader": reader, **outcome})
    json.dump(report, open(work / "report.json", "w"), indent=1, ensure_ascii=False)
    summary = collections.Counter((r["reader"], r["status"]) for r in report["results"])
    for (reader, status), n in sorted(summary.items()):
        print(f"{reader}\t{status}\t{n}")
    for r in report["results"]:
        if r["status"] != "ok":
            detail = r.get("error") or ", ".join(c["column"] for c in r["columns"])
            print(f"{r['reader']}\t{r['dataset']}\t{r['variant']}\t{r['status']}\t{detail[:200]}")
