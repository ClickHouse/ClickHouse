"""Delta Lake part of the corpus generator: writer profiles, table collection, and test SQL.

Tables are created by Spark with Delta Lake (`delta-spark`, two versions) and by delta-rs
(`deltalake`). Every version of every table is read back by the writer; the readback of each
version is the reference for time travel with `delta_lake_snapshot_version`, the last one is the
reference for the current state. For tables with the change data feed, the writer's
`table_changes` from version 0 is the reference for reading the change data feed with
`delta_lake_snapshot_start_version`.
"""

import json
import os
import shutil
import subprocess
import sys

import corpus_lib as cl

SPARK_4 = ["pyspark==4.1.1", "delta-spark==4.4.1", "pyarrow==26.0.0", "pandas==2.3.3", "numpy==2.3.5"]
SPARK_3 = ["pyspark==3.5.5", "delta-spark==3.3.2", "pyarrow==17.0.0", "pandas==2.2.3", "numpy==1.26.4"]

DELTA_WRITERS = {
    "spark-4.1.1-delta-4.4.1": {
        "script": "spark_delta_writer.py", "python": "3.12", "with": SPARK_4, "java": True,
        "job": {"prefix": "", "tables": "ALL"},
    },
    "spark-3.5.5-delta-3.3.2": {
        "script": "spark_delta_writer.py", "python": "3.11", "with": SPARK_3, "java": True,
        "job": {"prefix": "delta332_", "tables": "OLD"},
    },
    "deltalake-1.6.6": {
        "script": "deltars_delta_writer.py", "python": "3.12", "with": ["deltalake==1.6.6", "pyarrow==26.0.0"],
        "job": {"prefix": "rs_", "tables": "ALL_RS"},
    },
}

PREDICATES = [("id", "<", 10), ("id", ">=", 25), ("id", "=", 7), ("p", "=", 2)]

# ClickHouse limitations exposed by corpus tables. The checks of such a table are replaced by one read
# that records the error code in the reference, until the limitation is lifted.
KNOWN_ISSUES = {
    "delta/variant": {"issue": "the VARIANT type is not supported"},
}

# ClickHouse bugs exposed by corpus tables; their checks stay in the reference with the wrong results
# (`MISMATCH` or an error code) until they are fixed:
#  - with column mapping, the fields of a struct inside an `Array` or a `Map` are read as empty or NULL
#    (`column_mapping_id`, `delta332_column_mapping_id`, `iceberg_compat_v2`).


def run(g, args):
    """`g` is the `generate` module (paths, manifest, and helpers)."""
    only = set(args.only.split(",")) if args.only else None
    manifest = [e for e in g.load_manifest() if e["kind"] != "delta" or (only and e["writer_profile"] not in only)]
    for profile_name, profile in DELTA_WRITERS.items():
        if only and profile_name not in only:
            continue
        work = g.WORK / "delta" / profile_name
        if work.exists():
            shutil.rmtree(work)
        (work / "out").mkdir(parents=True)
        (work / "warehouse").mkdir()
        job = dict(profile["job"])
        sys.path.insert(0, str(g.HERE / "writers"))
        if job["tables"] in ("ALL", "OLD"):
            import spark_delta_writer as sdw
            job["tables"] = list(sdw.TABLES) if job["tables"] == "ALL" else list(sdw.OLD_TABLES)
        elif job["tables"] == "ALL_RS":
            job["tables"] = ["append_overwrite", "partitioned", "delete_update_merge", "schema_merge", "checkpoint_optimize", "all_types"]
        job.update({"warehouse": str(work / "warehouse"), "output_dir": str(work / "out"), "result": str(work / "result.json")})
        job_file = work / "job.json"
        json.dump(job, open(job_file, "w"), indent=1)
        cmd = ["uv", "run", "--isolated", "--no-project", "--managed-python", "--python", profile["python"]]
        for w in profile["with"]:
            cmd += ["--with", w]
        cmd += ["python", "-I", str(g.HERE / "writers" / profile["script"]), str(job_file)]
        env = dict(os.environ)
        env.pop("JAVA_TOOL_OPTIONS", None)
        env.pop("SPARK_HOME", None)
        if profile.get("java"):
            env["JAVA_HOME"] = g.JAVA17
            env["PATH"] = f"{g.JAVA17}/bin:" + env["PATH"]
        print("+", " ".join(cmd), flush=True)
        subprocess.run(cmd, check=True, env=env, cwd=str(work), timeout=3600)
        result = json.load(open(work / "result.json"))
        for r in result["results"]:
            manifest.append(collect_table(g, profile_name, result, r, work))
        g.save_manifest(manifest)
        print(f"{profile_name}: {len(result['results'])} tables", flush=True)
    g.gc_expected(manifest)


def _store(g, path):
    back = cl.read_ipc(path)
    rows = cl.normalize_table(back)
    compressed, raw = cl.rows_to_jsonl_zst(rows)
    exp = f"expected/{cl.jsonl_digest(raw)}.jsonl.zst"
    (g.CORPUS / "expected").mkdir(parents=True, exist_ok=True)
    if not (g.CORPUS / exp).exists():
        (g.CORPUS / exp).write_bytes(compressed)
    return exp, back, rows


def collect_table(g, profile_name, result, r, work):
    rel = f"delta/{r['name']}"
    dest = g.CORPUS / rel
    if dest.exists():
        shutil.rmtree(dest)
    # The checksums of the data files (`.<file>.crc`) are not needed, the `<version>.crc` files of the log are kept.
    shutil.copytree(r["location"], dest, ignore=shutil.ignore_patterns(".*.crc", "_staged_commits"))
    snapshots = []
    back = None
    for v in r["versions"]:
        exp, back, rows = _store(g, work / "out" / f"{r['name']}.{v['version']}.arrow")
        snapshots.append({**v, "expected": exp, "rows": back.num_rows, "columns": back.column_names, "_rows": rows})
    current = snapshots[-1]
    entry = {
        "file": rel, "kind": "delta", "group": "delta", "writer": result["library"], "writer_version": result["version"],
        "writer_profile": profile_name, "status": "ok", "protocol": r["protocol"],
        "size": sum(p.stat().st_size for p in dest.rglob("*") if p.is_file()),
        "expected": current["expected"], "rows": current["rows"], "columns": current["columns"],
        "subcolumns": g.subcolumns_for(back.schema),
        "predicates": [{"where": f"{g.ident(c)} {op} {g._sql_value(v)}", "count": g._eval_pred(current["_rows"], c, op, v)}
                       for c, op, v in PREDICATES if c in current["columns"]],
        "snapshots": [{k: v for k, v in s.items() if k != "_rows"} for s in snapshots],
    }
    if r.get("cdf"):
        entry["cdf_expected"], cdf, _ = _store(g, work / "out" / f"{r['name']}.cdf.arrow")
        entry["cdf_rows"] = cdf.num_rows
    if r.get("statements"):
        entry["statements"] = r["statements"]
    return entry


def checks_sql(g, entries):
    lines = ["-- Generated by utils/data-lake-corpus/generate.py checks. Do not edit.", ""]
    for e in entries:
        name = e["file"]
        src = f"deltaLakeLocal({g._q('{CORPUS}/' + name)})"

        def label(check):
            return g._q(f"{name} {check}".replace("\\", "/").replace("'", '"'))

        lines.append(f"SELECT {g._q('@@ ' + name)};")
        issue = KNOWN_ISSUES.get(name)
        if issue:
            lines.append(f"SELECT {label('known issue ' + issue['issue'])}, count(), sum(sipHash64(toString(tuple(*)))) FROM {src};")
            lines.append("")
            continue
        for k, snap in enumerate(e["snapshots"]):
            last = k == len(e["snapshots"]) - 1
            sws = "" if last else f" SETTINGS delta_lake_snapshot_version = {snap['version']}"
            tag = "" if last else f" version {snap['version']} {snap['operation']}"
            lines.append("DROP TABLE IF EXISTS t; DROP TABLE IF EXISTS e;")
            lines.append(f"CREATE TABLE t ENGINE = Memory AS SELECT * FROM {src}{sws};")
            lines.append("CREATE TABLE e AS t ENGINE = Memory;")
            lines.append(f"INSERT INTO e SELECT * FROM file({g._q('{CORPUS}/' + snap['expected'])}, JSONEachRow);")
            if last:
                lines.append(f"SELECT {label('types')}, arrayStringConcat(groupArray(type), ', ') FROM system.columns "
                             "WHERE database = currentDatabase() AND table = 't';")
            lines.append(
                f"SELECT {label('content' + tag)}, (SELECT count() FROM t), "
                f"if((SELECT count(), sum(cityHash64(tuple(*))) FROM t) = (SELECT count(), sum(cityHash64(tuple(*))) FROM e), 'ok', 'MISMATCH');")
            lines.append(f"SELECT {label('count()' + tag)}, if((SELECT count() FROM {src}{sws}) = {snap['rows']}, 'ok', 'MISMATCH');")
        for x in [g.ident(c) for c in e["columns"]] + g.checked_subcolumns(e):
            lines.append(
                f"SELECT {label(x)}, if((SELECT sum(cityHash64(tuple({x}))) FROM {src}) = "
                f"(SELECT sum(cityHash64(tuple({x}))) FROM e), 'ok', 'MISMATCH');")
        for pr in e.get("predicates", []):
            lines.append(
                f"SELECT {label('WHERE ' + pr['where'])}, if((SELECT count() FROM {src} WHERE {pr['where']}) = {pr['count']}, 'ok', 'MISMATCH');")
        if e.get("cdf_expected"):
            cws = " SETTINGS delta_lake_snapshot_start_version = 0"
            lines.append("DROP TABLE IF EXISTS t; DROP TABLE IF EXISTS e;")
            lines.append(f"CREATE TABLE t ENGINE = Memory AS SELECT * EXCEPT _commit_timestamp FROM {src}{cws};")
            lines.append("CREATE TABLE e AS t ENGINE = Memory;")
            lines.append(f"INSERT INTO e SELECT * FROM file({g._q('{CORPUS}/' + e['cdf_expected'])}, JSONEachRow);")
            lines.append(
                f"SELECT {label('change data feed')}, (SELECT count() FROM t), "
                f"if((SELECT count(), sum(cityHash64(tuple(*))) FROM t) = (SELECT count(), sum(cityHash64(tuple(*))) FROM e), 'ok', 'MISMATCH');")
        lines.append("")
    lines.append("DROP TABLE IF EXISTS t; DROP TABLE IF EXISTS e;")
    return "\n".join(lines) + "\n"
