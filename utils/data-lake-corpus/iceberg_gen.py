"""Iceberg part of the corpus generator: writer profiles, table collection, and test SQL.

Tables are created by Spark with the Iceberg Spark runtime (Hadoop catalog: `vN.metadata.json`
and `version-hint.text`) and by PyIceberg (SQL catalog: `<sequence>-<uuid>.metadata.json`).
Every snapshot is read back by the writer; the readback of each snapshot is the reference for
time travel with `iceberg_snapshot_id`, the last one is the reference for the current state.
"""

import json
import os
import shutil
import subprocess
import sys

import corpus_lib as cl

SPARK_ENV = ["pyspark==3.5.5", "pyarrow==17.0.0", "pandas==2.2.3", "numpy==1.26.4"]

ICEBERG_WRITERS = {
    "spark-3.5.5-iceberg-1.12.0": {
        "script": "spark_iceberg_writer.py", "python": "3.11", "with": SPARK_ENV, "java": True,
        "job": {"iceberg_version": "1.12.0", "prefix": "", "tables": "ALL"},
    },
    "spark-3.5.5-iceberg-1.4.3": {
        "script": "spark_iceberg_writer.py", "python": "3.11", "with": SPARK_ENV, "java": True,
        "job": {"iceberg_version": "1.4.3", "prefix": "ice143_", "tables": "OLD"},
    },
    "pyiceberg-0.12.0": {
        "script": "pyiceberg_writer.py", "python": "3.12", "with": ["pyiceberg[sql-sqlite,pyiceberg-core]==0.12.0", "pyarrow==26.0.0"],
        "uuid_columns": ["u"],
        "job": {"tables": [
            {"name": "py_v1_all_types", "builder": "py_all_types", "format_version": 1},
            {"name": "py_v2_all_types", "builder": "py_all_types", "format_version": 2},
            {"name": "py_v2_transforms", "builder": "py_transforms", "format_version": 2},
            {"name": "py_v1_month_day", "builder": "py_month_day", "format_version": 1},
            {"name": "py_v2_overwrite_delete", "builder": "py_overwrite_delete", "format_version": 2},
            {"name": "py_v2_schema_evolution", "builder": "py_schema_evolution", "format_version": 2},
            {"name": "py_v2_name_mapping", "builder": "py_name_mapping", "format_version": 2},
        ]},
    },
}

PREDICATES = [("id", "<", 10), ("id", ">=", 25), ("id", "=", 7), ("p", "=", 2)]


def run(g, args):
    """`g` is the `generate` module (paths, manifest, and helpers)."""
    only = set(args.only.split(",")) if args.only else None
    manifest = [e for e in g.load_manifest() if e["kind"] != "iceberg" or (only and e["writer_profile"] not in only)]
    for profile_name, profile in ICEBERG_WRITERS.items():
        if only and profile_name not in only:
            continue
        work = g.WORK / "iceberg" / profile_name
        if work.exists():
            shutil.rmtree(work)
        (work / "out").mkdir(parents=True)
        job = dict(profile["job"])
        if job["tables"] in ("ALL", "OLD"):
            sys.path.insert(0, str(g.HERE / "writers"))
            import spark_iceberg_writer as siw
            job["tables"] = list(siw.TABLES) if job["tables"] == "ALL" else list(siw.OLD_TABLES)
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
            manifest.append(collect_table(g, profile_name, profile, result, r, work))
        g.save_manifest(manifest)
        print(f"{profile_name}: {len(result['results'])} tables", flush=True)
    g.gc_expected(manifest)


def collect_table(g, profile_name, profile, result, r, work):
    rel = f"iceberg/{r['name']}"
    dest = g.CORPUS / rel
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(r["location"], dest, ignore=shutil.ignore_patterns("*.crc"))
    snapshots = []
    back = None
    for s in r["snapshots"]:
        back = cl.read_ipc(work / "out" / f"{r['name']}.{s['snapshot_id']}.arrow")
        rows = cl.normalize_table(back, uuid_columns=profile.get("uuid_columns", ()))
        compressed, raw = cl.rows_to_jsonl_zst(rows)
        exp = f"expected/{cl.jsonl_digest(raw)}.jsonl.zst"
        (g.CORPUS / "expected").mkdir(parents=True, exist_ok=True)
        if not (g.CORPUS / exp).exists():
            (g.CORPUS / exp).write_bytes(compressed)
        snapshots.append({**s, "expected": exp, "rows": back.num_rows, "columns": back.column_names, "_rows": rows})
    current = snapshots[-1]
    entry = {
        "file": rel, "kind": "iceberg", "group": "iceberg", "writer": result["library"], "writer_version": result["version"],
        "writer_profile": profile_name, "status": "ok",
        "size": sum(p.stat().st_size for p in dest.rglob("*") if p.is_file()),
        "expected": current["expected"], "rows": current["rows"], "columns": current["columns"],
        "subcolumns": g.subcolumns_for(back.schema),
        "predicates": [{"where": f"{g.ident(c)} {op} {g._sql_value(v)}", "count": g._eval_pred(current["_rows"], c, op, v)}
                       for c, op, v in PREDICATES if c in current["columns"]],
        "snapshots": [{k: v for k, v in s.items() if k != "_rows"} for s in snapshots],
    }
    for key in ("statements", "format_version"):
        if r.get(key):
            entry[key] = r[key]
    return entry


def checks_sql(g, entries):
    lines = ["-- Generated by utils/data-lake-corpus/generate.py checks. Do not edit.", ""]
    for e in entries:
        name = e["file"]
        src = f"icebergLocal({g._q('{CORPUS}/' + name)})"

        def label(check):
            return g._q(f"{name} {check}".replace("\\", "/").replace("'", '"'))

        lines.append(f"SELECT {g._q('@@ ' + name)};")
        issue = g._known_issue(e)
        base_settings = [issue["settings"]] if issue and issue.get("settings") else []
        ws = " SETTINGS " + ", ".join(base_settings) if base_settings else ""
        for k, snap in enumerate(e["snapshots"]):
            last = k == len(e["snapshots"]) - 1
            settings = base_settings + ([] if last else [f"iceberg_snapshot_id = {snap['snapshot_id']}"])
            sws = " SETTINGS " + ", ".join(settings) if settings else ""
            tag = "" if last else f" snapshot {k + 1} {snap['operation']}"
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
                f"SELECT {label(x)}, if((SELECT sum(cityHash64(tuple({x}))) FROM {src}{ws}) = "
                f"(SELECT sum(cityHash64(tuple({x}))) FROM e), 'ok', 'MISMATCH');")
        for pr in e.get("predicates", []):
            lines.append(
                f"SELECT {label('WHERE ' + pr['where'])}, if((SELECT count() FROM {src} WHERE {pr['where']}{ws}) = {pr['count']}, 'ok', 'MISMATCH');")
        if issue:
            lines.append(f"SELECT {label('known issue ' + issue['issue'])}, {issue['query'].replace('{SRC}', src)};")
        lines.append("")
    lines.append("DROP TABLE IF EXISTS t; DROP TABLE IF EXISTS e;")
    return "\n".join(lines) + "\n"
