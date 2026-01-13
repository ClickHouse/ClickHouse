#!/usr/bin/env python3
import argparse
import json
from collections import defaultdict, Counter
from pathlib import Path


def load_rows(path: Path):
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                rows.append(json.loads(s))
            except Exception:
                continue
    return rows


def main():
    ap = argparse.ArgumentParser(description="Summarize Keeper stress JSONL sidecar")
    ap.add_argument("path", help="Path to keeper_metrics.jsonl")
    args = ap.parse_args()
    p = Path(args.path)
    if not p.exists():
        print(f"missing file: {p}")
        return 2
    rows = load_rows(p)
    if not rows:
        print("no rows")
        return 0

    # Bench ran check per (scenario, run_id)
    bench_runs = defaultdict(int)
    for r in rows:
        if r.get("source") == "bench" and r.get("name") == "bench_ran":
            key = (r.get("scenario"), r.get("run_id"))
            try:
                bench_runs[key] += int(float(r.get("value", 0)) > 0)
            except Exception:
                bench_runs[key] += 0

    # Prom rows count by stage
    prom_counts = Counter()
    for r in rows:
        if r.get("source") == "prom":
            prom_counts[r.get("stage", "")] += 1

    print("# Bench runs")
    for (scn, rid), cnt in sorted(bench_runs.items()):
        print(f"scenario={scn} run_id={rid} bench_ran_rows={cnt}")
    if not bench_runs:
        print("no bench_ran rows found")

    print("\n# Prom rows by stage")
    for stage, cnt in sorted(prom_counts.items()):
        print(f"stage={stage} rows={cnt}")
    if not prom_counts:
        print("no prom rows found")

    # Derived sanity: znode_count and dirs_bytes deltas per node (pre -> post)
    pre_zn = {}
    post_zn = {}
    pre_db = {}
    post_db = {}
    for r in rows:
        node = r.get("node")
        if not node:
            continue
        if r.get("stage") == "pre" and r.get("source") == "mntr" and r.get("name") == "zk_znode_count":
            try:
                pre_zn[node] = float(r.get("value"))
            except Exception:
                pass
        if r.get("stage") == "post" and r.get("source") == "mntr" and r.get("name") == "zk_znode_count":
            try:
                post_zn[node] = float(r.get("value"))
            except Exception:
                pass
        if r.get("stage") == "pre" and r.get("source") == "dirs" and r.get("name") == "bytes":
            try:
                pre_db[node] = float(r.get("value"))
            except Exception:
                pass
        if r.get("stage") == "post" and r.get("source") == "dirs" and r.get("name") == "bytes":
            try:
                post_db[node] = float(r.get("value"))
            except Exception:
                pass

    print("\n# Derived sanity (per-node deltas)")
    nodes = sorted(set(list(pre_zn.keys()) + list(post_zn.keys()) + list(pre_db.keys()) + list(post_db.keys())))
    for n in nodes:
        dz = post_zn.get(n, 0.0) - pre_zn.get(n, 0.0)
        db = post_db.get(n, 0.0) - pre_db.get(n, 0.0)
        print(f"node={n} znode_delta={int(dz)} dirs_bytes_delta={int(db)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
