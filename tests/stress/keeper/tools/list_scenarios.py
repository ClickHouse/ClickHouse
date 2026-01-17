#!/usr/bin/env python3
import argparse
import os
import pathlib
import sys

import yaml

# Ensure repo root is on sys.path so 'tests' namespace can be imported
# list_scenarios.py is at tests/stress/keeper/tools/list_scenarios.py
# repo root is parents[4]
REPO = pathlib.Path(__file__).parents[4]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from keeper.pytest_plugins import scenario_loader as loader

SCN_BASE = pathlib.Path(__file__).parents[1] / "scenarios"


def load_files(files_arg):
    # CLI --files takes precedence; else env; else 'all'
    env_target = files_arg or os.environ.get("KEEPER_SCENARIO_FILE", "all")
    files = []
    if env_target.lower() == "all":
        files = sorted(
            p
            for p in SCN_BASE.glob("*.yaml")
            if p.name not in ("keeper_e2e.yaml", "e2e_unique.yaml")
        )
    elif "," in env_target:
        files = [SCN_BASE / p.strip() for p in env_target.split(",") if p.strip()]
    else:
        files = [SCN_BASE / env_target]
    extra = os.environ.get("KEEPER_EXTRA_SCENARIOS", "")
    for p in [x.strip() for x in extra.split(",") if x.strip()]:
        files.append(pathlib.Path(p))
    return files


def should_run(sid, total, index):
    if total <= 1:
        return True
    import hashlib

    h = int(hashlib.sha1(sid.encode()).hexdigest(), 16)
    return (h % total) == index


def main():
    ap = argparse.ArgumentParser(
        description="List Keeper stress scenarios after injections (no cluster)"
    )
    ap.add_argument(
        "--files",
        default=os.environ.get("KEEPER_SCENARIO_FILE", "all"),
        help="Scenario file spec (core.yaml, a.yaml,b.yaml, or all)",
    )
    ap.add_argument("--include-ids", default=os.environ.get("KEEPER_INCLUDE_IDS", ""))
    ap.add_argument(
        "--total-shards",
        type=int,
        default=int(os.environ.get("KEEPER_TOTAL_SHARDS", "1") or "1"),
    )
    ap.add_argument(
        "--shard-index",
        type=int,
        default=int(os.environ.get("KEEPER_SHARD_INDEX", "0") or "0"),
    )
    ap.add_argument(
        "--matrix-backends", default=os.environ.get("KEEPER_MATRIX_BACKENDS", "")
    )
    # TLS dimension removed
    ap.add_argument(
        "--matrix-topologies", default=os.environ.get("KEEPER_MATRIX_TOPOLOGIES", "")
    )
    args = ap.parse_args()

    files = load_files(args.files)
    scenarios_raw = []
    for f in files:
        if f.exists():
            d = yaml.safe_load(f.read_text())
            if isinstance(d, dict) and isinstance(d.get("scenarios"), list):
                scenarios_raw.extend(d["scenarios"])

    include_ids = {sid for sid in (args.include_ids or "").split(",") if sid}

    # parse matrix dimensions (TLS dimension removed)
    mb = [x.strip() for x in (args.matrix_backends or "").split(",") if x.strip()]
    mtops = [
        int(x.strip()) for x in (args.matrix_topologies or "").split(",") if x.strip()
    ]

    out = []
    seen = set()
    for s in scenarios_raw:
        # Expand presets if used
        if isinstance(s, dict) and s.get("preset") and loader._presets is not None:
            name = str(s.get("preset")).strip()
            fn = getattr(loader._presets, f"build_{name}", None)
            if fn is None:
                print(f"unknown preset: {name}", file=sys.stderr)
                sys.exit(2)
            args_map = dict(s.get("preset_args", {}))
            if s.get("id"):
                args_map.setdefault("sid", s.get("id"))
            if s.get("name"):
                args_map.setdefault("name", s.get("name"))
            if s.get("topology"):
                args_map.setdefault("topology", int(s.get("topology")))
            if s.get("backend"):
                args_map.setdefault("backend", s.get("backend"))
            s = fn(**args_map)
        sid = s.get("id")
        if sid in seen:
            continue
        if include_ids and sid not in include_ids:
            continue
        if not should_run(sid, int(args.total_shards or 1), int(args.shard_index or 0)):
            continue
        loader.inject_gate_macros(s)
        loader.inject_prefix_tags(s)
        errs = loader.validate_scenario(s)
        if errs:
            print(f"invalid scenario {sid}: {', '.join(errs)}", file=sys.stderr)
            sys.exit(2)
        seen.add(sid)
        # matrix expands (TLS removed)
        for clone in loader.expand_matrix_clones(s, mb, mtops):
            gates = [g.get("type") for g in (clone.get("gates", []) or [])]
            out.append(
                {
                    "id": clone.get("id"),
                    "name": clone.get("name"),
                    "backend": clone.get("backend"),
                    "topology": clone.get("topology"),
                    "tags": clone.get("tags", []),
                    "gates": gates,
                    "opts": clone.get("opts", {}),
                }
            )
    json.dump(out, sys.stdout, indent=2)


if __name__ == "__main__":
    main()
