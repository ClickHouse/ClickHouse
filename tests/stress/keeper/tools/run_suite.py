#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys
from pathlib import Path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--files", default=os.environ.get("KEEPER_SCENARIO_FILE", "all"))
    ap.add_argument(
        "--duration", type=int, default=int(os.environ.get("KEEPER_DURATION", "120"))
    )
    ap.add_argument("--seeds", default=os.environ.get("KEEPER_SEEDS", "1,2"))
    ap.add_argument("--jobs", default=os.environ.get("KEEPER_JOBS", ""))
    ap.add_argument("--include-ids", default=os.environ.get("KEEPER_INCLUDE_IDS", ""))
    ap.add_argument(
        "--matrix-backends", default=os.environ.get("KEEPER_MATRIX_BACKENDS", "")
    )
    ap.add_argument(
        "--matrix-topologies", default=os.environ.get("KEEPER_MATRIX_TOPOLOGIES", "")
    )
    ap.add_argument(
        "--include-faults-off",
        action="store_true",
        default=os.environ.get("KEEPER_INCLUDE_FAULTS_OFF", "") == "1",
    )
    # Sink URL/env not needed: metrics are sent via CI helper like other tests
    ap.add_argument(
        "--feature-flags", default=os.environ.get("KEEPER_FEATURE_FLAGS", "")
    )
    ap.add_argument(
        "--coord-overrides-xml",
        default=os.environ.get("KEEPER_COORD_OVERRIDES_XML", ""),
    )
    ap.add_argument("--keep-on-fail", action="store_true")
    ap.add_argument(
        "--rerun-failures",
        type=int,
        default=int(os.environ.get("KEEPER_RERUN_FAILURES", "0")),
    )
    args = ap.parse_args()
    repo = Path(__file__).parents[4]
    tests_path = "tests/stress/keeper/tests"
    env = os.environ.copy()
    env.setdefault("KEEPER_SCENARIO_FILE", args.files)
    if args.feature_flags:
        env["KEEPER_FEATURE_FLAGS"] = args.feature_flags
    if args.coord_overrides_xml:
        env["KEEPER_COORD_OVERRIDES_XML"] = args.coord_overrides_xml
    if args.keep_on_fail:
        env["KEEPER_KEEP_ON_FAIL"] = "1"
    if args.include_ids:
        env.setdefault("KEEPER_INCLUDE_IDS", args.include_ids)
    base = [sys.executable, "-m", "pytest", "-q"]
    if args.jobs:
        base += ["-n", args.jobs]
    base += [tests_path, f"--duration={args.duration}"]
    if args.matrix_backends:
        base += [f"--matrix-backends={args.matrix_backends}"]
    if args.matrix_topologies:
        base += [f"--matrix-topologies={args.matrix_topologies}"]

    seq = []
    if args.include_faults_off:
        seq.append(base + ["--faults=off"])
    seq.append(base + ["--faults=on"])
    seeds = [s.strip() for s in (args.seeds or "").split(",") if s.strip()]
    for s in seeds:
        seq.append(base + ["--faults=on", f"--seed={s}"])
    rc = 0
    for cmd in seq:
        p = subprocess.run(cmd, cwd=str(repo), env=env)
        if p.returncode != 0:
            rc = p.returncode
            break
    # Optional rerun of last failures N times (serial) for flakiness assessment
    if rc != 0 and int(args.rerun_failures or 0) > 0:
        lf_base = [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            tests_path,
            "--lf",
            f"--duration={args.duration}",
        ]
        if args.matrix_backends:
            lf_base += [f"--matrix-backends={args.matrix_backends}"]
        if args.matrix_topologies:
            lf_base += [f"--matrix-topologies={args.matrix_topologies}"]
        for _ in range(int(args.rerun_failures)):
            p = subprocess.run(lf_base, cwd=str(repo), env=env)
            # If passed, exit success
            if p.returncode == 0:
                rc = 0
                break
    sys.exit(rc)


if __name__ == "__main__":
    main()
