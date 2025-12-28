import copy
import hashlib
import os
import pathlib
import random

import pytest
import yaml

from ..framework.core.schema import validate_scenario
from ..framework.core.settings import DEFAULT_ERROR_RATE, DEFAULT_P99_MS

_SCN_BASE = pathlib.Path(__file__).parents[1] / "scenarios"
from ..framework import presets as _presets
from ..framework.fuzz import generate_fuzz_scenario


def _tags_ok(tags):
    inc = set([t for t in os.environ.get("KEEPER_INCLUDE_TAGS", "").split(",") if t])
    exc = set([t for t in os.environ.get("KEEPER_EXCLUDE_TAGS", "").split(",") if t])
    if not inc and not exc:
        return True
    ts = set(tags or [])
    if inc and ts.isdisjoint(inc):
        return False
    if exc and not ts.isdisjoint(exc):
        return False
    return True


def _should_run(sid, total, index):
    if total <= 1:
        return True
    h = int(hashlib.sha1(sid.encode()).hexdigest(), 16)
    return (h % total) == index


def _has_gate(s, gate_type):
    return any(g.get("type") == gate_type for g in (s.get("gates") or []))


def _append_gate(s, gate):
    gs = s.get("gates")
    if gs is None:
        s["gates"] = [gate]
    else:
        gs.append(gate)


def _inject_gate_macros(s):
    """Inject common gate blocks based on scenario id patterns.

    This reduces duplication without altering the YAML file directly.
    The injection is idempotent: existing gates are preserved.
    """
    sid = s.get("id", "")
    # Reconfiguration scenarios: ensure converge + member count + single_leader + backlog drain
    if isinstance(sid, str) and sid.startswith("RCFG-"):
        if not _has_gate(s, "single_leader"):
            _append_gate(s, {"type": "single_leader"})
        if not _has_gate(s, "config_converged"):
            _append_gate(s, {"type": "config_converged", "timeout_s": 30})
        if not _has_gate(s, "config_members_len_eq"):
            exp = None
            try:
                exp = int((s.get("opts") or {}).get("expected_members"))
            except Exception:
                exp = None
            if exp is None:
                try:
                    exp = int(s.get("topology", 3))
                except Exception:
                    exp = 3
                # known special case where we expect two members after a remove
                if sid == "RCFG-02":
                    exp = 2
            _append_gate(s, {"type": "config_members_len_eq", "expected": exp})
        if not _has_gate(s, "backlog_drains"):
            _append_gate(s, {"type": "backlog_drains"})
    # INT scenarios: ensure backlog drains after count gate
    if isinstance(sid, str) and sid.startswith("INT-"):
        if not _has_gate(s, "backlog_drains"):
            _append_gate(s, {"type": "backlog_drains"})
    # Default: inject health_precheck to validate baseline config early
    if not _has_gate(s, "health_precheck"):
        _append_gate(s, {"type": "health_precheck"})
    # Add generic SLO guards for scenarios with workload
    if s.get("workload"):
        if not _has_gate(s, "error_rate_le"):
            _append_gate(
                s, {"type": "error_rate_le", "max_ratio": float(DEFAULT_ERROR_RATE)}
            )
        if not _has_gate(s, "p99_le"):
            _append_gate(s, {"type": "p99_le", "max_ms": int(DEFAULT_P99_MS)})
    # Always append log_sanity_ok as a last guard (non-intrusive if logs empty)
    if not _has_gate(s, "log_sanity_ok"):
        _append_gate(s, {"type": "log_sanity_ok"})


def _inject_prefix_tags(s):
    sid = str(s.get("id", ""))
    if not sid:
        return
    prefix = sid.split("-", 1)[0].lower()
    tag_map = {
        "sem": "sem",
        "cfg": "cfg",
        "dur": "dur",
        "dsk": "dsk",
        "ldr": "ldr",
        "res": "res",
        "perf": "perf",
        "sec": "sec",
        "obs": "obs",
        "mig": "mig",
        "int": "int",
        "lin": "lin",
        "cha": "cha",
        "nbank": "nbank",
        "bnd": "bnd",
        "df": "df",
        "clk": "clk",
        "soak": "soak",
        "ops": "ops",
        "load": "load",
        "imp": "imp",
    }
    t = tag_map.get(prefix)
    if not t:
        return
    tags = s.get("tags") or []
    if t not in tags:
        s["tags"] = tags + [t]


def _getopt(cfg, name, env_name=None, default=""):
    try:
        v = cfg.getoption(name)
    except Exception:
        v = None
    if v not in (None, ""):
        return v
    if env_name:
        ev = os.environ.get(env_name)
        if ev not in (None, ""):
            return ev
    return default


def pytest_generate_tests(metafunc):
    if "scenario" not in metafunc.fixturenames:
        return
    # accept alt env names; robust to missing CLI options
    total = int(
        _getopt(metafunc.config, "--total-shards", "KEEPER_TOTAL_SHARDS", 1) or 1
    )
    index = int(_getopt(metafunc.config, "--shard-index", "KEEPER_SHARD_INDEX", 0) or 0)
    # Load primary scenario file(s) + optional extras
    # Default: full modular suite (exclude legacy keeper_e2e.yaml)
    env_target = os.environ.get("KEEPER_SCENARIO_FILE", "all")
    if env_target.lower() == "all":
        files = sorted(
            p
            for p in _SCN_BASE.glob("*.yaml")
            if p.name not in ("keeper_e2e.yaml", "e2e_unique.yaml")
        )
    elif "," in env_target:
        files = [(_SCN_BASE / p.strip()) for p in env_target.split(",") if p.strip()]
    else:
        files = [_SCN_BASE / env_target]
    extra = os.environ.get("KEEPER_EXTRA_SCENARIOS", "")
    for p in [x.strip() for x in extra.split(",") if x.strip()]:
        files.append(pathlib.Path(p))
    scenarios_raw = []
    for f in files:
        if f.exists():
            d = yaml.safe_load(f.read_text())
            if isinstance(d, dict) and isinstance(d.get("scenarios"), list):
                scenarios_raw.extend(d["scenarios"])
    data = {"scenarios": scenarios_raw}
    include_ids = set(
        [
            sid
            for sid in (
                _getopt(
                    metafunc.config, "--keeper-include-ids", "KEEPER_INCLUDE_IDS", ""
                )
                or ""
            ).split(",")
            if sid
        ]
    )
    scenarios = []
    # matrix options (TLS removed) - allow env override via KEEPER_MATRIX_BACKENDS / KEEPER_MATRIX_TOPOLOGIES
    mb = (
        _getopt(
            metafunc.config, "--matrix-backends", "KEEPER_MATRIX_BACKENDS", ""
        )
        or ""
    ).split(",")
    mb = [x.strip() for x in mb if x.strip()]
    mtops = (
        _getopt(
            metafunc.config, "--matrix-topologies", "KEEPER_MATRIX_TOPOLOGIES", ""
        )
        or ""
    ).split(",")
    mtops = [int(x.strip()) for x in mtops if x.strip()]

    params = []
    seen_ids = set()
    for s in data["scenarios"]:
        # Expand preset-defined scenarios to keep YAML thin
        if isinstance(s, dict) and s.get("preset") and _presets is not None:
            name = str(s.get("preset")).strip()
            fn = getattr(_presets, f"build_{name}", None)
            if fn is None:
                raise AssertionError(f"unknown preset: {name}")
            args = dict(s.get("preset_args", {}))
            # allow id/name override via top-level keys
            if s.get("id"):
                args.setdefault("sid", s.get("id"))
            if s.get("name"):
                args.setdefault("name", s.get("name"))
            if s.get("topology"):
                args.setdefault("topology", int(s.get("topology", 0)))
            if s.get("backend"):
                args.setdefault("backend", s.get("backend"))
            try:
                s = fn(**args)
            except TypeError as e:
                raise AssertionError(f"preset {name} arg error: {e}")
        sid_val = s.get("id")
        if sid_val in seen_ids:
            continue
        if include_ids and sid_val not in include_ids:
            continue
        if not _should_run(s["id"], total, index):
            continue
        if not _tags_ok(s.get("tags")):
            continue
        _inject_gate_macros(s)
        _inject_prefix_tags(s)
        errs = validate_scenario(s)
        if errs:
            raise AssertionError(f"Scenario {s.get('id')} invalid: {', '.join(errs)}")
        seen_ids.add(s.get("id"))
        # Build matrix expansions
        for clone in expand_matrix_clones(s, mb, mtops):
            params.append(pytest.param(clone, id=clone["id"]))
    # Optional fuzz scenario
    if bool(_getopt(metafunc.config, "--fuzz", None, False)):
        seed = int(_getopt(metafunc.config, "--fuzz-seed", None, 0) or 0)
        if seed <= 0:
            seed = random.randint(1, 2**31 - 1)
        steps = int(_getopt(metafunc.config, "--fuzz-steps", None, 8) or 8)
        dur = int(_getopt(metafunc.config, "--fuzz-duration", None, 180) or 180)
        # parse weights
        wstr = _getopt(metafunc.config, "--fuzz-weights", None, "") or ""
        weights = {}
        for part in wstr.split(","):
            if not part.strip():
                continue
            if "=" in part:
                k, v = part.split("=", 1)
                k = k.strip()
                v = v.strip()
                try:
                    weights[k] = int(v)
                except Exception:
                    continue
        dmin = int(_getopt(metafunc.config, "--fuzz-dur-min", None, 15) or 15)
        dmax = int(_getopt(metafunc.config, "--fuzz-dur-max", None, 120) or 120)
        fs = generate_fuzz_scenario(
            seed, steps, dur, weights=weights, dur_min=dmin, dur_max=dmax
        )
        fs["id"] = f"FUZZ-{seed}"
        errs = validate_scenario(fs)
        if errs:
            raise AssertionError(f"Fuzz scenario invalid: {', '.join(errs)}")
        params.append(pytest.param(fs, id=fs["id"]))
    metafunc.parametrize("scenario", params)


def inject_gate_macros(s):
    _inject_gate_macros(s)


def inject_prefix_tags(s):
    _inject_prefix_tags(s)


def expand_matrix_clones(s, backends, topologies):
    clones = []
    backs = backends or [s.get("backend") or "default"]
    topos = topologies or [int(s.get("topology", 3))]
    for b in backs:
        for topo in topos:
            c = copy.deepcopy(s)
            c["backend"] = b
            c["topology"] = int(topo)
            sid = c.get("id") or "SCN"
            suffix = f"[{b}|t{topo}]"
            c["id"] = f"{sid}{suffix}"
            clones.append(c)
    return clones
