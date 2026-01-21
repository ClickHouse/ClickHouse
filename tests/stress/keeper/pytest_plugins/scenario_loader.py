import copy
import inspect
import os
import pathlib

import pytest
import yaml
from keeper.framework.core.schema import validate_scenario
from keeper.framework.core.settings import DEFAULT_ERROR_RATE, DEFAULT_P99_MS

_SCN_BASE = pathlib.Path(__file__).parents[1] / "scenarios"
_PREFIX_TAGS = {
    "sem",
    "cfg",
    "dur",
    "dsk",
    "ldr",
    "res",
    "perf",
    "sec",
    "obs",
    "mig",
    "int",
    "lin",
    "cha",
    "nbank",
    "bnd",
    "df",
    "clk",
    "soak",
    "ops",
    "load",
    "imp",
}
from keeper.framework import presets as _presets

def _effective_duration(s, defaults, cli_duration=None):
    if cli_duration is not None:
        return cli_duration
    for src in (s, defaults if isinstance(defaults, dict) else {}):
        dur = src.get("duration") if isinstance(src, dict) else None
        if dur is not None:
            try:
                return int(dur)
            except (ValueError, TypeError):
                pass
    return None


def apply_file_defaults_to_scenario(s, defaults):
    if not isinstance(s, dict) or not isinstance(defaults, dict) or not defaults:
        return s
    sc = copy.deepcopy(s)
    if "topology" not in sc and defaults.get("topology") is not None:
        sc["topology"] = defaults.get("topology")
    return sc


def build_preset_scenario(s, duration_s=None):
    if not isinstance(s, dict) or not s.get("preset") or _presets is None:
        return s
    name = str(s["preset"]).strip()
    fn = getattr(_presets, f"build_{name}", None)
    if fn is None:
        raise AssertionError(f"unknown preset: {name}")
    args = dict(s.get("preset_args") or {})
    if duration_s is not None and "duration_s" not in args:
        try:
            if "duration_s" in inspect.signature(fn).parameters:
                args["duration_s"] = int(duration_s)
        except (ValueError, TypeError):
            pass
    for src_key, dst_key, conv in (
        ("id", "sid", None),
        ("name", "name", None),
        ("topology", "topology", int),
        ("backend", "backend", None),
    ):
        if (val := s.get(src_key)) is not None:
            args.setdefault(dst_key, conv(val) if conv else val)
    try:
        return fn(**args)
    except TypeError as e:
        raise AssertionError(f"preset {name} arg error: {e}")


def normalize_scenario_durations(s, defaults=None, cli_duration=None):
    if not isinstance(s, dict):
        return s
    out = copy.deepcopy(s)
    eff_dur = _effective_duration(out, defaults or {}, cli_duration=cli_duration)
    if eff_dur is None:
        return out
    try:
        eff_i = int(eff_dur)
    except (ValueError, TypeError):
        return out
    if eff_i <= 0:
        return out
    try:
        out["_duration_s"] = int(eff_i)
    except (ValueError, TypeError):
        pass
    try:
        out["duration"] = int(eff_i)
    except (ValueError, TypeError):
        pass
    return out


def _tags_ok(tags):
    inc = {t for t in os.environ.get("KEEPER_INCLUDE_TAGS", "").split(",") if t}
    exc = {t for t in os.environ.get("KEEPER_EXCLUDE_TAGS", "").split(",") if t}
    if not inc and not exc:
        return True
    ts = set(tags or [])
    if inc and ts.isdisjoint(inc):
        return False
    if exc and not ts.isdisjoint(exc):
        return False
    return True


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
    # Default: inject health_precheck to validate baseline config early
    if not _has_gate(s, "health_precheck"):
        _append_gate(s, {"type": "health_precheck"})
    # Always append log_sanity_ok as a last guard (non-intrusive if logs empty)
    if not _has_gate(s, "log_sanity_ok"):
        _append_gate(s, {"type": "log_sanity_ok"})


def _inject_prefix_tags(s):
    sid = str(s.get("id", ""))
    if not sid:
        return
    prefix = sid.split("-", 1)[0].lower()
    if prefix not in _PREFIX_TAGS:
        return
    tags = s.get("tags") or []
    if prefix not in tags:
        s["tags"] = tags + [prefix]


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


def _resolve_cli_duration(cfg):
    cli_duration = _getopt(cfg, "--duration", "KEEPER_DURATION", "")
    if cli_duration:
        try:
            val = int(cli_duration)
            return val if val > 0 else None
        except (ValueError, TypeError):
            pass
    return None


def _resolve_scenario_files():
    env_target = os.environ.get("KEEPER_SCENARIO_FILE", "all")
    if env_target.lower() == "all":
        files = sorted(p for p in _SCN_BASE.glob("*.yaml"))
    elif "," in env_target:
        files = [_SCN_BASE / p.strip() for p in env_target.split(",") if p.strip()]
    else:
        files = [_SCN_BASE / env_target]
    return files


def _load_scenarios_from_files(files):
    scenarios_raw = []
    for f in files:
        if not f.exists():
            continue
        d = yaml.safe_load(f.read_text())
        if not isinstance(d, dict) or not isinstance(d.get("scenarios"), list):
            continue
        defaults = d.get("defaults") or {}
        if not isinstance(defaults, dict):
            defaults = {}
        for s in d["scenarios"]:
            if not isinstance(s, dict):
                continue
            sc = apply_file_defaults_to_scenario(s, defaults)
            if defaults:
                sc["_defaults"] = defaults
            scenarios_raw.append(sc)
    return scenarios_raw


def _parse_include_ids(cfg):
    include_ids_str = _getopt(cfg, "--keeper-include-ids", "KEEPER_INCLUDE_IDS", "")
    return {sid for sid in include_ids_str.split(",") if sid} if include_ids_str else set()


def _resolve_matrix_backends(cfg):
    mb_raw = _getopt(cfg, "--matrix-backends", "KEEPER_MATRIX_BACKENDS", "") or ""
    if mb := [x.strip() for x in mb_raw.split(",") if x.strip()]:
        return mb
    return []


def _resolve_matrix_topologies(cfg):
    mtops_raw = _getopt(cfg, "--matrix-topologies", "KEEPER_MATRIX_TOPOLOGIES", "")
    return [int(x.strip()) for x in mtops_raw.split(",") if x.strip()]


def _build_params(scenarios_raw, *, cli_duration, include_ids, mb, mtops):
    params = []
    seen_ids = set()
    for s in scenarios_raw:
        defaults = s.pop("_defaults", {}) if isinstance(s, dict) else {}
        eff_dur = _effective_duration(s, defaults or {}, cli_duration=cli_duration)
        s = build_preset_scenario(s, duration_s=eff_dur)
        s = normalize_scenario_durations(s, defaults=defaults, cli_duration=cli_duration)
        sid_val = s.get("id")
        if sid_val in seen_ids:
            continue
        if include_ids and sid_val not in include_ids:
            continue
        if not _tags_ok(s.get("tags")):
            continue
        _inject_gate_macros(s)
        _inject_prefix_tags(s)
        if errs := validate_scenario(s):
            raise AssertionError(f"Scenario {sid_val} invalid: {', '.join(errs)}")
        seen_ids.add(sid_val)
        params.extend(
            pytest.param(clone, id=clone["id"])
            for clone in expand_matrix_clones(s, mb, mtops)
        )
    return params


def pytest_generate_tests(metafunc):
    if "scenario" not in metafunc.fixturenames:
        return
    cli_duration = _resolve_cli_duration(metafunc.config)

    files = _resolve_scenario_files()
    scenarios_raw = _load_scenarios_from_files(files)
    include_ids = _parse_include_ids(metafunc.config)

    mb = _resolve_matrix_backends(metafunc.config)
    mtops = _resolve_matrix_topologies(metafunc.config)

    params = _build_params(
        scenarios_raw,
        cli_duration=cli_duration,
        include_ids=include_ids,
        mb=mb,
        mtops=mtops,
    )
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
