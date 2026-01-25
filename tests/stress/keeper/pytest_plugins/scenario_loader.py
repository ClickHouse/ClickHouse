import copy
import os
import pathlib

import pytest
import yaml

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


def validate_scenario(s):
    if not isinstance(s, dict):
        return ["scenario_not_dict"]

    errs = []

    # Validate id
    sid = s.get("id")
    if not isinstance(sid, str) or not sid.strip():
        errs.append("missing_id")

    # Validate duration and topology
    for k in ("duration", "topology"):
        if k in s:
            val = s.get(k)
            try:
                int(val)
            except Exception:
                errs.append(f"{k}_not_int")

    # Validate types for backend, faults, post, gates
    type_checks = [
        ("backend", str, "backend_not_str"),
        ("faults", list, "faults_not_list"),
        ("post", list, "post_not_list"),
        ("gates", list, "gates_not_list"),
    ]
    for key, typ, err in type_checks:
        if key in s and not isinstance(s.get(key), typ):
            errs.append(err)

    # Validate workload
    wl = s.get("workload")
    if "workload" in s:
        if not isinstance(wl, dict):
            errs.append("workload_not_dict")
        elif "duration" in wl:
            errs.append("workload_duration_not_supported")

    return errs


def _effective_duration(s, defaults, cli_duration=None):
    if cli_duration is not None:
        s["duration"] = int(cli_duration)
        return s["duration"]
    dur = s.get("duration")
    if dur is not None:
        return int(dur)
    if defaults:
        dur = defaults.get("duration")
        if dur is not None:
            s["duration"] = int(dur)
            return s["duration"]
    raise AssertionError(f"duration not found in scenario {s}")

def apply_file_defaults_to_scenario(s, defaults):
    if not defaults:
        return s
    sc = copy.deepcopy(s)
    if "topology" not in sc and defaults.get("topology") is not None:
        sc["topology"] = defaults.get("topology")
    return sc




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
    gs = s.get("gates", [])
    gs.append(gate)


def inject_gate_macros(s):
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


def inject_prefix_tags(s):
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


def _resolve_scenario_duration(cfg):
    cli_duration = _getopt(cfg, "--duration", "KEEPER_DURATION", "")
    if cli_duration:
        try:
            val = int(cli_duration)
            return val if val > 0 else None
        except (ValueError, TypeError):
            print(f"invalid duration: {cli_duration}")
    return None


def _resolve_scenario_files():
    return sorted(p for p in _SCN_BASE.glob("*.yaml"))


def _load_scenarios_from_files(files):
    scenarios_raw = []
    for f in files:
        print(f"loading scenarios from file: {f}")
        if not f.exists():
            print(f"file not found: {f}")
            continue
        d = yaml.safe_load(f.read_text())
        if not isinstance(d, dict) or not isinstance(d.get("scenarios"), list):
            print(f"invalid scenario file: {f}, not a dict or not a list of scenarios")
            continue
        defaults = d.get("defaults", {})
        for s in d["scenarios"]:
            if not isinstance(s, dict):
                continue
            sc = apply_file_defaults_to_scenario(s, defaults)
            if defaults:
                sc["_defaults"] = defaults
            scenarios_raw.append(sc)
    return scenarios_raw


def _parse_include_ids(cfg):
    include_ids_str = _getopt(cfg, "--keeper-include-ids")
    return {sid for sid in include_ids_str.split(",") if sid} if include_ids_str else set()


def _resolve_matrix_backends(cfg):
    mb_raw = _getopt(cfg, "--matrix-backends")
    if mb := [x.strip() for x in mb_raw.split(",") if x.strip()]:
        return mb
    return []


def _resolve_matrix_topologies(cfg):
    mtops_raw = _getopt(cfg, "--matrix-topologies")
    return [int(x.strip()) for x in mtops_raw.split(",") if x.strip()]


def _build_params(scenarios_raw, *, cli_duration, include_ids, mb, mtops):
    params = []
    seen_ids = set()
    for s in scenarios_raw:
        defaults = s.pop("_defaults", {}) if isinstance(s, dict) else {}
        eff_dur = _effective_duration(s, defaults or {}, cli_duration=cli_duration)
        sid_val = s.get("id")
        if sid_val in seen_ids:
            print(f"scenario id already seen: {sid_val}")
            continue
        if include_ids and sid_val not in include_ids:
            print(f"scenario id not in include_ids: {sid_val}")
            continue
        if not _tags_ok(s.get("tags")):
            print(f"scenario tags not ok: {s.get('tags')}")
            continue
        inject_gate_macros(s)
        inject_prefix_tags(s)
        if errs := validate_scenario(s):
            raise AssertionError(f"Scenario {sid_val} invalid: {', '.join(errs)}")
        seen_ids.add(sid_val)
        clones = expand_matrix_clones(s, mb, mtops)
        params.extend(pytest.param(clone, id=clone["id"]) for clone in clones)
    return params


def pytest_generate_tests(metafunc):
    if "scenario" not in metafunc.fixturenames:
        print("scenario not in fixturenames, skipping")
        return
    cli_duration = _resolve_scenario_duration(metafunc.config)

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


def expand_matrix_clones(s, backends, topologies):
    backs = backends or [s.get("backend")]
    topos = topologies or [int(s.get("topology"))]
    clones = []
    for b in backs:
        for topo in topos:
            clone = copy.deepcopy(s)
            clone["backend"] = b
            clone["topology"] = topo
            sid = clone.get("id")
            clone["id"] = f"{sid}[{b}|t{topo}]"
            clones.append(clone)
    return clones
