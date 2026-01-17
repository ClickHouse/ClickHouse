import os
from typing import Dict, List, Optional


def _split_csv(val: Optional[str]) -> Optional[List[str]]:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    return [p for p in (s.split(",") or []) if p]


def compute_prom_config(opts: Optional[Dict] = None, default_interval: int = 5) -> Dict:
    opts = opts or {}

    # Interval: env > opts > default
    env_int = os.environ.get("KEEPER_MONITOR_INTERVAL_S", "").strip()
    if env_int:
        interval = int(env_int)
    elif opts.get("monitor_interval_s") is not None:
        interval = int(opts["monitor_interval_s"])
    else:
        interval = int(default_interval)

    # Allow prefixes: env > opts
    prom_allow = _split_csv(os.environ.get("KEEPER_PROM_ALLOW_PREFIXES", ""))
    if prom_allow is None:
        prom_allow = opts.get("prom_allow_prefixes") if isinstance(opts, dict) else None

    # Name allowlist: env > opts
    prom_name_allow = _split_csv(os.environ.get("KEEPER_PROM_NAME_ALLOWLIST", ""))
    if prom_name_allow is None and isinstance(opts, dict):
        x = opts.get("prom_name_allowlist")
        if isinstance(x, list):
            prom_name_allow = [str(y) for y in x if str(y)]

    # Exclude label keys: env > opts
    prom_excl_labels = _split_csv(os.environ.get("KEEPER_PROM_EXCLUDE_LABEL_KEYS", ""))
    if prom_excl_labels is None and isinstance(opts, dict):
        x = opts.get("prom_exclude_label_keys")
        if isinstance(x, list):
            prom_excl_labels = [str(y) for y in x if str(y)]

    # Prom sampling every N: env > opts > default (3)
    pen = os.environ.get("KEEPER_PROM_EVERY_N", "").strip()
    if pen:
        prom_every_n = int(pen)
    elif isinstance(opts, dict) and opts.get("prom_every_n") is not None:
        prom_every_n = int(opts["prom_every_n"])
    else:
        prom_every_n = 3

    return {
        "interval_s": interval,
        "prom_allow_prefixes": prom_allow,
        "prom_name_allowlist": prom_name_allow,
        "prom_exclude_label_keys": prom_excl_labels,
        "prom_every_n": prom_every_n,
    }
