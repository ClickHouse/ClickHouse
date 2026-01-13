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
    # Interval
    try:
        env_int = os.environ.get("KEEPER_MONITOR_INTERVAL_S")
        interval = int(env_int) if env_int not in (None, "") else None
    except Exception:
        interval = None
    if interval is None:
        try:
            interval = int(opts.get("monitor_interval_s", default_interval))
        except Exception:
            interval = int(default_interval)

    # Allow prefixes
    try:
        prom_allow = _split_csv(os.environ.get("KEEPER_PROM_ALLOW_PREFIXES", ""))
    except Exception:
        prom_allow = None
    if prom_allow is None:
        prom_allow = opts.get("prom_allow_prefixes") if isinstance(opts, dict) else None

    # Name allowlist
    try:
        prom_name_allow = _split_csv(os.environ.get("KEEPER_PROM_NAME_ALLOWLIST", ""))
    except Exception:
        prom_name_allow = None
    if prom_name_allow is None and isinstance(opts, dict):
        x = opts.get("prom_name_allowlist")
        # Preserve empty list (interpreted by sampler as disable-name-filter)
        if isinstance(x, list):
            prom_name_allow = [str(y) for y in x if str(y)]

    # Exclude label keys
    try:
        prom_excl_labels = _split_csv(os.environ.get("KEEPER_PROM_EXCLUDE_LABEL_KEYS", ""))
    except Exception:
        prom_excl_labels = None
    if prom_excl_labels is None and isinstance(opts, dict):
        x = opts.get("prom_exclude_label_keys")
        if isinstance(x, list):
            prom_excl_labels = [str(y) for y in x if str(y)]

    # Prom sampling every N
    try:
        pen = os.environ.get("KEEPER_PROM_EVERY_N")
        prom_every_n = int(pen) if pen not in (None, "") else None
    except Exception:
        prom_every_n = None
    if prom_every_n is None and isinstance(opts, dict):
        try:
            prom_every_n = int(opts.get("prom_every_n"))
        except Exception:
            prom_every_n = None
    if prom_every_n is None:
        prom_every_n = 3

    return {
        "interval_s": int(interval),
        "prom_allow_prefixes": prom_allow,
        "prom_name_allowlist": prom_name_allow,
        "prom_exclude_label_keys": prom_excl_labels,
        "prom_every_n": int(prom_every_n),
    }
