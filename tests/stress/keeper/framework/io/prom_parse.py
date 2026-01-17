import json
import re

_METRIC_RE = re.compile(
    r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{([^}]*)\})?\s+([+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?)\s*$"
)
_LABEL_RE = re.compile(r"\s*([a-zA-Z_][a-zA-Z0-9_]*)=\"([^\"]*)\"\s*")

DEFAULT_PREFIXES = (
    "clickhouse_keeper_",
    "ClickHouseKeeper_",
    "keeper_",
    "raft_",
)


def _unescape_label_value(s):
    # Prometheus text format escapes: \\, \", \n, \t, \r
    out = []
    i = 0
    while i < len(s):
        c = s[i]
        if c == "\\" and i + 1 < len(s):
            n = s[i + 1]
            if n == "n":
                out.append("\n")
                i += 2
                continue
            if n == "t":
                out.append("\t")
                i += 2
                continue
            if n == "r":
                out.append("\r")
                i += 2
                continue
            if n == '"':
                out.append('"')
                i += 2
                continue
            if n == "\\":
                out.append("\\")
                i += 2
                continue
        out.append(c)
        i += 1
    return "".join(out)


def parse_prometheus_text(
    text, allow_prefixes=DEFAULT_PREFIXES, name_allowlist=None, exclude_label_keys=None
):
    rows = []
    if not text:
        return rows
    name_allow = set(name_allowlist or []) if name_allowlist else None
    excl_label_keys = set(exclude_label_keys or []) if exclude_label_keys else None
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        m = _METRIC_RE.match(line.strip())
        if not m:
            continue
        name, _, label_blob, val = m.groups()
        if allow_prefixes and not any(name.startswith(p) for p in allow_prefixes):
            continue
        if name_allow is not None and name not in name_allow:
            continue
        labels = {}
        if label_blob:
            for lm in _LABEL_RE.finditer(label_blob):
                labels[lm.group(1)] = _unescape_label_value(lm.group(2))
        if excl_label_keys is not None and any(
            (k in excl_label_keys) for k in labels.keys()
        ):
            continue
        try:
            value = float(val)
        except Exception:
            continue
        rows.append(
            {
                "name": name,
                "value": value,
                "labels_json": json.dumps(labels, ensure_ascii=False),
            }
        )
    return rows
