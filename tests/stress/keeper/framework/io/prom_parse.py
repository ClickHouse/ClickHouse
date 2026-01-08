import re, json

_METRIC_RE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{([^}]*)\})?\s+([+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?)\s*$")
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
    it = iter(range(len(s)))
    i = 0
    while i < len(s):
        c = s[i]
        if c == "\\" and i + 1 < len(s):
            n = s[i + 1]
            if n == "n": out.append("\n"); i += 2; continue
            if n == "t": out.append("\t"); i += 2; continue
            if n == "r": out.append("\r"); i += 2; continue
            if n == '"': out.append('"'); i += 2; continue
            if n == "\\": out.append("\\"); i += 2; continue
        out.append(c)
        i += 1
    return "".join(out)

def parse_prometheus_text(text, allow_prefixes=DEFAULT_PREFIXES):
    rows = []
    if not text:
        return rows
    for line in text.splitlines():
        if not line or line.startswith('#'):
            continue
        m = _METRIC_RE.match(line.strip())
        if not m:
            continue
        name, _, label_blob, val = m.groups()
        if allow_prefixes and not any(name.startswith(p) for p in allow_prefixes):
            continue
        labels={}
        if label_blob:
            for lm in _LABEL_RE.finditer(label_blob):
                labels[lm.group(1)] = _unescape_label_value(lm.group(2))
        try:
            value=float(val)
        except Exception:
            continue
        rows.append({"name": name, "value": value, "labels_json": json.dumps(labels, ensure_ascii=False)})
    return rows
