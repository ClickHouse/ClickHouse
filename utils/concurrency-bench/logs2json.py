#!/usr/bin/env python3
"""Parse clickhouse-benchmark log files into JSON.

Output formats:
  Combined (default): {"Q0.log":[[concurrency,qps,p0,...,p99_99],...], ...}
  Per-line (-L): one JSON object per file line: {"Q0.log":[...]}

Usage:
  logs2json.py [-L] [files...]
  logs2json.py -h
"""
import sys, re, json, glob

WANTED_PCTS = ["0","10","20","30","40","50","60","70","80","90","95","99","99.9","99.99"]

re_conc = re.compile(r'\bConcurrency\b[^0-9]*([0-9]+)', re.I)
re_qps  = re.compile(r'\b(QPS|Queries/?s|Queries/sec|Queries per second)\b[^0-9]*([0-9]+(?:\.[0-9]+)?)', re.I)
re_pct  = re.compile(r'(\d+(?:\.\d+)?)%\s+([0-9.]+)\s*(ms|msec|milliseconds?|s|sec\.?|seconds?)', re.I)


def norm_pct(s: str) -> str:
    if '.' in s:
        s = s.rstrip('0').rstrip('.')
    return s


def unit_to_seconds(val: float, unit: str) -> float:
    unit = unit.lower()
    if unit.startswith('ms') or 'msec' in unit:
        return val / 1000.0
    return val


def parse_file(path: str):
    rows = []
    current = {"concurrency": None, "qps": None, "percentiles": {}}

    def finish_run(force=False):
        nonlocal current
        if not force and len(current["percentiles"]) < len(WANTED_PCTS):
            return
        if current["concurrency"] is None and current["qps"] is None and not current["percentiles"]:
            return
        row = [
            current["concurrency"] or 0,
            current["qps"] or 0.0,
            *[current["percentiles"].get(p, 0.0) for p in WANTED_PCTS]
        ]
        rows.append(row)
        current = {"concurrency": None, "qps": None, "percentiles": {}}

    with open(path, 'r', errors='ignore') as f:
        for line in f:
            m = re_conc.search(line)
            if m:
                if (current["concurrency"] is not None or current["percentiles"] or current["qps"]):
                    finish_run(force=True)
                current["concurrency"] = int(m.group(1))
                continue
            m = re_qps.search(line)
            if m:
                current["qps"] = float(m.group(2))
            for pm in re_pct.finditer(line):
                pct_raw, val_raw, unit = pm.groups()
                pct = norm_pct(pct_raw)
                if pct in WANTED_PCTS and pct not in current["percentiles"]:
                    try:
                        val = unit_to_seconds(float(val_raw), unit)
                        current["percentiles"][pct] = val
                    except ValueError:
                        pass
            if len(current["percentiles"]) == len(WANTED_PCTS):
                finish_run()
    finish_run(force=True)
    return rows


def usage():
    print("Usage: logs2json.py [-L|--lines] [log files]\n"
          "Parse clickhouse-benchmark log files into JSON.\n"
          "  -L, --lines  Emit one JSON object per line (one file per line)\n"
          "  -h, --help   Show this help", file=sys.stderr)


def main():
    per_line = False
    args = []
    for a in sys.argv[1:]:
        if a in ("-L","--lines"):
            per_line = True
        elif a in ("-h","--help"):
            usage()
            return
        else:
            args.append(a)
    files = args or sorted(glob.glob("Q*.log"))
    if not files:
        if not per_line:
            print("{}")
        return
    if per_line:
        for f in files:
            try:
                rows = parse_file(f)
            except OSError:
                rows = []
            sys.stdout.write(json.dumps({f: rows}, separators=(",",":")) + "\n")
        return
    result = {}
    for f in files:
        try:
            result[f] = parse_file(f)
        except OSError:
            result[f] = []
    # Reformatted output: one line per key
    print('{')
    items = list(result.items())
    for i, (k, v) in enumerate(items):
        comma = ',' if i < len(items) - 1 else ''
        print(f'{json.dumps(k)}:{json.dumps(v, separators=(",",":"))}{comma}')
    print('}')
    if sys.stdout.isatty():
        print()


if __name__ == "__main__":
    main()
