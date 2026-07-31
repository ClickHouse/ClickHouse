#!/usr/bin/env python3
"""Offline detector of intersecting / covered MergeTree data parts.

A non-replicated ``MergeTree`` refuses to load a table when two active parts in
the same partition partially overlap (``LOGICAL_ERROR`` "Part ... intersects
previous/next part"), and it does so by throwing on the *first* such pair it
meets, so a failed startup never reveals the full picture. This tool
reconstructs the whole picture from the part names you feed it -- no running
server, no table attach -- by replaying the exact classification the server
performs in ``PartLoadingTree`` / ``MergeTreePartInfo``:

* ``contains``     -- one part supersedes another (a merge result over its
                      sources); the covered one would load as ``Outdated``.
* ``isDisjoint``   -- the parts share no block numbers; both stay active.
* neither          -- a partial overlap; this is what aborts startup.

You supply the list of parts yourself (from ``find``, ``clickhouse-disks
list``, ``system.parts``, etc.), one per line on stdin. It reports, per table
and partition:

* PARTIAL OVERLAP  -- the crash-causers you must resolve before the table loads.
* covered layers   -- parts a survivor name-covers; they load fine, but a
                      re-issued block range can make a survivor *falsely* cover
                      a part with unique data, so they are worth auditing.

The tool is strictly read-only: it never moves, attaches, or deletes anything.
"""

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass, field, replace
from typing import Dict, List, Optional, Tuple

# Mirrors src/Storages/MergeTree/MergeTreePartInfo.h
MAX_LEVEL = 999999999
LEGACY_MAX_LEVEL = 4294967295  # numeric_limits<UInt32>::max()

# Mirrors src/Storages/MergeTree/MergeTreeDataFormatVersion.h
FORMAT_VERSION_OLD = 0  # YYYYMMDD_YYYYMMDD_min_max_level (pre custom partitioning)
FORMAT_VERSION_CUSTOM = 1  # partitionid_min_max_level[_mutation]

# New-format part name: <partition_id>_<min>_<max>_<level>[_<mutation>].
# partition_id is taken non-greedily so the numeric tail binds to min/max/level
# (+ optional mutation). This is exact for numeric, hash and "all" partition ids
# -- i.e. every id that carries no underscore, which is the realistic case.
_NEW_NAME_RE = re.compile(r"^(?P<pid>.+?)_(?P<min>\d+)_(?P<max>\d+)_(?P<level>\d+)(?:_(?P<mut>\d+))?$")
# Old-format part name: <min_date>_<max_date>_<min>_<max>_<level>.
_OLD_NAME_RE = re.compile(r"^(?P<dmin>\d{6,8})_(?P<dmax>\d{6,8})_(?P<min>\d+)_(?P<max>\d+)_(?P<level>\d+)$")


@dataclass(frozen=True)
class PartInfo:
    partition_id: str
    min_block: int
    max_block: int
    level: int
    mutation: int
    name: str
    # Full path to the part directory, when the input line carried one. Excluded
    # from equality/hashing so it does not affect classification.
    path: Optional[str] = field(default=None, compare=False)

    def contains(self, rhs: "PartInfo") -> bool:
        """True if this part supersedes ``rhs`` (mirror of MergeTreePartInfo::contains)."""
        strictly_contains_block_range = (
            (self.min_block == rhs.min_block and self.max_block == rhs.max_block)
            or self.level > rhs.level
            or self.level == MAX_LEVEL
            or self.level == LEGACY_MAX_LEVEL
        )
        return (
            self.partition_id == rhs.partition_id
            and self.min_block <= rhs.min_block
            and self.max_block >= rhs.max_block
            and self.level >= rhs.level
            and self.mutation >= rhs.mutation
            and strictly_contains_block_range
        )

    def is_disjoint(self, rhs: "PartInfo") -> bool:
        """True if the parts share no block numbers (mirror of MergeTreePartInfo::isDisjoint)."""
        return (
            self.partition_id != rhs.partition_id
            or self.min_block > rhs.max_block
            or self.max_block < rhs.min_block
        )


def parse_part_name(name: str, format_version: int = FORMAT_VERSION_CUSTOM) -> Optional[PartInfo]:
    """Parse a part-directory name into a PartInfo, or None if it is not a part.

    ``format_version`` selects the on-disk naming scheme (0 = legacy, 1 = custom
    partitioning), exactly as the server reads it from ``format_version.txt``.
    """
    if format_version == FORMAT_VERSION_OLD:
        m = _OLD_NAME_RE.match(name)
        if not m:
            return None
        return PartInfo(
            partition_id=m.group("dmin")[:6],  # YYYYMM month partition
            min_block=int(m.group("min")),
            max_block=int(m.group("max")),
            level=int(m.group("level")),
            mutation=0,
            name=name,
        )

    m = _NEW_NAME_RE.match(name)
    if not m:
        return None
    return PartInfo(
        partition_id=m.group("pid"),
        min_block=int(m.group("min")),
        max_block=int(m.group("max")),
        level=int(m.group("level")),
        mutation=int(m.group("mut")) if m.group("mut") is not None else 0,
        name=name,
    )


@dataclass
class Conflict:
    """A partial overlap between two parts that would abort table load."""

    a: PartInfo
    b: PartInfo

    def overlap(self) -> Tuple[int, int]:
        return (max(self.a.min_block, self.b.min_block), min(self.a.max_block, self.b.max_block))


@dataclass
class PartitionReport:
    partition_id: str
    maximal: List[PartInfo] = field(default_factory=list)  # would-be-active parts
    covered: List[PartInfo] = field(default_factory=list)  # superseded layers
    conflicts: List[Conflict] = field(default_factory=list)

    @property
    def has_conflicts(self) -> bool:
        return bool(self.conflicts)


def classify_partition(parts: List[PartInfo]) -> PartitionReport:
    """Split one partition's parts into covering (maximal), covered, and conflicting.

    A part is *covered* when some other part ``contains`` it; the rest are
    *maximal*. Two maximal parts that are neither disjoint nor in a containment
    relation are a partial overlap -- the load-aborting conflict.
    """
    report = PartitionReport(partition_id=parts[0].partition_id if parts else "")

    for p in parts:
        covered_by_other = any(q is not p and q.contains(p) for q in parts)
        if covered_by_other:
            report.covered.append(p)
        else:
            report.maximal.append(p)

    ordered = sorted(report.maximal, key=lambda p: (p.min_block, p.max_block, p.level))
    for i in range(len(ordered)):
        for j in range(i + 1, len(ordered)):
            a, b = ordered[i], ordered[j]
            if a.max_block < b.min_block:  # sorted by min_block: nothing further can overlap a
                break
            if not a.is_disjoint(b) and not a.contains(b) and not b.contains(a):
                report.conflicts.append(Conflict(a, b))

    report.maximal = ordered
    report.covered.sort(key=lambda p: (p.min_block, p.max_block, p.level))
    return report


def classify(parts: List[PartInfo]) -> Dict[str, PartitionReport]:
    by_partition: Dict[str, List[PartInfo]] = {}
    for p in parts:
        by_partition.setdefault(p.partition_id, []).append(p)
    return {pid: classify_partition(ps) for pid, ps in by_partition.items()}


# --------------------------------------------------------------------------- #
# Input
# --------------------------------------------------------------------------- #

def read_parts_from_stream(stream, format_version: int) -> Dict[str, List[PartInfo]]:
    """Read parts from a plain-text stream (e.g. the output of ``find``), one per
    line, grouped by table.

    Each non-empty, non-``#`` line is::

        [<table>\\t]<token>

    ``<token>`` is a part name (``20260722_98_20874_190``) or a full path to the
    part directory. When a path is given, its basename is the part name, the path
    is remembered (so ``--emit-detach-commands`` can produce a concrete ``mv``),
    and the parent directory is the default table. Lines that are not part names
    are skipped with a warning.
    """
    tables: Dict[str, List[PartInfo]] = {}
    for raw in stream:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        table: Optional[str] = None
        token = line
        if "\t" in line:
            table, token = line.split("\t", 1)
            table, token = table.strip(), token.strip()
        path = token if "/" in token else None
        name = os.path.basename(token.rstrip("/")) if path else token
        if table is None:
            table = os.path.dirname(path.rstrip("/")) if path else "(input)"
        info = parse_part_name(name, format_version)
        if info is None:
            print(f"warning: not a part name, skipped: {name}", file=sys.stderr)
            continue
        if path:
            info = replace(info, path=path)
        tables.setdefault(table, []).append(info)
    return tables


# --------------------------------------------------------------------------- #
# Reporting
# --------------------------------------------------------------------------- #

def suggest_keep(cluster: List[PartInfo]) -> PartInfo:
    """Pick the part to keep active in an overlapping cluster: highest level,
    then widest block range, then lexicographically smallest name (stable)."""
    return max(cluster, key=lambda p: (p.level, p.max_block - p.min_block, p.name))


def build_report(tables: Dict[str, List[PartInfo]]) -> dict:
    out: dict = {"tables": {}, "has_conflicts": False}
    for table, parts in sorted(tables.items()):
        per_partition = classify(parts)
        partitions: dict = {}
        for pid, rep in sorted(per_partition.items()):
            if not rep.conflicts and not rep.covered:
                continue
            conflict_parts = {p.name for c in rep.conflicts for p in (c.a, c.b)}
            detach: List[str] = []
            if conflict_parts:
                cluster = [p for p in rep.maximal if p.name in conflict_parts]
                keep = suggest_keep(cluster)
                detach = sorted(n for n in conflict_parts if n != keep.name)
            partitions[pid] = {
                "conflicts": [
                    {"a": c.a.name, "b": c.b.name, "overlap_blocks": list(c.overlap())}
                    for c in rep.conflicts
                ],
                "covered_layers": [p.name for p in rep.covered],
                "suggested_detach": detach,
            }
            if rep.conflicts:
                out["has_conflicts"] = True
        if partitions:
            out["tables"][table] = {"partitions": partitions}
    return out


def print_report(report: dict) -> None:
    if not report["tables"]:
        print("No intersecting or covered parts found.")
        return
    for table, entry in report["tables"].items():
        print(f"\n=== table: {table} ===")
        for pid, pinfo in entry["partitions"].items():
            print(f"  partition {pid}:")
            for c in pinfo["conflicts"]:
                lo, hi = c["overlap_blocks"]
                print(f"    PARTIAL OVERLAP (aborts load): {c['a']}  <->  {c['b']}  "
                      f"[shared blocks {lo}..{hi}]")
            if pinfo["suggested_detach"]:
                print(f"      suggested: keep the survivor, DETACH + REATTACH: "
                      f"{', '.join(pinfo['suggested_detach'])}")
            if pinfo["covered_layers"]:
                print(f"    covered layers (load fine; audit for a false dominator): "
                      f"{', '.join(pinfo['covered_layers'])}")


def _sh_quote(s: str) -> str:
    return "'" + s.replace("'", "'\\''") + "'"


def emit_detach_commands(tables: Dict[str, List[PartInfo]]) -> List[str]:
    """Produce a shell script that moves each suggested-detach part into its
    table's detached/ dir. Read-only: the lines are printed, never executed.

    Concrete ``mv`` lines are produced only for parts fed as a path; for parts
    fed by bare name the path is unknown and a placeholder comment is emitted.
    """
    lines = [
        "#!/bin/sh",
        "# READ-ONLY SUGGESTION -- review every line before running; this tool does not execute it.",
        "# Move the intersecting parts aside so the table can load, WITHOUT losing them.",
        "# The server must not have these tables loaded while you move files (they are the",
        "# tables that failed to attach). After moving, reload them (DETACH/ATTACH DATABASE",
        "# or a restart), then ATTACH PART each moved part to bring its rows back with a",
        "# fresh block number, then reconcile duplicates in the overlapping ranges.",
        "set -eu",
    ]
    reattach: List[str] = []
    any_cmd = False
    for table, parts in sorted(tables.items()):
        per_partition = classify(parts)
        header_done = False
        for pid, rep in sorted(per_partition.items()):
            if not rep.conflicts:
                continue
            conflict_names = {p.name for c in rep.conflicts for p in (c.a, c.b)}
            cluster = [p for p in rep.maximal if p.name in conflict_names]
            keep = suggest_keep(cluster)
            for p in sorted((x for x in cluster if x.name != keep.name), key=lambda x: x.name):
                if not header_done:
                    lines.append(f"\n# table: {table}")
                    header_done = True
                if p.path:
                    table_dir = os.path.dirname(p.path.rstrip("/"))
                    detached_dir = os.path.join(table_dir, "detached")
                    dest = os.path.join(detached_dir, p.name)
                    lines.append(f"#   partition {pid}: detach {p.name} (keep {keep.name})")
                    lines.append(f"mkdir -p {_sh_quote(detached_dir)}")
                    lines.append(f"mv -- {_sh_quote(p.path)} {_sh_quote(dest)}")
                    any_cmd = True
                else:
                    lines.append(f"#   partition {pid}: detach {p.name} (keep {keep.name}) "
                                 f"-- path unknown; feed the part's full path to get an mv command")
                reattach.append(f"#   ALTER TABLE <db>.<table> ATTACH PART '{p.name}';")

    if reattach:
        lines.append("\n# After the tables are reloaded, bring the moved parts back:")
        lines.extend(reattach)
    if not any_cmd and not reattach:
        lines.append("\n# No intersecting parts to detach.")
    return lines


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__.splitlines()[0],
        epilog="Feed part names (or full paths), one per line, on stdin. E.g.: "
               "find /var/lib/clickhouse/store -mindepth 3 -maxdepth 3 -type d | %(prog)s",
    )
    parser.add_argument("--format-version", type=int, choices=[0, 1], default=FORMAT_VERSION_CUSTOM,
                        help="on-disk part-name format version (default: 1, custom partitioning)")
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    parser.add_argument("--emit-detach-commands", action="store_true",
                        help="print a read-only shell script that moves the suggested parts "
                             "into detached/ (never executed)")
    args = parser.parse_args(argv)

    tables = read_parts_from_stream(sys.stdin, args.format_version)
    report = build_report(tables)

    if args.emit_detach_commands:
        print("\n".join(emit_detach_commands(tables)))
    elif args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print_report(report)

    return 2 if report["has_conflicts"] else 0


if __name__ == "__main__":
    sys.exit(main())
