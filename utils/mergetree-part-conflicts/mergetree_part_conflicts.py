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

* PARTIAL OVERLAP      -- the crash-causers you must resolve before the table loads.
* CROSS-DISK COVERED   -- a covered part whose surviving coverer is on a DIFFERENT
                          disk. The table loads, but CH silently retires the covered
                          part on startup; if the coverage is false (a re-issued
                          block range) that is silent data loss, so it is detached to
                          preserve it. (Needs part paths to tell the disk.)
* covered layers       -- a covered part on the SAME disk as its coverer: an ordinary
                          not-yet-cleaned merge source, left alone.

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

# Part name: <partition_id>_<min_block>_<max_block>_<level>[_<mutation>] -- the
# custom-partitioning format used by every table created since 2016. partition_id
# is matched non-greedily so the numeric tail binds to min/max/level (+ optional
# mutation); this is exact for numeric, hash and "all" partition ids, i.e. every
# id that carries no underscore, which is the realistic case.
_NAME_RE = re.compile(r"^(?P<pid>.+?)_(?P<min>\d+)_(?P<max>\d+)_(?P<level>\d+)(?:_(?P<mut>\d+))?$")


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


def parse_part_name(name: str) -> Optional[PartInfo]:
    """Parse a part-directory name into a PartInfo, or None if it is not a part."""
    m = _NAME_RE.match(name)
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

def read_parts_from_stream(stream) -> Dict[str, List[PartInfo]]:
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
        info = parse_part_name(name)
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


def _group_by_partition(parts: List[PartInfo]) -> Dict[str, List[PartInfo]]:
    groups: Dict[str, List[PartInfo]] = {}
    for p in parts:
        groups.setdefault(p.partition_id, []).append(p)
    return groups


def _table_dir(p: PartInfo) -> Optional[str]:
    """The part's containing (table-on-a-disk) directory, or None without a path.
    Two parts of one table on different disks have different table dirs."""
    return os.path.dirname(p.path.rstrip("/")) if p.path else None


# Reasons a part ends up in the detach set.
DETACH_OVERLAP = "overlap"                       # partial overlap -- aborts load
DETACH_XDISK_COVERED = "cross-disk-covered"      # covered by a part on another disk


def compute_detach(parts: List[PartInfo]) -> List[Tuple[PartInfo, str]]:
    """Parts (of one partition) to move to detached/, each with the reason.

    Two independent reasons:

    (a) OVERLAP -- for each partial overlap, drop the lower part so the table can
        load. Done as a fixpoint, because detaching a covering part exposes the
        parts it covered; if such a part then overlaps a survivor it is dropped in
        a later round.

    (b) CROSS-DISK-COVERED -- among what survives, a covered part whose surviving
        coverer sits on a *different disk* is moved aside too. CH would otherwise
        retire it as Outdated and clean it on startup, and if that coverage is
        false (a re-issued block range) that is silent data loss. Same-disk covered
        parts are ordinary not-yet-cleaned merge sources and are left alone. This
        needs the part path to know the disk; bare-name parts are skipped here.
    """
    by_name = {p.name: p for p in parts}
    result: List[Tuple[PartInfo, str]] = []
    seen = set()

    # (a) resolve partial overlaps
    remaining = list(parts)
    while True:
        rep = classify_partition(remaining)
        if not rep.conflicts:
            break
        losers = set()
        for c in rep.conflicts:
            keep = suggest_keep([c.a, c.b])
            loser = c.a if keep.name == c.b.name else c.b
            losers.add(loser.name)
        for name in sorted(losers):
            if name not in seen:
                seen.add(name)
                result.append((by_name[name], DETACH_OVERLAP))
        remaining = [p for p in remaining if p.name not in losers]

    # (b) cross-disk covered parts among the survivors
    rep = classify_partition(remaining)
    for c in sorted(rep.covered, key=lambda p: (p.min_block, p.max_block, p.level, p.name)):
        coverer = next((s for s in rep.maximal if s.contains(c)), None)
        if coverer is None or c.name in seen:
            continue
        if _table_dir(c) is not None and _table_dir(coverer) is not None \
                and _table_dir(c) != _table_dir(coverer):
            seen.add(c.name)
            result.append((c, DETACH_XDISK_COVERED))
    return result


def build_report(tables: Dict[str, List[PartInfo]]) -> dict:
    out: dict = {"tables": {}, "has_conflicts": False, "has_detach": False}
    for table, parts in sorted(tables.items()):
        partitions: dict = {}
        for pid, ps in sorted(_group_by_partition(parts).items()):
            rep = classify_partition(ps)
            if not rep.conflicts and not rep.covered:
                continue
            detach = compute_detach(ps)
            detach_set = {p.name for p, _ in detach}
            partitions[pid] = {
                "conflicts": [
                    {"a": c.a.name, "b": c.b.name, "overlap_blocks": list(c.overlap())}
                    for c in rep.conflicts
                ],
                "suggested_detach": sorted(p.name for p, _ in detach),
                "cross_disk_covered": sorted(p.name for p, r in detach if r == DETACH_XDISK_COVERED),
                # covered layers that are NOT being detached are benign same-disk
                # merge sources that load as Outdated and get cleaned normally.
                "covered_layers": [p.name for p in rep.covered if p.name not in detach_set],
            }
            if rep.conflicts:
                out["has_conflicts"] = True
            if detach:
                out["has_detach"] = True
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
            if pinfo["cross_disk_covered"]:
                print(f"    CROSS-DISK COVERED (CH would silently remove on start): "
                      f"{', '.join(pinfo['cross_disk_covered'])}")
            if pinfo["suggested_detach"]:
                print(f"      suggested: move to detached/ before reload: "
                      f"{', '.join(pinfo['suggested_detach'])}")
            if pinfo["covered_layers"]:
                print(f"    same-disk covered layers (load fine, cleaned normally): "
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
        "# Move the flagged parts aside so the table loads cleanly (overlaps) AND so the server",
        "# does not silently drop cross-disk-covered parts on startup -- WITHOUT losing data.",
        "# Do this with the server stopped, or with the affected tables not loaded. After moving,",
        "# reload the tables (DETACH/ATTACH DATABASE, or restart), then decide which moved parts",
        "# to re-attach and reconcile any duplicates in overlapping ranges.",
        "set -eu",
    ]
    any_line = False
    for table, parts in sorted(tables.items()):
        header_done = False
        for pid, ps in sorted(_group_by_partition(parts).items()):
            for p, reason in compute_detach(ps):
                if not header_done:
                    lines.append(f"\n# table: {table}")
                    header_done = True
                any_line = True
                if p.path:
                    table_dir = os.path.dirname(p.path.rstrip("/"))
                    detached_dir = os.path.join(table_dir, "detached")
                    dest = os.path.join(detached_dir, p.name)
                    lines.append(f"#   partition {pid}: detach {p.name} ({reason})")
                    lines.append(f"mkdir -p {_sh_quote(detached_dir)}")
                    lines.append(f"mv -- {_sh_quote(p.path)} {_sh_quote(dest)}")
                else:
                    lines.append(f"#   partition {pid}: detach {p.name} ({reason}) "
                                 f"-- path unknown; feed the part's full path to get an mv command")

    if not any_line:
        lines.append("\n# No intersecting parts to detach.")
    return lines


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__.splitlines()[0],
        epilog="Feed part names (or full paths), one per line, on stdin. E.g.: "
               "find /var/lib/clickhouse/store -mindepth 3 -maxdepth 3 -type d | %(prog)s",
    )
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    parser.add_argument("--emit-detach-commands", action="store_true",
                        help="print a read-only shell script that moves the suggested parts "
                             "into detached/ (never executed)")
    args = parser.parse_args(argv)

    tables = read_parts_from_stream(sys.stdin)
    report = build_report(tables)

    if args.emit_detach_commands:
        print("\n".join(emit_detach_commands(tables)))
    elif args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print_report(report)

    return 2 if report["has_detach"] else 0


if __name__ == "__main__":
    sys.exit(main())
