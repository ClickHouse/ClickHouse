# mergetree-part-conflicts

Offline detector of intersecting / covered `MergeTree` data parts, working from
the part names you feed it -- no running server.

A non-replicated `MergeTree` refuses to load a table when two active parts in
the same partition partially overlap (`LOGICAL_ERROR`, "Part ... intersects
previous/next part"). It throws on the **first** such pair, so a failed startup
never shows the full set, the table is not attached (absent from `system.parts`),
and there is no setting to load past it. This tool reconstructs the complete
picture by replaying the server's own classification (`MergeTreePartInfo::contains`
/ `isDisjoint`, as used by `PartLoadingTree`):

* **partial overlap** -- neither part contains the other; this is what aborts load.
* **covered layer** -- a survivor name-covers the part (normally a merge source);
  reported because a re-issued block range can make a survivor *falsely* cover a
  part that holds unique data.

The tool is strictly read-only. It never moves, attaches, or deletes anything.

## Input

Part names (or full part-directory paths), one per line on stdin -- typically the
output of `find`, `clickhouse-disks list`, or a `system.parts` query. A line may
be prefixed with `<table>\t` to group by table; otherwise the parent directory of
a path is the table, and bare names all fall under one group. Blank lines, `#`
comments, and non-part entries (e.g. `detached`) are ignored.

## Examples

```bash
# Bare names for one table (find with -printf '%f'):
find /var/lib/clickhouse/store/b11/b11e7407 -mindepth 1 -maxdepth 1 -type d -printf '%f\n' \
    | ./mergetree_part_conflicts.py

# Full paths across a whole disk, all tables at once (parent dir groups by table):
find /var/lib/clickhouse/store -mindepth 3 -maxdepth 3 -type d \
    | ./mergetree_part_conflicts.py

# From a running server (a healthy table, as a sanity check):
clickhouse-client -q "SELECT database || '.' || table || '\t' || name FROM system.parts
                      WHERE active FORMAT TSVRaw" \
    | ./mergetree_part_conflicts.py

# Machine-readable output:
find ... -type d | ./mergetree_part_conflicts.py --json

# Emit a read-only recovery script (concrete `mv` needs full paths as input):
find /var/lib/clickhouse/store/b11/b11e7407 -mindepth 1 -maxdepth 1 -type d \
    | ./mergetree_part_conflicts.py --emit-detach-commands
```

Example report:

```
=== table: /var/lib/clickhouse/store/b11/b11e7407 ===
  partition 20260722:
    PARTIAL OVERLAP (aborts load): 20260722_98_20874_190  <->  20260722_2313_113249_107  [shared blocks 2313..20874]
      suggested: keep the survivor, DETACH + REATTACH: 20260722_2313_113249_107
    covered layers (load fine; audit for a false dominator): 20260722_98_20874_50
```

Exit code is `2` when any partial overlap is found, `0` otherwise.

## Tests

```bash
python3 -m unittest -v
```
