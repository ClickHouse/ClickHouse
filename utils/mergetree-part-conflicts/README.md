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
* **cross-disk covered** -- a covered part whose surviving coverer is on a *different*
  disk. The table loads, but CH silently retires the covered part on startup; if that
  coverage is false (a re-issued block range) it is silent data loss, so the tool
  detaches it to preserve it. Needs part paths to tell the disk.
* **covered layer** -- a covered part on the *same* disk as its coverer: an ordinary
  not-yet-cleaned merge source, cleaned normally, left alone.

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

# One table whose parts are spread across several disks (tiered storage). Label every
# line with the same table name so parts from all disks are analysed together -- a part
# on one disk can intersect a part on another. Full paths are kept, so
# --emit-detach-commands moves each part into detached/ on its own disk.
for disk in /var/lib/clickhouse /mnt/ngx2/clickhouse; do
    find "$disk/store/b11/b11e7407" -mindepth 1 -maxdepth 1 -type d -printf 'nat\t%p\n'
done | ./mergetree_part_conflicts.py

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
