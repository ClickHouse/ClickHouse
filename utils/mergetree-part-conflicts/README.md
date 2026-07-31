# mergetree-part-conflicts

Offline detector of intersecting / covered `MergeTree` data parts, working from
part-directory names alone -- no running server.

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
* **same part on more than one disk** -- an interrupted move left a stale copy.

The tool is strictly read-only. It never moves, attaches, or deletes anything.

## Usage

```bash
# Scan one or more data roots / disk mount points (auto-discovers table dirs by
# their format_version.txt; handles Atomic store/<prefix>/<uuid>/ and Ordinary
# data/<db>/<table>/ layouts, and tiered disks when several roots are given):
./mergetree_part_conflicts.py /var/lib/clickhouse /mnt/ngx2/clickhouse

# Feed part names directly (e.g. from clickhouse-disks list, find, or system.parts):
find /var/lib/clickhouse/store -mindepth 3 -maxdepth 3 -type d -printf '%f\n' \
    | ./mergetree_part_conflicts.py --stdin

# Machine-readable output:
./mergetree_part_conflicts.py --json /var/lib/clickhouse
```

Exit code is `2` when any partial overlap is found, `0` otherwise.

## Tests

```bash
python3 -m unittest -v
```
