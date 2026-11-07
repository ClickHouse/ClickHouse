#!/usr/bin/env python3
"""Wraps every case of `table-spec/**/cases.json` into a minimal Iceberg table directory
`<out>/<group>__<id>/metadata/v1.metadata.json` without snapshots, and prints `<group>__<id> <valid>` per case.

The cases of `schema/*` are schemas, they become the table schema. The cases of `types/*` are types,
they become the type of an optional column `c` with field id 1000 (the ids inside the cases are smaller).
"""
import glob
import json
import os
import sys

src, out = sys.argv[1], sys.argv[2]


def metadata(schema):
    return {
        "format-version": 2,
        "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
        "location": "file:///unused",
        "last-sequence-number": 0,
        "last-updated-ms": 1,
        "last-column-id": 1000,
        "current-schema-id": schema.get("schema-id", 0),
        "schemas": [schema],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 999,
        "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "properties": {},
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": [],
    }


for path in sorted(glob.glob(os.path.join(src, "table-spec", "*", "*", "cases.json"))):
    kind, group = os.path.relpath(os.path.dirname(path), os.path.join(src, "table-spec")).split(os.sep)
    with open(path) as f:
        cases = json.load(f)["cases"]
    for case in cases:
        if kind == "schema":
            schema = dict(case["input"])
            schema.setdefault("schema-id", 0)
        else:
            schema = {"type": "struct", "schema-id": 0, "fields": [{"id": 1000, "name": "c", "required": False, "type": case["input"]}]}
        name = "%s_%s__%s" % (kind, group, case["id"])
        os.makedirs(os.path.join(out, name, "metadata"))
        with open(os.path.join(out, name, "metadata", "v1.metadata.json"), "w") as f:
            json.dump(metadata(schema), f)
        print(name, "valid" if case["valid"] else "invalid")
