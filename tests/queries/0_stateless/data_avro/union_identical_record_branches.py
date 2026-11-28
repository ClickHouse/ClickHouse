#!/usr/bin/env python3
"""Regenerates union_identical_record_branches.avro — unions whose named branches
(records, enums) have identical structure, so they map to the same ClickHouse type
and the inferred Variant has fewer types than the union has branches.

Requires the `fastavro` python package:  pip install fastavro
(the `avro` package cannot pick between structurally identical union branches).
"""
from fastavro import parse_schema, writer

record_fields = [{"name": "x", "type": "int"}, {"name": "s", "type": "string"}]
enum_symbols = ["a", "b"]

schema = parse_schema({
    "type": "record", "name": "row", "fields": [
        {"name": "id", "type": "int"},
        # Accepted and Rejected are distinct records with the same fields.
        {"name": "payload", "type": [
            "null",
            {"type": "record", "name": "Accepted", "fields": record_fields},
            {"type": "record", "name": "Rejected", "fields": record_fields},
            {"type": "record", "name": "Other", "fields": [{"name": "y", "type": "double"}]}]},
        # Same for two enums with the same symbols.
        {"name": "status", "type": [
            {"type": "enum", "name": "Side", "symbols": enum_symbols},
            {"type": "enum", "name": "Venue", "symbols": enum_symbols},
            {"type": "array", "items": "int"}]},
    ]})

# (branch name, value) tuples select the union branch explicitly.
rows = [
    {"id": 1, "payload": ("Accepted", {"x": 1, "s": "accepted"}), "status": ("Side", "a")},
    {"id": 2, "payload": ("Rejected", {"x": 2, "s": "rejected"}), "status": ("Venue", "b")},
    {"id": 3, "payload": ("Other", {"y": 3.5}), "status": [1, 2, 3]},
    {"id": 4, "payload": None, "status": ("Venue", "a")},
]

with open("union_identical_record_branches.avro", "wb") as f:
    writer(f, schema, rows, codec="null")
