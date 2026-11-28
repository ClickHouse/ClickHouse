#!/usr/bin/env python3
"""Regenerates union_same_name_record_branches.avro: a union of two records with the same simple name `R`
in different namespaces, so the flattened path `u.R.x` exists in both branches.

Requires the `avro` python package (tested with 1.12.1): pip install avro
"""
import json

import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter


class BranchPickingWriter(DatumWriter):
    # `DatumWriter.write` validates the whole datum first and raises on the first union branch that does not match it;
    # `write_data` picks the first branch the datum validates against.
    def write(self, datum, encoder):
        self.write_data(self.writers_schema, datum, encoder)


schema = avro.schema.parse(json.dumps({
    "type": "record", "name": "row", "fields": [
        {"name": "id", "type": "int"},
        {"name": "u", "type": [
            "null",
            {"type": "record", "name": "R", "namespace": "second",
             "fields": [{"name": "x", "type": "int"}, {"name": "y", "type": "string"}]},
            {"type": "record", "name": "R", "namespace": "first",
             "fields": [{"name": "x", "type": "int"}]}]},
    ]}))

with open("union_same_name_record_branches.avro", "wb") as f:
    writer = DataFileWriter(f, BranchPickingWriter(), schema, codec="null")
    writer.append({"id": 1, "u": {"x": 1}})            # first.R
    writer.append({"id": 2, "u": {"x": 2, "y": "b"}})  # second.R
    writer.append({"id": 3, "u": None})
    writer.close()
