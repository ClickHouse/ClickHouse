#!/usr/bin/env python3
"""Regenerates map_as_array_of_records.avro — an Avro file whose fields use the
array-of-{key,value}-record encoding that Iceberg / Spark emit for maps with a
non-string key (Avro native maps only allow string keys). See issue #109282.

Requires the `avro` python package:  pip install avro
"""
import json
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

schema = avro.schema.parse(json.dumps({
    "type": "record", "name": "row", "fields": [
        # Int32->Int32 map, Iceberg-style, carrying the logicalType:"map" hint.
        {"name": "m_int", "type": {
            "type": "array", "logicalType": "map",
            "items": {"type": "record", "name": "k_v_int", "fields": [
                {"name": "key", "type": "int"},
                {"name": "value", "type": "int"}]}}},
        # Same array-of-record shape WITHOUT the logicalType hint, and with the
        # value field declared before the key field (routing must be by name).
        {"name": "m_swapped", "type": {
            "type": "array",
            "items": {"type": "record", "name": "v_k", "fields": [
                {"name": "value", "type": "long"},
                {"name": "key", "type": "string"}]}}},
        # Nullable value inside the map value record.
        {"name": "m_nullable_val", "type": {
            "type": "array", "logicalType": "map",
            "items": {"type": "record", "name": "k_v_nul", "fields": [
                {"name": "key", "type": "int"},
                {"name": "value", "type": ["null", "long"]}]}}},
        # Only ONE canonical name ("value") present: routing must stay positional
        # (field 0 = key, field 1 = value), NOT flip because "value" is field 0.
        {"name": "m_one_name_value", "type": {
            "type": "array",
            "items": {"type": "record", "name": "value_extra", "fields": [
                {"name": "value", "type": "int"},
                {"name": "extra", "type": "int"}]}}},
        # Only ONE canonical name ("key") present, as field 1: routing must stay
        # positional (field 0 = key, field 1 = value), NOT flip to make field 1 the key.
        {"name": "m_one_name_key", "type": {
            "type": "array",
            "items": {"type": "record", "name": "extra_key", "fields": [
                {"name": "extra", "type": "int"},
                {"name": "key", "type": "int"}]}}},
    ]}))

with DataFileWriter(open("map_as_array_of_records.avro", "wb"), DatumWriter(), schema) as w:
    w.append({
        "m_int": [{"key": 1, "value": 42}],
        "m_swapped": [{"value": 100, "key": "a"}],
        "m_nullable_val": [{"key": 1, "value": 10}],
        "m_one_name_value": [{"value": 10, "extra": 20}],
        "m_one_name_key": [{"extra": 30, "key": 40}],
    })
    w.append({
        "m_int": [{"key": 2, "value": 7}, {"key": 3, "value": 8}],
        "m_swapped": [{"value": 200, "key": "b"}, {"value": 300, "key": "c"}],
        "m_nullable_val": [{"key": 2, "value": None}, {"key": 3, "value": 30}],
        "m_one_name_value": [{"value": 11, "extra": 21}, {"value": 12, "extra": 22}],
        "m_one_name_key": [{"extra": 31, "key": 41}, {"extra": 32, "key": 42}],
    })
    w.append({
        "m_int": [], "m_swapped": [], "m_nullable_val": [],
        "m_one_name_value": [], "m_one_name_key": [],
    })
print("wrote map_as_array_of_records.avro")
