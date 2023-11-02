#!/usr/bin/env python3

# Tests for the backupview utility.
# Use pytest ./test.py to run.

import pytest

import os.path
import sys
import tempfile
import pathlib

script_dir = os.path.dirname(os.path.realpath(__file__))
backupview_dir = os.path.abspath(os.path.join(script_dir, ".."))
if backupview_dir not in sys.path:
    sys.path.append(backupview_dir)
from backupview import open_backup, FileInfo, ExtractionInfo


def calculate_num_files_and_total_size(dir):
    count = 0
    total_size = 0
    for root, _, files in os.walk(dir, topdown=False):
        for name in files:
            if name:
                count += 1
                total_size += os.path.getsize(os.path.join(dir, root, name))
    return (count, total_size)


###########################################################################################
# Actual tests


def test_backup_1():
    with open_backup(os.path.join(script_dir, "test_backup_1.zip")) as b:
        assert b.get_subdirs("/") == ["shards"]
        assert b.dir_exists("/shards")
        assert not b.file_exists("/shards")
        assert b.get_subdirs("/shards/") == ["1"]
        assert b.get_subdirs("/shards/1/replicas/1") == ["data", "metadata"]
        assert b.get_subdirs("/shards/1/replicas/1/metadata/") == ["mydb"]
        assert b.get_files_in_dir("/shards/1/replicas/1/metadata/") == ["mydb.sql"]
        assert b.file_exists("/shards/1/replicas/1/metadata/mydb.sql")

        assert b.get_subdirs("/shards/1/replicas/1/data/mydb/tbl1") == [
            "all_0_0_0",
            "all_1_1_0",
            "all_2_2_0",
            "all_3_3_0",
        ]

        assert b.get_files_in_dir("/shards/1/replicas/1/data/mydb/tbl1/all_0_0_0") == [
            "checksums.txt",
            "columns.txt",
            "count.txt",
            "data.bin",
            "data.cmrk3",
            "default_compression_codec.txt",
            "metadata_version.txt",
            "serialization.json",
        ]

        assert b.get_databases() == ["mydb"]
        assert b.get_tables(database="mydb") == ["tbl1", "tbl2"]
        assert b.get_tables() == [("mydb", "tbl1"), ("mydb", "tbl2")]

        assert b.get_create_query(database="mydb").startswith("CREATE DATABASE mydb")

        assert b.get_create_query(table="mydb.tbl1").startswith(
            "CREATE TABLE mydb.tbl1"
        )

        assert b.get_create_query(table=("mydb", "tbl1")).startswith(
            "CREATE TABLE mydb.tbl1"
        )

        assert b.get_create_query(database="mydb", table="tbl2").startswith(
            "CREATE TABLE mydb.tbl2"
        )

        assert b.get_partitions(table="mydb.tbl2") == ["all"]
        assert b.get_parts(table="mydb.tbl2") == [
            "all_0_0_0",
            "all_1_1_0",
            "all_2_2_0",
        ]

        assert b.get_parts(table="mydb.tbl2", partition="all") == [
            "all_0_0_0",
            "all_1_1_0",
            "all_2_2_0",
        ]

        assert (
            b.get_table_data_path(table="mydb.tbl1")
            == "/shards/1/replicas/1/data/mydb/tbl1/"
        )

        assert b.get_table_data_files(table="mydb.tbl1", part="all_1_1_0") == [
            "/shards/1/replicas/1/data/mydb/tbl1/checksums.txt",
            "/shards/1/replicas/1/data/mydb/tbl1/columns.txt",
            "/shards/1/replicas/1/data/mydb/tbl1/count.txt",
            "/shards/1/replicas/1/data/mydb/tbl1/data.bin",
            "/shards/1/replicas/1/data/mydb/tbl1/data.cmrk3",
            "/shards/1/replicas/1/data/mydb/tbl1/default_compression_codec.txt",
            "/shards/1/replicas/1/data/mydb/tbl1/metadata_version.txt",
            "/shards/1/replicas/1/data/mydb/tbl1/serialization.json",
        ]

        assert (
            b.read_file(
                "/shards/1/replicas/1/data/mydb/tbl1/all_1_1_0/default_compression_codec.txt"
            )
            == b"CODEC(LZ4)"
        )

        with b.open_file(
            "/shards/1/replicas/1/data/mydb/tbl1/all_1_1_0/default_compression_codec.txt"
        ) as f:
            assert f.read() == b"CODEC(LZ4)"

        assert b.get_file_info(
            "/shards/1/replicas/1/data/mydb/tbl1/all_1_1_0/default_compression_codec.txt"
        ) == FileInfo(
            name="/shards/1/replicas/1/data/mydb/tbl1/all_1_1_0/default_compression_codec.txt",
            size=10,
            checksum="557036eda0fb0a277a7caf9b9c8d4dd6",
            data_file="/shards/1/replicas/1/data/mydb/tbl1/all_0_0_0/default_compression_codec.txt",
        )

        temp_dir = tempfile.TemporaryDirectory()
        temp_dir_path = temp_dir.name

        assert b.extract_table_data(
            table="mydb.tbl1", out=temp_dir_path
        ) == ExtractionInfo(num_files=32, num_bytes=1728)

        assert calculate_num_files_and_total_size(temp_dir_path) == (32, 1728)
        temp_dir.cleanup()
