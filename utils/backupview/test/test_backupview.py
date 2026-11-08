#!/usr/bin/env python3

# Tests for the clickhouse_backupview utility.
# Use pytest ./test_backupview.py to run.

import os.path
import sys
import tempfile

import pytest

script_dir = os.path.dirname(os.path.realpath(__file__))
backupview_dir = os.path.abspath(os.path.join(script_dir, ".."))
if backupview_dir not in sys.path:
    sys.path.append(backupview_dir)
from clickhouse_backupview import S3, FileInfo, open_backup


def calculate_num_files(dir):
    count = 0
    for _, _, files in os.walk(dir, topdown=False):
        count += len([1 for name in files if name])
    return count


def calculate_total_size(dir):
    total_size = 0
    for root, _, files in os.walk(dir, topdown=False):
        total_size += sum(
            [os.path.getsize(os.path.join(dir, root, name)) for name in files if name]
        )
    return total_size


###########################################################################################
# Actual tests


def test_backupview_1():
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

        with tempfile.TemporaryDirectory() as temp_dir:
            res = b.extract_table_data(table="mydb.tbl1", out=temp_dir)
            num_files = res.num_files
            num_bytes = res.num_bytes
            assert calculate_num_files(temp_dir) == num_files
            assert calculate_total_size(temp_dir) == num_bytes
            assert num_files == 32
            assert num_bytes == 1728
