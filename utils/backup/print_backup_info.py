#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
print_backup_info: Extract information about a backup from ".backup" file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Usage: print_backup_info <path-to-backup-xml>
"""
import sys
import os
import xml.etree.ElementTree as ET


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(1)
    backup_xml = sys.argv[1]

    if not os.path.isfile(backup_xml):
        print("error: {} does not exist".format(backup_xml))
        sys.exit(1)

    # Process the file line-by-line
    tree = ET.parse(backup_xml)
    root = tree.getroot()
    contents = root.find("contents")

    version_node = root.find("version")
    version = int(version_node.text) if (version_node != None) else None

    timestamp_node = root.find("timestamp")
    timestamp = timestamp_node.text if (timestamp_node != None) else None

    base_backup_node = root.find("base_backup")
    base_backup = base_backup_node.text if (base_backup_node != None) else None

    number_of_files = 0
    size_of_files = 0
    number_of_files_from_base_backup = 0
    size_of_files_from_base_backup = 0
    databases = set()
    tables = {}

    for file in contents:
        name = file.find("name").text
        size = int(file.find("size").text)

        use_base_node = file.find("use_base")
        use_base = (use_base_node.text == "true") if (use_base_node != None) else False

        if use_base:
            base_size_node = file.find("base_size")
            base_size = int(base_size_node.text) if (base_size_node != None) else size
        else:
            base_size = 0

        data_file_node = file.find("data_file")
        data_file = data_file_node.text if (data_file_node != None) else name

        has_data_file = name == data_file

        if has_data_file:
            if size > base_size:
                number_of_files += 1
                size_of_files += size - base_size
            if base_size > 0:
                number_of_files_from_base_backup += 1
                size_of_files_from_base_backup += base_size

        table_name = extract_table_name_from_path(name)
        if table_name:
            if table_name not in tables:
                tables[table_name] = [0, 0, 0, 0]
            if not name.endswith(".sql") and has_data_file:
                table_info = tables[table_name]
                if size > base_size:
                    table_info[0] += 1
                    table_info[1] += size - base_size
                if base_size > 0:
                    table_info[2] += 1
                    table_info[3] += base_size
                tables[table_name] = table_info

        database_name = extract_database_name_from_path(name)
        if database_name:
            databases.add(database_name)

    size_of_backup = size_of_files + os.path.getsize(backup_xml)

    print(f"version={version}")
    print(f"timestamp={timestamp}")
    print(f"base_backup={base_backup}")
    print(f"size_of_backup={size_of_backup}")
    print(f"number_of_files={number_of_files}")
    print(f"size_of_files={size_of_files}")
    print(f"number_of_files_from_base_backup={number_of_files_from_base_backup}")
    print(f"size_of_files_from_base_backup={size_of_files_from_base_backup}")
    print(f"number_of_databases={len(databases)}")
    print(f"number_of_tables={len(tables)}")

    print()

    print(f"{len(databases)} database(s):")
    for database_name in sorted(databases):
        print(database_name)

    print()

    print(f"{len(tables)} table(s):")
    table_info_format = "{:>70} | {:>20} | {:>20} | {:>26} | {:>30}"
    table_info_separator_line = (
        "{:->70}-+-{:->20}-+-{:->20}-+-{:->26}-+-{:->30}".format("", "", "", "", "")
    )
    table_info_title_line = table_info_format.format(
        "table name",
        "num_files",
        "size_of_files",
        "num_files_from_base_backup",
        "size_of_files_from_base_backup",
    )
    print(table_info_title_line)
    print(table_info_separator_line)
    for table_name in sorted(tables):
        table_info = tables[table_name]
        print(
            table_info_format.format(
                table_name, table_info[0], table_info[1], table_info[2], table_info[3]
            )
        )


# Extracts a table name from a path inside a backup.
# For example, extracts 'default.tbl' from 'shards/1/replicas/1/data/default/tbl/all_0_0_0/data.bin'.
def extract_table_name_from_path(path):
    path = strip_shards_replicas_from_path(path)
    if not path:
        return None
    if path.startswith("metadata/"):
        path = path[len("metadata/") :]
        sep = path.find("/")
        if sep == -1:
            return None
        database_name = path[:sep]
        path = path[sep + 1 :]
        sep = path.find(".sql")
        if sep == -1:
            return None
        table_name = path[:sep]
        return database_name + "." + table_name
    if path.startswith("data/"):
        path = path[len("data/") :]
        sep = path.find("/")
        if sep == -1:
            return None
        database_name = path[:sep]
        path = path[sep + 1 :]
        sep = path.find("/")
        if sep == -1:
            return None
        table_name = path[:sep]
        return database_name + "." + table_name
    return None


# Extracts a database name from a path inside a backup.
# For example, extracts 'default' from 'shards/1/replicas/1/data/default/tbl/all_0_0_0/data.bin'.
def extract_database_name_from_path(path):
    path = strip_shards_replicas_from_path(path)
    if not path:
        return None
    if path.startswith("metadata/"):
        path = path[len("metadata/") :]
        sep = path.find(".sql")
        if sep == -1 or path.find("/") != -1:
            return None
        return path[:sep]
    if path.startswith("data/"):
        path = path[len("data/") :]
        sep = path.find("/")
        if sep == -1:
            return None
        return path[:sep]
    return None


# Removes a prefix "shards/<number>/replicas/<number>/" from a path.
def strip_shards_replicas_from_path(path):
    if path.startswith("shards"):
        sep = path.find("/")
        if sep == -1:
            return None
        sep = path.find("/", sep + 1)
        if sep == -1:
            return None
        path = path[sep + 1 :]
    if path.startswith("replicas"):
        sep = path.find("/")
        if sep == -1:
            return None
        sep = path.find("/", sep + 1)
        if sep == -1:
            return None
        path = path[sep + 1 :]
    return path


if __name__ == "__main__":
    main()
