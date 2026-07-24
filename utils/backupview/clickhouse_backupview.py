#!/usr/bin/env python3

import bisect
import os.path
import shutil
import xml.etree.ElementTree as ET
import zipfile  # For reading backups from zip archives
from urllib.parse import urlparse

import boto3  # For reading backups from S3
import botocore

## Examples:
## from backupview import open_backup
##
## Get information about the backup's contents:
## backup = open_backup("/path/to/backup/")
## print(backup.get_databases()))
## for database in backup.get_databases():
##     print(backup.get_create_query(database=database))
##     for table in backup.get_tables(database=database):
##         print(backup.get_create_query(database=database, table=table))
##         print(backup.get_partitions(database=database, table=table))
##         print(backup.get_parts(database=database, table=table))
##
## Extract everything from the backup to a folder:
## backup.extract_all(out="/where/to/extract/1/")
##
## Extract the data of a single table:
## backup.extract_table_data(database="mydb", table="mytable", out="/where/to/extract/2/")
## backup.extract_table_data(table="mydb.mytable", part="all_1_1", out="/where/to/extract/3/")
## backup.extract_table_data(database="mydb", table="mytable", partition="2022", out="/where/to/extract/4/")
## backup.extract_table_metadata(table=('mydb', 'mytable'), out="/where/to/extract/5.sql")
##
## Get a list of all files in the backup:
## print(backup.get_files())
##
## Get information about files in the backup:
## print(backup.get_file_infos())
##
## Extract files to a folder:
## backup.extract_dir("/shards/1/replicas/1/", out="/where/to/extract/6/")
## backup.extract_file("/shards/1/replicas/1/metadata/mydb/mytable.sql", out="/where/to/extract/7.sql")
##
## Reading from S3:
## backup = open_backup(S3("uri", "access_key_id", "secret_access_key"))
## backup.extract_table_data(table="mydb.mytable", partition="2022", out="/where/to/extract/8/")


# Opens a backup for viewing.
def open_backup(backup_name, base_backup=None):
    return Backup(backup_name, base_backup=base_backup)


# Main class, an instance of Backup is returned by the open_backup() function.
class Backup:
    def __init__(self, backup_name, base_backup=None):
        self.__location = None
        self.__close_base_backup = False
        self.__base_backup = base_backup
        self.__reader = None

        try:
            self.__location = Location(backup_name)
            if TypeChecks.is_location_like(base_backup):
                self.__base_backup = Location(base_backup)
            self.__reader = self.__location.create_reader()
            self.__parse_backup_metadata()
        except:
            self.close()
            raise

    def close(self):
        if self.__reader is not None:
            self.__reader.close()
            self.__reader = None
        if (
            (self.__base_backup is not None)
            and (not TypeChecks.is_location_like(self.__base_backup))
            and self.__close_base_backup
        ):
            self.__base_backup.close()
            self.__base_backup = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    # Get general information about the backup.

    # Returns the name of the backup, e.g. File('/path/to/backup/')
    def get_name(self):
        return str(self.get_location())

    def get_location(self):
        return self.__location

    def __repr__(self):
        return "Backup(" + repr(self.get_location()) + ")"

    # Returns the base backup or None if there is no base backup.
    def get_base_backup(self):
        if TypeChecks.is_location_like(self.__base_backup):
            self.__close_base_backup = True
            self.__base_backup = open_backup(self.__base_backup)
        return self.__base_backup

    def get_base_backup_location(self):
        if self.__base_backup is None:
            return None
        if TypeChecks.is_location_like(self.__base_backup):
            return self.__base_backup
        return self.__base_backup.get_location()

    def get_base_backup_name(self):
        if self.__base_backup is None:
            return None
        return str(self.get_base_backup_location())

    # Returns the version of the backup.
    def get_version(self):
        return self.__version

    # Returns the timestamp of the backup.
    def get_timestamp(self):
        return self.__timestamp

    # Get high-level information about the contents of the backup.

    # Returns shards stored in the backup.
    def get_shards(self):
        if self.dir_exists("/shards/"):
            return self.get_subdirs("/shards/")
        return ["1"]

    # Returns replicas stored in the backup.
    def get_replicas(self, shard="1"):
        if self.dir_exists(f"/shards/{shard}/replicas/"):
            return self.get_subdirs(f"/shards/{shard}/replicas/")
        elif self.dir_exists("/replicas/"):
            return self.get_subdirs("/replicas/")
        else:
            return ["1"]

    # Returns databases stored in the backup.
    def get_databases(self, shard="1", replica="1"):
        res = []
        for path in self.__get_paths_in_backup(shard=shard, replica=replica):
            dir = path + "metadata/"
            if self.dir_exists(dir):
                files = self.get_files_in_dir(dir)
                subdirs = self.get_subdirs(dir)
                res += [Backup.__unescape_for_filename(name) for name in subdirs]
                res += [
                    Backup.__unescape_for_filename(os.path.splitext(name)[0])
                    for name in files
                    if name.endswith(".sql")
                ]
        return sorted(set(res))

    # Returns tables stored in the backup.
    # b.get_tables(database='mydb') returns the names of tables in that database 'mydb';
    # b.get_tables() returns a list of tuples (db, table) for all tables in the backup.
    def get_tables(self, database=None, shard="1", replica="1"):
        if database is None:
            databases = self.get_databases(shard=shard, replica=replica)
        else:
            databases = [database]
        res = []
        paths = self.__get_paths_in_backup(shard=shard, replica=replica)
        for path in paths:
            if self.dir_exists(f"{path}metadata/"):
                for db in databases:
                    dir = path + "metadata/" + Backup.__escape_for_filename(db) + "/"
                    if self.dir_exists(dir):
                        files = self.get_files_in_dir(dir)
                        tables = [
                            Backup.__unescape_for_filename(os.path.splitext(name)[0])
                            for name in files
                            if name.endswith(".sql")
                        ]
                        if database is None:
                            tables = [(db, table) for table in tables]
                        res += tables
        return sorted(set(res))

    # Returns the create query of a table or a database.
    # The function can return None if there is no create query in the backup for such table or database.
    # b.get_create_query(database='mydb') returns the create query of the database `mydb`;
    # b.get_create_query(database='mydb', table='mytable') returns the create query of the table `mydb`.`mytable`;
    # b.get_create_query(table='mydb.mytable') and b.get_create_query(table=('mydb', 'mytable')) also returns the create query of the table `mydb`.`mytable`.
    def get_create_query(self, table=None, database=None, shard="1", replica="1"):
        path = self.get_create_query_path(
            table=table, database=database, shard=shard, replica=replica
        )
        if path is None:
            return None
        return self.read_file(path).decode("utf-8")

    def get_table_metadata(self, table, database=None, shard="1", replica="1"):
        return self.get_create_query(
            table=table, database=database, shard=shard, replica=replica
        )

    def get_database_metadata(self, database, shard="1", replica="1"):
        return self.get_create_query(database=database, shard=shard, replica=replica)

    # Like get_create_query(), but returns the path to the corresponding file containing the create query in the backup.
    def get_create_query_path(self, table=None, database=None, shard="1", replica="1"):
        if database is None:
            database, table = Backup.__split_database_table(table)
        if table is None:
            suffix = "metadata/" + Backup.__escape_for_filename(database) + ".sql"
        else:
            suffix = (
                "metadata/"
                + Backup.__escape_for_filename(database)
                + "/"
                + Backup.__escape_for_filename(table)
                + ".sql"
            )
        for path in self.__get_paths_in_backup(shard=shard, replica=replica):
            metadata_path = path + suffix
            if self.file_exists(metadata_path):
                return metadata_path
        return None

    def get_table_metadata_path(self, table, database=None, shard="1", replica="1"):
        return self.get_create_query_path(
            table=table, database=database, shard=shard, replica=replica
        )

    def get_database_metadata_path(self, database, shard="1", replica="1"):
        return self.get_create_query_path(
            database=database, shard=shard, replica=replica
        )

    # Returns the names of parts of a specified table.
    # If the 'partition' parameter is specified, the function returns only parts related to that partition.
    # The table can be specified either as b.get_parts(database='mydb', table='mytable') or
    # b.get_parts(table='mydb.mytable') or b.get_parts(table=('mydb', 'mytable')).
    def get_parts(self, table, database=None, partition=None, shard="1", replica="1"):
        data_path = self.get_table_data_path(
            table=table, database=database, shard=shard, replica=replica
        )
        if data_path is None:
            return []
        part_names = self.get_subdirs(data_path)
        if "mutations" in part_names:
            part_names.remove("mutations")
        if partition is not None:
            part_names = [
                part_name
                for part_name in part_names
                if Backup.__extract_partition_id_from_part_name(part_name) == partition
            ]
        return part_names

    # Returns the names of partitions of a specified table.
    # The table can be specified either as b.get_partitions(database='mydb', table='mytable') or
    # b.get_partitions(table='mydb.mytable') or b.get_partitions(table=('mydb', 'mytable'))
    def get_partitions(self, table, database=None, shard="1", replica="1"):
        parts = self.get_parts(
            table=table, database=database, shard=shard, replica=replica
        )
        partitions = []
        prev_partition = None
        for part in parts:
            partition = Backup.__extract_partition_id_from_part_name(part)
            if partition != prev_partition:
                partitions.append(partition)
                prev_partition = partition
        return partitions

    # Returns the path to the 'data' folder of a specified table in the backup.
    # The function can return None if there is no such folder in the backup.
    # The table can be specified either as b.get_table_data_path(database='mydb', table='mytable')
    # b.get_table_data_path(table='mydb.mytable') or b.get_table_data_path(table=('mydb', 'mytable'))
    def get_table_data_path(self, table, database=None, shard="1", replica="1"):
        if database is None:
            database, table = Backup.__split_database_table(table)
        suffix = (
            "metadata/"
            + Backup.__escape_for_filename(database)
            + "/"
            + Backup.__escape_for_filename(table)
            + ".sql"
        )
        for path in self.__get_paths_in_backup(shard=shard, replica=replica):
            if self.file_exists(path + suffix):
                data_path = (
                    path
                    + "data/"
                    + Backup.__escape_for_filename(database)
                    + "/"
                    + Backup.__escape_for_filename(table)
                    + "/"
                )
                return data_path if self.dir_exists(data_path) else None
        return None

    # Returns the paths to files in the 'data' folder of a specified table in the backup.
    # If any of the parameters 'part' and 'partition' is specified the function returns only the files related to that part or partition.
    # The table can be specified either as b.get_table_data_files(database='mydb', table='mytable')
    # b.get_table_data_files(table='mydb.mytable') or b.get_table_data_files(table=('mydb', 'mytable'))
    def get_table_data_files(
        self, table, database=None, part=None, partition=None, shard="1", replica="1"
    ):
        data_path = self.get_table_data_path(
            table=table, database=database, shard=shard, replica=replica
        )
        if data_path is None:
            return []
        if (part is not None) and (partition is not None):
            raise Exception(
                "get_table_data_files: `only_part` and `only_partition` cannot be set together"
            )
        files = []
        if part is not None:
            files = self.get_files_in_dir(os.path.join(data_path, part), recursive=True)
        elif partition is not None:
            for part in self.get_parts(
                table=table,
                database=database,
                partition=partition,
                shard=shard,
                replica=replica,
            ):
                files += self.get_files_in_dir(
                    os.path.join(data_path, part), recursive=True
                )
        else:
            files = self.get_files_in_dir(data_path, recursive=True)
        return [data_path + file for file in files]

    # Extracts the create query of a table or a database to a specified destination.
    # The function returns a tuple (files_extracted, bytes_extracted).
    # The function does nothing if there is no create query for such table or database in the backup.
    def extract_create_query(
        self, table=None, database=None, shard="1", replica="1", out=None, out_path=""
    ):
        file = self.get_create_query_path(
            table=table, database=database, shard=shard, replica=replica
        )
        if file is None:
            return (0, 0)
        return self.extract_file(path=file, out=out, out_path=out_path)

    def extract_table_metadata(
        self, table, database=None, shard="1", replica="1", out=None, out_path=""
    ):
        return self.extract_create_query(
            table=table,
            database=database,
            shard=shard,
            replica=replica,
            out=out,
            out_path=out_path,
        )

    def extract_database_metadata(
        self, database, shard="1", replica="1", out=None, out_path=""
    ):
        return self.extract_create_query(
            database=database, shard=shard, replica=replica, out=out, out_path=out_path
        )

    # Extracts the data of a table or a database to a specified destination.
    # The function returns a tuple (files_extracted, bytes_extracted).
    # The function does nothing if there is no data for such table in the backup.
    def extract_table_data(
        self,
        table,
        database=None,
        part=None,
        partition=None,
        shard="1",
        replica="1",
        out=None,
        out_path="",
    ):
        files = self.get_table_data_files(
            table=table,
            database=database,
            part=part,
            partition=partition,
            shard=shard,
            replica=replica,
        )
        data_path = self.get_table_data_path(
            table=table, database=database, shard=shard, replica=replica
        )
        return self.extract_files(
            path=data_path,
            files=Backup.__remove_prefix_path(files, data_path),
            out=out,
            out_path=out_path,
        )

    # Get low-level information about files in the backup.

    # Returns a list of all files in the backup.
    def get_files(self):
        return self.get_files_in_dir(path="/", recursive=True)

    # Returns True if a specified file exists in the backup.
    def file_exists(self, path):
        if not path.startswith("/"):
            path = "/" + path
        return path in self.__file_infos

    # Returns True if a specified folder exists in the backup.
    def dir_exists(self, path):
        if not path.startswith("/"):
            path = "/" + path
        if not path.endswith("/"):
            path += "/"
        if path == "/":
            return True
        pos = bisect.bisect_left(self.__file_paths, path)
        return (pos < len(self.__file_paths)) and self.__file_paths[pos].startswith(
            path
        )

    # Returns the size of a file in the backup.
    # The function raises an exception of the file doesn't exist.
    def get_file_size(self, path):
        fi = self.get_file_info(path)
        return fi.size

    # Returns the information about a file in the backup.
    # The function raises an exception of the file doesn't exist.
    def get_file_info(self, path):
        if not path.startswith("/"):
            path = "/" + path
        fi = self.__file_infos.get(path)
        if fi is None:
            raise Exception(f"File {path} not found in backup {self}")
        return fi

    # Returns the information about multiple or all files files in the backup.
    def get_file_infos(self, paths=None):
        if paths is None:
            return self.__file_infos.values()
        return [self.get_file_info(path) for path in paths]

    # Finds the information about a file in the backup by its checksum.
    # The function raises an exception of the file doesn't exist.
    def get_file_info_by_checksum(self, checksum):
        fi = self.__file_infos_by_checksum.get(checksum)
        if fi is None:
            raise Exception(f"File with checksum={checksum} not found in backup {self}")
        return fi

    # Returns all files in a directory inside the backup.
    def get_files_in_dir(self, path, recursive=False):
        if not path.startswith("/"):
            path = "/" + path
        if not path.endswith("/"):
            path += "/"
        if path == "/" and recursive:
            return self.__file_paths
        pos = bisect.bisect_left(self.__file_paths, path)
        files = []
        while pos < len(self.__file_paths):
            file = self.__file_paths[pos]
            if not file.startswith(path):
                break
            file = file[len(path) :]
            if recursive or (file.find("/") == -1):
                files.append(file)
            pos += 1
        return files

    # Returns all subdirectories in a directory inside the backup.
    def get_subdirs(self, path):
        if not path.startswith("/"):
            path = "/" + path
        if not path.endswith("/"):
            path += "/"
        pos = bisect.bisect_left(self.__file_paths, path)
        subdirs = []
        prev_subdir = ""
        while pos < len(self.__file_paths):
            file = self.__file_paths[pos]
            if not file.startswith(path):
                break
            file = file[len(path) :]
            sep = file.find("/")
            if sep != -1:
                subdir = file[:sep]
                if subdir != prev_subdir:
                    subdirs.append(subdir)
                    prev_subdir = subdir
            pos += 1
        return subdirs

    # Opens a file for reading from the backup.
    def open_file(self, path):
        fi = self.get_file_info(path)
        if fi.size == 0:
            return EmptyFileObj()
        elif fi.base_size == 0:
            return self.__reader.open_file(fi.data_file)
        elif fi.size == fi.base_size:
            base_fi = self.get_base_backup().get_file_info_by_checksum(fi.base_checksum)
            return self.get_base_backup().open_file(base_fi.name)
        else:
            base_fi = self.get_base_backup().get_file_info_by_checksum(fi.base_checksum)
            base_stream = self.get_base_backup().open_file(base_fi.name)
            stream = self.__reader.open_file(fi.data_file)
            return ConcatFileObj(base_stream, stream)

    # Reads a file and returns its contents.
    def read_file(self, path):
        fi = self.get_file_info(path)
        if fi.size == 0:
            return b""
        elif fi.base_size == 0:
            return self.__reader.read_file(fi.data_file)
        elif fi.size == fi.base_size:
            base_fi = self.get_base_backup().get_file_info_by_checksum(fi.base_checksum)
            return self.get_base_backup().read_file(base_fi.name)
        else:
            base_fi = self.get_base_backup().get_file_info_by_checksum(fi.base_checksum)
            return self.get_base_backup().read_file(
                base_fi.name
            ) + self.__reader.read_file(fi.data_file)

    # Extracts a file from the backup to a specified destination.
    def extract_file(self, path, out=None, out_path="", make_dirs=True):
        if (out is None) and (len(out_path) > 0):
            return self.extract_file(path, out=out_path, make_dirs=make_dirs)

        if TypeChecks.is_file_opened_for_writing(out):
            ostream = out
            fi = self.get_file_info(path)
            with self.open_file(path) as istream:
                shutil.copyfileobj(istream, ostream)
            return ExtractionInfo(num_files=1, num_bytes=fi.size)

        if TypeChecks.is_location_like(out):
            with Location(out).create_writer() as writer:
                return self.extract_file(
                    path, out=writer, out_path=out_path, make_dirs=make_dirs
                )

        TypeChecks.check_is_writer(out)
        writer = out

        fi = self.get_file_info(path)

        if make_dirs:
            sep = out_path.rfind("/")
            if sep != -1:
                subdir = out_path[: sep + 1]
                writer.make_dirs(subdir)

        if fi.size == 0:
            writer.create_empty_file(out_path)
        elif fi.base_size == 0:
            self.__reader.extract_file(fi.data_file, writer=writer, out_path=out_path)
        elif fi.size == fi.base_size:
            base_fi = self.get_base_backup().get_file_info_by_checksum(fi.base_checksum)
            self.get_base_backup().extract_file(
                path=base_fi.name, out=writer, out_path=out_path
            )
        else:
            with self.open_file(path) as istream:
                with writer.open_file(out_path) as ostream:
                    shutil.copyfileobj(istream, ostream)

        return ExtractionInfo(num_files=1, num_bytes=fi.size)

    # Extracts multiple files from the backup to a specified destination.
    def extract_files(self, path, files, out=None, out_path=""):
        if (out is None) and (len(out_path) > 0):
            return self.extract_files(path, files, out=out_path)

        if TypeChecks.is_location_like(out):
            with Location(out).create_writer() as writer:
                return self.extract_files(path, files, out=writer, out_path=out_path)

        TypeChecks.check_is_writer(out)
        writer = out

        subdirs = set()
        for file in files:
            sep = file.rfind("/")
            if sep != -1:
                subdirs.add(file[: sep + 1])
        for subdir in subdirs:
            writer.make_dirs(os.path.join(out_path, subdir))

        extracted_files_info = ExtractionInfo()
        for file in files:
            extracted_files_info.add(
                self.extract_file(
                    os.path.join(path, file),
                    out=writer,
                    out_path=os.path.join(out_path, file),
                    make_dirs=False,
                )
            )

        return extracted_files_info

    def extract_dir(self, path, out=None, out_path=""):
        files = self.get_files_in_dir(path, recursive=True)
        return self.extract_files(path=path, files=files, out=out, out_path=out_path)

    def extract_all(self, out=None, out_path=""):
        return self.extract_dir("/", out=out, out_path=out_path)

    # Checks that all files in the backup exist and have the expected sizes.
    def check_files(self):
        data_files = {}
        for fi in self.__file_infos.values():
            if fi.size > fi.base_size:
                data_file = fi.data_file
                if data_file in data_files:
                    prev_fi = data_files[data_file]
                    if (
                        (fi.size != prev_fi.size)
                        or (fi.checksum != prev_fi.checksum)
                        or (fi.use_base != prev_fi.use_base)
                        or (fi.base_size != prev_fi.base_size)
                        or (fi.base_checksum != prev_fi.base_checksum)
                        or (fi.encrypted_by_disk != prev_fi.encrypted_by_disk)
                    ):
                        raise Exception(
                            f"Files {prev_fi.name} and {fi.name} uses the same data file but their file infos are different: {prev_fi} and {fi}, backup: {self}"
                        )
                else:
                    data_files[data_file] = fi

            if fi.base_size > 0:
                fi_base = self.get_base_backup().get_file_info_by_checksum(
                    fi.base_checksum
                )
                if fi.base_size != fi_base.size:
                    raise Exception(
                        f"Size of file {fi_base.name} in the base backup is different ({fi.base_size} != {fi_base.size}) "
                        f"from it's base size in this backup, backup={self}, base_backup={self.get_base_backup()}"
                    )
                if fi.size < fi_base.size:
                    raise Exception(
                        f"File {fi.name} has a smaller size ({fi.size} < {fi_base.size}) than the size of a corresponding file {fi_base.name} "
                        f"in the base backup, backup={self}, base_backup={self.get_base_backup()}"
                    )

        for fi in data_files.values():
            if not self.__reader.file_exists(fi.data_file):
                raise Exception(
                    f"File {fi.data_file} must exist but not found inside backup {self} "
                )
            actual_size = self.__reader.get_file_size(fi.data_file)
            expected_size = fi.size - fi.base_size
            if actual_size != expected_size:
                raise Exception(
                    f"File {fi.data_file} has unexpected size {actual_size} != {expected_size} inside backup {self}"
                )

        if self.get_base_backup() is not None:
            self.get_base_backup().check_files()

    def __parse_backup_metadata(self):
        metadata_str = self.__reader.read_file(".backup")

        xmlroot = ET.fromstring(metadata_str)

        version_node = xmlroot.find("version")
        self.__version = int(version_node.text) if (version_node is not None) else None

        timestamp_node = xmlroot.find("timestamp")
        self.__timestamp = timestamp_node.text if (timestamp_node is not None) else None

        if self.__base_backup is None:
            base_backup_node = xmlroot.find("base_backup")
            if base_backup_node is not None:
                self.__base_backup = Location(base_backup_node.text)

        self.__file_infos = {}
        self.__file_infos_by_checksum = {}
        self.__file_paths = []

        contents = xmlroot.find("contents")
        for file in contents:
            name = file.find("name").text
            if not name.startswith("/"):
                name = "/" + name

            fi = FileInfo(name)
            fi.size = int(file.find("size").text)

            if fi.size != 0:
                checksum_node = file.find("checksum")
                fi.checksum = checksum_node.text

                encrypted_by_disk_node = file.find("encrypted_by_disk")
                if encrypted_by_disk_node is not None:
                    fi.encrypted_by_disk = encrypted_by_disk_node.text == "true"

                base_size_node = file.find("base_size")
                if base_size_node is not None:
                    fi.base_size = int(base_size_node.text)
                else:
                    use_base_node = file.find("use_base")
                    if (use_base_node is not None) and (use_base_node.text == "true"):
                        fi.base_size = fi.size

                if fi.base_size > 0:
                    fi.use_base = True

                if fi.use_base:
                    if fi.base_size == fi.size:
                        fi.base_checksum = fi.checksum
                    else:
                        base_checksum_node = file.find("base_checksum")
                        fi.base_checksum = base_checksum_node.text

                if fi.size > fi.base_size:
                    data_file_node = file.find("data_file")
                    data_file = (
                        data_file_node.text if (data_file_node is not None) else fi.name
                    )
                    if not data_file.startswith("/"):
                        data_file = "/" + data_file
                    fi.data_file = data_file

            self.__file_infos[fi.name] = fi
            if fi.size > 0:
                self.__file_infos_by_checksum[fi.checksum] = fi
            self.__file_paths.append(fi.name)

        metadata_fi = FileInfo("/.backup")
        metadata_fi.size = len(metadata_str)
        metadata_fi.data_file = metadata_fi.name
        self.__file_infos[metadata_fi.name] = metadata_fi
        self.__file_paths.append(metadata_fi.name)

        self.__file_paths.sort()

    def __get_paths_in_backup(self, shard, replica):
        paths = []
        if self.dir_exists(f"/shards/{shard}/replicas/{replica}/metadata/"):
            paths.append(f"/shards/{shard}/replicas/{replica}/")
        if self.dir_exists(f"/shards/{shard}metadata/"):
            paths.append(f"/shards/{shard}/")
        if self.dir_exists(f"/replicas/{replica}/metadata/"):
            paths.append(f"/replicas/{replica}/")
        if self.dir_exists(f"/metadata/"):
            paths.append(f"/")
        return paths

    def __split_database_table(table):
        if isinstance(table, tuple):
            return table[0], table[1]
        elif isinstance(table, str) and (table.find(".") != -1):
            return table.split(".", maxsplit=1)

    def __remove_prefix_path(files, prefix_path):
        for file in files:
            if not file.startswith(prefix_path):
                raise Exception(
                    f"remove_prefix_path: File '{file}' doesn't have the expected prefix '{prefix_path}'"
                )
        return [file[len(prefix_path) :] for file in files]

    def __escape_for_filename(text):
        res = ""
        for c in text:
            if (c.isascii() and c.isalnum()) or c == "_":
                res += c
            else:
                for b in c.encode("utf-8"):
                    res += f"%{b:X}"
        return res

    def __unescape_for_filename(text):
        res = b""
        i = 0
        while i < len(text):
            c = text[i]
            if c == "%" and i + 2 < len(text):
                res += bytes.fromhex(text[i + 1 : i + 3])
                i += 3
            else:
                res += c.encode("ascii")
                i += 1
        return res.decode("utf-8")

    def __extract_partition_id_from_part_name(part_name):
        underscore = part_name.find("_")
        if underscore <= 0:
            return None
        return part_name[:underscore]


# Information about a single file inside a backup.
class FileInfo:
    def __init__(
        self,
        name,
        size=0,
        checksum="00000000000000000000000000000000",
        data_file="",
        use_base=False,
        base_size=0,
        base_checksum="00000000000000000000000000000000",
        encrypted_by_disk=False,
    ):
        self.name = name
        self.size = size
        self.checksum = checksum
        self.data_file = data_file
        self.use_base = use_base
        self.base_size = base_size
        self.base_checksum = base_checksum
        self.encrypted_by_disk = encrypted_by_disk

    def __repr__(self):
        res = "FileInfo("
        res += f"name='{self.name}'"
        res += f", size={self.size}"
        if self.checksum != "00000000000000000000000000000000":
            res += f", checksum='{self.checksum}'"
        if self.data_file:
            res += f", data_file='{self.data_file}'"
        if self.use_base:
            res += f", use_base={self.use_base}"
            res += f", base_size={self.base_size}"
            res += f", base_checksum='{self.base_checksum}'"
        if self.encrypted_by_disk:
            res += f", encrypted_by_disk={self.encrypted_by_disk}"
        res += ")"
        return res

    def __eq__(self, other):
        if not isinstance(other, FileInfo):
            return False
        return (
            (self.name == other.name)
            and (self.size == other.size)
            and (self.checksum == other.checksum)
            and (self.data_file == other.data_file)
            and (self.use_base == other.use_base)
            and (self.base_size == other.base_size)
            and (self.base_checksum == other.base_checksum)
            and (self.encrypted_by_disk == other.encrypted_by_disk)
        )


# Information about extracted files.
class ExtractionInfo:
    def __init__(self, num_files=0, num_bytes=0):
        self.num_files = num_files
        self.num_bytes = num_bytes

    def __repr__(self):
        return f"ExtractionInfo(num_files={self.num_files}, num_bytes={self.num_bytes})"

    def __eq__(self, other):
        if not isinstance(other, ExtractionInfo):
            return False
        return self.num_files == other.num_files and self.num_bytes == other.num_bytes

    def add(self, other):
        self.num_files += other.num_files
        self.num_bytes += other.num_bytes


# File('<path>') can be used to specify the location of a backup or a destination for extracting data.
class File:
    def __init__(self, path):
        self.path = path

    def __repr__(self):
        return f"File('{self.path}')"


# S3('<uri>', '<access_key_id>', '<secret_access_key>') can be used to specify the location of a backup.
class S3:
    def __init__(self, uri, access_key_id=None, secret_access_key=None):
        self.uri = uri
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key

    def __repr__(self):
        str = f"S3('{self.uri}'"
        if self.access_key_id:
            str += f", '{self.access_key_id}'"
        if self.secret_access_key:
            str += f", '{self.secret_access_key}'"
        str += ")"
        return str


####################################################################################################
# Implementation - helper classes and functions.


# Helps to check types.
class TypeChecks:
    def is_location_like(obj):
        return Location.can_init_from(obj)

    def is_file_opened_for_reading(obj):
        return callable(getattr(obj, "read", None))

    def is_file_opened_for_writing(obj):
        return callable(getattr(obj, "write", None))

    def is_reader(obj):
        return (
            isinstance(obj, FileReader)
            or isinstance(obj, S3Reader)
            or isinstance(obj, ZipReader)
        )

    def is_writer(obj):
        return isinstance(obj, FileWriter)

    def check_is_writer(obj):
        if TypeChecks.is_writer(obj):
            return
        raise Exception(f"{obj} is not a writer")


# Helps to represents either File() or S3() location and to parse them from a string.
class Location:
    def __init__(self, obj):
        self.__location = None
        if isinstance(obj, Location):
            self.__location = obj.__location
        elif isinstance(obj, File) or isinstance(obj, S3):
            self.__location = obj
        elif isinstance(obj, str) and len(obj) > 0:
            self.__location = Location.__parse_location(obj)
        else:
            raise Exception("Cannot parse a location from {obj}")

    def can_init_from(obj):
        if isinstance(obj, Location):
            return True
        elif isinstance(obj, File) or isinstance(obj, S3):
            return True
        elif isinstance(obj, str) and len(obj) > 0:
            return True
        else:
            return False

    def __repr__(self):
        return repr(self.__location)

    def create_reader(self):
        if isinstance(self.__location, File):
            path = self.__location.path
            path, zip_filename = Location.__split_filename_if_archive(path)
            reader = FileReader(path)
            if zip_filename is not None:
                reader = ZipReader(reader, zip_filename)
            return reader

        if isinstance(self.__location, S3):
            uri = self.__location.uri
            uri, zip_filename = Location.__split_filename_if_archive(uri)
            reader = S3Reader(
                uri,
                self.__location.access_key_id,
                self.__location.secret_access_key,
            )
            if zip_filename is not None:
                reader = ZipReader(reader, zip_filename)
            return reader

        raise Exception(f"Couldn't create a reader from {self}")

    def create_writer(self):
        if isinstance(self.__location, File):
            return FileWriter(self.__location.path)

        raise Exception(f"Couldn't create a writer to {self}")

    def __parse_location(desc):
        startpos = len(desc) - len(desc.lstrip())

        opening_parenthesis = desc.find("(", startpos)
        if opening_parenthesis == -1:
            endpos = len(desc.rstrip())
            if startpos == endpos:
                raise Exception(
                    f"Couldn't parse a location from '{desc}': empty string"
                )
            return File(desc[startpos:endpos])

        closing_parenthesis = desc.find(")", opening_parenthesis)
        if closing_parenthesis == -1:
            raise Exception(
                f"Couldn't parse a location from '{desc}': No closing parenthesis"
            )

        name = desc[startpos:opening_parenthesis]
        args = desc[opening_parenthesis + 1 : closing_parenthesis].split(",")
        args = [Location.__unquote_argument(arg.strip()) for arg in args]
        endpos = closing_parenthesis + 1

        if name == "File":
            if len(args) == 1:
                return File(args[0])
            else:
                raise Exception(
                    f"Couldn't parse a location from '{desc}': File(<path>) requires a single argument, got {len(args)} arguments"
                )

        if name == "S3":
            if 1 <= len(args) and len(args) <= 3:
                return S3(*args)
            else:
                raise Exception(
                    f"Couldn't parse a location from '{desc}': S3(<uri> [, <access_key_id>, <secret_access_key>]) requires from 1 to 3 arguments, got {len(args)} arguments"
                )

        raise Exception(
            f"Couldn't parse a location from '{desc}': Unknown type {name} (only File and S3 are supported)"
        )

    def __unquote_argument(arg):
        if arg.startswith("'"):
            return arg.strip("'")
        elif arg.startswith('"'):
            return arg.strip('"')
        else:
            return arg

    def __split_filename_if_archive(path):
        is_archive = path.endswith(".zip") or path.endswith(".zipx")
        if not is_archive:
            return path, None
        sep = path.rfind("/")
        if sep == -1:
            return "", path
        return path[: sep + 1], path[sep + 1 :]


# Represents an empty file object.
class EmptyFileObj:
    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def read(self, count=None):
        return b""


# Represent a file object which concatenate data from two file objects.
class ConcatFileObj:
    def __init__(self, fileobj1, fileobj2):
        self.__fileobj1 = fileobj1
        self.__fileobj2 = fileobj2
        self.__first_is_already_read = False

    def close(self):
        if self.__fileobj1 is not None:
            self.__fileobj1.close()
            self.__fileobj1 = None
        if self.__fileobj2 is not None:
            self.__fileobj2.close()
            self.__fileobj2 = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def read(self, count=None):
        read_data = b""

        if count != 0 and not self.__first_is_already_read:
            read_data += self.__fileobj1.read(count)
            if (count is None) or (count > len(read_data)):
                self.__first_is_already_read = True
            if count is not None:
                count -= len(read_data)

        if count != 0:
            read_data += self.__fileobj2.read(count)

        return read_data


# Helps to read a File() backup.
class FileReader:
    def __init__(self, root_path):
        self.__root_path = root_path

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def file_exists(self, path):
        return os.path.isfile(self.get_abs_path(path))

    def get_file_size(self, path):
        return os.path.getsize(self.get_abs_path(path))

    def read_file(self, path):
        with self.open_file(path) as f:
            return f.read()

    def open_file(self, path):
        return open(self.get_abs_path(path), "rb")

    def extract_file(self, path, writer, out_path):
        if isinstance(writer, FileWriter):
            shutil.copyfile(self.get_abs_path(path), writer.get_abs_path(out_path))
        else:
            with self.open_file(path) as istream:
                with writer.open_file(out_path) as ostream:
                    shutil.copyfileobj(istream, ostream)

    def get_abs_path(self, path):
        if path.startswith("/"):
            path = path[1:]
        return os.path.join(self.__root_path, path)


# Helps to extract files to a File() destination.
class FileWriter:
    def __init__(self, root_path):
        self.__root_path = root_path

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def open_file(self, path):
        return open(self.get_abs_path(path), "wb")

    def create_empty_file(self, path):
        with self.open_file(path) as file:
            pass

    def make_dirs(self, path):
        abs_path = self.get_abs_path(path)
        if not os.path.isdir(abs_path):
            os.makedirs(abs_path)

    def get_abs_path(self, path):
        if path.startswith("/"):
            path = path[1:]
        return os.path.join(self.__root_path, path)


# Helps to read a S3() backup.
class S3Reader:
    def __init__(self, uri, access_key_id, secret_access_key):
        s3_uri = S3URI(uri)
        self.__bucket = s3_uri.bucket
        self.__key = s3_uri.key
        self.__client = None

        try:
            self.__client = boto3.client(
                "s3",
                endpoint_url=s3_uri.endpoint,
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key,
            )
        except:
            self.close()
            raise

    def close(self):
        if self.__client is not None:
            self.__client.close()
            self.__client = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def file_exists(self, path):
        try:
            self.__client.head_object(Bucket=self.__bucket, Key=self.get_key(path))
            return True
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                raise

    def get_file_size(self, path):
        response = self.__client.head_object(
            Bucket=self.__bucket, Key=self.get_key(path)
        )
        return response["ContentLength"]

    def read_file(self, path):
        with self.open_file(path) as f:
            return f.read()

    def open_file(self, path):
        response = self.__client.get_object(
            Bucket=self.__bucket, Key=self.get_key(path)
        )
        return response["Body"]

    def extract_file(self, path, writer, out_path):
        if isinstance(writer, FileWriter):
            self.__client.download_file(
                Bucket=self.__bucket,
                Key=self.get_key(path),
                Filename=writer.get_abs_path(out_path),
            )
        else:
            with writer.open_file(out_path) as ostream:
                self.__client.download_fileobj(
                    Bucket=self.__bucket, Key=self.get_key(path), Fileobj=ostream
                )

    def get_key(self, path):
        if path.startswith("/"):
            path = path[1:]
        return self.__key + "/" + path


# Parses a S3 URI with detecting endpoint, bucket name, and key.
class S3URI:
    def __init__(self, uri):
        parsed_url = urlparse(uri, allow_fragments=False)
        if not self.__parse_virtual_hosted(parsed_url) and not self.__parse_path_style(
            parsed_url
        ):
            raise Exception(f"S3URI: Could not parse {uri}")

    # https://bucket-name.s3.Region.amazonaws.com/key
    def __parse_virtual_hosted(self, parsed_url):
        host = parsed_url.netloc
        if host.find(".s3") == -1:
            return False
        self.bucket, new_host = host.split(".s3", maxsplit=1)
        if len(self.bucket) < 3:
            return False
        new_host = "s3" + new_host
        self.endpoint = parsed_url.scheme + "://" + new_host
        path = parsed_url.path
        if path.startswith("/"):
            path = path[1:]
        if path.endswith("/"):
            path = path[:-1]
        self.key = path
        return True

    # https://s3.Region.amazonaws.com/bucket-name/key
    def __parse_path_style(self, parsed_url):
        self.endpoint = parsed_url.scheme + "://" + parsed_url.netloc
        path = parsed_url.path
        if path.startswith("/"):
            path = path[1:]
        if path.endswith("/"):
            path = path[:-1]
        if path.find("/") == -1:
            self.bucket = path
            self.key = ""
        else:
            self.bucket, self.key = path.split("/", maxsplit=1)
        if len(self.bucket) < 3:
            return False
        return True


# Helps to read a backup from a zip-archive.
class ZipReader:
    def __init__(self, base_reader, archive_name):
        self.__base_reader = None
        self.__zipfileobj = None
        self.__zipfile = None

        try:
            self.__base_reader = base_reader
            self.__zipfileobj = base_reader.open_file(archive_name)
            self.__zipfile = zipfile.ZipFile(self.__zipfileobj)
        except:
            self.close()
            raise

    def close(self):
        if self.__zipfile is not None:
            self.__zipfile.close()
            self.__zipfile = None
        if self.__zipfileobj is not None:
            self.__zipfileobj.close()
            self.__zipfileobj = None
        if self.__base_reader is not None:
            self.__base_reader.close()
            self.__base_reader = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def file_exists(self, path):
        return self.__get_zippath(path).is_file()

    def get_file_size(self, path):
        return self.__get_zipinfo(path).file_size

    def read_file(self, path):
        return self.__get_zippath(path).read_bytes()

    def open_file(self, path):
        return self.__get_zippath(path).open(mode="rb")

    def extract_file(self, path, writer, out_path):
        with self.open_file(path) as istream:
            with writer.open_file(out_path) as ostream:
                shutil.copyfileobj(istream, ostream)

    def __get_zippath(self, path):
        if path.startswith("/"):
            path = path[1:]
        return zipfile.Path(self.__zipfile, path)

    def __get_zipinfo(self, path):
        if path.startswith("/"):
            path = path[1:]
        return self.__zipfile.getinfo(path)
