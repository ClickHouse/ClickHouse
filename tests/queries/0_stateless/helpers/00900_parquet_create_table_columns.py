#!/usr/bin/env python3

import json
import sys

TYPE_PARQUET_CONVERTED_TO_CLICKHOUSE = {
    "TIMESTAMP_MICROS": "DateTime('Europe/Moscow')",
    "TIMESTAMP_MILLIS": "DateTime('Europe/Moscow')",
    "UTF8": "String",
}

TYPE_PARQUET_PHYSICAL_TO_CLICKHOUSE = {
    "BOOLEAN": "UInt8",
    "INT32": "Int32",
    "INT64": "Int64",
    "FLOAT": "Float32",
    "DOUBLE": "Float64",
    "BYTE_ARRAY": "String",
    "INT96": "Int64",  # TODO!
}


def read_file(filename):
    with open(filename, "rb") as f:
        return f.read().decode("raw_unicode_escape")


def get_column_name(column):
    return column["Name"].split(".", 1)[0]


def resolve_clickhouse_column_type(column):
    column_name = get_column_name(column)
    logical_type = column.get("LogicalType", {})
    converted_type = column.get("ConvertedType", "").upper()
    physical_type = column.get("PhysicalType", "").upper()
    if logical_type and logical_type.get("Type", "").upper() == "DECIMAL":
        precision = int(logical_type["precision"])
        scale = int(logical_type["scale"])
        if precision < 1 or precision > 76:
            raise RuntimeError(
                "Column {} has invalid Decimal precision {}".format(
                    column_name, precision
                )
            )
        if precision > 38:
            raise RuntimeError(
                "Column {} has unsupported Decimal precision {}".format(
                    column_name, precision
                )
            )
        if scale < 0 or scale > precision:
            raise RuntimeError(
                "Column {} has invalid Decimal scale {} for precision {}".format(
                    column_name, scale, precision
                )
            )
        return "Decimal({}, {})".format(precision, scale)
    if converted_type and converted_type != "NONE":
        result_type = TYPE_PARQUET_CONVERTED_TO_CLICKHOUSE.get(converted_type)
        if result_type:
            return result_type
        raise RuntimeError(
            "Column {} has unknown ConvertedType: {}".format(
                column_name, converted_type
            )
        )
    if physical_type and physical_type != "NONE":
        result_type = TYPE_PARQUET_PHYSICAL_TO_CLICKHOUSE.get(physical_type)
        if result_type:
            return result_type
        raise RuntimeError(
            "Column {} has unknown PhysicalType: {}".format(column_name, physical_type)
        )
    raise RuntimeError(
        "Column {} has invalid types: ConvertedType={}, PhysicalType={}".format(
            column_name, converted_type, physical_type
        )
    )


def dump_columns(obj):
    descr_by_column_name = {}
    columns_descr = []
    for column in obj["Columns"]:
        column_name = get_column_name(column)
        column_type = resolve_clickhouse_column_type(column)
        result_type = "Nullable({})".format(column_type)
        if column_name in descr_by_column_name:
            descr = descr_by_column_name[column_name]
            descr["types"].append(result_type)
        else:
            descr = {
                "name": column_name,
                "types": [result_type],
            }
            descr_by_column_name[column_name] = descr
            columns_descr.append(descr)

    # Make tuples from nested types. CH Server doesn't support such Arrow type but it makes Server Exceptions more relevant.
    def _format_type(types):
        if len(types) == 1:
            return types[0]
        else:
            return "Tuple({})".format(", ".join(types))

    print(
        ", ".join(
            map(
                lambda descr: "`{}` {}".format(
                    descr["name"], _format_type(descr["types"])
                ),
                columns_descr,
            )
        )
    )


def dump_columns_from_file(filename):
    dump_columns(json.loads(read_file(filename), strict=False))


if __name__ == "__main__":
    filename = sys.argv[1]
    dump_columns_from_file(filename)
