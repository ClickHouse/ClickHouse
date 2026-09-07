import io

import avro.datafile
import avro.io
import avro.schema

# Define the schema
schema = avro.schema.parse(
    """
    {
      "type": "record",
      "name": "TestRecord",
      "fields": [
        {"name": "string_only", "type": ["string"]},
        {"name": "string_or_null", "type": ["string", "null"]},
        {"name": "null_or_string", "type": ["null", "string"]},
        {"name": "double_or_string", "type": ["double", "string"]},
        {"name": "string_or_double", "type": ["string", "double"]},
        {"name": "null_or_string_or_double", "type": ["null", "string", "double"]},
        {"name": "string_or_double_or_null", "type": ["string", "double", "null"]},
        {"name": "string_or_float_or_long", "type": ["string", "float", "long"]},
        {"name": "long_or_string_or_float", "type": ["long", "string", "float"]},
        {"name": "double_or_null_or_string_or_long", "type": ["double", "null", "string", "long"]},
        {"name": "double_or_long_or_string_in_array", "type": {
          "type": "array",
          "items": ["double", "long", "string"]
        }},
        {"name": "double_or_string_or_long_or_null_in_map", "type": {
          "type": "map",
          "values": ["double", "string", "long", "null"]
        }}
      ]
    }
    """
)

records = [
    {
        "string_only": "alpha",
        "string_or_null": "bravo",
        "null_or_string": None,
        "double_or_string": 3.1415926535,
        "string_or_double": "charlie",
        "null_or_string_or_double": "delta",
        "string_or_double_or_null": 2.7182818284,
        "string_or_float_or_long": "echo",
        "long_or_string_or_float": 42,
        "double_or_null_or_string_or_long": -3.1415926535,
        "double_or_long_or_string_in_array": [1.4142135623, "foxtrot", -100],
        "double_or_string_or_long_or_null_in_map": {"key1": 3.1415926535, "key2": None},
    },
    {
        "string_only": "golf",
        "string_or_null": None,
        "null_or_string": "hotel",
        "double_or_string": "india",
        "string_or_double": 1.6180339887,
        "null_or_string_or_double": 3.1415926535,
        "string_or_double_or_null": "juliet",
        "string_or_float_or_long": 7.38906,
        "long_or_string_or_float": "kilo",
        "double_or_null_or_string_or_long": 1000,
        "double_or_long_or_string_in_array": [-1.6180339887, 0, "lima"],
        "double_or_string_or_long_or_null_in_map": {"key3": "mike", "key4": 1e-9},
    },
    {
        "string_only": "november",
        "string_or_null": "oscar",
        "null_or_string": None,
        "double_or_string": 1e10,
        "string_or_double": "papa",
        "null_or_string_or_double": None,
        "string_or_double_or_null": None,
        "string_or_float_or_long": -5000000,
        "long_or_string_or_float": 1.7320508,
        "double_or_null_or_string_or_long": "quebec",
        "double_or_long_or_string_in_array": [2.7182818284, 1729, "romeo"],
        "double_or_string_or_long_or_null_in_map": {
            "key5": -2.7182818284,
            "key6": "sierra",
        },
    },
    {
        "string_only": "tango",
        "string_or_null": None,
        "null_or_string": "uniform",
        "double_or_string": -1.4142135623,
        "string_or_double": -1.6180339887,
        "null_or_string_or_double": 1e-5,
        "string_or_double_or_null": "victor",
        "string_or_float_or_long": "whiskey",
        "long_or_string_or_float": -987654321,
        "double_or_null_or_string_or_long": None,
        "double_or_long_or_string_in_array": [-3.1415926535, "xray", 31415926535],
        "double_or_string_or_long_or_null_in_map": {"key7": "yankee", "key8": -987.654},
    },
    {
        "string_only": "zulu",
        "string_or_null": "alpha1",
        "null_or_string": "bravo1",
        "double_or_string": 2.718281828,
        "string_or_double": "charlie1",
        "null_or_string_or_double": None,
        "string_or_double_or_null": -1.7320508075,
        "string_or_float_or_long": 1e6,
        "long_or_string_or_float": "delta1",
        "double_or_null_or_string_or_long": -1.6180339887,
        "double_or_long_or_string_in_array": [-2.7182818284, 123456789, "echo1"],
        "double_or_string_or_long_or_null_in_map": {
            "key9": 9223372036854775807,
            "key10": None,
        },
    },
]

# Write the data to an Avro file
with open("union_in_complex_types.avro", "wb") as avro_file:
    writer = avro.datafile.DataFileWriter(avro_file, avro.io.DatumWriter(), schema)
    for record in records:
        writer.append(record)
    writer.close()
