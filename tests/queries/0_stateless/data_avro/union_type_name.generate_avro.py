import io
import avro.datafile
import avro.io
import avro.schema

schema = avro.schema.parse(
    """
    {
      "type": "record",
      "name": "Root",
      "fields": [
        {"name": "id", "type": "int"},
        {"name": "nullable_payload",
         "type": ["null",
                  {"type": "record", "name": "TypeA",
                   "fields": [{"name": "x", "type": "int"}]}]},
        {"name": "variant_payload",
         "type": [{"type": "record", "name": "TypeB",
                   "fields": [{"name": "y", "type": "string"}]},
                  {"type": "record", "name": "TypeC",
                   "fields": [{"name": "z", "type": "double"}]}]}
      ]
    }
    """
)

records = [
    {"id": 1, "nullable_payload": None,          "variant_payload": {"y": "hello"}},
    {"id": 2, "nullable_payload": {"x": 10},     "variant_payload": {"z": 3.14}},
    {"id": 3, "nullable_payload": {"x": 20},     "variant_payload": {"y": "world"}},
    {"id": 4, "nullable_payload": None,           "variant_payload": {"z": 2.71}},
    {"id": 5, "nullable_payload": {"x": 30},     "variant_payload": {"y": "foo"}},
]

with open("union_type_name.avro", "wb") as f:
    writer = avro.datafile.DataFileWriter(f, avro.io.DatumWriter(), schema)
    for record in records:
        writer.append(record)
    writer.close()
