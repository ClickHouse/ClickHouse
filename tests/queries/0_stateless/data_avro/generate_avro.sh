#!/usr/bin/env bash

#avro tools: https://www.apache.org/dyn/closer.cgi?path=avro/avro-1.9.1/java/avro-tools-1.9.1.jar


avro-tools fromjson  --schema-file primitive.avsc primitive.json > primitive.avro
avro-tools fromjson  --schema-file complex.avsc complex.json > complex.avro
avro-tools fromjson  --schema-file logical_types.avsc logical_types.json > logical_types.avro
avro-tools fromjson  --schema-file empty.avsc empty.json > empty.avro
avro-tools fromjson  --schema-file references.avsc references.json >  references.avro
avro-tools fromjson  --schema-file nested.avsc nested.json > nested.avro
avro-tools fromjson  --schema-file nested_complex.avsc nested_complex.json > nested_complex.avro

#compression
avro-tools fromjson --codec null  --schema-file simple.avsc simple.json > simple.null.avro
avro-tools fromjson --codec deflate  --schema-file simple.avsc simple.json > simple.deflate.avro
avro-tools fromjson --codec snappy  --schema-file simple.avsc simple.json > simple.snappy.avro
