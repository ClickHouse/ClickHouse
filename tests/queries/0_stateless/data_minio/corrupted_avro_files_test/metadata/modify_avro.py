#!/usr/bin/env python3
from fastavro import reader, writer
import sys

def modify_avro_file(input_file, output_file, new_path):
    records = []
    schema = None
    metadata = {}
    
    # Read the original file
    with open(input_file, 'rb') as f:
        avro_reader = reader(f)
        schema = avro_reader.writer_schema
        metadata = avro_reader.metadata
        
        for record in avro_reader:
            # Modify the file_path in data_file
            record['manifest_path'] = new_path
            records.append(record)
    
    # Write the modified records to new file
    with open(output_file, 'wb') as f:
        writer(f, schema, records, metadata=metadata)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python modify_avro.py <input.avro> <output.avro> <new_path>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] 
    new_path = sys.argv[3]
    
    modify_avro_file(input_file, output_file, new_path)
    print(f"Modified file saved as {output_file}")