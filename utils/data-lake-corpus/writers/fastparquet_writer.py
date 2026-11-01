"""Write Parquet files with fastparquet (pure Python/numba writer), read back with fastparquet.

Usage: python fastparquet_writer.py <job.json>
"""

import json
import os
import sys

import fastparquet
import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

NULLABLE = {
    pa.int8(): pd.Int8Dtype(), pa.int16(): pd.Int16Dtype(), pa.int32(): pd.Int32Dtype(), pa.int64(): pd.Int64Dtype(),
    pa.uint8(): pd.UInt8Dtype(), pa.uint16(): pd.UInt16Dtype(), pa.uint32(): pd.UInt32Dtype(), pa.uint64(): pd.UInt64Dtype(),
    pa.bool_(): pd.BooleanDtype(),
}


def main():
    job = json.load(open(sys.argv[1]))
    results = []
    for j in job["jobs"]:
        with pa.memory_map(os.path.join(job["input_dir"], j["input"])) as f:
            table = ipc.open_file(f).read_all()
        df = table.to_pandas(types_mapper=NULLABLE.get)
        out = os.path.join(job["output_dir"], j["name"] + ".parquet")
        fastparquet.write(out, df, write_index=False, **j.get("options", {}))
        back_df = fastparquet.ParquetFile(out).to_pandas()
        back = pa.Table.from_pandas(back_df, preserve_index=False)
        with pa.OSFile(os.path.join(job["output_dir"], j["name"] + ".arrow"), "wb") as f:
            w = ipc.new_file(f, back.schema)
            w.write_table(back)
            w.close()
        md = pq.ParquetFile(out).metadata
        results.append({"name": j["name"], "created_by": md.created_by, "num_row_groups": md.num_row_groups})
    json.dump({"library": "fastparquet", "version": fastparquet.__version__, "results": results}, open(job["result"], "w"), indent=1)


if __name__ == "__main__":
    main()
