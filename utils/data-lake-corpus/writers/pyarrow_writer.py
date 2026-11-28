"""Write Parquet files with pyarrow (Arrow C++), then read them back with the same pyarrow.

Usage: python pyarrow_writer.py <job.json>

Runs inside a pinned environment created by `generate.py` (`uv run --with pyarrow==X`),
so it must work with old pyarrow versions too: only use APIs that exist since pyarrow 2.0
unless the job explicitly asks for a newer option.
"""

import base64
import json
import os
import sys

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq


def read_ipc(path):
    with pa.memory_map(path) as f:
        return ipc.open_file(f).read_all()


def write_ipc(table, path):
    with pa.OSFile(path, "wb") as f:
        w = ipc.new_file(f, table.schema)
        w.write_table(table)
        w.close()


def encryption_properties():
    import pyarrow.parquet.encryption as pe

    class Kms(pe.KmsClient):
        def __init__(self, config):
            pe.KmsClient.__init__(self)

        def wrap_key(self, key_bytes, master_key_identifier):
            return base64.b64encode(key_bytes)

        def unwrap_key(self, wrapped_key, master_key_identifier):
            return base64.b64decode(wrapped_key)

    factory = pe.CryptoFactory(lambda config: Kms(config))
    config = pe.EncryptionConfiguration(footer_key="footer", column_keys={"col": ["id"]}, plaintext_footer=False)
    return factory.file_encryption_properties(pe.KmsConnectionConfig(), config)


def main():
    job = json.load(open(sys.argv[1]))
    results = []
    for j in job["jobs"]:
        table = read_ipc(os.path.join(job["input_dir"], j["input"]))
        out = os.path.join(job["output_dir"], j["name"] + ".parquet")
        opts = dict(j.get("options", {}))
        special = opts.pop("_special", None)
        encoding = opts.get("column_encoding")
        if encoding:
            opts["use_dictionary"] = [c for c in table.column_names if c not in encoding and opts.get("use_dictionary", True)]
        if "sorting_columns" in opts:
            opts["sorting_columns"] = [pq.SortingColumn(table.column_names.index(c), desc, nulls_first) for c, desc, nulls_first in opts["sorting_columns"]]
        if special == "encrypt":
            opts["encryption_properties"] = encryption_properties()
        if special == "empty_row_groups":
            with pq.ParquetWriter(out, table.schema, **opts) as w:
                w.write_table(table.slice(0, 0))
                w.write_table(table.slice(0, 5))
                w.write_table(table.slice(5, 0))
                w.write_table(table.slice(5))
        elif special == "no_row_groups":
            w = pq.ParquetWriter(out, table.schema, **opts)
            w.close()
        else:
            pq.write_table(table, out, **opts)
        res = {"name": j["name"]}
        try:
            md = pq.ParquetFile(out).metadata
            res["created_by"] = md.created_by
            res["num_row_groups"] = md.num_row_groups
            back = pq.read_table(out)
            write_ipc(back, os.path.join(job["output_dir"], j["name"] + ".arrow"))
        except Exception as e:  # encrypted files cannot be read back without keys
            res["readback_error"] = repr(e)
        results.append(res)
    json.dump({"library": "pyarrow", "version": pa.__version__, "results": results}, open(job["result"], "w"), indent=1)


if __name__ == "__main__":
    main()
