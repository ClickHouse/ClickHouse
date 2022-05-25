from argparse import ArgumentParser
import os

parser = ArgumentParser()
parser.add_argument("--path", type=str, required=True, help="temp directory for merge tree")
parser.add_argument("--source", type=str, required=True, help="directory of parquet files")
parser.add_argument("--dst", type=str, required=True, help="destination directory for merge tree")
parser.add_argument("--schema", type=str,
                    default="l_orderkey Int64,l_partkey Int64,l_suppkey Int64,l_linenumber Int64,l_quantity Float64,l_extendedprice Float64,l_discount Float64,l_tax Float64,l_returnflag String,l_linestatus String,l_shipdate Date,l_commitdate Date,l_receiptdate Date,l_shipinstruct String,l_shipmode String,l_comment String")


def get_transform_command(data_path,
                          parquet_file,
                          schema):
    return f"""
    clickhouse-local --no-system-tables  --path {data_path} --file "{parquet_file}" --input-format=Parquet \\
    -S "{schema}" \\
    --query " \\
    CREATE TABLE m1 ({schema}) ENGINE = MergeTree() order by tuple(); \\
    insert into m1 SELECT * FROM table;\\
    OPTIMIZE table m1 FINAL;
    "
    """


def get_move_command(data_path, dst_path, no):
    return f"cp -r {data_path}/data/_local/m1/all_1_* {dst_path}/all_{no}_{no}_0"


def get_clean_command(data_path):
    return f"rm -rf {data_path}/data/*"


def transform(data_path, source, schema, dst):
    assert os.path.exists(data_path), f"{data_path} is not exist"
    for no, file in enumerate([file for file in os.listdir(source) if file.endswith(".parquet")]):
        abs_file = f"{source}/{file}"
        if not os.path.exists(abs_file):
            raise f"{abs_file} not found"
        command1 = get_transform_command(data_path, abs_file, schema)
        command2 = get_move_command(data_path, dst, no+1)
        command3 = get_clean_command(data_path)
        if os.system(command3) != 0:
            raise Exception(command3 + " failed")
        if os.system(command1) != 0:
            raise Exception(command1 + " failed")
        if os.system(command2) != 0:
            raise Exception(command2 + " failed")
        print(f"{abs_file}")


"""
python3 parquet_to_mergetree.py --path=/root/data/tmp --source=/home/ubuntu/tpch-data-sf100/lineitem --dst=/root/data/mergetree
"""
if __name__ == '__main__':
    args = parser.parse_args()
    if not os.path.exists(args.dst):
        os.mkdir(args.dst)
    transform(args.path, args.source, args.schema, args.dst)
