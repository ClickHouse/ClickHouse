import os
import re
import subprocess
from argparse import ArgumentParser
from multiprocessing import Pool

parser = ArgumentParser()
parser.add_argument("--path", type=str, required=True, help="temp directory for merge tree")
parser.add_argument("--source", type=str, required=True, help="directory of parquet files")
parser.add_argument("--dst", type=str, required=True, help="destination directory for merge tree")
parser.add_argument("--schema", type=str,
                    default="l_orderkey Nullable(Int64),l_partkey Nullable(Int64),l_suppkey Nullable(Int64),l_linenumber Nullable(Int64),l_quantity Nullable(Float64),l_extendedprice Nullable(Float64),l_discount Nullable(Float64),l_tax Nullable(Float64),l_returnflag Nullable(String),l_linestatus Nullable(String),l_shipdate Nullable(Date),l_commitdate Nullable(Date),l_receiptdate Nullable(Date),l_shipinstruct Nullable(String),l_shipmode Nullable(String),l_comment Nullable(String)")


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
    return f"mkdir -p {dst_path}/all_{no}_{no}_1; cp -r {data_path}/data/_local/m1/all_1_1_1/* {dst_path}/all_{no}_{no}_1"


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

class Engine(object):
    def __init__(self, source, data_path, schema, dst):
        self.source = source
        self.data_path = data_path
        self.schema = schema
        self.dst = dst
    def __call__(self, ele):
        no = ele[0]
        file = ele[1]
        abs_file = f"{self.source}/{file}"
        print(abs_file)
        if not os.path.exists(abs_file):
            raise f"{abs_file} not found"
        private_path = f"{self.data_path}/{str(no)}"
        os.system(f"mkdir -p {private_path}")
        command1 = get_transform_command(private_path, abs_file, self.schema)
        command2 = get_move_command(private_path, self.dst, no+1)
        command3 = get_clean_command(private_path)
        if os.system(command3) != 0:
            raise Exception(command3 + " failed")
        if os.system(command1) != 0:
            raise Exception(command1 + " failed")
        if os.system(command2) != 0:
            raise Exception(command2 + " failed")
        print(f"{abs_file}")


def multi_transform(data_path, source, schema, dst):
    assert os.path.exists(data_path), f"{data_path} is not exist"
    data_inputs = enumerate([file for file in os.listdir(source) if file.endswith(".parquet")])
    pool = Pool()
    engine = Engine(source, data_path, schema, dst)
    pool.map(engine, list(data_inputs))  # process data_inputs iterable with pool


def check_version(version):
    proc = subprocess.Popen(["clickhouse-local", "--version"], stdout=subprocess.PIPE, shell=False)
    (out, err) = proc.communicate()
    if err:
        raise Exception(f"Fail to call clickhouse-local, error: {err}")
    ver = re.search(r'version\s*([\d.]+)', str(out)).group(1)
    ver_12 = float(ver.split('.')[0] + '.' + ver.split('.')[1])
    if ver_12 >= float(version):
        raise Exception(f"Version of clickhouse-local too high({ver}), should be <= 22.5")

"""
python3 parquet_to_mergetree.py --path=/root/data/tmp --source=/home/ubuntu/tpch-data-sf100/lineitem --dst=/root/data/mergetree
"""
if __name__ == '__main__':
    args = parser.parse_args()
    if not os.path.exists(args.dst):
        os.mkdir(args.dst)
    #transform(args.path, args.source, args.schema, args.dst)
    check_version('22.6')
    multi_transform(args.path, args.source, args.schema, args.dst)
