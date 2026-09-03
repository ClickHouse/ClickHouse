#!/usr/bin/env python3
import io
import random
import time
import uuid
import math
import threading
import requests
import pandas as pd
from threading import Thread
from argparse import ArgumentParser

localhost_url = "http://localhost:8123"

common_settings = "enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 1, min_bytes_for_full_part_storage = '128M', old_parts_lifetime = 1"

storage_mt = "MergeTree ORDER BY id SETTINGS {}".format(common_settings)

storage_smt = """
    SharedMergeTree('/zookeeper/{{database}}/updates_stress/', '1')
    ORDER BY id
    SETTINGS {}, storage_policy = 's3_with_keeper'
""".format(common_settings)

storage_rmt = """
    ReplicatedMergeTree('/zookeeper/{{database}}/updates_stress/', '1')
    ORDER BY id
    SETTINGS {}
""".format(common_settings)

engines = {
    "MergeTree": storage_mt,
    "SharedMergeTree": storage_smt,
    "ReplicatedMergeTree": storage_rmt,
}

parser = ArgumentParser()
parser.add_argument("--timeout", type=float, default=10)
parser.add_argument("--threads", type=int, default=8)
parser.add_argument("--columns", type=int, default=10)
parser.add_argument("--scale", type=int, default=100)
parser.add_argument("--batch", type=int, default=500)
parser.add_argument("--engine", type=str, choices=list(engines.keys()), default="SharedMergeTree")
parser.add_argument("--sync-mode", type=str, default="auto")

parser.add_argument("--skip-check", action="store_true")
parser.add_argument("--verbose", action="store_true")
parser.add_argument("--with-mutations", action="store_true")
parser.add_argument("--host", type=str, default=localhost_url)
parser.add_argument("--user", type=str, default="default")
parser.add_argument("--password", type=str, default="")

args = parser.parse_args()

table_name = "updates_stress"
table_name_data = table_name + "_data"
table_name_reference = table_name + "_reference"
query_id_prefix = table_name + "-" + str(random.randint(0, 10000000000))
cloud_mode = "clickhouse" in args.host

if cloud_mode:
    query_log_name = "clusterAllReplicas(default, system.query_log)"
    text_log_name = "clusterAllReplicas(default, system.text_log)"
    chosen_engine = engines["MergeTree"]
else:
    query_log_name = "system.query_log"
    text_log_name = "system.text_log"
    chosen_engine = engines[args.engine]

insert_num = 0
insert_lock = threading.Lock()
stop_event = threading.Event()


def generate_query_id():
    return query_id_prefix + "-" + str(uuid.uuid4())


def run_query(query, input_data=None, query_id=None):
    if not query_id:
        query_id = generate_query_id()

    if table_name_reference in query or table_name_data in query:
        host = localhost_url
        auth = None
    else:
        host = args.host
        auth = requests.auth.HTTPBasicAuth(args.user, args.password)

    params = {
        "query_id": query_id,
        "async_insert": 0,
        "max_parallel_replicas": 1,
        "allow_experimental_lightweight_update": 1,
        "apply_mutations_on_fly": 1,
        "apply_patch_parts": 1,
        "update_parallel_mode": args.sync_mode,
        "update_sequential_consistency": 1,
    }

    if input_data:
        params["query"] = query
        headers = {"Content-Type": "application/binary"}
        response = requests.post(host, params=params, data=input_data, headers=headers, auth=auth)
    else:
        response = requests.post(host, params=params, data=query, auth=auth)

    if response.status_code != 200:
        raise ValueError(response.text)

    return response.text


def get_create_query():
    create_query = "CREATE TABLE {}(id UInt64"

    for i in range(0, args.columns):
        create_query += ", col{} UInt64".format(i)

    create_query += ") ENGINE = {}"
    return create_query


def get_select_data_query():
    select_data_query = "SELECT number AS id"

    for i in range(0, args.columns):
        select_data_query += ", rand({}) % {} AS col{}".format(i, args.scale, i)

    select_data_query += " FROM numbers({{}} * {}, {}) FORMAT JSONEachRow".format(args.batch, args.batch)
    return select_data_query


print("Starting stress test. Timeout: {}, num threads: {}, query_id prefix: {}".format(args.timeout, args.threads, query_id_prefix))

run_query("DROP TABLE IF EXISTS {} SYNC".format(table_name))
run_query("DROP TABLE IF EXISTS {} SYNC".format(table_name_data))
run_query("DROP TABLE IF EXISTS {} SYNC".format(table_name_reference))

create_query = get_create_query()
run_query(create_query.format(table_name, chosen_engine))

create_reference_query = get_create_query()
run_query(create_reference_query.format(table_name_reference, "EmbeddedRocksDB PRIMARY KEY id"))

create_data_query = "CREATE TABLE {} (query_id String, data String) ENGINE = EmbeddedRocksDB PRIMARY KEY query_id".format(table_name_data)
run_query(create_data_query)

select_data_query = get_select_data_query()
insert_query = "INSERT INTO {} FORMAT JSONEachRow"
select_query = "SELECT sum(col{}) FROM {} SETTINGS apply_patch_parts = {}"
alter_query = "ALTER TABLE {} UPDATE col{} = {} WHERE col{} = {} SETTINGS mutations_sync = 0, alter_update_mode = '{}'"


if args.verbose:
    print()
    print(create_query)
    print(create_data_query)
    print(create_reference_query)
    print(select_data_query)
    print(insert_query)
    print(select_query)
    print(alter_query)


def run_queries(runner, sleep_seconds = 0):
    while True:
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

        if stop_event.is_set():
            break

        try:
            runner()
        except Exception as e:
            print(str(e))


def run_select(apply_patches):
    col = random.randint(0, args.columns - 1)
    query = select_query.format(col, table_name, apply_patches)
    run_query(query)


def run_update(update_mode):
    col1 = random.randint(0, args.columns - 1)
    col2 = random.randint(0, args.columns - 1)

    v1 = random.randint(0, int(args.scale - 1))
    v2 = random.randint(0, int(math.ceil(args.scale * 1.1)))

    query = alter_query.format(table_name, col1, v1, col2, v2, update_mode)
    run_query(query)


def run_insert():
    global insert_num

    with insert_lock:
        current_num = insert_num
        insert_num += 1

    input_data = run_query(select_data_query.format(current_num)).replace("\n", "")
    query_id = generate_query_id()

    run_query("INSERT INTO {} VALUES ('{}', '{}')".format(table_name_data, query_id, input_data))
    run_query(insert_query.format(table_name), input_data=input_data, query_id=query_id)


def flush_logs():
    if cloud_mode:
        run_query("SYSTEM FLUSH LOGS ON CLUSTER default")
    else:
        run_query("SYSTEM FLUSH LOGS")


def print_short_stats():
    print("\n----------------")

    res = run_query("""
        WITH max(event_time_microseconds) - min(event_time_microseconds) AS duration
        SELECT
            if (duration = 0, 0, count() / duration)
        FROM {}
        WHERE query LIKE '%ALTER TABLE%UPDATE%'
            AND query NOT LIKE '%query_log%'
            AND type = 'QueryFinish'
            AND Settings['alter_update_mode'] = 'lightweight'
            AND query_id LIKE '{}%'
        FORMAT CSV
    """.format(query_log_name, query_id_prefix))

    values = [round(float(x)) for x in res.split(',')]
    print("updates rps: {}".format(values[0]))

    res = run_query("""
        SELECT
            avgIf(query_duration_ms, Settings['apply_patch_parts'] == '0'),
            avgIf(query_duration_ms, Settings['apply_patch_parts'] == '1')
        FROM {}
        WHERE query LIKE '%SELECT sum%'
            AND query NOT LIKE '%query_log%'
            AND type = 'QueryFinish'
            AND query_id LIKE '{}%'
        FORMAT CSV
    """.format(query_log_name, query_id_prefix))

    values = [float(x) for x in res.split(',')]
    print("selects slowdown: {}".format(round(values[1] / values[0], 3)))

    res = run_query("""
        SELECT
            count(),
            countIf(startsWith(name, 'patch'))
        FROM system.parts
        WHERE table = '{}' AND active
        FORMAT CSV
    """.format(table_name))

    values = [int(x) for x in res.split(',')]
    print("total parts: {}".format(values[0]))
    print("patch parts: {}".format(values[1]))
    print("----------------")


def print_full_stats():
    print("\n----------------")

    res = run_query("""
        SELECT
            count(),
            avg(query_duration_ms)
        FROM {}
        WHERE query LIKE '%ALTER TABLE%UPDATE%'
            AND query NOT LIKE '%query_log%'
            AND type = 'QueryFinish'
            AND Settings['alter_update_mode'] = 'lightweight'
            AND query_id LIKE '{}%'
        FORMAT CSV
    """.format(query_log_name, query_id_prefix))

    values = [round(float(x), 3) for x in res.split(',')]
    print("num updates: {}".format(values[0]))
    print("avg update duration: {} ms".format(values[1]))

    res = run_query("""
        SELECT
            count(),
            avg(query_duration_ms)
        FROM {}
        WHERE query LIKE '%INSERT INTO%{}%'
            AND query NOT LIKE '%query_log%'
            AND type = 'QueryFinish'
            AND query_id LIKE '{}%'
        FORMAT CSV
    """.format(query_log_name, table_name, query_id_prefix))

    values = [round(float(x), 3) for x in res.split(',')]
    print("num inserts: {}".format(values[0]))
    print("avg insert duration: {} ms".format(values[1]))

    res = run_query("""
        SELECT
            count(),
            avg(query_duration_ms),
        FROM {}
        WHERE query LIKE '%SELECT sum%'
            AND query NOT LIKE '%query_log%'
            AND type = 'QueryFinish'
            AND query_id LIKE '{}%'
            AND Settings['apply_patch_parts'] == '0'
        FORMAT CSV
    """.format(query_log_name, query_id_prefix))

    values = [round(float(x), 3) for x in res.split(',')]
    print("num selects w/o patches: {}".format(values[0]))
    print("avg select duration w/o patches: {} ms".format(values[1]))

    res = run_query("""
        SELECT
            count(),
            avg(query_duration_ms),
            avg(ProfileEvents['PatchesMergeAppliedInAllReadTasks']),
            avg(ProfileEvents['PatchesJoinAppliedInAllReadTasks']),
        FROM {}
        WHERE query LIKE '%SELECT sum%'
            AND query NOT LIKE '%query_log%'
            AND type = 'QueryFinish'
            AND query_id LIKE '{}%'
            AND Settings['apply_patch_parts'] == '1'
        FORMAT CSV
    """.format(query_log_name, query_id_prefix))

    values = [round(float(x), 3) for x in res.split(',')]
    print("num selects w/ patches: {}".format(values[0]))
    print("avg select duration w/ patches: {} ms".format(values[1]))
    print("avg applied merge patches: {}".format(values[2]))
    print("avg applied join patches: {}".format(values[3]))
    print("----------------")


def run_stats():
    while not stop_event.is_set():
        time.sleep(5)
        flush_logs()
        print_short_stats()


def run_timeout():
    time.sleep(args.timeout)
    stop_event.set()


threads = []

if args.verbose:
    threads.append(Thread(target=run_stats))

for i in range(args.threads // 2):
    threads.append(Thread(target=run_queries, args=(lambda: run_select(0),)))

for i in range(args.threads // 2):
    threads.append(Thread(target=run_queries, args=(lambda: run_select(1),)))

if args.with_mutations:
    threads.append(Thread(target=run_queries, args=(lambda: run_update("heavy"), 0.5,)))

for i in range(args.threads):
    threads.append(Thread(target=run_queries, args=(run_insert,)))

for i in range(args.threads):
    threads.append(Thread(target=run_queries, args=(lambda: run_update("lightweight"),)))

timeout_thread = Thread(target=run_timeout)
timeout_thread.start()

for thread in threads:
    thread.start()

for thread in threads:
    thread.join()

timeout_thread.join()

print("\nFinished processing queries")
flush_logs()

res = run_query("""
    SELECT message FROM {}
    WHERE message LIKE '%Found patch part%that intersects mutation%' AND level = 'Error' AND message LIKE '%LOGICAL_ERROR%'
    LIMIT 1
""".format(text_log_name))

if res:
    print("Found intersecting patch part: {}".format(res))
    exit(1)

print_short_stats()
print_full_stats()
print()

if not args.skip_check:
    if args.engine == "ReplicatedMergeTree":
        run_query("SYSTEM SYNC REPLICA {}".format(table_name))

    if args.engine == "MergeTree" and not cloud_mode:
        args_func = "argMin"
        block_num_func = "min"
    else:
        args_func = "argMax"
        block_num_func = "max"

    res = run_query(f"""
        WITH toUInt64(value1) AS block_num, '{query_id_prefix}%' AS query_id_prefix
        SELECT
            query_id,
            {args_func}(query, block_num),
        FROM {text_log_name} AS text_log
        LEFT JOIN
        (
            SELECT
                query,
                query_id
            FROM {query_log_name}
            WHERE (query_id LIKE query_id_prefix) AND (type = 'QueryFinish')
        ) AS query_log USING (query_id)
        WHERE (query_id LIKE query_id_prefix) AND (message LIKE '%Allocated block number%') AND (value2 = 'all' OR value2 = '')
        GROUP BY query_id
        ORDER BY {block_num_func}(block_num) ASC
        FORMAT CSVWithNames
    """)

    df = pd.read_csv(io.StringIO(res))

    print("Replaying query log on RocksDB")

    for i, row in df.iterrows():
        if i % 100 == 0:
            print("processed {} out of {}".format(i, len(df)))

        query = row.iloc[1]
        query_id = row.iloc[0]

        inserted_data = run_query("SELECT data FROM {} WHERE query_id = '{}'".format(table_name_data, query_id))
        if inserted_data:
            run_query(insert_query.format(table_name_reference), input_data=inserted_data)
        else:
            run_query(query.replace(table_name, table_name_reference))

    res = run_query("SELECT count(), sum(cityHash64(*)) FROM {} SETTINGS select_sequential_consistency = 1".format(table_name))
    res_reference = run_query("SELECT count(), sum(cityHash64(*)) FROM {}".format(table_name_reference))

    if res != res_reference:
        print("Data differs in tables")
        print("In main table: ", res)
        print("In reference table: ", res_reference)
        exit(1)

print("OK")
