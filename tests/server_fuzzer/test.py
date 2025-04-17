import argparse
import atexit
import json
import pathlib
import random
import tempfile
import time
import sys

sys.path.append('..')
from integration.helpers.client import CommandRequest
from integration.helpers.cluster import ClickHouseCluster

parser = argparse.ArgumentParser()
parser.add_argument("-b", "--buzzhouse", type=pathlib.Path, required=True)
parser.add_argument("-m", "--mconfig", type=pathlib.Path)
parser.add_argument("-s", "--seed", type=int, default=0)
parser.add_argument("-u", "--uconfig", type=pathlib.Path)

args = parser.parse_args()

if args.seed == 0:
    import secrets

    seed = secrets.randbits(64)  # 64-bit random integer
    random.seed(seed)
    print("Using seed: ", seed)
else:
    random.seed(args.seed)

temp = tempfile.NamedTemporaryFile()
with open(args.buzzhouse, 'r') as file1:
    buzz_config = json.load(file1)
    buzz_config['seed'] = random.randint(1, 18446744073709551615)
    with open(temp.name, "w") as file2:
        file2.write(json.dumps(buzz_config))

# Start the cluster
cluster = ClickHouseCluster(__file__)
server = cluster.add_instance(
    "server",
    with_zookeeper=True,
    with_minio=True,
    stay_alive=True,
    main_configs=[args.mconfig] if args.mconfig is not None else [],
    user_configs=[args.uconfig] if args.uconfig is not None else [],
    macros={"shard": 1, "replica": 1}
)
cluster.start()
print("Starting cluster")
server.wait_start(8)

# Start the load generator, at the moment only BuzzHouse is available
print("Start load generator")
client = CommandRequest([cluster.client_bin_path, "--client", "--host", f"{server.ip_address}", "--port", "9000", f"--buzz-house-config={temp.name}"], stdin="", timeout=None, ignore_error=True, parse=False, stdout=sys.stdout, stderr=sys.stderr)
def client_cleanup():
    if client.process.poll() is None:
        client.process.kill()
atexit.register(client_cleanup)
time.sleep(3)

# This is the main loop, run while client and server are running
while True:
    if client.process.poll() is not None:
        print("Load generator finished")
        break
    try:
        server.query("SELECT 1;")
    except:
        print("The server is not running")
        break

    time.sleep(int(random.uniform(10, 20)))
    kill_server = random.randint(1, 2) == 1
    print("Restart the server with", "kill" if kill_server else "manual shutdown")
    server.restart_clickhouse(stop_start_wait_sec=10, kill=kill_server)

cluster.shutdown()
