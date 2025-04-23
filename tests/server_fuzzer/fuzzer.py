import argparse
import atexit
import pathlib
import random
import time
import sys

sys.path.append('..')
from integration.helpers.cluster import ClickHouseCluster
from generators import BuzzHouseGenerator
from properties import modify_server_settings_with_random_properties

parser = argparse.ArgumentParser()
parser.add_argument("-b", "--cbinary", type = pathlib.Path, help = 'Path to client binary')
parser.add_argument("-c", "--cconfig", type = pathlib.Path, help = 'Path to client configuration file')
parser.add_argument("-g", "--generator", choices =['buzzhouse'], type = str.lower, required = True, help = 'What generator to use')
parser.add_argument("-m", "--mconfig", type = pathlib.Path, help = 'Path to config.xml file')
parser.add_argument("-s", "--seed", type = int, default = 0, help = 'Server fuzzer seed')
parser.add_argument("-u", "--uconfig", type = pathlib.Path, help = 'Path to users.xml file')
args = parser.parse_args()

# Set seed first
if args.seed == 0:
    import secrets
    seed = secrets.randbits(64) #64 - bit random integer
    random.seed(seed)
    print("Using seed: ", seed)
else:
    random.seed(args.seed)

# Set generator, at the moment only BuzzHouse is available
generator = None
if args.generator == 'buzzhouse':
    generator = BuzzHouseGenerator(args.cbinary, args.cconfig)

# Use random server settings sometimes
server_settings = args.mconfig
if server_settings is not None and random.randint(1, 2) == 1:
    server_settings = modify_server_settings_with_random_properties(server_settings)

# Start the cluster
cluster = ClickHouseCluster(__file__)
server = cluster.add_instance("server",
                              with_zookeeper = True,
                              with_minio = True,
                              stay_alive = True,
                              main_configs = [server_settings] if server_settings is not None else [],
                              user_configs = [args.uconfig] if args.uconfig is not None else[],
                              macros = {"shard" : 1, "replica" : 1 })
cluster.start()
print("Starting cluster")
server.wait_start(8)

# Start the load generator
print("Start load generator")
client = generator.run_generator(server)
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
    server.restart_clickhouse(stop_start_wait_sec = 10, kill = kill_server)

cluster.shutdown()
