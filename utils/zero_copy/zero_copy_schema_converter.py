#!/usr/bin/env python3
import argparse
import socket
import uuid

from kazoo.client import KazooClient


def parse_args():
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--hosts",
        default=socket.getfqdn() + ":2181",
        help="ZooKeeper hosts (host:port,host:port,...)",
    )
    parser.add_argument(
        "-s",
        "--secure",
        default=False,
        action="store_true",
        help="Use secure connection",
    )
    parser.add_argument("--cert", default="", help="Client TLS certificate file")
    parser.add_argument("--key", default="", help="Client TLS key file")
    parser.add_argument("--ca", default="", help="Client TLS ca file")
    parser.add_argument("-u", "--user", default="", help="ZooKeeper ACL user")
    parser.add_argument("-p", "--password", default="", help="ZooKeeper ACL password")
    parser.add_argument(
        "-r", "--root", default="/clickhouse", help="ZooKeeper root path for ClickHouse"
    )
    parser.add_argument(
        "-z",
        "--zcroot",
        default="clickhouse/zero_copy",
        help="ZooKeeper node for new zero-copy data",
    )
    parser.add_argument(
        "--dryrun",
        default=False,
        action="store_true",
        help="Do not perform any actions",
    )
    parser.add_argument(
        "--cleanup", default=False, action="store_true", help="Clean old nodes"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", default=False, help="Verbose mode"
    )

    return parser.parse_args()


# Several folders to heuristic that zookeepr node is folder node
# May be false positive when someone creates set of tables with same paths
table_nodes = [
    "alter_partition_version",
    "block_numbers",
    "blocks",
    "columns",
    "leader_election",
]
zc_nodes = ["zero_copy_s3", "zero_copy_hdfs"]


def convert_node(client, args, path, zc_node):
    base_path = f"{path}/{zc_node}/shared"
    parts = client.get_children(base_path)
    table_id_path = f"{path}/table_shared_id"
    table_id = ""
    if client.exists(table_id_path):
        table_id = client.get(table_id_path)[0].decode("UTF-8")
    else:
        table_id = str(uuid.uuid4())
        if args.verbose:
            print(f'Make table_id "{table_id_path}" = "{table_id}"')
        if not args.dryrun:
            client.create(table_id_path, bytes(table_id, "UTF-8"))
    for part in parts:
        part_path = f"{base_path}/{part}"
        uniq_ids = client.get_children(part_path)
        for uniq_id in uniq_ids:
            uniq_path = f"{part_path}/{uniq_id}"
            replicas = client.get_children(uniq_path)
            for replica in replicas:
                replica_path = f"{uniq_path}/{replica}"
                new_path = f"{args.root}/{args.zcroot}/{zc_node}/{table_id}/{part}/{uniq_id}/{replica}"
                if not client.exists(new_path):
                    if args.verbose:
                        print(f'Make node "{new_path}"')
                    if not args.dryrun:
                        client.ensure_path(
                            f"{args.root}/{args.zcroot}/{zc_node}/{table_id}/{part}/{uniq_id}"
                        )
                        client.create(new_path, value=b"lock")
                if args.cleanup:
                    if args.verbose:
                        print(f'Remove node "{replica_path}"')
                    if not args.dryrun:
                        client.delete(replica_path)
            if args.cleanup and not args.dryrun:
                client.delete(uniq_path)
        if args.cleanup and not args.dryrun:
            client.delete(part_path)
    if args.cleanup and not args.dryrun:
        client.delete(base_path)
        client.delete(f"{path}/{zc_node}")


def convert_table(client, args, path, nodes):
    print(f'Convert table nodes by path "{path}"')
    for zc_node in zc_nodes:
        if zc_node in nodes:
            convert_node(client, args, path, zc_node)


def is_like_a_table(nodes):
    for tn in table_nodes:
        if tn not in nodes:
            return False
    return True


def scan_recursive(client, args, path):
    nodes = client.get_children(path)
    if is_like_a_table(nodes):
        convert_table(client, args, path, nodes)
    else:
        for node in nodes:
            scan_recursive(client, args, f"{path}/{node}")


def scan(client, args):
    nodes = client.get_children(args.root)
    for node in nodes:
        if node != args.zcroot:
            scan_recursive(client, args, f"{args.root}/{node}")


def get_client(args):
    client = KazooClient(
        connection_retry=3,
        command_retry=3,
        timeout=1,
        hosts=args.hosts,
        use_ssl=args.secure,
        certfile=args.cert,
        keyfile=args.key,
        ca=args.ca,
    )
    client.start()
    if args.user and args.password:
        client.add_auth("digest", f"{args.user}:{args.password}")
    return client


def main():
    args = parse_args()
    client = get_client(args)
    scan(client, args)


if __name__ == "__main__":
    main()
