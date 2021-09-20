import os

def onoff(v):
    if v in ["yes", "1", "on"]:
        return True
    elif v in ["no", "0", "off"]:
        return False
    raise ValueError(f"invalid {v}")

def argparser(parser):
    """Default argument parser for regressions.
    """
    parser.add_argument("--local",
        action="store_true",
        help="run regression in local mode", default=False)

    parser.add_argument("--clickhouse-binary-path",
        type=str, dest="clickhouse_binary_path",
        help="path to ClickHouse binary, default: /usr/bin/clickhouse", metavar="path",
        default=os.getenv("CLICKHOUSE_TESTS_SERVER_BIN_PATH", "/usr/bin/clickhouse"))

    parser.add_argument("--stress", action="store_true", default=False,
        help="enable stress testing (might take a long time)")

    parser.add_argument("--parallel", type=onoff, default=True, choices=["yes", "no", "on", "off", 0, 1],
        help="enable parallelism for tests that support it")