import os


def argparser(parser):
    """Default argument parser for regressions."""
    parser.add_argument(
        "--local",
        action="store_true",
        help="run regression in local mode",
        default=False,
    )

    parser.add_argument(
        "--clickhouse-version",
        type=str,
        dest="clickhouse_version",
        help="clickhouse server version",
        metavar="version",
        default=os.getenv("CLICKHOUSE_TESTS_SERVER_VERSION", None),
    )

    parser.add_argument(
        "--clickhouse-binary-path",
        type=str,
        dest="clickhouse_binary_path",
        help="path to ClickHouse binary, default: /usr/bin/clickhouse",
        metavar="path",
        default=os.getenv("CLICKHOUSE_TESTS_SERVER_BIN_PATH", "/usr/bin/clickhouse"),
    )

    parser.add_argument(
        "--stress",
        action="store_true",
        default=False,
        help="enable stress testing (might take a long time)",
    )
