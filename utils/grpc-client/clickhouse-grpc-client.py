#!/usr/bin/env python3

# This utility provides similar interface to clickhouse-client.
# This utility also can work in two modes: interactive and non-interactive (batch).
#
# For example,
# ./clickhouse_grpc_client.py - runs interactive mode; and
# ./clickhouse_grpc_client.py -u John -q "SELECT * FROM mytable" - runs only a specified query from the user John.
#
# Most of the command line options are the same, for more information type
# ./clickhouse_grpc_client.py --help

import grpc  # pip3 install grpcio
import grpc_tools  # pip3 install grpcio-tools
import argparse, cmd, os, signal, subprocess, sys, threading, time, uuid

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 9100
DEFAULT_USER_NAME = "default"
DEFAULT_OUTPUT_FORMAT_FOR_INTERACTIVE_MODE = "PrettyCompact"
HISTORY_FILENAME = "~/.clickhouse_grpc_history"
HISTORY_SIZE = 1000
STDIN_BUFFER_SIZE = 1048576
DEFAULT_ENCODING = "utf-8"


class ClickHouseGRPCError(Exception):
    pass


# Temporary override reaction on Ctrl+C.
class KeyboardInterruptHandlerOverride:
    # If `handler` return True that means pressing Ctrl+C has been handled, no need to call previous handler.
    def __init__(self, handler):
        self.handler = handler

    def __enter__(self):
        self.previous_handler = signal.signal(signal.SIGINT, self.__handler)

    def __exit__(self, exc_type, exc_value, traceback):
        signal.signal(signal.SIGINT, self.previous_handler)

    def __handler(self, signum, frame):
        if not self.handler():
            self.previous_handler(signum, frame)


# Print to stderr
def error_print(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


class ClickHouseGRPCClient(cmd.Cmd):
    prompt = "grpc :) "

    def __init__(
        self,
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        user_name=DEFAULT_USER_NAME,
        password="",
        database="",
        output_format="",
        settings="",
        verbatim=False,
        show_debug_info=False,
    ):
        super(ClickHouseGRPCClient, self).__init__(completekey=None)
        self.host = host
        self.port = port
        self.user_name = user_name
        self.password = password
        self.database = database
        self.output_format = output_format
        self.settings = settings
        self.verbatim = verbatim
        self.show_debug_info = show_debug_info
        self.channel = None
        self.stub = None
        self.session_id = None

    def __enter__(self):
        ClickHouseGRPCClient.__generate_pb2()
        ClickHouseGRPCClient.__import_pb2()
        self.__connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.__disconnect()

    # Executes a simple query and returns its output.
    def get_simple_query_output(self, query_text):
        result = self.stub.ExecuteQuery(
            clickhouse_grpc_pb2.QueryInfo(
                query=query_text,
                user_name=self.user_name,
                password=self.password,
                database=self.database,
                output_format="TabSeparated",
                settings=self.settings,
                session_id=self.session_id,
                query_id=str(uuid.uuid4()),
            )
        )
        if self.show_debug_info:
            print("\nresult={}".format(result))
        ClickHouseGRPCClient.__check_no_errors(result)
        return result.output.decode(DEFAULT_ENCODING)

    # Executes a query using the stream IO and with ability to cancel it by pressing Ctrl+C.
    def run_query(self, query_text, raise_exceptions=True, allow_cancel=False):
        start_time = time.time()
        cancelled = False
        cancel_tries = 0
        cancel_event = threading.Event()

        def keyboard_interrupt_handler():
            if allow_cancel:
                nonlocal cancel_tries
                cancel_tries = cancel_tries + 1
                if cancel_tries < 3:
                    self.verbatim_print("Cancelling query.")
                    if cancel_tries == 1:
                        cancel_event.set()
                    return True
                # third attempt to cancel - we use previous handler which will terminate the client.
                self.verbatim_print("Couldn't cancel the query, terminating.")
            return False

        with KeyboardInterruptHandlerOverride(keyboard_interrupt_handler):
            try:

                def send_query_info():
                    # send main query info
                    info = clickhouse_grpc_pb2.QueryInfo(
                        query=query_text,
                        user_name=self.user_name,
                        password=self.password,
                        database=self.database,
                        output_format=self.output_format,
                        settings=self.settings,
                        session_id=self.session_id,
                        query_id=str(uuid.uuid4()),
                    )
                    # send input data
                    if not sys.stdin.isatty():
                        while True:
                            info.input_data = sys.stdin.buffer.read(STDIN_BUFFER_SIZE)
                            if not info.input_data:
                                break
                            info.next_query_info = True
                            yield info
                            info = clickhouse_grpc_pb2.QueryInfo()
                    yield info
                    # wait for possible cancel
                    if allow_cancel:
                        cancel_event.wait()
                        if cancel_tries > 0:
                            yield clickhouse_grpc_pb2.QueryInfo(cancel=True)

                for result in self.stub.ExecuteQueryWithStreamIO(send_query_info()):
                    if self.show_debug_info:
                        print("\nresult={}".format(result))
                    ClickHouseGRPCClient.__check_no_errors(result)
                    sys.stdout.buffer.write(result.output)
                    sys.stdout.flush()
                    if result.cancelled:
                        cancelled = True
                        self.verbatim_print("Query was cancelled.")

                cancel_event.set()
                if not cancelled:
                    execution_time = time.time() - start_time
                    self.verbatim_print(
                        "\nElapsed: {execution_time} sec.\n".format(
                            execution_time=execution_time
                        )
                    )

            except Exception as e:
                if raise_exceptions:
                    raise
                error_print(e)

    # Establish connection.
    def __connect(self):
        self.verbatim_print(
            "Connecting to {host}:{port} as user {user_name}.".format(
                host=self.host, port=self.port, user_name=self.user_name
            )
        )
        # Secure channels are supported by server but not supported by this client.
        start_time = time.time()
        self.channel = grpc.insecure_channel(self.host + ":" + str(self.port))
        connection_time = 0
        timeout = 5
        while True:
            try:
                grpc.channel_ready_future(self.channel).result(timeout=timeout)
                break
            except grpc.FutureTimeoutError:
                connection_time += timeout
                self.verbatim_print(
                    "Couldn't connect to ClickHouse server in {connection_time} seconds.".format(
                        connection_time=connection_time
                    )
                )
        self.stub = clickhouse_grpc_pb2_grpc.ClickHouseStub(self.channel)
        connection_time = time.time() - start_time
        if self.verbatim:
            version = self.get_simple_query_output(
                "SELECT version() FORMAT TabSeparated"
            ).rstrip("\n")
            self.verbatim_print(
                "Connected to ClickHouse server version {version} via gRPC protocol in {connection_time}.".format(
                    version=version, connection_time=connection_time
                )
            )

    def __disconnect(self):
        if self.channel:
            self.channel.close()
        self.channel = None
        self.stub = None
        self.session_id = None

    @staticmethod
    def __check_no_errors(result):
        if result.HasField("exception"):
            raise ClickHouseGRPCError(result.exception.display_text)

    # Use grpcio-tools to generate *pb2.py files from *.proto.
    @staticmethod
    def __generate_pb2():
        script_dir = os.path.dirname(os.path.realpath(__file__))
        proto_dir = os.path.join(script_dir, "./protos")
        gen_dir = os.path.join(script_dir, "./_gen")
        if os.path.exists(os.path.join(gen_dir, "clickhouse_grpc_pb2_grpc.py")):
            return
        os.makedirs(gen_dir, exist_ok=True)
        cmd = [
            "python3",
            "-m",
            "grpc_tools.protoc",
            "-I" + proto_dir,
            "--python_out=" + gen_dir,
            "--grpc_python_out=" + gen_dir,
            proto_dir + "/clickhouse_grpc.proto",
        ]
        p = subprocess.Popen(cmd, stderr=subprocess.PIPE)
        # We don't want to show grpc_tools warnings.
        errors = p.stderr.read().decode().strip("\n").split("\n")
        only_warnings = all(("Warning" in error) for error in errors)
        if not only_warnings:
            error_print("\n".join(errors))

    # Import the generated *pb2.py files.
    @staticmethod
    def __import_pb2():
        script_dir = os.path.dirname(os.path.realpath(__file__))
        gen_dir = os.path.join(script_dir, "./_gen")
        sys.path.append(gen_dir)
        global clickhouse_grpc_pb2, clickhouse_grpc_pb2_grpc
        import clickhouse_grpc_pb2, clickhouse_grpc_pb2_grpc

    # Prints only if interactive mode is activated.
    def verbatim_print(self, *args, **kwargs):
        if self.verbatim:
            print(*args, **kwargs)

    # Overrides Cmd.preloop(). Executed once when cmdloop() is called.
    def preloop(self):
        super(ClickHouseGRPCClient, self).preloop()
        ClickHouseGRPCClient.__read_history()
        # we use session for interactive mode
        self.session_id = str(uuid.uuid4())

    # Overrides Cmd.postloop(). Executed once when cmdloop() is about to return.
    def postloop(self):
        super(ClickHouseGRPCClient, self).postloop()
        ClickHouseGRPCClient.__write_history()

    # Overrides Cmd.onecmd(). Runs single command.
    def onecmd(self, line):
        stripped = line.strip()
        if stripped == "exit" or stripped == "quit":
            return True
        if stripped == "":
            return False
        self.run_query(line, raise_exceptions=False, allow_cancel=True)
        return False

    # Enables history of commands for interactive mode.
    @staticmethod
    def __read_history():
        global readline
        try:
            import readline
        except ImportError:
            readline = None
        histfile = os.path.expanduser(HISTORY_FILENAME)
        if readline and os.path.exists(histfile):
            readline.read_history_file(histfile)

    @staticmethod
    def __write_history():
        global readline
        if readline:
            readline.set_history_length(HISTORY_SIZE)
            histfile = os.path.expanduser(HISTORY_FILENAME)
            readline.write_history_file(histfile)


# MAIN


def main(args):
    parser = argparse.ArgumentParser(
        description="ClickHouse client accessing server through gRPC protocol.",
        add_help=False,
    )
    parser.add_argument(
        "--help", help="Show this help message and exit", action="store_true"
    )
    parser.add_argument(
        "--host",
        "-h",
        help="The server name, ‘localhost’ by default. You can use either the name or the IPv4 or IPv6 address.",
        default="localhost",
    )
    parser.add_argument(
        "--port",
        help="The port to connect to. This port should be enabled on the ClickHouse server (see grpc_port in the config).",
        default=9100,
    )
    parser.add_argument(
        "--user",
        "-u",
        dest="user_name",
        help="The username. Default value: ‘default’.",
        default="default",
    )
    parser.add_argument(
        "--password", help="The password. Default value: empty string.", default=""
    )
    parser.add_argument(
        "--query",
        "-q",
        help="The query to process when using non-interactive mode.",
        default="",
    )
    parser.add_argument(
        "--database",
        "-d",
        help="Select the current default database. Default value: the current database from the server settings (‘default’ by default).",
        default="",
    )
    parser.add_argument(
        "--format",
        "-f",
        dest="output_format",
        help="Use the specified default format to output the result.",
        default="",
    )
    parser.add_argument(
        "--debug",
        dest="show_debug_info",
        help="Enables showing the debug information.",
        action="store_true",
    )
    args = parser.parse_args(args)

    if args.help:
        parser.print_help()
        sys.exit(0)

    interactive_mode = not args.query
    verbatim = interactive_mode

    output_format = args.output_format
    if not output_format and interactive_mode:
        output_format = DEFAULT_OUTPUT_FORMAT_FOR_INTERACTIVE_MODE

    try:
        with ClickHouseGRPCClient(
            host=args.host,
            port=args.port,
            user_name=args.user_name,
            password=args.password,
            database=args.database,
            output_format=output_format,
            verbatim=verbatim,
            show_debug_info=args.show_debug_info,
        ) as client:
            if interactive_mode:
                client.cmdloop()
            else:
                client.run_query(args.query)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        error_print(e)

    if verbatim:
        print("\nBye")


if __name__ == "__main__":
    main(sys.argv[1:])
