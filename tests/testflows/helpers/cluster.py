import os
import time
import inspect
import threading
import tempfile

from testflows.core import *
from testflows.asserts import error
from testflows.connect import Shell

class QueryRuntimeException(Exception):
    """Exception during query execution on the server.
    """
    pass

class Node(object):
    """Generic cluster node.
    """
    config_d_dir = "/etc/clickhouse-server/config.d/"

    def __init__(self, cluster, name):
        self.cluster = cluster
        self.name = name

    def repr(self):
        return f"Node(name='{self.name}')"

    def restart(self, timeout=120, safe=True):
        """Restart node.
        """
        with self.cluster.lock:
            for key in list(self.cluster._bash.keys()):
                if key.endswith(f"-{self.name}"):
                    shell = self.cluster._bash.pop(key)
                    shell.__exit__(None, None, None)

        self.cluster.command(None, f'{self.cluster.docker_compose} restart {self.name}', timeout=timeout)

    def command(self, *args, **kwargs):
        return self.cluster.command(self.name, *args, **kwargs)

class ClickHouseNode(Node):
    """Node with ClickHouse server.
    """
    def wait_healthy(self, timeout=120):
        with By(f"waiting until container {self.name} is healthy"):
            start_time = time.time()
            while True:
                if self.query("select 1", no_checks=1, timeout=120, steps=False).exitcode == 0:
                    break
                if time.time() - start_time < timeout:
                    time.sleep(2)
                    continue
                assert False, "container is not healthy"

    def restart(self, timeout=120, safe=True):
        """Restart node.
        """
        if safe:
            self.query("SYSTEM STOP MOVES")
            self.query("SYSTEM STOP MERGES")
            self.query("SYSTEM FLUSH LOGS")
            with By("waiting for 5 sec for moves and merges to stop"):
                time.sleep(5)
            with And("forcing to sync everything to disk"):
                self.command("sync", timeout=30)

        with self.cluster.lock:
            for key in list(self.cluster._bash.keys()):
                if key.endswith(f"-{self.name}"):
                    shell = self.cluster._bash.pop(key)
                    shell.__exit__(None, None, None)

        self.cluster.command(None, f'{self.cluster.docker_compose} restart {self.name}', timeout=timeout)

        self.wait_healthy(timeout)

    def query(self, sql, message=None, exitcode=None, steps=True, no_checks=False,
              raise_on_exception=False, step=By, settings=None, *args, **kwargs):
        """Execute and check query.

        :param sql: sql query
        :param message: expected message that should be in the output, default: None
        :param exitcode: expected exitcode, default: None
        """
        if len(sql) > 1024:
            with tempfile.NamedTemporaryFile("w", encoding="utf-8") as query:
                query.write(sql)
                query.flush()
                command = f"cat \"{query.name}\" | {self.cluster.docker_compose} exec -T {self.name} clickhouse client -n"
                for setting in settings or []:
                    name, value = setting
                    command += f" --{name} \"{value}\""
                description = f"""
                    echo -e \"{sql[:100]}...\" > {query.name} 
                    {command}
                """
                with step("executing command", description=description) if steps else NullStep():
                    r = self.cluster.bash(None)(command, *args, **kwargs)
        else:
            command = f"echo -e \"{sql}\" | clickhouse client -n"
            for setting in settings or []:
                name, value = setting
                command += f" --{name} \"{value}\""
            with step("executing command", description=command) if steps else NullStep():
                r = self.cluster.bash(self.name)(command, *args, **kwargs)

        if no_checks:
            return r

        if exitcode is not None:
            with Then(f"exitcode should be {exitcode}") if steps else NullStep():
                assert r.exitcode == exitcode, error(r.output)

        if message is not None:
            with Then(f"output should contain message", description=message) if steps else NullStep():
                assert message in r.output, error(r.output)

        if message is None or "Exception:" not in message:
            with Then("check if output has exception") if steps else NullStep():
                if "Exception:" in r.output:
                    if raise_on_exception:
                        raise QueryRuntimeException(r.output)
                    assert False, error(r.output)

        return r

class Cluster(object):
    """Simple object around docker-compose cluster.
    """
    def __init__(self, local=False, 
            clickhouse_binary_path=None, configs_dir=None, 
            nodes=None,
            docker_compose="docker-compose", docker_compose_project_dir=None, 
            docker_compose_file="docker-compose.yml"):
        
        self._bash = {}
        self.clickhouse_binary_path = clickhouse_binary_path
        self.configs_dir = configs_dir
        self.local = local
        self.nodes = nodes or {}
        self.docker_compose = docker_compose

        frame = inspect.currentframe().f_back
        caller_dir = os.path.dirname(os.path.abspath(frame.f_globals["__file__"]))

        # auto set configs directory
        if self.configs_dir is None:
            caller_configs_dir = caller_dir
            if os.path.exists(caller_configs_dir):
                self.configs_dir = caller_configs_dir

        if not os.path.exists(self.configs_dir):
            raise TypeError("configs directory '{self.configs_dir}' does not exist")

        # auto set docker-compose project directory
        if docker_compose_project_dir is None:
            caller_project_dir = os.path.join(caller_dir, "docker-compose")
            if os.path.exists(caller_project_dir):
                docker_compose_project_dir = caller_project_dir

        docker_compose_file_path = os.path.join(docker_compose_project_dir or "", docker_compose_file) 

        if not os.path.exists(docker_compose_file_path):
            raise TypeError("docker compose file '{docker_compose_file_path}' does not exist")

        self.docker_compose += f" --project-directory \"{docker_compose_project_dir}\" --file \"{docker_compose_file_path}\""
        self.lock = threading.Lock()

    def shell(self, node, timeout=120):
        """Returns unique shell terminal to be used.
        """
        if node is None:
            return Shell()

        shell = Shell(command=[
                "/bin/bash", "--noediting", "-c", f"{self.docker_compose} exec {node} bash --noediting"
            ], name=node)

        shell.timeout = timeout
        return shell

    def bash(self, node, timeout=120):
        """Returns thread-local bash terminal
        to a specific node.

        :param node: name of the service
        """
        current_thread = threading.current_thread()
        id = f"{current_thread.ident}-{node}"
        with self.lock:
            if self._bash.get(id) is None:
                if node is None:
                    self._bash[id] = Shell().__enter__()
                else:
                    self._bash[id] = Shell(command=[
                        "/bin/bash", "--noediting", "-c", f"{self.docker_compose} exec {node} bash --noediting"
                    ], name=node).__enter__()
                self._bash[id].timeout = timeout
            return self._bash[id]

    def __enter__(self):
        with Given("docker-compose cluster"):
            self.up()
        return self

    def __exit__(self, type, value, traceback):
        try:
            with Finally("I clean up"):
                self.down()
        finally:
            with self.lock:
                for shell in self._bash.values():
                    shell.__exit__(type, value, traceback)

    def node(self, name):
        """Get object with node bound methods.

        :param name: name of service name
        """
        if name.startswith("clickhouse"):
            return ClickHouseNode(self, name)
        return Node(self, name)

    def down(self, timeout=120):
        """Bring cluster down by executing docker-compose down."""
        try:
            bash = self.bash(None)
            with self.lock:
                # remove and close all not None node terminals
                for id in list(self._bash.keys()):
                    shell = self._bash.pop(id)
                    if shell is not bash:
                        shell.__exit__(None, None, None)
                    else:
                        self._bash[id] = shell
        finally:
            return self.command(None, f"{self.docker_compose} down", timeout=timeout)

    def up(self):
        if self.local:
            with Given("I am running in local mode"):
                with Then("check --clickhouse-binary-path is specified"):
                    assert self.clickhouse_binary_path, "when running in local mode then --clickhouse-binary-path must be specified"
                with And("path should exist"):
                    assert os.path.exists(self.clickhouse_binary_path)

            os.environ["CLICKHOUSE_TESTS_SERVER_BIN_PATH"] = self.clickhouse_binary_path
            os.environ["CLICKHOUSE_TESTS_ODBC_BRIDGE_BIN_PATH"] = os.path.join(os.path.dirname(self.clickhouse_binary_path),
                                                                               "clickhouse-odbc-bridge")
            os.environ["CLICKHOUSE_TESTS_DIR"] = self.configs_dir

            with Given("docker-compose"):
                self.command(None, "env | grep CLICKHOUSE")
                cmd = self.command(None, f'{self.docker_compose} up -d 2>&1 | tee', timeout=30 * 60)
        else:
            with Given("docker-compose"):
                cmd = self.command(None, f'{self.docker_compose} up -d --no-recreate 2>&1 | tee')

        with Then("check there are no unhealthy containers"):
            assert "is unhealthy" not in cmd.output, error()

        with Then("wait all nodes report healhy"):
            for name in self.nodes["clickhouse"]:
                self.node(name).wait_healthy()

    def command(self, node, command, message=None, exitcode=None, steps=True, *args, **kwargs):
        """Execute and check command.

        :param node: name of the service
        :param command: command
        :param message: expected message that should be in the output, default: None
        :param exitcode: expected exitcode, default: None
        :param steps: don't break command into steps, default: True
        """
        debug(f"command() {node}, {command}")
        with By("executing command", description=command) if steps else NullStep():
            r = self.bash(node)(command, *args, **kwargs)
        if exitcode is not None:
            with Then(f"exitcode should be {exitcode}") if steps else NullStep():
                assert r.exitcode == exitcode, error(r.output)
        if message is not None:
            with Then(f"output should contain message", description=message) if steps else NullStep():
                assert message in r.output, error(r.output)
        return r
