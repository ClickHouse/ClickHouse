import os
import time
from contextlib import contextmanager

import testflows.settings as settings
from testflows.core import *
from testflows.asserts import error
from ldap.authentication.tests.common import (
    getuid,
    Config,
    ldap_servers,
    add_config,
    modify_config,
    restart,
)
from ldap.authentication.tests.common import (
    xmltree,
    xml_indent,
    xml_append,
    xml_with_utf8,
)
from ldap.authentication.tests.common import (
    ldap_user,
    ldap_users,
    add_user_to_ldap,
    delete_user_from_ldap,
)
from ldap.authentication.tests.common import (
    change_user_password_in_ldap,
    change_user_cn_in_ldap,
)
from ldap.authentication.tests.common import create_ldap_servers_config_content
from ldap.authentication.tests.common import randomword


@contextmanager
def table(name, create_statement, on_cluster=False):
    node = current().context.node
    try:
        with Given(f"I have a {name} table"):
            node.query(create_statement.format(name=name))
        yield name
    finally:
        with Finally("I drop the table"):
            if on_cluster:
                node.query(f"DROP TABLE IF EXISTS {name} ON CLUSTER {on_cluster}")
            else:
                node.query(f"DROP TABLE IF EXISTS {name}")


@contextmanager
def rbac_users(*users, node=None):
    if node is None:
        node = current().context.node
    try:
        with Given("I have local users"):
            for user in users:
                with By(f"creating user {user['cn']}", format_name=False):
                    node.query(
                        f"CREATE USER OR REPLACE {user['cn']} IDENTIFIED WITH PLAINTEXT_PASSWORD BY '{user['userpassword']}'"
                    )
        yield users
    finally:
        with Finally("I drop local users"):
            for user in users:
                with By(f"dropping user {user['cn']}", flags=TE, format_name=False):
                    node.query(f"DROP USER IF EXISTS {user['cn']}")


@contextmanager
def rbac_roles(*roles, node=None):
    if node is None:
        node = current().context.node
    try:
        with Given("I have roles"):
            for role in roles:
                with By(f"creating role {role}"):
                    node.query(f"CREATE ROLE OR REPLACE {role}")
        yield roles
    finally:
        with Finally("I drop the roles"):
            for role in roles:
                with By(f"dropping role {role}", flags=TE):
                    node.query(f"DROP ROLE IF EXISTS {role}")


def verify_ldap_user_exists(server, username, password):
    """Check that LDAP user is defined on the LDAP server."""
    with By("searching LDAP database"):
        ldap_node = current().context.cluster.node(server)
        r = ldap_node.command(
            f"ldapwhoami -H ldap://localhost -D 'cn={user_name},ou=users,dc=company,dc=com' -w {password}"
        )
        assert r.exitcode == 0, error()


def create_ldap_external_user_directory_config_content(
    server=None, roles=None, **kwargs
):
    """Create LDAP external user directory configuration file content."""
    return create_entries_ldap_external_user_directory_config_content(
        entries=[([server], [roles])], **kwargs
    )


def create_entries_ldap_external_user_directory_config_content(
    entries,
    config_d_dir="/etc/clickhouse-server/config.d",
    config_file="ldap_external_user_directories.xml",
):
    """Create configurattion file content that contains
    one or more entries for the LDAP external user directory.

    For example,

    ```xml
        <user_directories>
            <ldap>
                <server>my_ldap_server</server>
                <roles>
                    <my_local_role1 />
                    <my_local_role2 />
                </roles>
            </ldap>
        </user_directories>
    ```
    """
    uid = getuid()
    path = os.path.join(config_d_dir, config_file)
    name = config_file

    root = xmltree.fromstring(
        "<clickhouse><user_directories></user_directories></clickhouse>"
    )
    xml_user_directories = root.find("user_directories")
    xml_user_directories.append(
        xmltree.Comment(text=f"LDAP external user directories {uid}")
    )

    for entry in entries:
        servers, roles_entries = entry
        xml_directory = xmltree.Element("ldap")
        for server in servers:
            if server is not None:
                xml_append(xml_directory, "server", server)
        if roles_entries:
            for roles_entry in roles_entries:
                xml_roles = xmltree.Element("roles")
                if roles_entry:
                    for role in roles_entry:
                        if role is not None:
                            xml_append(xml_roles, role, "")
                xml_directory.append(xml_roles)
        xml_user_directories.append(xml_directory)

    xml_indent(root)
    content = xml_with_utf8 + str(
        xmltree.tostring(root, short_empty_elements=False, encoding="utf-8"), "utf-8"
    )

    return Config(content, path, name, uid, "config.xml")


def invalid_ldap_external_user_directory_config(
    server, roles, message, tail=30, timeout=60, config=None
):
    """Check that ClickHouse errors when trying to load invalid LDAP external user directory
    configuration file.
    """
    cluster = current().context.cluster
    node = current().context.node

    if config is None:
        config = create_ldap_external_user_directory_config_content(
            server=server, roles=roles
        )

    try:
        with Given("I prepare the error log by writting empty lines into it"):
            node.command(
                'echo -e "%s" > /var/log/clickhouse-server/clickhouse-server.err.log'
                % ("-\\n" * tail)
            )

        with When("I add the config", description=config.path):
            command = f"cat <<HEREDOC > {config.path}\n{config.content}\nHEREDOC"
            node.command(command, steps=False, exitcode=0)

        with Then(
            f"{config.preprocessed_name} should be updated",
            description=f"timeout {timeout}",
        ):
            started = time.time()
            command = f"cat /var/lib/clickhouse/preprocessed_configs/{config.preprocessed_name} | grep {config.uid}{' > /dev/null' if not settings.debug else ''}"
            while time.time() - started < timeout:
                exitcode = node.command(command, steps=False).exitcode
                if exitcode == 0:
                    break
                time.sleep(1)
            assert exitcode == 0, error()

        with When("I restart ClickHouse to apply the config changes"):
            node.restart(safe=False, wait_healthy=False)

    finally:
        with Finally(f"I remove {config.name}"):
            with By("removing invalid configuration file"):
                system_config_path = os.path.join(
                    current_dir(),
                    "..",
                    "configs",
                    node.name,
                    "config.d",
                    config.path.split("config.d/")[-1],
                )
                cluster.command(
                    None, f"rm -rf {system_config_path}", timeout=timeout, exitcode=0
                )

            with And("restarting the node"):
                node.restart(safe=False)

    with Then("error log should contain the expected error message"):
        started = time.time()
        command = f'tail -n {tail} /var/log/clickhouse-server/clickhouse-server.err.log | grep "{message}"'
        while time.time() - started < timeout:
            exitcode = node.command(command, steps=False).exitcode
            if exitcode == 0:
                break
            time.sleep(1)
        assert exitcode == 0, error()


@contextmanager
def ldap_external_user_directory(
    server,
    roles,
    config_d_dir="/etc/clickhouse-server/config.d",
    config_file=None,
    timeout=60,
    restart=True,
    config=None,
):
    """Add LDAP external user directory."""
    if config_file is None:
        config_file = f"ldap_external_user_directory_{getuid()}.xml"
    if config is None:
        config = create_ldap_external_user_directory_config_content(
            server=server,
            roles=roles,
            config_d_dir=config_d_dir,
            config_file=config_file,
        )
    return add_config(config, restart=restart)


def login(servers, directory_server, *users, config=None):
    """Configure LDAP server and LDAP external user directory and
    try to login and execute a query"""
    with ldap_servers(servers):
        with rbac_roles(f"role_{getuid()}") as roles:
            with ldap_external_user_directory(
                server=servers[directory_server]["host"],
                roles=roles,
                restart=True,
                config=config,
            ):
                for user in users:
                    if user.get("login", False):
                        with When(f"I login as {user['username']} and execute query"):
                            current().context.node.query(
                                "SELECT 1",
                                settings=[
                                    ("user", user["username"]),
                                    ("password", user["password"]),
                                ],
                                exitcode=user.get("exitcode", None),
                                message=user.get("message", None),
                            )


@TestStep(When)
@Name("I login as {username} and execute query")
def login_and_execute_query(
    self,
    username,
    password,
    exitcode=None,
    message=None,
    steps=True,
    timeout=60,
    poll=False,
):
    if poll:
        start_time = time.time()
        attempt = 0

        with By("repeatedly trying to login until successful or timeout"):
            while True:
                with When(f"attempt #{attempt}"):
                    r = self.context.node.query(
                        "SELECT 1",
                        settings=[("user", username), ("password", password)],
                        no_checks=True,
                        steps=False,
                        timeout=timeout,
                    )

                if r.exitcode == (0 if exitcode is None else exitcode) and (
                    message in r.output if message is not None else True
                ):
                    break

                if time.time() - start_time > timeout:
                    fail(f"timeout {timeout} trying to login")

                attempt += 1
    else:
        self.context.node.query(
            "SELECT 1",
            settings=[("user", username), ("password", password)],
            exitcode=(0 if exitcode is None else exitcode),
            message=message,
            steps=steps,
            timeout=timeout,
        )
