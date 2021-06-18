import os
import uuid
import time
import string
import random
import textwrap
import xml.etree.ElementTree as xmltree

from collections import namedtuple
from contextlib import contextmanager

import testflows.settings as settings

from testflows.core import *
from testflows.asserts import error

def getuid():
    return str(uuid.uuid1()).replace('-', '_')

xml_with_utf8 = '<?xml version="1.0" encoding="utf-8"?>\n'

def xml_indent(elem, level=0, by="  "):
    i = "\n" + level * by
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + by
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
        for elem in elem:
            xml_indent(elem, level + 1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i

def xml_append(root, tag, text):
    element = xmltree.Element(tag)
    element.text = text
    root.append(element)
    return element

Config = namedtuple("Config", "content path name uid preprocessed_name")

ASCII_CHARS = string.ascii_lowercase +  string.ascii_uppercase + string.digits

def randomword(length, chars=ASCII_CHARS):
    return ''.join(random.choice(chars) for i in range(length))

def restart(node=None, safe=False, timeout=60):
    """Restart ClickHouse server and wait for config to be reloaded.
    """
    with When("I restart ClickHouse server node"):
        if node is None:
            node = current().context.node

        with node.cluster.shell(node.name) as bash:
            bash.expect(bash.prompt)

            with By("closing terminal to the node to be restarted"):
                bash.close()

            with And("getting current log size"):
                logsize = \
                    node.command("stat --format=%s /var/log/clickhouse-server/clickhouse-server.log").output.split(" ")[
                    0].strip()

            with And("restarting ClickHouse server"):
                node.restart(safe=safe)

            with Then("tailing the log file from using previous log size as the offset"):
                bash.prompt = bash.__class__.prompt
                bash.open()
                bash.send(f"tail -c +{logsize} -f /var/log/clickhouse-server/clickhouse-server.log")

            with And("waiting for config reload message in the log file"):
                bash.expect(
                    f"ConfigReloader: Loaded config '/etc/clickhouse-server/config.xml', performed update on configuration",
                    timeout=timeout)

def add_config(config, timeout=60, restart=False, modify=False):
    """Add dynamic configuration file to ClickHouse.

    :param node: node
    :param config: configuration file description
    :param timeout: timeout, default: 20 sec
    """
    node = current().context.node

    def check_preprocessed_config_is_updated(after_removal=False):
        """Check that preprocessed config is updated.
        """
        started = time.time()
        command = f"cat /var/lib/clickhouse/preprocessed_configs/{config.preprocessed_name} | grep {config.uid}{' > /dev/null' if not settings.debug else ''}"

        while time.time() - started < timeout:
            exitcode = node.command(command, steps=False).exitcode
            if after_removal:
                if exitcode == 1:
                    break
            else:
                if exitcode == 0:
                    break
            time.sleep(1)

        if settings.debug:
            node.command(f"cat /var/lib/clickhouse/preprocessed_configs/{config.preprocessed_name}")

        if after_removal:
            assert exitcode == 1, error()
        else:
            assert exitcode == 0, error()

    def wait_for_config_to_be_loaded():
        """Wait for config to be loaded.
        """
        if restart:
            with When("I close terminal to the node to be restarted"):
                bash.close()

            with And("I stop ClickHouse to apply the config changes"):
                node.stop(safe=False)

            with And("I get the current log size"):
                cmd = node.cluster.command(None,
                    f"stat --format=%s {os.environ['CLICKHOUSE_TESTS_DIR']}/_instances/{node.name}/logs/clickhouse-server.log")
                logsize = cmd.output.split(" ")[0].strip()

            with And("I start ClickHouse back up"):
                node.start()

            with Then("I tail the log file from using previous log size as the offset"):
                bash.prompt = bash.__class__.prompt
                bash.open()
                bash.send(f"tail -c +{logsize} -f /var/log/clickhouse-server/clickhouse-server.log")

        with Then("I wait for config reload message in the log file"):
            if restart:
                bash.expect(
                    f"ConfigReloader: Loaded config '/etc/clickhouse-server/config.xml', performed update on configuration",
                    timeout=timeout)
            else:
                bash.expect(
                    f"ConfigReloader: Loaded config '/etc/clickhouse-server/{config.preprocessed_name}', performed update on configuration",
                    timeout=timeout)

    try:
        with Given(f"{config.name}"):
            if settings.debug:
                with When("I output the content of the config"):
                    debug(config.content)

            with node.cluster.shell(node.name) as bash:
                bash.expect(bash.prompt)
                bash.send("tail -n 0 -f /var/log/clickhouse-server/clickhouse-server.log")

                with When("I add the config", description=config.path):
                    command = f"cat <<HEREDOC > {config.path}\n{config.content}\nHEREDOC"
                    node.command(command, steps=False, exitcode=0)

                with Then(f"{config.preprocessed_name} should be updated", description=f"timeout {timeout}"):
                    check_preprocessed_config_is_updated()

                with And("I wait for config to be reloaded"):
                    wait_for_config_to_be_loaded()
        yield
    finally:
        if not modify:
            with Finally(f"I remove {config.name}"):
                with node.cluster.shell(node.name) as bash:
                    bash.expect(bash.prompt)
                    bash.send("tail -n 0 -f /var/log/clickhouse-server/clickhouse-server.log")

                    with By("removing the config file", description=config.path):
                        node.command(f"rm -rf {config.path}", exitcode=0)

                    with Then(f"{config.preprocessed_name} should be updated", description=f"timeout {timeout}"):
                        check_preprocessed_config_is_updated(after_removal=True)

                    with And("I wait for config to be reloaded"):
                        wait_for_config_to_be_loaded()

def create_ldap_servers_config_content(servers, config_d_dir="/etc/clickhouse-server/config.d", config_file="ldap_servers.xml"):
    """Create LDAP servers configuration content.
    """
    uid = getuid()
    path = os.path.join(config_d_dir, config_file)
    name = config_file

    root = xmltree.fromstring("<yandex><ldap_servers></ldap_servers></yandex>")
    xml_servers = root.find("ldap_servers")
    xml_servers.append(xmltree.Comment(text=f"LDAP servers {uid}"))

    for _name, server in list(servers.items()):
        xml_server = xmltree.Element(_name)
        for key, value in list(server.items()):
            xml_append(xml_server, key, value)
        xml_servers.append(xml_server)

    xml_indent(root)
    content = xml_with_utf8 + str(xmltree.tostring(root, short_empty_elements=False, encoding="utf-8"), "utf-8")

    return Config(content, path, name, uid, "config.xml")

@contextmanager
def modify_config(config, restart=False):
    """Apply updated configuration file.
    """
    return add_config(config, restart=restart, modify=True)

@contextmanager
def ldap_servers(servers, config_d_dir="/etc/clickhouse-server/config.d", config_file="ldap_servers.xml",
        timeout=60, restart=False, config=None):
    """Add LDAP servers configuration.
    """
    if config is None:
        config = create_ldap_servers_config_content(servers, config_d_dir, config_file)
    return add_config(config, restart=restart)

def create_ldap_users_config_content(*users, config_d_dir="/etc/clickhouse-server/users.d", config_file="ldap_users.xml"):
    """Create LDAP users configuration file content.
    """
    uid = getuid()
    path = os.path.join(config_d_dir, config_file)
    name = config_file

    root = xmltree.fromstring("<yandex><users></users></yandex>")
    xml_users = root.find("users")
    xml_users.append(xmltree.Comment(text=f"LDAP users {uid}"))

    for user in users:
        xml_user = xmltree.Element(user['username'])
        xml_user_server = xmltree.Element("ldap")
        xml_append(xml_user_server, "server", user["server"])
        xml_user.append(xml_user_server)
        xml_users.append(xml_user)

    xml_indent(root)
    content = xml_with_utf8 + str(xmltree.tostring(root, short_empty_elements=False, encoding="utf-8"), "utf-8")

    return Config(content, path, name, uid, "users.xml")

def add_users_identified_with_ldap(*users):
    """Add one or more users that are identified via
    an ldap server using RBAC.
    """
    node = current().context.node
    try:
        with Given("I create users"):
            for user in users:
                node.query(f"CREATE USER '{user['username']}' IDENTIFIED WITH ldap SERVER '{user['server']}'")
        yield
    finally:
        with Finally("I remove users"):
            for user in users:
                with By(f"dropping user {user['username']}", flags=TE):
                    node.query(f"DROP USER IF EXISTS '{user['username']}'")

@contextmanager
def ldap_authenticated_users(*users, config_d_dir="/etc/clickhouse-server/users.d",
        config_file=None, timeout=60, restart=True, config=None, rbac=False):
    """Add LDAP authenticated users.
    """
    if rbac:
        return add_users_identified_with_ldap(*users)
    else:
        if config_file is None:
            config_file = f"ldap_users_{getuid()}.xml"
        if config is None:
            config = create_ldap_users_config_content(*users, config_d_dir=config_d_dir, config_file=config_file)
        return add_config(config, restart=restart)

def invalid_server_config(servers, message=None, tail=30, timeout=60):
    """Check that ClickHouse errors when trying to load invalid LDAP servers configuration file.
    """
    node = current().context.node
    if message is None:
        message = "Exception: Failed to merge config with '/etc/clickhouse-server/config.d/ldap_servers.xml'"

    config = create_ldap_servers_config_content(servers)
    try:
        node.command("echo -e \"%s\" > /var/log/clickhouse-server/clickhouse-server.err.log" % ("-\\n" * tail))

        with When("I add the config", description=config.path):
            command = f"cat <<HEREDOC > {config.path}\n{config.content}\nHEREDOC"
            node.command(command, steps=False, exitcode=0)

        with Then("server shall fail to merge the new config"):
            started = time.time()
            command = f"tail -n {tail} /var/log/clickhouse-server/clickhouse-server.err.log | grep \"{message}\""
            while time.time() - started < timeout:
                exitcode = node.command(command, steps=False).exitcode
                if exitcode == 0:
                    break
                time.sleep(1)
            assert exitcode == 0, error()
    finally:
        with Finally(f"I remove {config.name}"):
            with By("removing the config file", description=config.path):
                node.command(f"rm -rf {config.path}", exitcode=0)

def invalid_user_config(servers, config, message=None, tail=30, timeout=60):
    """Check that ClickHouse errors when trying to load invalid LDAP users configuration file.
    """
    node = current().context.node
    if message is None:
        message = "Exception: Failed to merge config with '/etc/clickhouse-server/users.d/ldap_users.xml'"

    with ldap_servers(servers):
        try:
            node.command("echo -e \"%s\" > /var/log/clickhouse-server/clickhouse-server.err.log" % ("\\n" * tail))
            with When("I add the config", description=config.path):
                command = f"cat <<HEREDOC > {config.path}\n{config.content}\nHEREDOC"
                node.command(command, steps=False, exitcode=0)

            with Then("server shall fail to merge the new config"):
                started = time.time()
                command = f"tail -n {tail} /var/log/clickhouse-server/clickhouse-server.err.log | grep \"{message}\""
                while time.time() - started < timeout:
                    exitcode = node.command(command, steps=False).exitcode
                    if exitcode == 0:
                        break
                    time.sleep(1)
                assert exitcode == 0, error()
        finally:
            with Finally(f"I remove {config.name}"):
                with By("removing the config file", description=config.path):
                    node.command(f"rm -rf {config.path}", exitcode=0)

def add_user_to_ldap(cn, userpassword, givenname=None, homedirectory=None, sn=None, uid=None, uidnumber=None, node=None):
    """Add user entry to LDAP."""
    if node is None:
        node = current().context.ldap_node
    if uid is None:
        uid = cn
    if givenname is None:
        givenname = "John"
    if homedirectory is None:
        homedirectory = "/home/users"
    if sn is None:
        sn = "User"
    if uidnumber is None:
        uidnumber = 2000

    user = {
        "dn": f"cn={cn},ou=users,dc=company,dc=com",
        "cn": cn,
        "gidnumber": 501,
        "givenname": givenname,
        "homedirectory": homedirectory,
        "objectclass": ["inetOrgPerson", "posixAccount", "top"],
        "sn": sn,
        "uid": uid,
        "uidnumber": uidnumber,
        "userpassword": userpassword,
        "_server": node.name
    }

    lines = []

    for key, value in list(user.items()):
        if key.startswith("_"):
            continue
        elif key == "objectclass":
            for cls in value:
                lines.append(f"objectclass: {cls}")
        else:
            lines.append(f"{key}: {value}")

    ldif = "\n".join(lines)

    r = node.command(
        f"echo -e \"{ldif}\" | ldapadd -x -H ldap://localhost -D \"cn=admin,dc=company,dc=com\" -w admin")
    assert r.exitcode == 0, error()

    return user

def delete_user_from_ldap(user, node=None, exitcode=0):
    """Delete user entry from LDAP."""
    if node is None:
        node = current().context.ldap_node
    r = node.command(
        f"ldapdelete -x -H ldap://localhost -D \"cn=admin,dc=company,dc=com\" -w admin \"{user['dn']}\"")
    if exitcode is not None:
        assert r.exitcode == exitcode, error()

def change_user_password_in_ldap(user, new_password, node=None, exitcode=0):
    """Change user password in LDAP."""
    if node is None:
        node = current().context.ldap_node

    ldif = (f"dn: {user['dn']}\n"
        "changetype: modify\n"
        "replace: userpassword\n"
        f"userpassword: {new_password}")

    r = node.command(
        f"echo -e \"{ldif}\" | ldapmodify -x -H ldap://localhost -D \"cn=admin,dc=company,dc=com\" -w admin")

    if exitcode is not None:
        assert r.exitcode == exitcode, error()

def change_user_cn_in_ldap(user, new_cn, node=None, exitcode=0):
    """Change user password in LDAP."""
    if node is None:
        node = current().context.ldap_node

    new_user = dict(user)
    new_user['dn'] = f"cn={new_cn},ou=users,dc=company,dc=com"
    new_user['cn'] = new_cn

    ldif = (
        f"dn: {user['dn']}\n"
        "changetype: modrdn\n"
        f"newrdn: cn = {new_user['cn']}\n"
        f"deleteoldrdn: 1\n"
        )

    r = node.command(
        f"echo -e \"{ldif}\" | ldapmodify -x -H ldap://localhost -D \"cn=admin,dc=company,dc=com\" -w admin")

    if exitcode is not None:
        assert r.exitcode == exitcode, error()

    return new_user

@contextmanager
def ldap_user(cn, userpassword, givenname=None, homedirectory=None, sn=None, uid=None, uidnumber=None, node=None):
    """Add new user to the LDAP server."""
    try:
        user = None
        with Given(f"I add user {cn} to LDAP"):
            user = add_user_to_ldap(cn, userpassword, givenname, homedirectory, sn, uid, uidnumber, node=node)
        yield user
    finally:
        with Finally(f"I delete user {cn} from LDAP"):
            if user is not None:
                delete_user_from_ldap(user, node=node)

@contextmanager
def ldap_users(*users, node=None):
    """Add multiple new users to the LDAP server."""
    try:
        _users = []
        with Given("I add users to LDAP"):
            for user in users:
                with By(f"adding user {user['cn']}"):
                    _users.append(add_user_to_ldap(**user, node=node))
        yield _users
    finally:
        with Finally(f"I delete users from LDAP"):
            for _user in _users:
                delete_user_from_ldap(_user, node=node)

def login(servers, *users, config=None):
    """Configure LDAP server and LDAP authenticated users and
    try to login and execute a query"""
    with ldap_servers(servers):
        with ldap_authenticated_users(*users, restart=True, config=config):
            for user in users:
                if user.get("login", False):
                    with When(f"I login as {user['username']} and execute query"):
                        current().context.node.query("SELECT 1",
                            settings=[("user", user["username"]), ("password", user["password"])],
                            exitcode=user.get("exitcode", None),
                            message=user.get("message", None))
