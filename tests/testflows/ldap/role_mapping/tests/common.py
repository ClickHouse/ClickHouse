import os

from testflows.core import *
from testflows.asserts import error

from helpers.common import create_xml_config_content, add_config
from ldap.authentication.tests.common import (
    getuid,
    create_ldap_servers_config_content,
    ldap_authenticated_users,
)
from ldap.external_user_directory.tests.common import rbac_roles, rbac_users, ldap_users


@TestStep(Given)
def create_table(self, name, create_statement, on_cluster=False, node=None):
    """Create table."""
    if node is None:
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


@TestStep(Given)
def add_ldap_servers_configuration(
    self,
    servers,
    config=None,
    config_d_dir="/etc/clickhouse-server/config.d",
    config_file="ldap_servers.xml",
    timeout=60,
    restart=False,
    node=None,
):
    """Add LDAP servers configuration to config.xml."""
    if config is None:
        config = create_ldap_servers_config_content(servers, config_d_dir, config_file)
    return add_config(config, restart=restart, node=node)


@TestStep(Given)
def add_ldap_groups(self, groups, node=None):
    """Add multiple new groups to the LDAP server."""
    try:
        _groups = []
        for group in groups:
            with By(f"adding group {group['cn']}"):
                _groups.append(add_group_to_ldap(**group, node=node))
        yield _groups
    finally:
        with Finally(f"I delete groups from LDAP"):
            for _group in _groups:
                delete_group_from_ldap(_group, node=node)


@TestStep(Given)
def add_ldap_external_user_directory(
    self,
    server,
    roles=None,
    role_mappings=None,
    config_d_dir="/etc/clickhouse-server/config.d",
    config_file=None,
    timeout=60,
    restart=True,
    config=None,
    node=None,
):
    """Add LDAP external user directory."""
    if config_file is None:
        config_file = f"ldap_external_user_directory_with_role_mapping_{getuid()}.xml"

    if config is None:
        config = create_ldap_external_user_directory_config_content(
            server=server,
            roles=roles,
            role_mappings=role_mappings,
            config_d_dir=config_d_dir,
            config_file=config_file,
        )

    return add_config(config, restart=restart, node=node)


@TestStep(Given)
def add_rbac_roles(self, roles, node=None):
    """Add RBAC roles."""
    with rbac_roles(*roles, node=node) as _roles:
        yield _roles


@TestStep(Given)
def add_rbac_users(self, users, node=None):
    """Add RBAC users."""
    if node is None:
        node = self.context.node
    try:
        with Given(f"I create local users on {node}"):
            for user in users:
                username = user.get("username", None) or user["cn"]
                password = user.get("password", None) or user["userpassword"]
                with By(f"creating user {username}"):
                    node.query(
                        f"CREATE USER OR REPLACE {username} IDENTIFIED WITH PLAINTEXT_PASSWORD BY '{password}'"
                    )
        yield users
    finally:
        with Finally(f"I drop local users on {node}"):
            for user in users:
                username = user.get("username", None) or user["cn"]
                with By(f"dropping user {username}", flags=TE):
                    node.query(f"DROP USER IF EXISTS {username}")


@TestStep(Given)
def add_ldap_users(self, users, node=None):
    """Add LDAP users."""
    with ldap_users(*users, node=node) as _users:
        yield _users


@TestStep(Given)
def add_ldap_authenticated_users(
    self, users, config_file=None, rbac=False, node=None, restart=True
):
    """Add LDAP authenticated users."""
    if config_file is None:
        config_file = f"ldap_users_{getuid()}.xml"

    with ldap_authenticated_users(
        *users, config_file=config_file, restart=restart, rbac=rbac, node=node
    ):
        yield users


def add_group_to_ldap(cn, gidnumber=None, node=None, _gidnumber=[600], exitcode=0):
    """Add new group entry to LDAP."""
    _gidnumber[0] += 1

    if node is None:
        node = current().context.ldap_node

    if gidnumber is None:
        gidnumber = _gidnumber[0]

    group = {
        "dn": f"cn={cn},ou=groups,dc=company,dc=com",
        "objectclass": ["top", "groupOfUniqueNames"],
        "uniquemember": "cn=admin,dc=company,dc=com",
        "_server": node.name,
    }

    lines = []

    for key, value in list(group.items()):
        if key.startswith("_"):
            continue
        elif type(value) is list:
            for v in value:
                lines.append(f"{key}: {v}")
        else:
            lines.append(f"{key}: {value}")

    ldif = "\n".join(lines)

    r = node.command(
        f'echo -e "{ldif}" | ldapadd -x -H ldap://localhost -D "cn=admin,dc=company,dc=com" -w admin'
    )

    if exitcode is not None:
        assert r.exitcode == exitcode, error()

    return group


def delete_group_from_ldap(group, node=None, exitcode=0):
    """Delete group entry from LDAP."""
    if node is None:
        node = current().context.ldap_node

    with By(f"deleting group {group['dn']}"):
        r = node.command(
            f"ldapdelete -x -H ldap://localhost -D \"cn=admin,dc=company,dc=com\" -w admin \"{group['dn']}\""
        )

    if exitcode is not None:
        assert r.exitcode == exitcode, error()


def fix_ldap_permissions(node=None, exitcode=0):
    """Fix LDAP access permissions."""
    if node is None:
        node = current().context.ldap_node

    ldif = (
        "dn: olcDatabase={1}mdb,cn=config\n"
        "changetype: modify\n"
        "delete: olcAccess\n"
        "-\n"
        "add: olcAccess\n"
        'olcAccess: to attrs=userPassword,shadowLastChange by self write by dn=\\"cn=admin,dc=company,dc=com\\" write by anonymous auth by * none\n'
        'olcAccess: to * by self write by dn=\\"cn=admin,dc=company,dc=com\\" read by users read by * none'
    )

    r = node.command(f'echo -e "{ldif}" | ldapmodify -Y EXTERNAL -Q -H ldapi:///')

    if exitcode is not None:
        assert r.exitcode == exitcode, error()


def add_user_to_group_in_ldap(user, group, node=None, exitcode=0):
    """Add user to a group in LDAP."""
    if node is None:
        node = current().context.ldap_node

    ldif = (
        f"dn: {group['dn']}\n"
        "changetype: modify\n"
        "add: uniquemember\n"
        f"uniquemember: {user['dn']}"
    )

    with By(f"adding user {user['dn']} to group {group['dn']}"):
        r = node.command(
            f'echo -e "{ldif}" | ldapmodify -x -H ldap://localhost -D "cn=admin,dc=company,dc=com" -w admin'
        )

    if exitcode is not None:
        assert r.exitcode == exitcode, error()


def delete_user_from_group_in_ldap(user, group, node=None, exitcode=0):
    """Delete user from a group in LDAP."""
    if node is None:
        node = current().context.ldap_node

    ldif = (
        f"dn: {group['dn']}\n"
        "changetype: modify\n"
        "delete: uniquemember\n"
        f"uniquemember: {user['dn']}"
    )

    with By(f"deleting user {user['dn']} from group {group['dn']}"):
        r = node.command(
            f'echo -e "{ldif}" | ldapmodify -x -H ldap://localhost -D "cn=admin,dc=company,dc=com" -w admin'
        )

    if exitcode is not None:
        assert r.exitcode == exitcode, error()


def create_ldap_external_user_directory_config_content(
    server=None, roles=None, role_mappings=None, **kwargs
):
    """Create LDAP external user directory configuration file content."""
    kwargs["config_file"] = kwargs.pop(
        "config_file", "external_ldap_user_directory.xml"
    )

    entries = {"user_directories": {"ldap": {}}}

    entries["user_directories"]["ldap"] = []

    if server:
        entries["user_directories"]["ldap"].append({"server": server})

    if roles:
        entries["user_directories"]["ldap"].append(
            {"roles": [{r: None} for r in roles]}
        )

    if role_mappings:
        for role_mapping in role_mappings:
            entries["user_directories"]["ldap"].append({"role_mapping": role_mapping})

    return create_xml_config_content(entries, **kwargs)


def create_entries_ldap_external_user_directory_config_content(entries, **kwargs):
    """Create LDAP external user directory configuration file content."""
    kwargs["config_file"] = kwargs.pop(
        "config_file", "external_ldap_user_directory.xml"
    )
    return create_xml_config_content(entries, **kwargs)
