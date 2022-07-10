from testflows.core import *

from ldap.external_user_directory.tests.common import *
from ldap.external_user_directory.requirements import *


@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_LDAPUserDirectory_MoreThanOne(
        "2.0"
    )
)
def more_than_one_user_directory(self):
    """Check when more than one LDAP user directory is
    defined inside a configuration file.
    """
    message = "DB::Exception: Duplicate storage type 'ldap' at user_directories"
    servers = {
        "openldap1": {
            "host": "openldap1",
            "port": "389",
            "enable_tls": "no",
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com",
        },
        "openldap2": {
            "host": "openldap2",
            "port": "636",
            "enable_tls": "yes",
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com",
            "tls_require_cert": "never",
        },
    }
    users = [
        {
            "server": "openldap1",
            "username": "user1",
            "password": "user1",
            "login": True,
        },
        {
            "server": "openldap2",
            "username": "user2",
            "password": "user2",
            "login": True,
        },
    ]
    role = f"role_{getuid()}"
    entries = [(["openldap1"], [(role,)]), (["openldap2"], [(role,)])]

    with ldap_servers(servers):
        with rbac_roles(role) as roles:
            config = create_entries_ldap_external_user_directory_config_content(entries)

            with ldap_external_user_directory(
                server=None, roles=None, restart=True, config=config
            ):
                with When(
                    f"I login as {users[0]['username']} authenticated using openldap1"
                ):
                    current().context.node.query(
                        f"SELECT 1",
                        settings=[
                            ("user", users[0]["username"]),
                            ("password", users[0]["password"]),
                        ],
                    )

                with And(
                    f"I login as {users[1]['username']} authenticated using openldap2"
                ):
                    current().context.node.query(
                        f"SELECT 1",
                        settings=[
                            ("user", users[1]["username"]),
                            ("password", users[1]["password"]),
                        ],
                    )


@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Server_Empty(
        "1.0"
    )
)
def empty_server(self, timeout=300):
    """Check that empty string in a `server` field is not allowed."""
    message = "DB::Exception: Empty 'server' field for LDAP user directory"
    servers = {
        "openldap1": {
            "host": "openldap1",
            "port": "389",
            "enable_tls": "no",
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com",
        },
    }

    with ldap_servers(servers):
        with rbac_roles(f"role_{getuid()}") as roles:
            invalid_ldap_external_user_directory_config(
                server="", roles=roles, message=message, timeout=timeout
            )


@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Server_Missing(
        "1.0"
    )
)
def missing_server(self, timeout=300):
    """Check that missing `server` field is not allowed."""
    message = "DB::Exception: Missing 'server' field for LDAP user directory"
    servers = {
        "openldap1": {
            "host": "openldap1",
            "port": "389",
            "enable_tls": "no",
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com",
        },
    }

    with ldap_servers(servers):
        with rbac_roles(f"role_{getuid()}") as roles:
            invalid_ldap_external_user_directory_config(
                server=None, roles=roles, message=message, timeout=timeout
            )


@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Server_MoreThanOne(
        "1.0"
    )
)
def defined_twice_server(self):
    """Check that when `server` field is defined twice that only the first
    entry is used.
    """
    servers = {
        "openldap1": {
            "host": "openldap1",
            "port": "389",
            "enable_tls": "no",
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com",
        },
    }
    user = {
        "server": "openldap1",
        "username": "user1",
        "password": "user1",
        "login": True,
    }

    role = f"role_{getuid()}"
    entries = [(["openldap1", "openldap2"], [(role,)])]

    with ldap_servers(servers):
        with rbac_roles(role) as roles:
            config = create_entries_ldap_external_user_directory_config_content(entries)
            with ldap_external_user_directory(
                server=None, roles=None, restart=True, config=config
            ):
                with When(f"I login as {user['username']} and execute query"):
                    current().context.node.query(
                        "SELECT 1",
                        settings=[
                            ("user", user["username"]),
                            ("password", user["password"]),
                        ],
                    )


@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Server_Invalid(
        "1.0"
    )
)
def invalid_server(self):
    """Check when `server` field value is invalid."""
    servers = {
        "openldap1": {
            "host": "openldap1",
            "port": "389",
            "enable_tls": "no",
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com",
        },
    }
    user = {
        "server": "openldap1",
        "username": "user1",
        "password": "user1",
        "login": True,
    }
    role = f"role_{getuid()}"

    entries = [(["openldap2"], [(role,)])]

    with ldap_servers(servers):
        with rbac_roles(role) as roles:
            config = create_entries_ldap_external_user_directory_config_content(entries)
            with ldap_external_user_directory(
                server=None, roles=None, restart=True, config=config
            ):
                with When(f"I login as {user['username']} and execute query"):
                    current().context.node.query(
                        "SELECT 1",
                        settings=[
                            ("user", user["username"]),
                            ("password", user["password"]),
                        ],
                        exitcode=4,
                        message="DB::Exception: user1: Authentication failed: password is incorrect or there is no user with such name.",
                    )


@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Roles_Empty(
        "1.0"
    )
)
def empty_roles(self):
    """Check when `roles` parameter is empty then user can't read any tables."""
    message = "DB::Exception: user1: Not enough privileges."
    exitcode = 241
    servers = {
        "openldap1": {
            "host": "openldap1",
            "port": "389",
            "enable_tls": "no",
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com",
        },
    }
    user = {"server": "openldap1", "username": "user1", "password": "user1"}

    entries = [(["openldap1"], [[]])]

    with ldap_servers(servers):
        with table(
            f"table_{getuid()}",
            "CREATE TABLE {name} (d DATE, s String, i UInt8) ENGINE = Memory()",
        ) as table_name:
            config = create_entries_ldap_external_user_directory_config_content(entries)
            with ldap_external_user_directory(
                server=None, roles=None, restart=True, config=config
            ):
                with When(f"I login as {user['username']} and execute query"):
                    current().context.node.query(
                        f"SELECT * FROM {table_name} LIMIT 1",
                        settings=[
                            ("user", user["username"]),
                            ("password", user["password"]),
                        ],
                        exitcode=exitcode,
                        message=message,
                    )


@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Roles_MoreThanOne(
        "1.0"
    )
)
def defined_twice_roles(self):
    """Check that when `roles` is defined twice then only the first entry is used."""
    node = self.context.node

    create_statement = (
        "CREATE TABLE {name} (d DATE, s String, i UInt8) ENGINE = Memory()"
    )
    servers = {
        "openldap1": {
            "host": "openldap1",
            "port": "389",
            "enable_tls": "no",
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com",
        },
    }
    user = {
        "server": "openldap1",
        "username": "user1",
        "password": "user1",
        "login": True,
    }
    roles = [f"role0_{getuid()}", f"role1_{getuid()}"]
    entries = [(["openldap1"], [[roles[0]], [roles[1]]])]

    with ldap_servers(servers):
        with rbac_roles(*roles):
            with table(f"table0_{getuid()}", create_statement) as table0_name, table(
                f"table1_{getuid()}", create_statement
            ) as table1_name:

                with Given(
                    "I grant select privilege for the first table to the first role"
                ):
                    node.query(f"GRANT SELECT ON {table0_name} TO {roles[0]}")

                with And(
                    "I grant select privilege for the second table to the second role"
                ):
                    node.query(f"GRANT SELECT ON {table1_name} TO {roles[1]}")

                config = create_entries_ldap_external_user_directory_config_content(
                    entries
                )

                with ldap_external_user_directory(
                    server=None, roles=None, restart=True, config=config
                ):
                    with When(
                        f"I login as {user['username']} and try to read from the first table"
                    ):
                        current().context.node.query(
                            f"SELECT * FROM {table0_name} LIMIT 1",
                            settings=[
                                ("user", user["username"]),
                                ("password", user["password"]),
                            ],
                        )

                    with And(
                        f"I login as {user['username']} again and try to read from the second table"
                    ):
                        current().context.node.query(
                            f"SELECT * FROM {table0_name} LIMIT 1",
                            settings=[
                                ("user", user["username"]),
                                ("password", user["password"]),
                            ],
                        )


@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Roles_Invalid(
        "2.0"
    )
)
def invalid_role_in_roles(self):
    """Check that no error is returned when LDAP users try to authenticate
    if an invalid role is specified inside the `roles` section.
    """
    servers = {
        "openldap1": {
            "host": "openldap1",
            "port": "389",
            "enable_tls": "no",
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com",
        },
    }
    user = {"server": "openldap1", "username": "user1", "password": "user1"}

    with ldap_servers(servers):
        with ldap_external_user_directory("openldap1", roles=["foo"], restart=True):
            with When(f"I login as {user['username']} and execute query"):
                current().context.node.query(
                    "SELECT 1",
                    settings=[
                        ("user", user["username"]),
                        ("password", user["password"]),
                    ],
                )


@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Roles_Missing(
        "1.0"
    )
)
def missing_roles(self):
    """Check that when the `roles` are missing then
    LDAP users can still login but can't read from any table.
    """
    message = "DB::Exception: user1: Not enough privileges."
    exitcode = 241
    servers = {
        "openldap1": {
            "host": "openldap1",
            "port": "389",
            "enable_tls": "no",
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com",
        },
    }
    user = {"server": "openldap1", "username": "user1", "password": "user1"}
    entries = [(["openldap1"], None)]

    with ldap_servers(servers):
        with table(
            f"table_{getuid()}",
            "CREATE TABLE {name} (d DATE, s String, i UInt8) ENGINE = Memory()",
        ) as table_name:

            config = create_entries_ldap_external_user_directory_config_content(entries)

            with ldap_external_user_directory(
                server=None, roles=None, restart=True, config=config
            ):
                with When(f"I login as {user['username']} and execute query"):
                    current().context.node.query(
                        f"SELECT * FROM {table_name} LIMIT 1",
                        settings=[
                            ("user", user["username"]),
                            ("password", user["password"]),
                        ],
                        exitcode=exitcode,
                        message=message,
                    )


@TestFeature
@Name("external user directory config")
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Syntax("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Server("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_LDAPUserDirectory("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_Definition("1.0"),
)
def feature(self, node="clickhouse1"):
    """Check LDAP external user directory configuration."""
    self.context.node = self.context.cluster.node(node)

    for scenario in loads(current_module(), Scenario):
        scenario()
