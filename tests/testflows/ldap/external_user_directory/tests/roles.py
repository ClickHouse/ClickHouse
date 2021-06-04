from testflows.core import *

from ldap.external_user_directory.tests.common import *
from ldap.external_user_directory.requirements import *

@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Role_New("1.0")
)
def new_role(self, server):
    """Check that new roles can't be assigned to any LDAP user
    authenticated using external user directory.
    """
    node = self.context.node
    uid = getuid()

    self.context.ldap_node = self.context.cluster.node(server)

    users = [
        {"username": f"user0_{uid}", "password": "user0_password"},
        {"username": f"user1_{uid}", "password": "user1_password"}
    ]

    with rbac_roles(f"role0_{uid}", f"role1_{uid}") as roles:
        with table(f"table_{getuid()}", "CREATE TABLE {name} (d DATE, s String, i UInt8) ENGINE = Memory()") as table_name:
            with ldap_external_user_directory(server=server, roles=roles, restart=True):
                with ldap_users(*[{"cn": user["username"], "userpassword": user["password"]} for user in users]):

                    with When(f"I login and execute query simple query to cache the LDAP user"):
                        node.query(f"SELECT 1",
                            settings=[("user", users[0]["username"]), ("password", users[0]["password"])])

                    with rbac_roles(f"new_role0_{uid}") as new_roles:

                        message = "DB::Exception: Cannot update user `{user}` in ldap because this storage is readonly"
                        exitcode = 239

                        with And("I try to grant new role to the cached LDAP user"):
                            node.query(f"GRANT {new_roles[0]} TO {users[0]['username']}",
                                exitcode=exitcode, message=message.format(user=users[0]["username"]))

                        message = "DB::Exception: There is no role `{user}` in user directories"
                        exitcode = 255

                        with And("I try to grant new role to the non-cached LDAP user"):
                            node.query(f"GRANT {new_roles[0]} TO {users[1]['username']}",
                                exitcode=exitcode, message=message.format(user=users[1]["username"]))

@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Role_NewPrivilege("1.0")
)
def add_privilege(self, server):
    """Check that we can add privilege to a role used
    in the external user directory configuration.
    """
    node = self.context.node
    uid = getuid()
    message = "DB::Exception: {user}: Not enough privileges."
    exitcode = 241

    self.context.ldap_node = self.context.cluster.node(server)

    users = [
        {"username": f"user0_{uid}", "password": "user0_password"},
        {"username": f"user1_{uid}", "password": "user1_password"}
    ]

    with rbac_roles(f"role0_{uid}", f"role1_{uid}") as roles:
        with table(f"table_{getuid()}", "CREATE TABLE {name} (d DATE, s String, i UInt8) ENGINE = Memory()") as table_name:
            with ldap_external_user_directory(server=server, roles=roles, restart=True):
                with ldap_users(*[{"cn": user["username"], "userpassword": user["password"]} for user in users]):

                    with When(f"I login and execute query that requires no privileges"):
                        node.query(f"SELECT 1",
                            settings=[("user", users[0]["username"]), ("password", users[0]["password"])])

                    with And(f"I login and try to read from the table without having select privilege"):
                        node.query(f"SELECT * FROM {table_name} LIMIT 1",
                            settings=[("user", users[0]["username"]), ("password", users[0]["password"])],
                            exitcode=exitcode, message=message.format(user=users[0]["username"]))

                    with When(f"I grant select privilege to one of the two roles assigned to LDAP users"):
                        node.query(f"GRANT SELECT ON {table_name} TO {roles[0]}")

                    with And(f"I login again and expect that cached LDAP user can successfully read from the table"):
                        node.query(f"SELECT * FROM {table_name} LIMIT 1",
                            settings=[("user", users[0]["username"]), ("password", users[0]["password"])])

                    with And(f"I login again and expect that non-cached LDAP user can successfully read from the table"):
                        node.query(f"SELECT * FROM {table_name} LIMIT 1",
                            settings=[("user", users[1]["username"]), ("password", users[1]["password"])])

@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Role_RemovedPrivilege("1.0")
)
def remove_privilege(self, server):
    """Check that we can remove privilege from a role used
    in the external user directory configuration.
    """
    node = self.context.node
    uid = getuid()
    message = "DB::Exception: {user}: Not enough privileges."
    exitcode = 241

    self.context.ldap_node = self.context.cluster.node(server)

    users = [
        {"username": f"user0_{uid}", "password": "user0_password"},
        {"username": f"user1_{uid}", "password": "user1_password"}
    ]

    with rbac_roles(f"role0_{uid}", f"role1_{uid}") as roles:
        with table(f"table_{getuid()}", "CREATE TABLE {name} (d DATE, s String, i UInt8) ENGINE = Memory()") as table_name:

            with When(f"I grant select privilege to one of the two roles assigned to LDAP users"):
                node.query(f"GRANT SELECT ON {table_name} TO {roles[0]}")

            with ldap_external_user_directory(server=server, roles=roles, restart=True):
                with ldap_users(*[{"cn": user["username"], "userpassword": user["password"]} for user in users]):

                    with When(f"I login then LDAP user should be able to read from the table"):
                        node.query(f"SELECT * FROM {table_name} LIMIT 1",
                            settings=[("user", users[0]["username"]), ("password", users[0]["password"])])

                    with When(f"I revoke select privilege from all the roles assigned to LDAP users"):
                        node.query(f"REVOKE SELECT ON {table_name} FROM {roles[0]}")

                    with When(f"I login again then cached LDAP user should not be able to read from the table"):
                        node.query(f"SELECT * FROM {table_name} LIMIT 1",
                            settings=[("user", users[0]["username"]), ("password", users[0]["password"])],
                            exitcode=exitcode, message=message.format(user=users[0]["username"]))

                    with When(f"I login with non-cached LDAP user then the user should also not be able to read from the table"):
                        node.query(f"SELECT * FROM {table_name} LIMIT 1",
                            settings=[("user", users[1]["username"]), ("password", users[1]["password"])],
                            exitcode=exitcode, message=message.format(user=users[1]["username"]))

@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Role_Removed("2.0")
)
def remove_role(self, server):
    """Check that when a role used in the external user directory configuration
    is dynamically removed then any LDAP users should still be authenticated using
    LDAP external user directory.
    """
    node = self.context.node
    uid = getuid()

    self.context.ldap_node = self.context.cluster.node(server)

    users = [
        {"username": f"user0_{uid}", "password": "user0_password"},
        {"username": f"user1_{uid}", "password": "user1_password"}
    ]

    with rbac_roles(f"role0_{uid}", f"role1_{uid}") as roles:
            with ldap_external_user_directory(server=server, roles=roles, restart=True):
                with ldap_users(*[{"cn": user["username"], "userpassword": user["password"]} for user in users]):
                    with When(f"I login and execute query that requires no privileges"):
                        node.query(f"SELECT 1",
                            settings=[("user", users[0]["username"]), ("password", users[0]["password"])])

                    with And("I remove one of the roles"):
                        node.query(f"DROP ROLE {roles[1]}")

                    with And(f"I try to login using cached LDAP user"):
                        node.query(f"SELECT 1",
                            settings=[("user", users[0]["username"]), ("password", users[0]["password"])])

                    with And(f"I try to login again using non-cached LDAP user"):
                        node.query(f"SELECT 1",
                            settings=[("user", users[1]["username"]), ("password", users[1]["password"])])

@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Role_Removed_Privileges("1.0")
)
def remove_privilege_by_removing_role(self, server):
    """Check that when the role used in the external user directory configuration
    is dynamically removed then privileges are removed from all
    LDAP users that are authenticated using external user directory.
    """
    node = self.context.node
    message = "DB::Exception: {user}: Not enough privileges."
    exitcode = 241
    uid = getuid()

    self.context.ldap_node = self.context.cluster.node(server)

    users = [
        {"username": f"user0_{uid}", "password": "user0_password"},
        {"username": f"user1_{uid}", "password": "user1_password"}
    ]

    with rbac_roles(f"role0_{uid}", f"role1_{uid}") as roles:
            with table(f"table_{getuid()}", "CREATE TABLE {name} (d DATE, s String, i UInt8) ENGINE = Memory()") as table_name:

                with When(f"I grant select privilege to one of the two roles assigned to LDAP users"):
                    node.query(f"GRANT SELECT ON {table_name} TO {roles[0]}")

                with ldap_external_user_directory(server=server, roles=roles, restart=True):
                    with ldap_users(*[{"cn": user["username"], "userpassword": user["password"]} for user in users]):

                        with When(f"I login and expect that LDAP user can read from the table"):
                            node.query(f"SELECT * FROM {table_name} LIMIT 1",
                                settings=[("user", users[0]["username"]), ("password", users[0]["password"])])

                        with And("I remove the role that grants the privilege"):
                            node.query(f"DROP ROLE {roles[0]}")

                        with And(f"I try to relogin and expect that cached LDAP user can login "
                                "but does not have privilege that was provided by the removed role"):
                            node.query(f"SELECT * FROM {table_name} LIMIT 1",
                                settings=[("user", users[0]["username"]), ("password", users[0]["password"])],
                                exitcode=exitcode, message=message.format(user=users[0]["username"]))

@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Role_Readded_Privileges("1.0")
)
def readd_privilege_by_readding_role(self, server):
    """Check that when the role used in the external user directory configuration
    is dynamically removed then all the privileges are removed from any
    LDAP users authenticated using external user directory but when the role is re-added
    then privileges are restored.
    """
    node = self.context.node
    uid = getuid()

    self.context.ldap_node = self.context.cluster.node(server)

    users = [
        {"username": f"user0_{uid}", "password": "user0_password"},
        {"username": f"user1_{uid}", "password": "user1_password"}
    ]

    with rbac_roles(f"role0_{uid}", f"role1_{uid}") as roles:
            with table(f"table_{getuid()}", "CREATE TABLE {name} (d DATE, s String, i UInt8) ENGINE = Memory()") as table_name:

                with When(f"I grant select privilege to one of the two roles assigned to LDAP users"):
                    node.query(f"GRANT SELECT ON {table_name} TO {roles[0]}")

                with ldap_external_user_directory(server=server, roles=roles, restart=True):
                    with ldap_users(*[{"cn": user["username"], "userpassword": user["password"]} for user in users]):

                        with When(f"I login and expect that LDAP user can read from the table"):
                            node.query(f"SELECT * FROM {table_name} LIMIT 1",
                                settings=[("user", users[0]["username"]), ("password", users[0]["password"])])

                        with And("I remove the role that grants the privilege"):
                            node.query(f"DROP ROLE {roles[0]}")

                        message = "DB::Exception: {user}: Not enough privileges."
                        exitcode = 241

                        with And(f"I try to relogin and expect that cached LDAP user can login "
                                "but does not have privilege that was provided by the removed role"):
                            node.query(f"SELECT * FROM {table_name} LIMIT 1",
                                settings=[("user", users[0]["username"]), ("password", users[0]["password"])],
                                exitcode=exitcode, message=message.format(user=users[0]["username"]))

                        with And(f"I try to login using non-cached LDAP user and expect it to succeed"):
                            node.query(f"SELECT 1",
                                settings=[("user", users[1]["username"]), ("password", users[1]["password"])])

                        with When("I re-add the role"):
                            node.query(f"CREATE ROLE {roles[0]}")

                        with And(f"I grant select privilege to the re-added role"):
                            node.query(f"GRANT SELECT ON {table_name} TO {roles[0]}")

                        with And(f"I try to relogin and expect that cached LDAP user can login "
                                "and again has the privilege that is provided by the role"):
                            node.query(f"SELECT * FROM {table_name} LIMIT 1",
                                settings=[("user", users[0]["username"]), ("password", users[0]["password"])])

                        with And("I try to login using non-cached LDAP expect it to also work again and expect"
                                "for the user also to have privilege provided by the role"):
                            node.query(f"SELECT * FROM {table_name} LIMIT 1",
                                settings=[("user", users[1]["username"]), ("password", users[1]["password"])])

@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Role_NotPresent_Added("1.0")
)
def not_present_role_added(self, server):
    """Check that when the role used in the external user directory configuration
    which was not present during LDAP user authentication
    is dynamically added then all the privileges granted by the role
    are given to all users authenticated using external LDAP user directory.
    """
    node = self.context.node
    uid = getuid()

    self.context.ldap_node = self.context.cluster.node(server)

    users = [
        {"username": f"user0_{uid}", "password": "user0_password"},
        {"username": f"user1_{uid}", "password": "user1_password"}
    ]

    roles = [f"role0_{uid}", f"role1_{uid}"]

    with table(f"table_{getuid()}", "CREATE TABLE {name} (d DATE, s String, i UInt8) ENGINE = Memory()") as table_name:
        with ldap_external_user_directory(server=server, roles=roles, restart=True):
            with ldap_users(*[{"cn": user["username"], "userpassword": user["password"]} for user in users]):
                with When(f"I login using clickhouse-client"):
                    with self.context.cluster.shell(node=node.name) as shell:
                        with shell(f"TERM=dumb clickhouse client --user {users[0]['username']} --password {users[0]['password']} | tee",
                                asynchronous=True, name="client") as client:
                            client.app.expect("clickhouse1 :\) ")

                            with When("I execute select on the table"):
                                client.app.send(f"SELECT * FROM {table_name} LIMIT 1")

                            with Then("I expect to get not enough privileges error"):
                                client.app.expect("Not enough privileges")
                                client.app.expect("clickhouse1 :\) ")

                            try:
                                with Given("I add the role and grant the select privilege to it for the table"):
                                    node.query(f"CREATE ROLE {roles[0]}")
                                    node.query(f"GRANT SELECT ON {table_name} TO {roles[0]}")

                                with When("I re-execute select on the table"):
                                    client.app.send(f"SELECT * FROM {table_name} LIMIT 1")

                                with Then("I expect to get no errors"):
                                    client.app.expect("Ok\.")
                                    client.app.expect("clickhouse1 :\) ")

                            finally:
                                with Finally("I delete the role"):
                                    node.query(f"DROP ROLE IF EXISTS {roles[0]}")

@TestFeature
@Name("roles")
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Roles("1.0")
)
def feature(self, node="clickhouse1"):
    """Check that all the users that are authenticated using
    LDAP external user directory are assigned the roles specified
    in the configuration of the LDAP external user directory.
    """
    self.context.node = self.context.cluster.node(node)

    servers = {
        "openldap1": {
            "host": "openldap1", "port": "389", "enable_tls": "no",
            "auth_dn_prefix": "cn=", "auth_dn_suffix": ",ou=users,dc=company,dc=com"
        },
    }
    user = {"server": "openldap1", "username": "user1", "password": "user1"}

    with ldap_servers(servers):
        for scenario in loads(current_module(), Scenario):
            scenario(server="openldap1")
