import random

from testflows.core import *
from testflows.asserts import error

from ldap.external_user_directory.tests.common import *
from ldap.external_user_directory.requirements import *


@TestScenario
def one_external_user_directory(self, node="clickhouse1"):
    """Check that we can restart ClickHouse server when one
    LDAP external user directory is configured.
    """
    self.context.node = self.context.cluster.node(node)

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
        with rbac_roles("ldap_role") as roles:
            with ldap_external_user_directory(
                server="openldap1", roles=roles, restart=True
            ):
                with Given("I login and execute query"):
                    login_and_execute_query(username="user1", password="user1")

                with When("I then restart the server"):
                    restart()

                with Then("I should be able to login and execute query after restart"):
                    login_and_execute_query(username="user1", password="user1")


@TestScenario
def multiple_external_user_directories(self, node="clickhouse1"):
    """Check that we can restart ClickHouse server when two
    LDAP external user directory are configured.
    """
    self.context.node = self.context.cluster.node(node)

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

    with Given("I have two LDAP servers"):
        entries = [(["openldap1"], []), (["openldap2"], [])]

    with And(
        "I create config file to define LDAP external user directory for each LDAP server"
    ):
        config = create_entries_ldap_external_user_directory_config_content(entries)

    with ldap_servers(servers):
        with ldap_external_user_directory(
            server=None, roles=None, restart=True, config=config
        ):
            with Given(
                "I login and execute query using a user defined in the first LDAP server"
            ):
                login_and_execute_query(username="user1", password="user1")

            with And(
                "I login and execute query using a user defined the second LDAP server"
            ):
                login_and_execute_query(username="user2", password="user2")

            with When("I restart the server"):
                restart()

            with Then(
                "I should be able to login and execute query again using a user defined in the first LDAP server"
            ):
                login_and_execute_query(username="user1", password="user1")

            with And(
                "I should be able to login and execute query again using a user defined in the second LDAP server"
            ):
                login_and_execute_query(username="user2", password="user2")


@TestScenario
def dynamically_added_users(self, node="clickhouse1", count=10):
    """Check that we can restart ClickHouse server when one
    LDAP external user directory is configured and the login
    with an LDAP users that are dynamically added after restart.
    """
    self.context.node = self.context.cluster.node(node)

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
        with rbac_roles("ldap_role") as roles:
            with ldap_external_user_directory(
                server="openldap1", roles=roles, restart=True
            ):
                with Given("I login and execute query using existing LDAP user"):
                    login_and_execute_query(username="user1", password="user1")

                with When("I then restart the server"):
                    restart()

                with Then(
                    "after restart I should be able to login and execute query using existing LDAP user"
                ):
                    login_and_execute_query(username="user1", password="user1")

                dynamic_users = []
                with When("I define dynamically added LDAP users"):
                    for i in range(count):
                        dynamic_users.append(
                            {"cn": f"dynamic_user{i}", "userpassword": randomword(20)}
                        )

                with ldap_users(
                    *dynamic_users, node=self.context.cluster.node("openldap1")
                ):
                    with Then(
                        "I should be able to login and execute queries using dynamically added users"
                    ):
                        for dynamic_user in dynamic_users:
                            with When(
                                f"using dynamically added user {dynamic_user['cn']}"
                            ):
                                login_and_execute_query(
                                    username=dynamic_user["cn"],
                                    password=dynamic_user["userpassword"],
                                )


@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Restart_Server_ParallelLogins("1.0")
)
def parallel_login(self, server=None, user_count=10, timeout=300):
    """Check that login of valid and invalid users works in parallel
    using local users defined using RBAC and LDAP users authenticated using
    multiple LDAP external user directories when server is restarted
    in the middle of parallel login attempts. After server is restarted
    makes sure that parallel logins work as expected.
    """
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

    with Given("I have two LDAP servers"):
        entries = [(["openldap1"], []), (["openldap2"], [])]

    with And("I define a group of users to be created on each LDAP server"):
        user_groups = {
            "openldap1_users": [
                {"cn": f"openldap1_parallel_user{i}", "userpassword": randomword(20)}
                for i in range(user_count)
            ],
            "openldap2_users": [
                {"cn": f"openldap2_parallel_user{i}", "userpassword": randomword(20)}
                for i in range(user_count)
            ],
            "local_users": [
                {"cn": f"local_parallel_user{i}", "userpassword": randomword(20)}
                for i in range(user_count)
            ],
        }

    @TestStep(When)
    @Name("I login as {username} and execute query")
    def login_and_execute_query_during_restart(
        self, username, password, exitcode, message, steps=True, timeout=60
    ):
        """Execute a query and ignore exitcode and message as
        during restart exit codes and messages vary based on the state
        of the restarted container and the ClickHouse server
        and there are too many cases and complete list is not fully known
        therefore trying to list all possible cases produces random fails.
        """
        r = self.context.cluster.command(
            None,
            f"{self.context.cluster.docker_compose} exec {self.context.node.name} "
            + f'clickhouse client -q "SELECT 1" --user {username} --password {password}',
            steps=steps,
            timeout=timeout,
        )

        return r

    @TestStep(When)
    @Name("I login as {username} and execute query")
    def login_and_execute_query(
        self, username, password, exitcode=None, message=None, steps=True, timeout=60
    ):
        self.context.node.query(
            "SELECT 1",
            settings=[("user", username), ("password", password)],
            exitcode=exitcode or 0,
            message=message,
            steps=steps,
            timeout=timeout,
        )

    def login_with_valid_username_and_password(
        users, i, iterations=10, during_restart=False
    ):
        """Login with valid username and password."""
        query = login_and_execute_query
        if during_restart:
            query = login_and_execute_query_during_restart

        with When(f"valid users try to login #{i}"):
            for i in range(iterations):
                random_user = users[random.randint(0, len(users) - 1)]

                query(
                    username=random_user["cn"],
                    password=random_user["userpassword"],
                    exitcode=0,
                    message="1",
                    steps=False,
                )

    def login_with_valid_username_and_invalid_password(
        users, i, iterations=10, during_restart=False
    ):
        """Login with valid username and invalid password."""
        query = login_and_execute_query
        if during_restart:
            query = login_and_execute_query_during_restart

        with When(f"users try to login with valid username and invalid password #{i}"):
            for i in range(iterations):
                random_user = users[random.randint(0, len(users) - 1)]

                query(
                    username=random_user["cn"],
                    password=(random_user["userpassword"] + randomword(1)),
                    exitcode=4,
                    message=f"DB::Exception: {random_user['cn']}: Authentication failed: password is incorrect or there is no user with such name",
                    steps=False,
                )

    def login_with_invalid_username_and_valid_password(
        users, i, iterations=10, during_restart=False
    ):
        """Login with invalid username and valid password."""
        query = login_and_execute_query
        if during_restart:
            query = login_and_execute_query_during_restart

        with When(f"users try to login with invalid username and valid password #{i}"):
            for i in range(iterations):
                random_user = dict(users[random.randint(0, len(users) - 1)])
                random_user["cn"] += randomword(1)

                query(
                    username=random_user["cn"],
                    password=random_user["userpassword"],
                    exitcode=4,
                    message=f"DB::Exception: {random_user['cn']}: Authentication failed: password is incorrect or there is no user with such name",
                    steps=False,
                )

    with And("I have a list of checks that I want to run for each user group"):
        checks = [
            login_with_valid_username_and_password,
            login_with_valid_username_and_invalid_password,
            login_with_invalid_username_and_valid_password,
        ]

    with And(
        "I create config file to define LDAP external user directory for each LDAP server"
    ):
        config = create_entries_ldap_external_user_directory_config_content(entries)

    with ldap_servers(servers):
        with ldap_external_user_directory(
            server=None, roles=None, restart=True, config=config
        ):
            with ldap_users(
                *user_groups["openldap1_users"],
                node=self.context.cluster.node("openldap1"),
            ):
                with ldap_users(
                    *user_groups["openldap2_users"],
                    node=self.context.cluster.node("openldap2"),
                ):
                    with rbac_users(*user_groups["local_users"]):
                        tasks = []
                        with Pool(4) as pool:
                            try:
                                with When(
                                    "I restart the server during parallel login of users in each group"
                                ):
                                    for users in user_groups.values():
                                        for check in checks:
                                            tasks.append(
                                                pool.submit(check, (users, 0, 25, True))
                                            )

                                    tasks.append(pool.submit(restart))
                            finally:
                                with Then("logins during restart should work"):
                                    for task in tasks:
                                        task.result(timeout=timeout)

                        tasks = []
                        with Pool(4) as pool:
                            try:
                                with When(
                                    "I perform parallel login of users in each group after restart"
                                ):
                                    for users in user_groups.values():
                                        for check in checks:
                                            tasks.append(
                                                pool.submit(
                                                    check, (users, 0, 10, False)
                                                )
                                            )
                            finally:
                                with Then("logins after restart should work"):
                                    for task in tasks:
                                        task.result(timeout=timeout)


@TestOutline(Feature)
@Name("restart")
@Requirements(RQ_SRS_009_LDAP_ExternalUserDirectory_Restart_Server("1.0"))
def feature(self, servers=None, server=None, node="clickhouse1"):
    """Check that we can restart ClickHouse server
    when one or more external user directories are configured.
    """
    self.context.node = self.context.cluster.node(node)

    for scenario in loads(current_module(), Scenario):
        Scenario(test=scenario)()
