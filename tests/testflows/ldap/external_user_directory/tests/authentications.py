# -*- coding: utf-8 -*-
import random

from testflows.core import *
from testflows.asserts import error

from ldap.external_user_directory.tests.common import *
from ldap.external_user_directory.requirements import *

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


@TestOutline
def add_user_to_ldap_and_login(
    self, server, user=None, ch_user=None, login=None, exitcode=None, message=None
):
    """Add user to LDAP and ClickHouse and then try to login."""
    self.context.ldap_node = self.context.cluster.node(server)

    if ch_user is None:
        ch_user = {}
    if login is None:
        login = {}
    if user is None:
        user = {"cn": "myuser", "userpassword": "myuser"}

    with ldap_user(**user) as user:
        username = login.get("username", user["cn"])
        password = login.get("password", user["userpassword"])

        login_and_execute_query(
            username=username, password=password, exitcode=exitcode, message=message
        )


def login_with_valid_username_and_password(users, i, iterations=10):
    """Login with valid username and password."""
    with When(f"valid users try to login #{i}"):
        for i in range(iterations):
            random_user = users[random.randint(0, len(users) - 1)]
            login_and_execute_query(
                username=random_user["cn"],
                password=random_user["userpassword"],
                steps=False,
            )


def login_with_valid_username_and_invalid_password(users, i, iterations=10):
    """Login with valid username and invalid password."""
    with When(f"users try to login with valid username and invalid password #{i}"):
        for i in range(iterations):
            random_user = users[random.randint(0, len(users) - 1)]
            login_and_execute_query(
                username=random_user["cn"],
                password=(random_user["userpassword"] + randomword(1)),
                exitcode=4,
                message=f"DB::Exception: {random_user['cn']}: Authentication failed: password is incorrect or there is no user with such name",
                steps=False,
            )


def login_with_invalid_username_and_valid_password(users, i, iterations=10):
    """Login with invalid username and valid password."""
    with When(f"users try to login with invalid username and valid password #{i}"):
        for i in range(iterations):
            random_user = dict(users[random.randint(0, len(users) - 1)])
            random_user["cn"] += randomword(1)
            login_and_execute_query(
                username=random_user["cn"],
                password=random_user["userpassword"],
                exitcode=4,
                message=f"DB::Exception: {random_user['cn']}: Authentication failed: password is incorrect or there is no user with such name",
                steps=False,
            )


@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_ValidAndInvalid(
        "1.0"
    ),
)
def parallel_login(self, server, user_count=10, timeout=300):
    """Check that login of valid and invalid LDAP authenticated users works in parallel."""
    self.context.ldap_node = self.context.cluster.node(server)
    user = None

    with Given("a group of LDAP users"):
        users = [
            {"cn": f"parallel_user{i}", "userpassword": randomword(20)}
            for i in range(user_count)
        ]

    with ldap_users(*users):
        tasks = []
        with Pool(4) as pool:
            try:
                with When(
                    "users try to login in parallel",
                    description="""
                    * with valid username and password
                    * with invalid username and valid password
                    * with valid username and invalid password
                    """,
                ):
                    for i in range(10):
                        tasks.append(
                            pool.submit(
                                login_with_valid_username_and_password,
                                (
                                    users,
                                    i,
                                    50,
                                ),
                            )
                        )
                        tasks.append(
                            pool.submit(
                                login_with_valid_username_and_invalid_password,
                                (
                                    users,
                                    i,
                                    50,
                                ),
                            )
                        )
                        tasks.append(
                            pool.submit(
                                login_with_invalid_username_and_valid_password,
                                (
                                    users,
                                    i,
                                    50,
                                ),
                            )
                        )

            finally:
                with Then("it should work"):
                    for task in tasks:
                        task.result(timeout=timeout)


@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_SameUser("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_ValidAndInvalid(
        "1.0"
    ),
)
def parallel_login_with_the_same_user(self, server, timeout=300):
    """Check that valid and invalid logins of the same
    LDAP authenticated user works in parallel.
    """
    self.context.ldap_node = self.context.cluster.node(server)
    user = None

    with Given("only one LDAP user"):
        users = [{"cn": f"parallel_user1", "userpassword": randomword(20)}]

    with ldap_users(*users):
        tasks = []
        with Pool(4) as pool:
            try:
                with When(
                    "the same user tries to login in parallel",
                    description="""
                    * with valid username and password
                    * with invalid username and valid password
                    * with valid username and invalid password
                    """,
                ):
                    for i in range(10):
                        tasks.append(
                            pool.submit(
                                login_with_valid_username_and_password,
                                (
                                    users,
                                    i,
                                    50,
                                ),
                            )
                        )
                        tasks.append(
                            pool.submit(
                                login_with_valid_username_and_invalid_password,
                                (
                                    users,
                                    i,
                                    50,
                                ),
                            )
                        )
                        tasks.append(
                            pool.submit(
                                login_with_invalid_username_and_valid_password,
                                (
                                    users,
                                    i,
                                    50,
                                ),
                            )
                        )
            finally:
                with Then("it should work"):
                    for task in tasks:
                        task.result(timeout=timeout)


@TestScenario
@Tags("custom config")
def login_after_ldap_external_user_directory_is_removed(self, server):
    """Check that ClickHouse stops authenticating LDAP users
    after LDAP external user directory is removed.
    """
    with When("I login after LDAP external user directory is added"):
        with ldap_external_user_directory(server="openldap2", roles=[], restart=True):
            login_and_execute_query(username="user2", password="user2")

    with And("I attempt to login after LDAP external user directory is removed"):
        exitcode = 4
        message = f"DB::Exception: user2: Authentication failed: password is incorrect or there is no user with such name"
        login_and_execute_query(
            username="user2", password="user2", exitcode=exitcode, message=message
        )


@TestScenario
@Tags("custom config")
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_SameUser("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_ValidAndInvalid(
        "1.0"
    ),
)
def parallel_login_with_the_same_user_multiple_servers(self, server, timeout=300):
    """Check that valid and invalid logins of the same
    user defined in multiple LDAP external user directories
    works in parallel.
    """
    with Given("I have two LDAP servers"):
        entries = [(["openldap1"], []), (["openldap2"], [])]

    with Given("I define only one LDAP user"):
        users = [{"cn": f"parallel_user1", "userpassword": randomword(20)}]

    with And(
        "I create config file to define LDAP external user directory for each LDAP server"
    ):
        config = create_entries_ldap_external_user_directory_config_content(entries)

    with ldap_external_user_directory(
        server=None, roles=None, restart=True, config=config
    ):
        with ldap_users(*users, node=self.context.cluster.node("openldap1")):
            with ldap_users(*users, node=self.context.cluster.node("openldap2")):
                tasks = []
                with Pool(4) as pool:
                    try:
                        with When(
                            "the same user tries to login in parallel",
                            description="""
                            * with valid username and password
                            * with invalid username and valid password
                            * with valid username and invalid password
                            """,
                        ):
                            for i in range(10):
                                tasks.append(
                                    pool.submit(
                                        login_with_valid_username_and_password,
                                        (
                                            users,
                                            i,
                                            50,
                                        ),
                                    )
                                )
                                tasks.append(
                                    pool.submit(
                                        login_with_valid_username_and_invalid_password,
                                        (
                                            users,
                                            i,
                                            50,
                                        ),
                                    )
                                )
                                tasks.append(
                                    pool.submit(
                                        login_with_invalid_username_and_valid_password,
                                        (
                                            users,
                                            i,
                                            50,
                                        ),
                                    )
                                )
                    finally:
                        with Then("it should work"):
                            for task in tasks:
                                task.result(timeout=timeout)


@TestScenario
@Tags("custom config")
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_MultipleServers(
        "1.0"
    ),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_ValidAndInvalid(
        "1.0"
    ),
)
def parallel_login_with_multiple_servers(self, server, user_count=10, timeout=300):
    """Check that login of valid and invalid LDAP authenticated users works in parallel
    using multiple LDAP external user directories.
    """
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
        }

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

    with ldap_external_user_directory(
        server=None, roles=None, restart=True, config=config
    ):
        with ldap_users(
            *user_groups["openldap1_users"], node=self.context.cluster.node("openldap1")
        ):
            with ldap_users(
                *user_groups["openldap2_users"],
                node=self.context.cluster.node("openldap2"),
            ):
                tasks = []
                with Pool(4) as pool:
                    try:
                        with When(
                            "users in each group try to login in parallel",
                            description="""
                            * with valid username and password
                            * with invalid username and valid password
                            * with valid username and invalid password
                            """,
                        ):
                            for i in range(10):
                                for users in user_groups.values():
                                    for check in checks:
                                        tasks.append(
                                            pool.submit(
                                                check,
                                                (
                                                    users,
                                                    i,
                                                    50,
                                                ),
                                            )
                                        )
                    finally:
                        with Then("it should work"):
                            for task in tasks:
                                task.result(timeout=timeout)


@TestScenario
@Tags("custom config")
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_LocalAndMultipleLDAP(
        "1.0"
    ),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_ValidAndInvalid(
        "1.0"
    ),
)
def parallel_login_with_rbac_and_multiple_servers(
    self, server, user_count=10, timeout=300
):
    """Check that login of valid and invalid users works in parallel
    using local users defined using RBAC and LDAP users authenticated using
    multiple LDAP external user directories.
    """
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

    with ldap_external_user_directory(
        server=None, roles=None, restart=True, config=config
    ):
        with ldap_users(
            *user_groups["openldap1_users"], node=self.context.cluster.node("openldap1")
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
                                "users in each group try to login in parallel",
                                description="""
                                * with valid username and password
                                * with invalid username and valid password
                                * with valid username and invalid password
                                """,
                            ):
                                for i in range(10):
                                    for users in user_groups.values():
                                        for check in checks:
                                            tasks.append(
                                                pool.submit(
                                                    check,
                                                    (
                                                        users,
                                                        i,
                                                        50,
                                                    ),
                                                )
                                            )
                        finally:
                            with Then("it should work"):
                                for task in tasks:
                                    task.result(timeout=timeout)


@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_LocalOnly("1.0")
)
def parallel_login_with_rbac_users(self, server, user_count=10, timeout=300):
    """Check that login of only valid and invalid local users created using RBAC
    works in parallel when server configuration includes LDAP external user directory.
    """
    self.context.ldap_node = self.context.cluster.node(server)
    user = None

    users = [
        {"cn": f"parallel_user{i}", "userpassword": randomword(20)}
        for i in range(user_count)
    ]

    with rbac_users(*users):
        tasks = []
        with Pool(4) as pool:
            try:
                with When("I login in parallel"):
                    for i in range(10):
                        tasks.append(
                            pool.submit(
                                login_with_valid_username_and_password,
                                (
                                    users,
                                    i,
                                    50,
                                ),
                            )
                        )
                        tasks.append(
                            pool.submit(
                                login_with_valid_username_and_invalid_password,
                                (
                                    users,
                                    i,
                                    50,
                                ),
                            )
                        )
                        tasks.append(
                            pool.submit(
                                login_with_invalid_username_and_valid_password,
                                (
                                    users,
                                    i,
                                    50,
                                ),
                            )
                        )
            finally:
                with Then("it should work"):
                    for task in tasks:
                        task.result(timeout=timeout)


@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Users_Authentication_NewUsers("1.0")
)
def login_after_user_is_added_to_ldap(self, server):
    """Check that user can login as soon as it is added to LDAP."""
    user = {"cn": "myuser", "userpassword": "myuser"}

    with When(f"I add user to LDAP and try to login"):
        add_user_to_ldap_and_login(user=user, server=server)


@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Invalid("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_DeletedUsers("1.0"),
)
def login_after_user_is_deleted_from_ldap(self, server):
    """Check that login fails after user is deleted from LDAP."""
    self.context.ldap_node = self.context.cluster.node(server)
    user = None

    try:
        with Given(f"I add user to LDAP"):
            user = {"cn": "myuser", "userpassword": "myuser"}
            user = add_user_to_ldap(**user)

        login_and_execute_query(username=user["cn"], password=user["userpassword"])

        with When("I delete this user from LDAP"):
            delete_user_from_ldap(user)

        with Then("when I try to login again it should fail"):
            login_and_execute_query(
                username=user["cn"],
                password=user["userpassword"],
                exitcode=4,
                message=f"DB::Exception: {user['cn']}: Authentication failed: password is incorrect or there is no user with such name",
            )
    finally:
        with Finally("I make sure LDAP user is deleted"):
            if user is not None:
                delete_user_from_ldap(user, exitcode=None)


@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Invalid("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_PasswordChanged("1.0"),
)
def login_after_user_password_changed_in_ldap(self, server):
    """Check that login fails after user password is changed in LDAP."""
    self.context.ldap_node = self.context.cluster.node(server)
    user = None

    try:
        with Given(f"I add user to LDAP"):
            user = {"cn": "myuser", "userpassword": "myuser"}
            user = add_user_to_ldap(**user)

        login_and_execute_query(username=user["cn"], password=user["userpassword"])

        with When("I change user password in LDAP"):
            change_user_password_in_ldap(user, "newpassword")

        with Then("when I try to login again it should fail"):
            login_and_execute_query(
                username=user["cn"],
                password=user["userpassword"],
                exitcode=4,
                message=f"DB::Exception: {user['cn']}: Authentication failed: password is incorrect or there is no user with such name",
            )

        with And("when I try to login with the new password it should work"):
            login_and_execute_query(username=user["cn"], password="newpassword")

    finally:
        with Finally("I make sure LDAP user is deleted"):
            if user is not None:
                delete_user_from_ldap(user, exitcode=None)


@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Invalid("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_UsernameChanged("1.0"),
)
def login_after_user_cn_changed_in_ldap(self, server):
    """Check that login fails after user cn is changed in LDAP."""
    self.context.ldap_node = self.context.cluster.node(server)
    user = None
    new_user = None

    try:
        with Given(f"I add user to LDAP"):
            user = {"cn": "myuser", "userpassword": "myuser"}
            user = add_user_to_ldap(**user)

        login_and_execute_query(username=user["cn"], password=user["userpassword"])

        with When("I change user password in LDAP"):
            new_user = change_user_cn_in_ldap(user, "myuser2")

        with Then("when I try to login again it should fail"):
            login_and_execute_query(
                username=user["cn"],
                password=user["userpassword"],
                exitcode=4,
                message=f"DB::Exception: {user['cn']}: Authentication failed: password is incorrect or there is no user with such name",
            )
    finally:
        with Finally("I make sure LDAP user is deleted"):
            if new_user is not None:
                delete_user_from_ldap(new_user, exitcode=None)


@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Valid("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_LDAPServerRestart("1.0"),
)
def login_after_ldap_server_is_restarted(self, server, timeout=60):
    """Check that login succeeds after LDAP server is restarted."""
    self.context.ldap_node = self.context.cluster.node(server)
    user = None

    try:
        with Given(f"I add user to LDAP"):
            user = {"cn": "myuser", "userpassword": getuid()}
            user = add_user_to_ldap(**user)

        login_and_execute_query(username=user["cn"], password=user["userpassword"])

        with When("I restart LDAP server"):
            self.context.ldap_node.restart()

        with Then(
            "I try to login until it works", description=f"timeout {timeout} sec"
        ):
            started = time.time()
            while True:
                r = self.context.node.query(
                    "SELECT 1",
                    settings=[("user", user["cn"]), ("password", user["userpassword"])],
                    no_checks=True,
                )
                if r.exitcode == 0:
                    break
                assert time.time() - started < timeout, error(r.output)
    finally:
        with Finally("I make sure LDAP user is deleted"):
            if user is not None:
                delete_user_from_ldap(user, exitcode=None)


@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Valid("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_ClickHouseServerRestart("1.0"),
)
def login_after_clickhouse_server_is_restarted(self, server, timeout=60):
    """Check that login succeeds after ClickHouse server is restarted."""
    self.context.ldap_node = self.context.cluster.node(server)
    user = None

    try:
        with Given(f"I add user to LDAP"):
            user = {"cn": "myuser", "userpassword": getuid()}
            user = add_user_to_ldap(**user)

        login_and_execute_query(username=user["cn"], password=user["userpassword"])

        with When("I restart ClickHouse server"):
            self.context.node.restart()

        with Then(
            "I try to login until it works", description=f"timeout {timeout} sec"
        ):
            started = time.time()
            while True:
                r = self.context.node.query(
                    "SELECT 1",
                    settings=[("user", user["cn"]), ("password", user["userpassword"])],
                    no_checks=True,
                )
                if r.exitcode == 0:
                    break
                assert time.time() - started < timeout, error(r.output)
    finally:
        with Finally("I make sure LDAP user is deleted"):
            if user is not None:
                delete_user_from_ldap(user, exitcode=None)


@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Invalid("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Password_Empty("1.0"),
)
def valid_username_with_valid_empty_password(self, server):
    """Check that we can't login using valid username that has empty password."""
    user = {"cn": "empty_password", "userpassword": ""}
    exitcode = 4
    message = f"DB::Exception: {user['cn']}: Authentication failed: password is incorrect or there is no user with such name"

    add_user_to_ldap_and_login(
        user=user, exitcode=exitcode, message=message, server=server
    )


@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Invalid("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Password_Empty("1.0"),
)
def valid_username_and_invalid_empty_password(self, server):
    """Check that we can't login using valid username but invalid empty password."""
    username = "user_non_empty_password"
    user = {"cn": username, "userpassword": username}
    login = {"password": ""}

    exitcode = 4
    message = f"DB::Exception: {username}: Authentication failed: password is incorrect or there is no user with such name"

    add_user_to_ldap_and_login(
        user=user, login=login, exitcode=exitcode, message=message, server=server
    )


@TestScenario
@Requirements(RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Valid("1.0"))
def valid_username_and_password(self, server):
    """Check that we can login using valid username and password."""
    username = "valid_username_and_password"
    user = {"cn": username, "userpassword": username}

    with When(f"I add user {username} to LDAP and try to login"):
        add_user_to_ldap_and_login(user=user, server=server)


@TestScenario
@Requirements(RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Invalid("1.0"))
def valid_username_and_password_invalid_server(self, server=None):
    """Check that we can't login using valid username and valid
    password but for a different server.
    """
    self.context.ldap_node = self.context.cluster.node("openldap1")

    exitcode = 4
    message = f"DB::Exception: user2: Authentication failed: password is incorrect or there is no user with such name"

    login_and_execute_query(
        username="user2", password="user2", exitcode=exitcode, message=message
    )


@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Valid("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Username_Long("1.0"),
)
def valid_long_username_and_short_password(self, server):
    """Check that we can login using valid very long username and short password."""
    username = "long_username_12345678901234567890123456789012345678901234567890123456789012345678901234567890"
    user = {"cn": username, "userpassword": "long_username"}

    add_user_to_ldap_and_login(user=user, server=server)


@TestScenario
@Requirements(RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Invalid("1.0"))
def invalid_long_username_and_valid_short_password(self, server):
    """Check that we can't login using slightly invalid long username but valid password."""
    username = "long_username_12345678901234567890123456789012345678901234567890123456789012345678901234567890"
    user = {"cn": username, "userpassword": "long_username"}
    login = {"username": f"{username}?"}

    exitcode = 4
    message = f"DB::Exception: {login['username']}: Authentication failed: password is incorrect or there is no user with such name"

    add_user_to_ldap_and_login(
        user=user, login=login, exitcode=exitcode, message=message, server=server
    )


@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Valid("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Password_Long("1.0"),
)
def valid_short_username_and_long_password(self, server):
    """Check that we can login using valid short username with very long password."""
    username = "long_password"
    user = {
        "cn": username,
        "userpassword": "long_password_12345678901234567890123456789012345678901234567890123456789012345678901234567890",
    }

    add_user_to_ldap_and_login(user=user, server=server)


@TestScenario
@Requirements(RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Invalid("1.0"))
def valid_short_username_and_invalid_long_password(self, server):
    """Check that we can't login using valid short username and invalid long password."""
    username = "long_password"
    user = {
        "cn": username,
        "userpassword": "long_password_12345678901234567890123456789012345678901234567890123456789012345678901234567890",
    }
    login = {"password": user["userpassword"] + "1"}

    exitcode = 4
    message = f"DB::Exception: {username}: Authentication failed: password is incorrect or there is no user with such name"

    add_user_to_ldap_and_login(
        user=user, login=login, exitcode=exitcode, message=message, server=server
    )


@TestScenario
@Requirements(RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Invalid("1.0"))
def valid_username_and_invalid_password(self, server):
    """Check that we can't login using valid username and invalid password."""
    username = "valid_username_and_invalid_password"
    user = {"cn": username, "userpassword": username}
    login = {"password": user["userpassword"] + "1"}

    exitcode = 4
    message = f"DB::Exception: {username}: Authentication failed: password is incorrect or there is no user with such name"

    add_user_to_ldap_and_login(
        user=user, login=login, exitcode=exitcode, message=message, server=server
    )


@TestScenario
@Requirements(RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Invalid("1.0"))
def invalid_username_and_valid_password(self, server):
    """Check that we can't login using slightly invalid username but valid password."""
    username = "invalid_username_and_valid_password"
    user = {"cn": username, "userpassword": username}
    login = {"username": user["cn"] + "1"}

    exitcode = 4
    message = f"DB::Exception: {login['username']}: Authentication failed: password is incorrect or there is no user with such name"

    add_user_to_ldap_and_login(
        user=user, login=login, exitcode=exitcode, message=message, server=server
    )


@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Valid("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Username_UTF8("1.0"),
)
def valid_utf8_username_and_ascii_password(self, server):
    """Check that we can login using valid utf-8 username with ascii password."""
    username = "utf8_username_Gãńdåłf_Thê_Gręât"
    user = {"cn": username, "userpassword": "utf8_username"}

    add_user_to_ldap_and_login(user=user, server=server)


@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Valid("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Password_UTF8("1.0"),
)
def valid_ascii_username_and_utf8_password(self, server):
    """Check that we can login using valid ascii username with utf-8 password."""
    username = "utf8_password"
    user = {"cn": username, "userpassword": "utf8_password_Gãńdåłf_Thê_Gręât"}

    add_user_to_ldap_and_login(user=user, server=server)


@TestScenario
def empty_username_and_empty_password(self, server=None):
    """Check that we can login using empty username and empty password as
    it will use the default user and that has an empty password.
    """
    login_and_execute_query(username="", password="")


@TestScenario
@Tags("verification_cooldown")
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_VerificationCooldown_Default(
        "1.0"
    )
)
def default_verification_cooldown_value(self, server, rbac=False):
    """Check that the default value (0) for the verification cooldown parameter
    disables caching and forces contacting the LDAP server for each
    authentication request.
    """

    error_message = "DB::Exception: testVCD: Authentication failed: password is incorrect or there is no user with such name"
    error_exitcode = 4
    user = None

    with Given(
        "I have an LDAP configuration that uses the default verification_cooldown value (0)"
    ):
        servers = {
            "openldap1": {
                "host": "openldap1",
                "port": "389",
                "enable_tls": "no",
                "auth_dn_prefix": "cn=",
                "auth_dn_suffix": ",ou=users,dc=company,dc=com",
            }
        }

        self.context.ldap_node = self.context.cluster.node(server)

    try:
        with Given("I add user to LDAP"):
            user = {"cn": "testVCD", "userpassword": "testVCD"}
            user = add_user_to_ldap(**user)

        with ldap_servers(servers):
            with rbac_roles("ldap_role") as roles:
                with ldap_external_user_directory(
                    server=server, roles=roles, restart=True
                ):
                    with When("I login and execute a query"):
                        login_and_execute_query(
                            username=user["cn"], password=user["userpassword"]
                        )

                    with And("I change user password in LDAP"):
                        change_user_password_in_ldap(user, "newpassword")

                    with Then(
                        "when I try to login immediately with the old user password it should fail"
                    ):
                        login_and_execute_query(
                            username=user["cn"],
                            password=user["userpassword"],
                            exitcode=error_exitcode,
                            message=error_message,
                        )

    finally:
        with Finally("I make sure LDAP user is deleted"):
            if user is not None:
                delete_user_from_ldap(user, exitcode=None)


@TestScenario
@Tags("verification_cooldown")
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_VerificationCooldown(
        "1.0"
    )
)
def valid_verification_cooldown_value_cn_change(self, server, rbac=False):
    """Check that we can perform requests without contacting the LDAP server
    after successful authentication when the verification_cooldown parameter
    is set and the user cn is changed.
    """

    error_message = "DB::Exception: testVCD: Authentication failed: password is incorrect or there is no user with such name"
    error_exitcode = 4
    user = None
    new_user = None

    with Given(
        "I have an LDAP configuration that sets verification_cooldown parameter to 2 sec"
    ):
        servers = {
            "openldap1": {
                "host": "openldap1",
                "port": "389",
                "enable_tls": "no",
                "auth_dn_prefix": "cn=",
                "auth_dn_suffix": ",ou=users,dc=company,dc=com",
                "verification_cooldown": "2",
            }
        }

        self.context.ldap_node = self.context.cluster.node(server)

    try:
        with Given("I add user to LDAP"):
            user = {"cn": "testVCD", "userpassword": "testVCD"}
            user = add_user_to_ldap(**user)

        with ldap_servers(servers):
            with rbac_roles("ldap_role") as roles:
                with ldap_external_user_directory(
                    server=server, roles=roles, restart=True
                ):
                    with When("I login and execute a query"):
                        login_and_execute_query(
                            username=user["cn"], password=user["userpassword"]
                        )

                    with And("I change user cn in LDAP"):
                        new_user = change_user_cn_in_ldap(user, "testVCD2")

                    with Then(
                        "when I try to login again with the old user cn it should work"
                    ):
                        login_and_execute_query(
                            username=user["cn"], password=user["userpassword"]
                        )

                    with And(
                        "when I sleep for 2 seconds and try to log in, it should fail"
                    ):
                        time.sleep(2)
                        login_and_execute_query(
                            username=user["cn"],
                            password=user["userpassword"],
                            exitcode=error_exitcode,
                            message=error_message,
                        )

    finally:
        with Finally("I make sure LDAP user is deleted"):
            if new_user is not None:
                delete_user_from_ldap(new_user, exitcode=None)


@TestScenario
@Tags("verification_cooldown")
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_VerificationCooldown(
        "1.0"
    )
)
def valid_verification_cooldown_value_password_change(self, server, rbac=False):
    """Check that we can perform requests without contacting the LDAP server
    after successful authentication when the verification_cooldown parameter
    is set and the user password is changed.
    """

    error_message = "DB::Exception: testVCD: Authentication failed: password is incorrect or there is no user with such name"
    error_exitcode = 4
    user = None

    with Given(
        "I have an LDAP configuration that sets verification_cooldown parameter to 2 sec"
    ):
        servers = {
            "openldap1": {
                "host": "openldap1",
                "port": "389",
                "enable_tls": "no",
                "auth_dn_prefix": "cn=",
                "auth_dn_suffix": ",ou=users,dc=company,dc=com",
                "verification_cooldown": "2",
            }
        }

        self.context.ldap_node = self.context.cluster.node(server)

    try:
        with Given("I add user to LDAP"):
            user = {"cn": "testVCD", "userpassword": "testVCD"}
            user = add_user_to_ldap(**user)

        with ldap_servers(servers):
            with rbac_roles("ldap_role") as roles:
                with ldap_external_user_directory(
                    server=server, roles=roles, restart=True
                ):
                    with When("I login and execute a query"):
                        login_and_execute_query(
                            username=user["cn"], password=user["userpassword"]
                        )

                    with And("I change user password in LDAP"):
                        change_user_password_in_ldap(user, "newpassword")

                    with Then(
                        "when I try to login again with the old password it should work"
                    ):
                        login_and_execute_query(
                            username=user["cn"], password=user["userpassword"]
                        )

                    with And(
                        "when I sleep for 2 seconds and try to log in, it should fail"
                    ):
                        time.sleep(2)
                        login_and_execute_query(
                            username=user["cn"],
                            password=user["userpassword"],
                            exitcode=error_exitcode,
                            message=error_message,
                        )

    finally:
        with Finally("I make sure LDAP user is deleted"):
            if user is not None:
                delete_user_from_ldap(user, exitcode=None)


@TestScenario
@Tags("verification_cooldown")
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_VerificationCooldown(
        "1.0"
    )
)
def valid_verification_cooldown_value_ldap_unavailable(self, server, rbac=False):
    """Check that we can perform requests without contacting the LDAP server
    after successful authentication when the verification_cooldown parameter
    is set, even when the LDAP server is offline.
    """

    error_message = "DB::Exception: testVCD: Authentication failed: password is incorrect or there is no user with such name"
    error_exitcode = 4
    user = None

    with Given(
        "I have an LDAP configuration that sets verification_cooldown parameter to 2 sec"
    ):
        servers = {
            "openldap1": {
                "host": "openldap1",
                "port": "389",
                "enable_tls": "no",
                "auth_dn_prefix": "cn=",
                "auth_dn_suffix": ",ou=users,dc=company,dc=com",
                "verification_cooldown": "300",
            }
        }

        self.context.ldap_node = self.context.cluster.node(server)

    try:
        with Given("I add a new user to LDAP"):
            user = {"cn": "testVCD", "userpassword": "testVCD"}
            user = add_user_to_ldap(**user)

        with ldap_servers(servers):
            with rbac_roles("ldap_role") as roles:
                with ldap_external_user_directory(
                    server=server, roles=roles, restart=True
                ):

                    with When("I login and execute a query"):
                        login_and_execute_query(
                            username=user["cn"], password=user["userpassword"]
                        )

                    try:
                        with And("then I stop the ldap server"):
                            self.context.ldap_node.stop()

                        with Then(
                            "when I try to login again with the server offline it should work"
                        ):
                            login_and_execute_query(
                                username=user["cn"], password=user["userpassword"]
                            )

                    finally:
                        with Finally("I start the ldap server back up"):
                            self.context.ldap_node.start()

    finally:
        with Finally("I make sure LDAP user is deleted"):
            if user is not None:
                delete_user_from_ldap(user, exitcode=None)


@TestOutline
def repeat_requests(self, server, iterations, vcd_value, rbac=False):
    """Run repeated requests from some user to the LDAP server."""

    user = None

    with Given(
        f"I have an LDAP configuration that sets verification_cooldown parameter to {vcd_value} sec"
    ):
        servers = {
            "openldap1": {
                "host": "openldap1",
                "port": "389",
                "enable_tls": "no",
                "auth_dn_prefix": "cn=",
                "auth_dn_suffix": ",ou=users,dc=company,dc=com",
                "verification_cooldown": vcd_value,
            }
        }

        self.context.ldap_node = self.context.cluster.node(server)

    try:
        with And("I add a new user to LDAP"):
            user = {"cn": "testVCD", "userpassword": "testVCD"}
            user = add_user_to_ldap(**user)

        with ldap_servers(servers):
            with rbac_roles("ldap_role") as roles:
                with ldap_external_user_directory(
                    server=server, roles=roles, restart=True
                ):
                    with When(f"I login and execute some query {iterations} times"):
                        start_time = time.time()
                        r = self.context.node.command(
                            f"time for i in {{1..{iterations}}}; do clickhouse client -q \"SELECT 1\" --user {user['cn']} --password {user['userpassword']} > /dev/null; done"
                        )
                        end_time = time.time()

                        return end_time - start_time

    finally:
        with Finally("I make sure LDAP user is deleted"):
            if user is not None:
                delete_user_from_ldap(user, exitcode=None)


@TestScenario
@Tags("verification_cooldown")
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_VerificationCooldown_Performance(
        "1.0"
    )
)
def verification_cooldown_performance(self, server, rbac=False, iterations=5000):
    """Check login performance when the verification cooldown
    parameter is set to a positive value when comparing to the case when
    the verification cooldown parameter is turned off.
    """

    vcd_time = 0
    no_vcd_time = 0

    with Example(
        f"Repeated requests with verification cooldown parameter set to 600 seconds, {iterations} iterations"
    ):
        vcd_time = repeat_requests(
            server=server, iterations=iterations, vcd_value="600", rbac=rbac
        )
        metric("login_with_vcd_value_600", units="seconds", value=vcd_time)

    with Example(
        f"Repeated requests with verification cooldown parameter set to 0 seconds, {iterations} iterations"
    ):
        no_vcd_time = repeat_requests(
            server=server, iterations=iterations, vcd_value="0", rbac=rbac
        )
        metric("login_with_vcd_value_0", units="seconds", value=no_vcd_time)

    with Then("Log the performance improvement as a percentage"):
        metric(
            "percentage_improvement",
            units="%",
            value=100 * (no_vcd_time - vcd_time) / vcd_time,
        )


@TestOutline
def check_verification_cooldown_reset_on_core_server_parameter_change(
    self, server, parameter_name, parameter_value, rbac=False
):
    """Check that the LDAP login cache is reset for all the LDAP authentication users
    when verification_cooldown parameter is set after one of the core server
    parameters is changed in the LDAP server configuration.
    """
    config_d_dir = "/etc/clickhouse-server/config.d"
    config_file = "ldap_servers.xml"
    error_message = "DB::Exception: {user}: Authentication failed: password is incorrect or there is no user with such name"
    error_exitcode = 4
    user = None
    config = None
    updated_config = None

    with Given(
        "I have an LDAP configuration that sets verification_cooldown parameter to 600 sec"
    ):
        servers = {
            "openldap1": {
                "host": "openldap1",
                "port": "389",
                "enable_tls": "no",
                "auth_dn_prefix": "cn=",
                "auth_dn_suffix": ",ou=users,dc=company,dc=com",
                "verification_cooldown": "600",
            }
        }

        self.context.ldap_node = self.context.cluster.node(server)

    with And("LDAP authenticated user"):
        users = [
            {"cn": f"testVCD_0", "userpassword": "testVCD_0"},
            {"cn": f"testVCD_1", "userpassword": "testVCD_1"},
        ]

    with And("I create LDAP servers configuration file"):
        config = create_ldap_servers_config_content(servers, config_d_dir, config_file)

    with ldap_users(*users) as users:
        with ldap_servers(servers=None, restart=False, config=config):
            with rbac_roles("ldap_role") as roles:
                with ldap_external_user_directory(
                    server=server, roles=roles, restart=True
                ):
                    with When("I login and execute a query"):
                        for user in users:
                            with By(f"as user {user['cn']}"):
                                login_and_execute_query(
                                    username=user["cn"], password=user["userpassword"]
                                )

                    with And("I change user password in LDAP"):
                        for user in users:
                            with By(f"for user {user['cn']}"):
                                change_user_password_in_ldap(user, "newpassword")

                    with And(
                        f"I change the server {parameter_name} core parameter",
                        description=f"{parameter_value}",
                    ):
                        servers["openldap1"][parameter_name] = parameter_value

                    with And(
                        "I create an updated the config file that has a different server host name"
                    ):
                        updated_config = create_ldap_servers_config_content(
                            servers, config_d_dir, config_file
                        )

                    with modify_config(updated_config, restart=False):
                        with Then(
                            "when I try to log in it should fail as cache should have been reset"
                        ):
                            for user in users:
                                with By(f"as user {user['cn']}"):
                                    login_and_execute_query(
                                        username=user["cn"],
                                        password=user["userpassword"],
                                        exitcode=error_exitcode,
                                        message=error_message.format(user=user["cn"]),
                                    )


@TestScenario
@Tags("verification_cooldown")
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_VerificationCooldown_Reset_ChangeInCoreServerParameters(
        "1.0"
    )
)
def verification_cooldown_reset_on_server_host_parameter_change(
    self, server, rbac=False
):
    """Check that the LDAP login cache is reset for all the LDAP authentication users
    when verification_cooldown parameter is set after server host name
    is changed in the LDAP server configuration.
    """
    check_verification_cooldown_reset_on_core_server_parameter_change(
        server=server, parameter_name="host", parameter_value="openldap2", rbac=rbac
    )


@TestScenario
@Tags("verification_cooldown")
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_VerificationCooldown_Reset_ChangeInCoreServerParameters(
        "1.0"
    )
)
def verification_cooldown_reset_on_server_port_parameter_change(
    self, server, rbac=False
):
    """Check that the LDAP login cache is reset for all the LDAP authentication users
    when verification_cooldown parameter is set after server port is changed in the
    LDAP server configuration.
    """
    check_verification_cooldown_reset_on_core_server_parameter_change(
        server=server, parameter_name="port", parameter_value="9006", rbac=rbac
    )


@TestScenario
@Tags("verification_cooldown")
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_VerificationCooldown_Reset_ChangeInCoreServerParameters(
        "1.0"
    )
)
def verification_cooldown_reset_on_server_auth_dn_prefix_parameter_change(
    self, server, rbac=False
):
    """Check that the LDAP login cache is reset for all the LDAP authentication users
    when verification_cooldown parameter is set after server auth_dn_prefix
    is changed in the LDAP server configuration.
    """
    check_verification_cooldown_reset_on_core_server_parameter_change(
        server=server,
        parameter_name="auth_dn_prefix",
        parameter_value="cxx=",
        rbac=rbac,
    )


@TestScenario
@Tags("verification_cooldown")
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_VerificationCooldown_Reset_ChangeInCoreServerParameters(
        "1.0"
    )
)
def verification_cooldown_reset_on_server_auth_dn_suffix_parameter_change(
    self, server, rbac=False
):
    """Check that the LDAP login cache is reset for all the LDAP authentication users
    when verification_cooldown parameter is set after server auth_dn_suffix
    is changed in the LDAP server configuration.
    """
    check_verification_cooldown_reset_on_core_server_parameter_change(
        server=server,
        parameter_name="auth_dn_suffix",
        parameter_value=",ou=company,dc=users,dc=com",
        rbac=rbac,
    )


@TestScenario
@Name("verification cooldown reset when invalid password is provided")
@Tags("verification_cooldown")
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_VerificationCooldown_Reset_InvalidPassword(
        "1.0"
    )
)
def scenario(self, server, rbac=False):
    """Check that cached bind requests for the user are discarded when
    the user provides invalid login credentials.
    """
    user = None
    error_exitcode = 4
    error_message = "DB::Exception: testVCD: Authentication failed: password is incorrect or there is no user with such name"

    with Given(
        "I have an LDAP configuration that sets verification_cooldown parameter to 600 sec"
    ):
        servers = {
            "openldap1": {
                "host": "openldap1",
                "port": "389",
                "enable_tls": "no",
                "auth_dn_prefix": "cn=",
                "auth_dn_suffix": ",ou=users,dc=company,dc=com",
                "verification_cooldown": "600",
            }
        }

        self.context.ldap_node = self.context.cluster.node(server)

    try:
        with Given("I add a new user to LDAP"):
            user = {"cn": "testVCD", "userpassword": "testVCD"}
            user = add_user_to_ldap(**user)

        with ldap_servers(servers):
            with rbac_roles("ldap_role") as roles:
                with ldap_external_user_directory(
                    server=server, roles=roles, restart=True
                ):

                    with When("I login and execute a query"):
                        login_and_execute_query(
                            username=user["cn"], password=user["userpassword"]
                        )

                    with And("I change user password in LDAP"):
                        change_user_password_in_ldap(user, "newpassword")

                    with Then(
                        "When I try to log in with the cached password it should work"
                    ):
                        login_and_execute_query(
                            username=user["cn"], password=user["userpassword"]
                        )

                    with And(
                        "When I try to log in with an incorrect password it should fail"
                    ):
                        login_and_execute_query(
                            username=user["cn"],
                            password="incorrect",
                            exitcode=error_exitcode,
                            message=error_message,
                        )

                    with And(
                        "When I try to log in with the cached password it should fail"
                    ):
                        login_and_execute_query(
                            username=user["cn"],
                            password="incorrect",
                            exitcode=error_exitcode,
                            message=error_message,
                        )

    finally:
        with Finally("I make sure LDAP user is deleted"):
            if user is not None:
                delete_user_from_ldap(user, exitcode=None)


@TestScenario
@Requirements(RQ_SRS_009_LDAP_ExternalUserDirectory_Users_Lookup_Priority("2.0"))
def user_lookup_priority(self, server):
    """Check that users are looked up in the same priority
    as they are defined in the `<user_dictionaries>` section
    of the `config.xml`. For this test we have the following priority list
    as defined by the configuration files:

    * users.xml
    * local directory
    * LDAP external user directory
    """
    self.context.ldap_node = self.context.cluster.node(server)

    message = "DB::Exception: {username}: Authentication failed: password is incorrect or there is no user with such name"
    exitcode = 4

    users = {
        "default": {"username": "default", "password": "userdefault"},
        "local": {"username": "local", "password": "userlocal"},
        "ldap": {"username": "ldap", "password": "userldap"},
    }

    with ldap_users(
        *[
            {"cn": user["username"], "userpassword": user["password"]}
            for user in users.values()
        ]
    ):
        with rbac_users({"cn": "local", "userpassword": "local"}):
            with When(
                "I try to login as 'default' user which is also defined in users.xml it should fail"
            ):
                login_and_execute_query(
                    **users["default"],
                    exitcode=exitcode,
                    message=message.format(username="default"),
                )

            with When(
                "I try to login as 'local' user which is also defined in local storage it should fail"
            ):
                login_and_execute_query(
                    **users["local"],
                    exitcode=exitcode,
                    message=message.format(username="local"),
                )

            with When(
                "I try to login as 'ldap' user defined only in LDAP it should work"
            ):
                login_and_execute_query(**users["ldap"])


@TestOutline(Feature)
@Name("user authentications")
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Connection_Authentication_Mechanism_NamePassword(
        "1.0"
    ),
)
def feature(self, servers=None, server=None, node="clickhouse1"):
    """Check that users can be authenticated using an LDAP external user directory."""
    self.context.node = self.context.cluster.node(node)

    if servers is None:
        servers = globals()["servers"]

    if server is None:
        server = "openldap1"

    with ldap_servers(servers):
        with rbac_roles("ldap_role") as roles:
            with ldap_external_user_directory(server=server, roles=roles, restart=True):
                for scenario in loads(
                    current_module(),
                    Scenario,
                    filter=~has.tag("custom config")
                    & ~has.tag("verification_cooldown"),
                ):
                    Scenario(test=scenario)(server=server)

        for scenario in loads(
            current_module(), Scenario, filter=has.tag("custom config")
        ):
            Scenario(test=scenario)(server=server)

    for scenario in loads(
        current_module(), Scenario, filter=has.tag("verification_cooldown")
    ):
        Scenario(test=scenario)(server=server)
