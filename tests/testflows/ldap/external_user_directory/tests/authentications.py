# -*- coding: utf-8 -*-
import random

from multiprocessing.dummy import Pool
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
        "auth_dn_suffix": ",ou=users,dc=company,dc=com"
    },
    "openldap2": {
        "host": "openldap2",
        "port": "636",
        "enable_tls": "yes",
        "auth_dn_prefix": "cn=",
        "auth_dn_suffix": ",ou=users,dc=company,dc=com",
        "tls_require_cert": "never",
    }
}

@TestOutline
def add_user_to_ldap_and_login(self, server, user=None, ch_user=None, login=None, exitcode=None, message=None):
    """Add user to LDAP and ClickHouse and then try to login.
    """
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

        login_and_execute_query(username=username, password=password, exitcode=exitcode, message=message)

def login_with_valid_username_and_password(users, i, iterations=10):
    """Login with valid username and password.
    """
    with When(f"valid users try to login #{i}"):
        for i in range(iterations):
            random_user = users[random.randint(0, len(users)-1)]
            login_and_execute_query(username=random_user["cn"], password=random_user["userpassword"], steps=False)

def login_with_valid_username_and_invalid_password(users, i, iterations=10):
    """Login with valid username and invalid password.
    """
    with When(f"users try to login with valid username and invalid password #{i}"):
        for i in range(iterations):
            random_user = users[random.randint(0, len(users)-1)]
            login_and_execute_query(username=random_user["cn"],
                password=(random_user["userpassword"] + randomword(1)),
                exitcode=4,
                message=f"DB::Exception: {random_user['cn']}: Authentication failed: password is incorrect or there is no user with such name",
                steps=False)

def login_with_invalid_username_and_valid_password(users, i, iterations=10):
    """Login with invalid username and valid password.
    """
    with When(f"users try to login with invalid username and valid password #{i}"):
        for i in range(iterations):
            random_user = dict(users[random.randint(0, len(users)-1)])
            random_user["cn"] += randomword(1)
            login_and_execute_query(username=random_user["cn"],
                password=random_user["userpassword"],
                exitcode=4,
                message=f"DB::Exception: {random_user['cn']}: Authentication failed: password is incorrect or there is no user with such name",
                steps=False)

@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_ValidAndInvalid("1.0")
)
def parallel_login(self, server, user_count=10, timeout=200):
    """Check that login of valid and invalid LDAP authenticated users works in parallel.
    """
    self.context.ldap_node = self.context.cluster.node(server)
    user = None

    with Given("a group of LDAP users"):
        users = [{"cn": f"parallel_user{i}", "userpassword": randomword(20)} for i in range(user_count)]

    with ldap_users(*users):
        tasks = []
        try:
            with When("users try to login in parallel", description="""
                * with valid username and password
                * with invalid username and valid password
                * with valid username and invalid password
                """):
                p = Pool(15)
                for i in range(25):
                    tasks.append(p.apply_async(login_with_valid_username_and_password, (users, i, 50,)))
                    tasks.append(p.apply_async(login_with_valid_username_and_invalid_password, (users, i, 50,)))
                    tasks.append(p.apply_async(login_with_invalid_username_and_valid_password, (users, i, 50,)))

        finally:
            with Then("it should work"):
                join(tasks, timeout)

@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_SameUser("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_ValidAndInvalid("1.0")
)
def parallel_login_with_the_same_user(self, server, timeout=200):
    """Check that valid and invalid logins of the same
    LDAP authenticated user works in parallel.
    """
    self.context.ldap_node = self.context.cluster.node(server)
    user = None

    with Given("only one LDAP user"):
        users = [{"cn": f"parallel_user1", "userpassword": randomword(20)}]

    with ldap_users(*users):
        tasks = []
        try:
            with When("the same user tries to login in parallel", description="""
                * with valid username and password
                * with invalid username and valid password
                * with valid username and invalid password
                """):
                p = Pool(15)
                for i in range(25):
                    tasks.append(p.apply_async(login_with_valid_username_and_password, (users, i, 50,)))
                    tasks.append(p.apply_async(login_with_valid_username_and_invalid_password, (users, i, 50,)))
                    tasks.append(p.apply_async(login_with_invalid_username_and_valid_password, (users, i, 50,)))

        finally:
            with Then("it should work"):
                join(tasks, timeout)

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
        login_and_execute_query(username="user2", password="user2", exitcode=exitcode, message=message)

@TestScenario
@Tags("custom config")
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_SameUser("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_ValidAndInvalid("1.0")
)
def parallel_login_with_the_same_user_multiple_servers(self, server, timeout=200):
    """Check that valid and invalid logins of the same
    user defined in multiple LDAP external user directories
    works in parallel.
    """
    with Given("I have two LDAP servers"):
        entries = [
            (["openldap1"], []),
            (["openldap2"], [])
        ]

    with Given("I define only one LDAP user"):
        users = [{"cn": f"parallel_user1", "userpassword": randomword(20)}]

    with And("I create config file to define LDAP external user directory for each LDAP server"):
        config = create_entries_ldap_external_user_directory_config_content(entries)

    with ldap_external_user_directory(server=None, roles=None, restart=True, config=config):
        with ldap_users(*users, node=self.context.cluster.node("openldap1")):
            with ldap_users(*users, node=self.context.cluster.node("openldap2")):
                tasks = []
                try:
                    with When("the same user tries to login in parallel", description="""
                        * with valid username and password
                        * with invalid username and valid password
                        * with valid username and invalid password
                        """):
                        p = Pool(15)
                        for i in range(25):
                            tasks.append(p.apply_async(login_with_valid_username_and_password, (users, i, 50,)))
                            tasks.append(p.apply_async(login_with_valid_username_and_invalid_password, (users, i, 50,)))
                            tasks.append(p.apply_async(login_with_invalid_username_and_valid_password, (users, i, 50,)))

                finally:
                    with Then("it should work"):
                        join(tasks, timeout)

@TestScenario
@Tags("custom config")
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_MultipleServers("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_ValidAndInvalid("1.0")
)
def parallel_login_with_multiple_servers(self, server, user_count=10, timeout=200):
    """Check that login of valid and invalid LDAP authenticated users works in parallel
    using multiple LDAP external user directories.
    """
    with Given("I have two LDAP servers"):
        entries = [
            (["openldap1"], []),
            (["openldap2"], [])
        ]

    with And("I define a group of users to be created on each LDAP server"):
        user_groups = {
            "openldap1_users": [{"cn": f"openldap1_parallel_user{i}", "userpassword": randomword(20)} for i in range(user_count)],
            "openldap2_users": [{"cn": f"openldap2_parallel_user{i}", "userpassword": randomword(20)} for i in range(user_count)]
        }

    with And("I have a list of checks that I want to run for each user group"):
        checks = [
            login_with_valid_username_and_password,
            login_with_valid_username_and_invalid_password,
            login_with_invalid_username_and_valid_password
        ]

    with And("I create config file to define LDAP external user directory for each LDAP server"):
        config = create_entries_ldap_external_user_directory_config_content(entries)

    with ldap_external_user_directory(server=None, roles=None, restart=True, config=config):
        with ldap_users(*user_groups["openldap1_users"], node=self.context.cluster.node("openldap1")):
            with ldap_users(*user_groups["openldap2_users"], node=self.context.cluster.node("openldap2")):
                tasks = []

                try:
                    with When("users in each group try to login in parallel", description="""
                        * with valid username and password
                        * with invalid username and valid password
                        * with valid username and invalid password
                        """):
                        p = Pool(15)
                        for i in range(25):
                            for users in user_groups.values():
                                for check in checks:
                                    tasks.append(p.apply_async(check, (users, i, 50,)))

                finally:
                    with Then("it should work"):
                        join(tasks, timeout)

@TestScenario
@Tags("custom config")
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_LocalAndMultipleLDAP("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_ValidAndInvalid("1.0")
)
def parallel_login_with_rbac_and_multiple_servers(self, server, user_count=10, timeout=200):
    """Check that login of valid and invalid users works in parallel
    using local users defined using RBAC and LDAP users authenticated using
    multiple LDAP external user directories.
    """
    with Given("I have two LDAP servers"):
        entries = [
            (["openldap1"], []),
            (["openldap2"], [])
        ]

    with And("I define a group of users to be created on each LDAP server"):
        user_groups = {
            "openldap1_users": [{"cn": f"openldap1_parallel_user{i}", "userpassword": randomword(20)} for i in range(user_count)],
            "openldap2_users": [{"cn": f"openldap2_parallel_user{i}", "userpassword": randomword(20)} for i in range(user_count)],
            "local_users": [{"cn": f"local_parallel_user{i}", "userpassword": randomword(20)} for i in range(user_count)]
        }

    with And("I have a list of checks that I want to run for each user group"):
        checks = [
            login_with_valid_username_and_password,
            login_with_valid_username_and_invalid_password,
            login_with_invalid_username_and_valid_password
        ]

    with And("I create config file to define LDAP external user directory for each LDAP server"):
        config = create_entries_ldap_external_user_directory_config_content(entries)

    with ldap_external_user_directory(server=None, roles=None, restart=True, config=config):
        with ldap_users(*user_groups["openldap1_users"], node=self.context.cluster.node("openldap1")):
            with ldap_users(*user_groups["openldap2_users"], node=self.context.cluster.node("openldap2")):
                with rbac_users(*user_groups["local_users"]):
                    tasks = []

                    try:
                        with When("users in each group try to login in parallel", description="""
                            * with valid username and password
                            * with invalid username and valid password
                            * with valid username and invalid password
                            """):
                            p = Pool(15)
                            for i in range(25):
                                for users in user_groups.values():
                                    for check in checks:
                                        tasks.append(p.apply_async(check, (users, i, 50,)))

                    finally:
                        with Then("it should work"):
                            join(tasks, timeout)

@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_LocalOnly("1.0")
)
def parallel_login_with_rbac_users(self, server, user_count=10, timeout=200):
    """Check that login of only valid and invalid local users created using RBAC
    works in parallel when server configuration includes LDAP external user directory.
    """
    self.context.ldap_node = self.context.cluster.node(server)
    user = None

    users = [{"cn": f"parallel_user{i}", "userpassword": randomword(20)} for i in range(user_count)]

    with rbac_users(*users):
        tasks = []
        try:
            with When("I login in parallel"):
                p = Pool(15)
                for i in range(25):
                    tasks.append(p.apply_async(login_with_valid_username_and_password, (users, i, 50,)))
                    tasks.append(p.apply_async(login_with_valid_username_and_invalid_password, (users, i, 50,)))
                    tasks.append(p.apply_async(login_with_invalid_username_and_valid_password, (users, i, 50,)))
        finally:
            with Then("it should work"):
                join(tasks, timeout)

@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Users_Authentication_NewUsers("1.0")
)
def login_after_user_is_added_to_ldap(self, server):
    """Check that user can login as soon as it is added to LDAP.
    """
    user = {"cn": "myuser", "userpassword": "myuser"}

    with When(f"I add user to LDAP and try to login"):
        add_user_to_ldap_and_login(user=user, server=server)

@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Invalid("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_DeletedUsers("1.0")
)
def login_after_user_is_deleted_from_ldap(self, server):
    """Check that login fails after user is deleted from LDAP.
    """
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
            login_and_execute_query(username=user["cn"], password=user["userpassword"],
                exitcode=4,
                message=f"DB::Exception: {user['cn']}: Authentication failed: password is incorrect or there is no user with such name"
            )
    finally:
        with Finally("I make sure LDAP user is deleted"):
            if user is not None:
                delete_user_from_ldap(user, exitcode=None)

@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Invalid("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_PasswordChanged("1.0")
)
def login_after_user_password_changed_in_ldap(self, server):
    """Check that login fails after user password is changed in LDAP.
    """
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
            login_and_execute_query(username=user["cn"], password=user["userpassword"],
                exitcode=4,
                message=f"DB::Exception: {user['cn']}: Authentication failed: password is incorrect or there is no user with such name"
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
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_UsernameChanged("1.0")
)
def login_after_user_cn_changed_in_ldap(self, server):
    """Check that login fails after user cn is changed in LDAP.
    """
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
            login_and_execute_query(username=user["cn"], password=user["userpassword"],
                exitcode=4,
                message=f"DB::Exception: {user['cn']}: Authentication failed: password is incorrect or there is no user with such name"
            )
    finally:
        with Finally("I make sure LDAP user is deleted"):
            if new_user is not None:
                delete_user_from_ldap(new_user, exitcode=None)

@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Valid("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_LDAPServerRestart("1.0")
)
def login_after_ldap_server_is_restarted(self, server, timeout=60):
    """Check that login succeeds after LDAP server is restarted.
    """
    self.context.ldap_node = self.context.cluster.node(server)
    user = None

    try:
        with Given(f"I add user to LDAP"):
            user = {"cn": "myuser", "userpassword": getuid()}
            user = add_user_to_ldap(**user)

        login_and_execute_query(username=user["cn"], password=user["userpassword"])

        with When("I restart LDAP server"):
            self.context.ldap_node.restart()

        with Then("I try to login until it works", description=f"timeout {timeout} sec"):
            started = time.time()
            while True:
                r = self.context.node.query("SELECT 1",
                    settings=[("user", user["cn"]), ("password", user["userpassword"])],
                    no_checks=True)
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
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_ClickHouseServerRestart("1.0")
)
def login_after_clickhouse_server_is_restarted(self, server, timeout=60):
    """Check that login succeeds after ClickHouse server is restarted.
    """
    self.context.ldap_node = self.context.cluster.node(server)
    user = None

    try:
        with Given(f"I add user to LDAP"):
            user = {"cn": "myuser", "userpassword": getuid()}
            user = add_user_to_ldap(**user)

        login_and_execute_query(username=user["cn"], password=user["userpassword"])

        with When("I restart ClickHouse server"):
            self.context.node.restart()

        with Then("I try to login until it works", description=f"timeout {timeout} sec"):
            started = time.time()
            while True:
                r = self.context.node.query("SELECT 1",
                    settings=[("user", user["cn"]), ("password", user["userpassword"])],
                    no_checks=True)
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
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Password_Empty("1.0")
)
def valid_username_with_valid_empty_password(self, server):
    """Check that we can't login using valid username that has empty password.
    """
    user = {"cn": "empty_password", "userpassword": ""}
    exitcode = 4
    message = f"DB::Exception: {user['cn']}: Authentication failed: password is incorrect or there is no user with such name"

    add_user_to_ldap_and_login(user=user, exitcode=exitcode, message=message, server=server)

@TestScenario
@Requirements(
   RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Invalid("1.0"),
   RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Password_Empty("1.0")
)
def valid_username_and_invalid_empty_password(self, server):
    """Check that we can't login using valid username but invalid empty password.
    """
    username = "user_non_empty_password"
    user = {"cn": username, "userpassword": username}
    login = {"password": ""}

    exitcode = 4
    message = f"DB::Exception: {username}: Authentication failed: password is incorrect or there is no user with such name"

    add_user_to_ldap_and_login(user=user, login=login, exitcode=exitcode, message=message, server=server)

@TestScenario
@Requirements(
   RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Valid("1.0")
)
def valid_username_and_password(self, server):
    """Check that we can login using valid username and password.
    """
    username = "valid_username_and_password"
    user = {"cn": username, "userpassword": username}

    with When(f"I add user {username} to LDAP and try to login"):
        add_user_to_ldap_and_login(user=user, server=server)

@TestScenario
@Requirements(
   RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Invalid("1.0")
)
def valid_username_and_password_invalid_server(self, server=None):
    """Check that we can't login using valid username and valid
    password but for a different server.
    """
    self.context.ldap_node = self.context.cluster.node("openldap1")

    exitcode = 4
    message = f"DB::Exception: user2: Authentication failed: password is incorrect or there is no user with such name"

    login_and_execute_query(username="user2", password="user2", exitcode=exitcode, message=message)

@TestScenario
@Requirements(
   RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Valid("1.0"),
   RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Username_Long("1.0"),
)
def valid_long_username_and_short_password(self, server):
    """Check that we can login using valid very long username and short password.
    """
    username = "long_username_12345678901234567890123456789012345678901234567890123456789012345678901234567890"
    user = {"cn": username, "userpassword": "long_username"}

    add_user_to_ldap_and_login(user=user, server=server)

@TestScenario
@Requirements(
   RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Invalid("1.0")
)
def invalid_long_username_and_valid_short_password(self, server):
    """Check that we can't login using slightly invalid long username but valid password.
    """
    username = "long_username_12345678901234567890123456789012345678901234567890123456789012345678901234567890"
    user = {"cn": username, "userpassword": "long_username"}
    login = {"username": f"{username}?"}

    exitcode = 4
    message=f"DB::Exception: {login['username']}: Authentication failed: password is incorrect or there is no user with such name"

    add_user_to_ldap_and_login(user=user, login=login, exitcode=exitcode, message=message, server=server)

@TestScenario
@Requirements(
   RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Valid("1.0"),
   RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Password_Long("1.0")
)
def valid_short_username_and_long_password(self, server):
    """Check that we can login using valid short username with very long password.
    """
    username = "long_password"
    user = {"cn": username, "userpassword": "long_password_12345678901234567890123456789012345678901234567890123456789012345678901234567890"}

    add_user_to_ldap_and_login(user=user, server=server)

@TestScenario
@Requirements(
   RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Invalid("1.0")
)
def valid_short_username_and_invalid_long_password(self, server):
    """Check that we can't login using valid short username and invalid long password.
    """
    username = "long_password"
    user = {"cn": username, "userpassword": "long_password_12345678901234567890123456789012345678901234567890123456789012345678901234567890"}
    login = {"password": user["userpassword"] + "1"}

    exitcode = 4
    message=f"DB::Exception: {username}: Authentication failed: password is incorrect or there is no user with such name"

    add_user_to_ldap_and_login(user=user, login=login, exitcode=exitcode, message=message, server=server)

@TestScenario
@Requirements(
   RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Invalid("1.0")
)
def valid_username_and_invalid_password(self, server):
    """Check that we can't login using valid username and invalid password.
    """
    username = "valid_username_and_invalid_password"
    user = {"cn": username, "userpassword": username}
    login = {"password": user["userpassword"] + "1"}

    exitcode = 4
    message=f"DB::Exception: {username}: Authentication failed: password is incorrect or there is no user with such name"

    add_user_to_ldap_and_login(user=user, login=login, exitcode=exitcode, message=message, server=server)

@TestScenario
@Requirements(
   RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Invalid("1.0")
)
def invalid_username_and_valid_password(self, server):
    """Check that we can't login using slightly invalid username but valid password.
    """
    username = "invalid_username_and_valid_password"
    user = {"cn": username, "userpassword": username}
    login = {"username": user["cn"] + "1"}

    exitcode = 4
    message=f"DB::Exception: {login['username']}: Authentication failed: password is incorrect or there is no user with such name"

    add_user_to_ldap_and_login(user=user, login=login, exitcode=exitcode, message=message, server=server)

@TestScenario
@Requirements(
   RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Valid("1.0"),
   RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Username_UTF8("1.0")
)
def valid_utf8_username_and_ascii_password(self, server):
    """Check that we can login using valid utf-8 username with ascii password.
    """
    username = "utf8_username_Gãńdåłf_Thê_Gręât"
    user = {"cn": username, "userpassword": "utf8_username"}

    add_user_to_ldap_and_login(user=user, server=server)

@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Valid("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Password_UTF8("1.0")
)
def valid_ascii_username_and_utf8_password(self, server):
    """Check that we can login using valid ascii username with utf-8 password.
    """
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
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Users_Lookup_Priority("2.0")
)
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

    message="DB::Exception: {username}: Authentication failed: password is incorrect or there is no user with such name"
    exitcode = 4

    users = {
        "default": {"username": "default", "password": "userdefault"},
        "local": {"username": "local", "password": "userlocal"},
        "ldap": {"username": "ldap", "password": "userldap"}
    }

    with ldap_users(*[{"cn": user["username"], "userpassword": user["password"]} for user in users.values()]):
        with rbac_users({"cn": "local", "userpassword": "local"}):
            with When("I try to login as 'default' user which is also defined in users.xml it should fail"):
                login_and_execute_query(**users["default"], exitcode=exitcode, message=message.format(username="default"))

            with When("I try to login as 'local' user which is also defined in local storage it should fail"):
                login_and_execute_query(**users["local"], exitcode=exitcode, message=message.format(username="local"))

            with When("I try to login as 'ldap' user defined only in LDAP it should work"):
                login_and_execute_query(**users["ldap"])


@TestOutline(Feature)
@Name("user authentications")
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Connection_Authentication_Mechanism_NamePassword("1.0")
)
def feature(self, servers=None, server=None, node="clickhouse1"):
    """Check that users can be authenticated using an LDAP external user directory.
    """
    self.context.node = self.context.cluster.node(node)

    if servers is None:
        servers = globals()["servers"]

    if server is None:
        server = "openldap1"

    with ldap_servers(servers):
        with rbac_roles("ldap_role") as roles:
            with ldap_external_user_directory(server=server, roles=roles, restart=True):
                for scenario in loads(current_module(), Scenario, filter=~has.tag("custom config")):
                    Scenario(test=scenario, flags=TE)(server=server)

        for scenario in loads(current_module(), Scenario, filter=has.tag("custom config")):
            Scenario(test=scenario, flags=TE)(server=server)
