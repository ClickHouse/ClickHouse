# -*- coding: utf-8 -*-
import random
import time

from helpers.common import Pool, join
from testflows.core import *
from testflows.asserts import error
from ldap.authentication.tests.common import *
from ldap.authentication.requirements import *

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

@TestStep(When)
@Name("I login as {username} and execute query")
@Args(format_name=True)
def login_and_execute_query(self, username, password, exitcode=None, message=None, steps=True):
    """Execute query as some user.
    """
    self.context.node.query("SELECT 1",
        settings=[("user", username), ("password", password)],
        exitcode=exitcode or 0,
        message=message, steps=steps)

@TestScenario
def add_user_to_ldap_and_login(self, server, user=None, ch_user=None, login=None, exitcode=None, message=None, rbac=False):
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
        ch_user["username"] = ch_user.get("username", user["cn"])
        ch_user["server"] = ch_user.get("server", user["_server"])

        with ldap_authenticated_users(ch_user, config_file=f"ldap_users_{getuid()}.xml", restart=True, rbac=rbac):
            username = login.get("username", user["cn"])
            password = login.get("password", user["userpassword"])
            login_and_execute_query(username=username, password=password, exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Authentication_Parallel("1.0"),
    RQ_SRS_007_LDAP_Authentication_Parallel_ValidAndInvalid("1.0")
)
def parallel_login(self, server, user_count=10, timeout=300, rbac=False):
    """Check that login of valid and invalid LDAP authenticated users works in parallel.
    """
    self.context.ldap_node = self.context.cluster.node(server)
    user = None

    users = [{"cn": f"parallel_user{i}", "userpassword": randomword(20)} for i in range(user_count)]

    with ldap_users(*users):
        with ldap_authenticated_users(*[{"username": user["cn"], "server": server} for user in users], rbac=rbac):

            def login_with_valid_username_and_password(users, i, iterations=10):
                with When(f"valid users try to login #{i}"):
                    for i in range(iterations):
                        random_user = users[random.randint(0, len(users)-1)]
                        login_and_execute_query(username=random_user["cn"], password=random_user["userpassword"], steps=False)

            def login_with_valid_username_and_invalid_password(users, i, iterations=10):
                with When(f"users try to login with valid username and invalid password #{i}"):
                    for i in range(iterations):
                        random_user = users[random.randint(0, len(users)-1)]
                        login_and_execute_query(username=random_user["cn"],
                                password=(random_user["userpassword"] + randomword(1)),
                                exitcode=4,
                                message=f"DB::Exception: {random_user['cn']}: Authentication failed: password is incorrect or there is no user with such name",
                                steps=False)

            def login_with_invalid_username_and_valid_password(users, i, iterations=10):
                with When(f"users try to login with invalid username and valid password #{i}"):
                    for i in range(iterations):
                        random_user = dict(users[random.randint(0, len(users)-1)])
                        random_user["cn"] += randomword(1)
                        login_and_execute_query(username=random_user["cn"],
                                password=random_user["userpassword"],
                                exitcode=4,
                                message=f"DB::Exception: {random_user['cn']}: Authentication failed: password is incorrect or there is no user with such name",
                                steps=False)

            with When("I login in parallel"):
                tasks = []
                with Pool(4) as pool:
                    try:
                        for i in range(5):
                            tasks.append(pool.apply_async(login_with_valid_username_and_password, (users, i, 50,)))
                            tasks.append(pool.apply_async(login_with_valid_username_and_invalid_password, (users, i, 50,)))
                            tasks.append(pool.apply_async(login_with_invalid_username_and_valid_password, (users, i, 50,)))
                    finally:
                        with Then("it should work"):
                            join(tasks, timeout=timeout)
        
@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Authentication_Invalid("1.0"),
    RQ_SRS_007_LDAP_Authentication_Invalid_DeletedUser("1.0")
)
def login_after_user_is_deleted_from_ldap(self, server, rbac=False):
    """Check that login fails after user is deleted from LDAP.
    """
    self.context.ldap_node = self.context.cluster.node(server)
    user = None

    try:
        with Given(f"I add user to LDAP"):
            user = {"cn": "myuser", "userpassword": "myuser"}
            user = add_user_to_ldap(**user)

        with ldap_authenticated_users({"username": user["cn"], "server": server}, config_file=f"ldap_users_{getuid()}.xml",
                restart=True, rbac=rbac):
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
    RQ_SRS_007_LDAP_Authentication_Invalid("1.0"),
    RQ_SRS_007_LDAP_Authentication_PasswordChanged("1.0")
)
def login_after_user_password_changed_in_ldap(self, server, rbac=False):
    """Check that login fails after user password is changed in LDAP.
    """
    self.context.ldap_node = self.context.cluster.node(server)
    user = None

    try:
        with Given(f"I add user to LDAP"):
            user = {"cn": "myuser", "userpassword": "myuser"}
            user = add_user_to_ldap(**user)

        with ldap_authenticated_users({"username": user["cn"], "server": server}, config_file=f"ldap_users_{getuid()}.xml",
                restart=True, rbac=rbac):
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
    RQ_SRS_007_LDAP_Authentication_Invalid("1.0"),
    RQ_SRS_007_LDAP_Authentication_UsernameChanged("1.0")
)
def login_after_user_cn_changed_in_ldap(self, server, rbac=False):
    """Check that login fails after user cn is changed in LDAP.
    """
    self.context.ldap_node = self.context.cluster.node(server)
    user = None
    new_user = None

    try:
        with Given(f"I add user to LDAP"):
            user = {"cn": "myuser", "userpassword": "myuser"}
            user = add_user_to_ldap(**user)

        with ldap_authenticated_users({"username": user["cn"], "server": server},
                config_file=f"ldap_users_{getuid()}.xml", restart=True, rbac=rbac):
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
    RQ_SRS_007_LDAP_Authentication_Valid("1.0"),
    RQ_SRS_007_LDAP_Authentication_LDAPServerRestart("1.0")
)
def login_after_ldap_server_is_restarted(self, server, timeout=300, rbac=False):
    """Check that login succeeds after LDAP server is restarted.
    """
    self.context.ldap_node = self.context.cluster.node(server)
    user = None

    try:
        with Given(f"I add user to LDAP"):
            user = {"cn": "myuser", "userpassword": getuid()}
            user = add_user_to_ldap(**user)

        with ldap_authenticated_users({"username": user["cn"], "server": server}, rbac=rbac):
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
    RQ_SRS_007_LDAP_Authentication_Valid("1.0"),
    RQ_SRS_007_LDAP_Authentication_ClickHouseServerRestart("1.0")
)
def login_after_clickhouse_server_is_restarted(self, server, timeout=300, rbac=False):
    """Check that login succeeds after ClickHouse server is restarted.
    """
    self.context.ldap_node = self.context.cluster.node(server)
    user = None

    try:
        with Given(f"I add user to LDAP"):
            user = {"cn": "myuser", "userpassword": getuid()}
            user = add_user_to_ldap(**user)

        with ldap_authenticated_users({"username": user["cn"], "server": server}, rbac=rbac):
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
    RQ_SRS_007_LDAP_Authentication_Invalid("1.0"),
    RQ_SRS_007_LDAP_Authentication_Password_Empty("1.0")
)
def valid_username_with_valid_empty_password(self, server, rbac=False):
    """Check that we can't login using valid username that has empty password.
    """
    user = {"cn": "empty_password", "userpassword": ""}
    exitcode = 4
    message = f"DB::Exception: {user['cn']}: Authentication failed: password is incorrect or there is no user with such name"

    add_user_to_ldap_and_login(user=user, exitcode=exitcode, message=message, server=server, rbac=rbac)

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Authentication_Invalid("1.0"),
    RQ_SRS_007_LDAP_Authentication_Password_Empty("1.0")
)
def valid_username_and_invalid_empty_password(self, server, rbac=False):
    """Check that we can't login using valid username but invalid empty password.
    """
    username = "user_non_empty_password"
    user = {"cn": username, "userpassword": username}
    login = {"password": ""}

    exitcode = 4
    message = f"DB::Exception: {username}: Authentication failed: password is incorrect or there is no user with such name"

    add_user_to_ldap_and_login(user=user, login=login, exitcode=exitcode, message=message, server=server, rbac=rbac)

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Authentication_Valid("1.0")
)
def valid_username_and_password(self, server, rbac=False):
    """Check that we can login using valid username and password.
    """
    username = "valid_username_and_password"
    user = {"cn": username, "userpassword": username}

    with When(f"I add user {username} to LDAP and try to login"):
        add_user_to_ldap_and_login(user=user, server=server, rbac=rbac)

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Authentication_Invalid("1.0")
)
def valid_username_and_password_invalid_server(self, server=None, rbac=False):
    """Check that we can't login using valid username and valid
    password but for a different server.
    """
    self.context.ldap_node = self.context.cluster.node("openldap1")

    user = {"username": "user2", "userpassword": "user2", "server": "openldap1"}

    exitcode = 4
    message = f"DB::Exception: user2: Authentication failed: password is incorrect or there is no user with such name"

    with ldap_authenticated_users(user, config_file=f"ldap_users_{getuid()}.xml", restart=True, rbac=rbac):
       login_and_execute_query(username="user2", password="user2", exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Authentication_Valid("1.0"),
    RQ_SRS_007_LDAP_Authentication_Username_Long("1.0"),
    RQ_SRS_007_LDAP_Configuration_User_Name_Long("1.0")
)
def valid_long_username_and_short_password(self, server, rbac=False):
    """Check that we can login using valid very long username and short password.
    """
    username = "long_username_12345678901234567890123456789012345678901234567890123456789012345678901234567890"
    user = {"cn": username, "userpassword": "long_username"}

    add_user_to_ldap_and_login(user=user, server=server, rbac=rbac)

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Authentication_Invalid("1.0")
)
def invalid_long_username_and_valid_short_password(self, server, rbac=False):
    """Check that we can't login using slightly invalid long username but valid password.
    """
    username = "long_username_12345678901234567890123456789012345678901234567890123456789012345678901234567890"
    user = {"cn": username, "userpassword": "long_username"}
    login = {"username": f"{username}?"}

    exitcode = 4
    message=f"DB::Exception: {login['username']}: Authentication failed: password is incorrect or there is no user with such name"

    add_user_to_ldap_and_login(user=user, login=login, exitcode=exitcode, message=message, server=server, rbac=rbac)

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Authentication_Valid("1.0"),
    RQ_SRS_007_LDAP_Authentication_Password_Long("1.0")
)
def valid_short_username_and_long_password(self, server, rbac=False):
    """Check that we can login using valid short username with very long password.
    """
    username = "long_password"
    user = {"cn": username, "userpassword": "long_password_12345678901234567890123456789012345678901234567890123456789012345678901234567890"}
    add_user_to_ldap_and_login(user=user, server=server, rbac=rbac)

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Authentication_Invalid("1.0")
)
def valid_short_username_and_invalid_long_password(self, server, rbac=False):
    """Check that we can't login using valid short username and invalid long password.
    """
    username = "long_password"
    user = {"cn": username, "userpassword": "long_password_12345678901234567890123456789012345678901234567890123456789012345678901234567890"}
    login = {"password": user["userpassword"] + "1"}

    exitcode = 4
    message=f"DB::Exception: {username}: Authentication failed: password is incorrect or there is no user with such name"

    add_user_to_ldap_and_login(user=user, login=login, exitcode=exitcode, message=message, server=server, rbac=rbac)

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Authentication_Invalid("1.0")
)
def valid_username_and_invalid_password(self, server, rbac=False):
    """Check that we can't login using valid username and invalid password.
    """
    username = "valid_username_and_invalid_password"
    user = {"cn": username, "userpassword": username}
    login = {"password": user["userpassword"] + "1"}

    exitcode = 4
    message=f"DB::Exception: {username}: Authentication failed: password is incorrect or there is no user with such name"

    add_user_to_ldap_and_login(user=user, login=login, exitcode=exitcode, message=message, server=server, rbac=rbac)

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Authentication_Invalid("1.0")
)
def invalid_username_and_valid_password(self, server, rbac=False):
    """Check that we can't login using slightly invalid username but valid password.
    """
    username = "invalid_username_and_valid_password"
    user = {"cn": username, "userpassword": username}
    login = {"username": user["cn"] + "1"}

    exitcode = 4
    message=f"DB::Exception: {login['username']}: Authentication failed: password is incorrect or there is no user with such name"

    add_user_to_ldap_and_login(user=user, login=login, exitcode=exitcode, message=message, server=server, rbac=rbac)

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Authentication_Valid("1.0"),
    RQ_SRS_007_LDAP_Authentication_Username_UTF8("1.0"),
    RQ_SRS_007_LDAP_Configuration_User_Name_UTF8("1.0")
)
def valid_utf8_username_and_ascii_password(self, server, rbac=False):
    """Check that we can login using valid utf-8 username with ascii password.
    """
    username = "utf8_username_Gãńdåłf_Thê_Gręât"
    user = {"cn": username, "userpassword": "utf8_username"}

    add_user_to_ldap_and_login(user=user, server=server, rbac=rbac)

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Authentication_Valid("1.0"),
    RQ_SRS_007_LDAP_Authentication_Password_UTF8("1.0")
)
def valid_ascii_username_and_utf8_password(self, server, rbac=False):
    """Check that we can login using valid ascii username with utf-8 password.
    """
    username = "utf8_password"
    user = {"cn": username, "userpassword": "utf8_password_Gãńdåłf_Thê_Gręât"}

    add_user_to_ldap_and_login(user=user, server=server, rbac=rbac)

@TestScenario
def empty_username_and_empty_password(self, server=None, rbac=False):
    """Check that we can login using empty username and empty password as
    it will use the default user and that has an empty password.
    """
    login_and_execute_query(username="", password="")

@TestScenario
@Tags("verification_cooldown")
@Requirements(
    RQ_SRS_007_LDAP_Configuration_Server_VerificationCooldown_Default("1.0")
)
def default_verification_cooldown_value(self, server, rbac=False):
    """Check that the default value (0) for the verification cooldown parameter
    disables caching and forces contacting the LDAP server for each
    authentication request.
    """

    error_message = "DB::Exception: testVCD: Authentication failed: password is incorrect or there is no user with such name"
    error_exitcode = 4
    user = None

    with Given("I have an LDAP configuration that uses the default verification_cooldown value (0)"):
        servers = {"openldap1": {"host": "openldap1", "port": "389", "enable_tls": "no",
            "auth_dn_prefix": "cn=", "auth_dn_suffix": ",ou=users,dc=company,dc=com"
        }}

        self.context.ldap_node = self.context.cluster.node(server)

    try:
        with Given("I add user to LDAP"):
            user = {"cn": "testVCD", "userpassword": "testVCD"}
            user = add_user_to_ldap(**user)

        with ldap_servers(servers):
            with ldap_authenticated_users({"username": user["cn"], "server": server}, config_file=f"ldap_users_{getuid()}.xml"):
                with When("I login and execute a query"):
                    login_and_execute_query(username=user["cn"], password=user["userpassword"])

                with And("I change user password in LDAP"):
                    change_user_password_in_ldap(user, "newpassword")

                with Then("when I try to login immediately with the old user password it should fail"):
                    login_and_execute_query(username=user["cn"], password=user["userpassword"],
                        exitcode=error_exitcode, message=error_message)

    finally:
        with Finally("I make sure LDAP user is deleted"):
            if user is not None:
                delete_user_from_ldap(user, exitcode=None)

@TestScenario
@Tags("verification_cooldown")
@Requirements(
    RQ_SRS_007_LDAP_Configuration_Server_VerificationCooldown("1.0")
)
def valid_verification_cooldown_value_cn_change(self, server, rbac=False):
    """Check that we can perform requests without contacting the LDAP server
    after successful authentication when the verification_cooldown parameter
    is set and the user cn is changed.
    """
    user = None
    new_user = None

    with Given("I have an LDAP configuration that sets verification_cooldown parameter to 2 sec"):
        servers = { "openldap1": {
            "host": "openldap1",
            "port": "389",
            "enable_tls": "no",
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com",
            "verification_cooldown": "600"
        }}

        self.context.ldap_node = self.context.cluster.node(server)

    try:
        with Given("I add user to LDAP"):
            user = {"cn": "testVCD", "userpassword": "testVCD"}
            user = add_user_to_ldap(**user)

        with ldap_servers(servers):
            with ldap_authenticated_users({"username": user["cn"], "server": server}, config_file=f"ldap_users_{getuid()}.xml"):
                with When("I login and execute a query"):
                    login_and_execute_query(username=user["cn"], password=user["userpassword"])

                with And("I change user cn in LDAP"):
                    new_user = change_user_cn_in_ldap(user, "testVCD2")

                with Then("when I try to login again with the old user cn it should work"):
                    login_and_execute_query(username=user["cn"], password=user["userpassword"])
    finally:
        with Finally("I make sure LDAP user is deleted"):
            if new_user is not None:
                delete_user_from_ldap(new_user, exitcode=None)

@TestScenario
@Tags("verification_cooldown")
@Requirements(
    RQ_SRS_007_LDAP_Configuration_Server_VerificationCooldown("1.0")
)
def valid_verification_cooldown_value_password_change(self, server, rbac=False):
    """Check that we can perform requests without contacting the LDAP server
    after successful authentication when the verification_cooldown parameter
    is set and the user password is changed.
    """
    user = None

    with Given("I have an LDAP configuration that sets verification_cooldown parameter to 2 sec"):
        servers = { "openldap1": {
            "host": "openldap1",
            "port": "389",
            "enable_tls": "no",
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com",
            "verification_cooldown": "600"
        }}

        self.context.ldap_node = self.context.cluster.node(server)

    try:
        with Given("I add user to LDAP"):
            user = {"cn": "testVCD", "userpassword": "testVCD"}
            user = add_user_to_ldap(**user)

        with ldap_servers(servers):
            with ldap_authenticated_users({"username": user["cn"], "server": server}, config_file=f"ldap_users_{getuid()}.xml"):
                with When("I login and execute a query"):
                    login_and_execute_query(username=user["cn"], password=user["userpassword"])

                with And("I change user password in LDAP"):
                    change_user_password_in_ldap(user, "newpassword")

                with Then("when I try to login again with the old password it should work"):
                    login_and_execute_query(username=user["cn"], password=user["userpassword"])
    finally:
        with Finally("I make sure LDAP user is deleted"):
            if user is not None:
                delete_user_from_ldap(user, exitcode=None)

@TestScenario
@Tags("verification_cooldown")
@Requirements(
    RQ_SRS_007_LDAP_Configuration_Server_VerificationCooldown("1.0")
)
def valid_verification_cooldown_value_ldap_unavailable(self, server, rbac=False):
    """Check that we can perform requests without contacting the LDAP server
    after successful authentication when the verification_cooldown parameter
    is set, even when the LDAP server is offline.
    """
    user = None

    with Given("I have an LDAP configuration that sets verification_cooldown parameter to 2 sec"):
        servers = { "openldap1": {
            "host": "openldap1",
            "port": "389",
            "enable_tls": "no",
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com",
            "verification_cooldown": "600"
        }}

        self.context.ldap_node = self.context.cluster.node(server)

    try:
        with Given("I add a new user to LDAP"):
            user = {"cn": "testVCD", "userpassword": "testVCD"}
            user = add_user_to_ldap(**user)

        with ldap_servers(servers):
            with ldap_authenticated_users({"username": user["cn"], "server": server},
                config_file=f"ldap_users_{getuid()}.xml"):

                with When("I login and execute a query"):
                    login_and_execute_query(username=user["cn"], password=user["userpassword"])

                try:
                    with And("then I stop the ldap server"):
                        self.context.ldap_node.stop()

                    with Then("when I try to login again with the server offline it should work"):
                        login_and_execute_query(username=user["cn"], password=user["userpassword"])
                finally:
                    with Finally("I start the ldap server back up"):
                        self.context.ldap_node.start()

    finally:
        with Finally("I make sure LDAP user is deleted"):
            if user is not None:
                delete_user_from_ldap(user, exitcode=None)

@TestOutline
def repeat_requests(self, server, iterations, vcd_value, rbac=False, timeout=600):
    """Run repeated requests from some user to the LDAP server.
    """

    user = None

    with Given(f"I have an LDAP configuration that sets verification_cooldown parameter to {vcd_value} sec"):
        servers = { "openldap1": {
            "host": "openldap1",
            "port": "389",
            "enable_tls": "no",
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com",
            "verification_cooldown": vcd_value
        }}

        self.context.ldap_node = self.context.cluster.node(server)

    try:
        with And("I add a new user to LDAP"):
            user = {"cn": "testVCD", "userpassword": "testVCD"}
            user = add_user_to_ldap(**user)

        with ldap_servers(servers):
            with ldap_authenticated_users({"username": user["cn"], "server": server}, config_file=f"ldap_users_{getuid()}.xml"):
                with When(f"I login and execute some query {iterations} times"):
                    start_time = time.time()
                    r = self.context.node.command(f"time for i in {{1..{iterations}}}; do clickhouse client -q \"SELECT 1\" --user {user['cn']} --password {user['userpassword']} > /dev/null; done", timeout=timeout)
                    end_time = time.time()

                    return end_time - start_time

    finally:
        with Finally("I make sure LDAP user is deleted"):
            if user is not None:
                delete_user_from_ldap(user, exitcode=None)

@TestScenario
@Tags("verification_cooldown")
@Requirements(
    RQ_SRS_007_LDAP_Authentication_VerificationCooldown_Performance("1.0")
)
def verification_cooldown_performance(self, server, rbac=False, iterations=5000):
    """Check that login performance is better when the verification cooldown
    parameter is set to a positive value when comparing to the case when
    the verification cooldown parameter is turned off.
    """

    vcd_time = 0
    no_vcd_time = 0

    with Example(f"Repeated requests with verification cooldown parameter set to 600 seconds, {iterations} iterations"):
        vcd_time = repeat_requests(server=server, iterations=iterations, vcd_value="600", rbac=rbac)
        metric("login_with_vcd_value_600", units="seconds", value=vcd_time)

    with Example(f"Repeated requests with verification cooldown parameter set to 0 seconds, {iterations} iterations"):
        no_vcd_time = repeat_requests(server=server, iterations=iterations, vcd_value="0", rbac=rbac)
        metric("login_with_vcd_value_0", units="seconds", value=no_vcd_time)

    with Then("Log the performance improvement as a percentage"):
        metric("percentage_improvement", units="%", value=100*(no_vcd_time - vcd_time)/vcd_time)

@TestOutline
def check_verification_cooldown_reset_on_core_server_parameter_change(self, server,
        parameter_name, parameter_value, rbac=False):
    """Check that the LDAP login cache is reset for all the LDAP authentication users
    when verification_cooldown parameter is set after one of the core server
    parameters is changed in the LDAP server configuration.
    """

    config_d_dir="/etc/clickhouse-server/config.d"
    config_file="ldap_servers.xml"
    error_message = "DB::Exception: {user}: Authentication failed: password is incorrect or there is no user with such name"
    error_exitcode = 4
    user = None
    config=None
    updated_config=None

    with Given("I have an LDAP configuration that sets verification_cooldown parameter to 600 sec"):
        servers = { "openldap1": {
            "host": "openldap1",
            "port": "389",
            "enable_tls": "no",
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com",
            "verification_cooldown": "600"
        }}

        self.context.ldap_node = self.context.cluster.node(server)

    with And("LDAP authenticated user"):
        users = [
            {"cn": f"testVCD_0", "userpassword": "testVCD_0"},
            {"cn": f"testVCD_1", "userpassword": "testVCD_1"}
        ]

    with And("I create LDAP servers configuration file"):
        config = create_ldap_servers_config_content(servers, config_d_dir, config_file)

    with ldap_users(*users) as users:
        with ldap_servers(servers, restart=True):
            with ldap_authenticated_users(*[{"username": user["cn"], "server": server} for user in users]):
                with When("I login and execute a query"):
                    for user in users:
                        with By(f"as user {user['cn']}"):
                            login_and_execute_query(username=user["cn"], password=user["userpassword"])

                with And("I change user password in LDAP"):
                    for user in users:
                        with By(f"for user {user['cn']}"):
                            change_user_password_in_ldap(user, "newpassword")

                with And(f"I change the server {parameter_name} core parameter", description=f"{parameter_value}"):
                    servers["openldap1"][parameter_name] = parameter_value

                with And("I create an updated the config file that has a different server host name"):
                    updated_config = create_ldap_servers_config_content(servers, config_d_dir, config_file)

                with modify_config(updated_config, restart=False):
                    with Then("when I try to log in it should fail as cache should have been reset"):
                        for user in users:
                            with By(f"as user {user['cn']}"):
                                login_and_execute_query(username=user["cn"], password=user["userpassword"],
                                    exitcode=error_exitcode, message=error_message.format(user=user["cn"]))

@TestScenario
@Tags("verification_cooldown")
@Requirements(
    RQ_SRS_007_LDAP_Authentication_VerificationCooldown_Reset_ChangeInCoreServerParameters("1.0")
)
def verification_cooldown_reset_on_server_host_parameter_change(self, server, rbac=False):
    """Check that the LDAP login cache is reset for all the LDAP authentication users
    when verification_cooldown parameter is set after server host name
    is changed in the LDAP server configuration.
    """

    check_verification_cooldown_reset_on_core_server_parameter_change(server=server,
        parameter_name="host", parameter_value="openldap2", rbac=rbac)

@TestScenario
@Tags("verification_cooldown")
@Requirements(
    RQ_SRS_007_LDAP_Authentication_VerificationCooldown_Reset_ChangeInCoreServerParameters("1.0")
)
def verification_cooldown_reset_on_server_port_parameter_change(self, server, rbac=False):
    """Check that the LDAP login cache is reset for all the LDAP authentication users
    when verification_cooldown parameter is set after server port is changed in the
    LDAP server configuration.
    """

    check_verification_cooldown_reset_on_core_server_parameter_change(server=server,
        parameter_name="port", parameter_value="9006", rbac=rbac)

@TestScenario
@Tags("verification_cooldown")
@Requirements(
    RQ_SRS_007_LDAP_Authentication_VerificationCooldown_Reset_ChangeInCoreServerParameters("1.0")
)
def verification_cooldown_reset_on_server_auth_dn_prefix_parameter_change(self, server, rbac=False):
    """Check that the LDAP login cache is reset for all the LDAP authentication users
    when verification_cooldown parameter is set after server auth_dn_prefix
    is changed in the LDAP server configuration.
    """

    check_verification_cooldown_reset_on_core_server_parameter_change(server=server,
        parameter_name="auth_dn_prefix", parameter_value="cxx=", rbac=rbac)

@TestScenario
@Tags("verification_cooldown")
@Requirements(
    RQ_SRS_007_LDAP_Authentication_VerificationCooldown_Reset_ChangeInCoreServerParameters("1.0")
)
def verification_cooldown_reset_on_server_auth_dn_suffix_parameter_change(self, server, rbac=False):
    """Check that the LDAP login cache is reset for all the LDAP authentication users
    when verification_cooldown parameter is set after server auth_dn_suffix
    is changed in the LDAP server configuration.
    """

    check_verification_cooldown_reset_on_core_server_parameter_change(server=server,
        parameter_name="auth_dn_suffix",
        parameter_value=",ou=company,dc=users,dc=com", rbac=rbac)


@TestScenario
@Name("verification cooldown reset when invalid password is provided")
@Tags("verification_cooldown")
@Requirements(
    RQ_SRS_007_LDAP_Authentication_VerificationCooldown_Reset_InvalidPassword("1.0")
)
def scenario(self, server, rbac=False):
    """Check that cached bind requests for the user are discarded when
    the user provides invalid login credentials.
    """

    user = None
    error_exitcode = 4
    error_message = "DB::Exception: testVCD: Authentication failed: password is incorrect or there is no user with such name"

    with Given("I have an LDAP configuration that sets verification_cooldown parameter to 600 sec"):
        servers = { "openldap1": {
            "host": "openldap1",
            "port": "389",
            "enable_tls": "no",
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com",
            "verification_cooldown": "600"
        }}

        self.context.ldap_node = self.context.cluster.node(server)

    try:
        with Given("I add a new user to LDAP"):
            user = {"cn": "testVCD", "userpassword": "testVCD"}
            user = add_user_to_ldap(**user)

        with ldap_servers(servers):
            with ldap_authenticated_users({"username": user["cn"], "server": server},
                config_file=f"ldap_users_{getuid()}.xml"):

                with When("I login and execute a query"):
                    login_and_execute_query(username=user["cn"], password=user["userpassword"])

                with And("I change user password in LDAP"):
                    change_user_password_in_ldap(user, "newpassword")

                with Then("When I try to log in with the cached password it should work"):
                    login_and_execute_query(username=user["cn"], password=user["userpassword"])

                with And("When I try to log in with an incorrect password it should fail"):
                    login_and_execute_query(username=user["cn"], password="incorrect", exitcode=error_exitcode,
                        message=error_message)

                with And("When I try to log in with the cached password it should fail"):
                    login_and_execute_query(username=user["cn"], password="incorrect", exitcode=error_exitcode,
                        message=error_message)

    finally:
        with Finally("I make sure LDAP user is deleted"):
            if user is not None:
                delete_user_from_ldap(user, exitcode=None)

@TestFeature
def verification_cooldown(self, rbac, servers=None, node="clickhouse1"):
    """Check verification cooldown parameter functionality.
    """
    for scenario in loads(current_module(), Scenario, filter=has.tag("verification_cooldown")):
        scenario(server="openldap1", rbac=rbac)


@TestOutline(Feature)
@Name("user authentications")
@Requirements(
    RQ_SRS_007_LDAP_Authentication_Mechanism_NamePassword("1.0")
)
@Examples("rbac", [
    (False,),
    (True, Requirements(RQ_SRS_007_LDAP_Configuration_User_RBAC("1.0")))
])
def feature(self, rbac, servers=None, node="clickhouse1"):
    """Check that users can be authenticated using an LDAP server when
    users are configured either using an XML configuration file or RBAC.
    """
    self.context.node = self.context.cluster.node(node)

    if servers is None:
        servers = globals()["servers"]

    with ldap_servers(servers):
        for scenario in loads(current_module(), Scenario, filter=~has.tag("verification_cooldown")):
            scenario(server="openldap1", rbac=rbac)

    Feature(test=verification_cooldown)(rbac=rbac, servers=servers, node=node)




