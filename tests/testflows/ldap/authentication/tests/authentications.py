# -*- coding: utf-8 -*-
import random

from multiprocessing.dummy import Pool
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
def parallel_login(self, server, user_count=10, timeout=200, rbac=False):
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
                p = Pool(15)
                tasks = []
                for i in range(5):
                    tasks.append(p.apply_async(login_with_valid_username_and_password, (users, i, 50,)))
                    tasks.append(p.apply_async(login_with_valid_username_and_invalid_password, (users, i, 50,)))
                    tasks.append(p.apply_async(login_with_invalid_username_and_valid_password, (users, i, 50,)))

            with Then("it should work"):
                for task in tasks:
                    task.get(timeout=timeout)

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
def login_after_ldap_server_is_restarted(self, server, timeout=60, rbac=False):
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
def login_after_clickhouse_server_is_restarted(self, server, timeout=60, rbac=False):
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
        for scenario in loads(current_module(), Scenario):
            scenario(server="openldap1", rbac=rbac)
