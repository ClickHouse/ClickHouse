from contextlib import contextmanager

from testflows.core import *

from rbac.requirements import *
import rbac.tests.errors as errors

@TestFeature
@Name("set default role")
@Args(format_description=False)
def feature(self, node="clickhouse1"):
    """Check set default role query syntax.

    ```sql
    SET DEFAULT ROLE {NONE | role [,...] | ALL | ALL EXCEPT role [,...]} TO {user|CURRENT_USER} [,...]
    ```
    """
    node = self.context.cluster.node(node)

    @contextmanager
    def setup(users=2,roles=2):
        try:
            with Given("I have some users"):
                for i in range(users):
                    node.query(f"CREATE USER OR REPLACE user{i}")
            with And("I have some roles"):
                for i in range(roles):
                    node.query(f"CREATE ROLE OR REPLACE role{i}")
            yield
        finally:
            with Finally("I drop the users"):
                for i in range(users):
                    node.query(f"DROP USER IF EXISTS user{i}")
            with And("I drop the roles"):
                for i in range(roles):
                    node.query(f"DROP ROLE IF EXISTS role{i}")
    
    with Scenario("I set default a nonexistent role to user", requirements=[
            RQ_SRS_006_RBAC_SetDefaultRole("1.0")]):
        with setup(1,0):
            with When("I set default nonexistent role to a user"):
                exitcode, message = errors.role_not_found_in_disk(name="role0")
                node.query("SET DEFAULT ROLE role0 TO user0", exitcode=exitcode, message=message)

    with Scenario("I set default ALL EXCEPT a nonexistent role to user", requirements=[
            RQ_SRS_006_RBAC_SetDefaultRole("1.0")]):
        with setup(1,0):
            with When("I set default nonexistent role to a user"):
                exitcode, message = errors.role_not_found_in_disk(name="role0")
                node.query("SET DEFAULT ROLE ALL EXCEPT role0 TO user0", exitcode=exitcode, message=message)

    with Scenario("I set default a role to a nonexistent user", requirements=[
            RQ_SRS_006_RBAC_SetDefaultRole("1.0")]):
        with setup(0,1):
            with When("I set default role to a nonexistent user"):
                exitcode, message = errors.user_not_found_in_disk(name="user0")
                node.query("SET DEFAULT ROLE role0 TO user0", exitcode=exitcode, message=message)

    #in SET DEFAULT ROLE, the nonexistent user is noticed first and becomes the thrown exception
    with Scenario("I set default a nonexistent role to a nonexistent user", requirements=[
            RQ_SRS_006_RBAC_SetDefaultRole("1.0")]):
        with setup(0,0):
            with When("I set default nonexistent role to a nonexistent user"):
                exitcode, message = errors.user_not_found_in_disk(name="user0")
                node.query("SET DEFAULT ROLE role0 TO user0", exitcode=exitcode, message=message)

    try:
        with Given("I have some roles and some users"):
            for i in range(2):
                node.query(f"CREATE ROLE role{i}")
                node.query(f"CREATE USER user{i}")
            node.query(f"GRANT role0, role1 TO user0, user1")

        with Scenario("I set default role for a user to none", flags = TE, requirements=[
                RQ_SRS_006_RBAC_SetDefaultRole_None("1.0")]):
            with When("I set no roles default for user"):
                node.query("SET DEFAULT ROLE NONE TO user0")

        with Scenario("I set one default role for a user", flags = TE, requirements=[
                RQ_SRS_006_RBAC_SetDefaultRole("1.0")]):
            with When("I set a default role for user "):
                node.query("SET DEFAULT ROLE role0 TO user0")

        with Scenario("I set one default role for user default, throws exception", flags = TE, requirements=[
                RQ_SRS_006_RBAC_SetDefaultRole("1.0")]):
            with When("I set a default role for default"):
                exitcode, message = errors.cannot_update_default()
                node.query("SET DEFAULT ROLE role0 TO default", exitcode=exitcode, message=message)

        with Scenario("I set multiple default roles for a user", flags = TE, requirements=[
                RQ_SRS_006_RBAC_SetDefaultRole("1.0")]):
            with When("I set multiple default roles to user"):
                node.query("SET DEFAULT ROLE role0, role1 TO user0")

        with Scenario("I set multiple default roles for multiple users", flags = TE, requirements=[
                RQ_SRS_006_RBAC_SetDefaultRole("1.0")]):
            with When("I set multiple default roles to multiple users"):
                node.query("SET DEFAULT ROLE role0, role1 TO user0, user1")

        with Scenario("I set all roles as default for a user", flags = TE, requirements=[
                RQ_SRS_006_RBAC_SetDefaultRole_All("1.0")]):
            with When("I set all roles default to user"):
                node.query("SET DEFAULT ROLE ALL TO user0")

        with Scenario("I set all roles except one for a user", flags = TE, requirements=[
                RQ_SRS_006_RBAC_SetDefaultRole_AllExcept("1.0")]):
            with When("I set all except one role default to user"):
                node.query("SET DEFAULT ROLE ALL EXCEPT role0 TO user0")

        with Scenario("I set default role for current user", flags = TE, requirements=[
                RQ_SRS_006_RBAC_SetDefaultRole_CurrentUser("1.0")]):
            with When("I set default role to current user"):
                node.query("GRANT ACCESS MANAGEMENT ON *.* TO user0")
                node.query("SET DEFAULT ROLE role0 TO CURRENT_USER", settings = [("user","user0")])

    finally:
        with Finally("I drop the roles and users"):
            for i in range(2):
                node.query(f"DROP ROLE IF EXISTS role{i}")
                node.query(f"DROP USER IF EXISTS user{i}")
