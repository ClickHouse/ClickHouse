from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

@TestSuite
def privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is only able to execute ATTACH DICTIONARY when they have required privilege, either directly or via role.
    """
    role_name = f"role_{getuid()}"
    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with Suite("user with direct privilege", setup=instrument_clickhouse_server_log):
        with user(node, user_name):

            with When(f"I run checks that {user_name} is only able to execute CREATE DICTIONARY with required privileges"):
                privilege_check(grant_target_name=user_name, user_name=user_name, node=node)

    with Suite("user with privilege via role", setup=instrument_clickhouse_server_log):
        with user(node, user_name), role(node, role_name):

            with When("I grant the role to the user"):
                node.query(f"GRANT {role_name} TO {user_name}")

            with And(f"I run checks that {user_name} with {role_name} is only able to execute CREATE DICTIONARY with required privileges"):
                privilege_check(grant_target_name=role_name, user_name=user_name, node=node)

def privilege_check(grant_target_name, user_name, node=None):
    """Run scenarios to check the user's access with different privileges.
    """
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    with Scenario("user without privilege", setup=instrument_clickhouse_server_log):
        dict_name = f"dict_{getuid()}"

        try:
            with When("I attempt to attach a dictionary without privilege"):
                node.query(f"ATTACH DICTIONARY {dict_name}", settings = [("user", user_name)],
                    exitcode=exitcode, message=message)

        finally:
            with Finally("I drop the dictionary"):
                node.query(f"DROP DICTIONARY IF EXISTS {dict_name}")

    with Scenario("user with privilege", setup=instrument_clickhouse_server_log):
        dict_name = f"dict_{getuid()}"

        try:
            with When("I grant create dictionary privilege"):
                node.query(f"GRANT CREATE DICTIONARY ON {dict_name} TO {grant_target_name}")

            with Then("I attempt to attach a dictionary"):
                node.query(f"ATTACH DICTIONARY {dict_name}", settings = [("user", user_name)],
                    exitcode=231, message=f"DB::Exception: Dictionary `{dict_name}` doesn't exist.")

        finally:
            with Finally("I drop the dictionary"):
                node.query(f"DROP DICTIONARY IF EXISTS {dict_name}")

    with Scenario("user with revoked privilege", setup=instrument_clickhouse_server_log):
        dict_name = f"dict_{getuid()}"

        try:
            with When("I grant the create dictionary privilege"):
                node.query(f"GRANT CREATE DICTIONARY ON {dict_name} TO {grant_target_name}")

            with And("I revoke the create dictionary privilege"):
                node.query(f"REVOKE CREATE DICTIONARY ON {dict_name} FROM {grant_target_name}")

            with Then("I attempt to attach a dictionary"):
                node.query(f"ATTACH DICTIONARY {dict_name}", settings = [("user", user_name)],
                    exitcode=exitcode, message=message)

        finally:
            with Finally("I drop the dictionary"):
                node.query(f"DROP DICTIONARY IF EXISTS {dict_name}")

@TestFeature
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AttachDictionary("1.0"),
)
@Name("attach dictionary")
def feature(self, node="clickhouse1", stress=None, parallel=None):
    """Check the RBAC functionality of ATTACH DICTIONARY.
    """
    self.context.node = self.context.cluster.node(node)

    if parallel is not None:
        self.context.parallel = parallel
    if stress is not None:
        self.context.stress = stress

    with Suite(test=privilege_granted_directly_or_via_role):
        privilege_granted_directly_or_via_role()
