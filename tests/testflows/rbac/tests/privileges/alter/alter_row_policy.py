from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

@TestSuite
def privileges_granted_directly(self, node=None):
    """Check that a user is able to execute `ALTER ROW POLICY` with privileges are granted directly.
    """

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(run=alter_row_policy,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[user_name,user_name]) for row in alter_row_policy.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestSuite
def privileges_granted_via_role(self, node=None):
    """Check that a user is able to execute `ALTER ROW POLICY` with privileges are granted through a role.
    """

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(run=alter_row_policy,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[role_name,user_name]) for row in alter_row_policy.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestOutline(Suite)
@Examples("privilege",[
    ("ALL",),
    ("ACCESS MANAGEMENT",),
    ("ALTER ROW POLICY",),
    ("ALTER POLICY",),
])
def alter_row_policy(self, privilege, grant_target_name, user_name, node=None):
    """Check that user is only able to execute `ALTER ROW POLICY` when they have the necessary privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("ALTER ROW POLICY without privilege"):
        alter_row_policy_name = f"alter_row_policy_{getuid()}"
        table_name = f"table_name_{getuid()}"

        try:
            with Given("I have a row policy"):
                node.query(f"CREATE ROW POLICY {alter_row_policy_name} ON {table_name}")

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then("I check the user can't alter a row policy"):
                node.query(f"ALTER ROW POLICY {alter_row_policy_name} ON {table_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)

        finally:
            with Finally("I drop the row policy"):
                node.query(f"DROP ROW POLICY IF EXISTS {alter_row_policy_name} ON {table_name}")

    with Scenario("ALTER ROW POLICY with privilege"):
        alter_row_policy_name = f"alter_row_policy_{getuid()}"
        table_name = f"table_name_{getuid()}"

        try:
            with Given("I have a row policy"):
                node.query(f"CREATE ROW POLICY {alter_row_policy_name} ON {table_name}")

            with When(f"I grant {privilege}"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with Then("I check the user can alter a row policy"):
                node.query(f"ALTER ROW POLICY {alter_row_policy_name} ON {table_name}", settings = [("user", f"{user_name}")])

        finally:
            with Finally("I drop the row policy"):
                node.query(f"DROP ROW POLICY IF EXISTS {alter_row_policy_name} ON {table_name}")

    with Scenario("ALTER ROW POLICY on cluster"):
        alter_row_policy_name = f"alter_row_policy_{getuid()}"
        table_name = f"table_name_{getuid()}"

        try:
            with Given("I have a row policy on a cluster"):
                node.query(f"CREATE ROW POLICY {alter_row_policy_name} ON CLUSTER sharded_cluster ON {table_name}")

            with When(f"I grant {privilege}"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with Then("I check the user can alter a row policy"):
                node.query(f"ALTER ROW POLICY {alter_row_policy_name} ON CLUSTER sharded_cluster ON {table_name}", settings = [("user", f"{user_name}")])

        finally:
            with Finally("I drop the user"):
                node.query(f"DROP ROW POLICY IF EXISTS {alter_row_policy_name} ON CLUSTER sharded_cluster ON {table_name}")

    with Scenario("ALTER ROW POLICY with revoked privilege"):
        alter_row_policy_name = f"alter_row_policy_{getuid()}"
        table_name = f"table_name_{getuid()}"

        try:
            with Given("I have a row policy"):
                node.query(f"CREATE ROW POLICY {alter_row_policy_name} ON {table_name}")

            with When(f"I grant {privilege} on the database"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with And(f"I revoke {privilege} on the database"):
                node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

            with Then("I check the user cannot alter row policy"):
                node.query(f"ALTER ROW POLICY {alter_row_policy_name} ON {table_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)
        finally:
             with Finally("I drop the row policy"):
                node.query(f"DROP ROW POLICY IF EXISTS {alter_row_policy_name} ON {table_name}")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Restriction("1.0")
)
def no_grants(self, node=None):
    """Check that user is unable to select from a table without a row policy
    after a row policy has been altered to have a condition.
    """

    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):

        with Given("I have a row policy"):
            row_policy(name=pol_name, table=table_name)

        with When("The table has some values"):
            node.query(f"INSERT INTO {table_name} (y) VALUES (1)")

        with Then("I try to select from the table"):
            output = node.query(f"SELECT * FROM {table_name}").output
            assert '1' in output, error()

        with When("I alter the row policy to have a condition"):
            node.query(f"ALTER POLICY {pol_name} ON {table_name} FOR SELECT USING 1")

        with Then("I try to select from the table"):
            output = node.query(f"SELECT * FROM {table_name}").output
            assert '' == output, error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Alter_Access_Permissive("1.0"),
)
def permissive(self, node=None):
    """Check that user is able to see from a table when they have a PERMISSIVE policy.
    """

    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):

        with Given("I have a row policy"):
            row_policy(name=pol_name, table=table_name)

        with And("The table has some values"):
            node.query(f"INSERT INTO {table_name} (y) VALUES (1), (2)")

        with When("I alter a row policy to be permissive"):
            node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} AS PERMISSIVE FOR SELECT USING y=1 TO default")

        with Then("I try to select from the table"):
            output = node.query(f"SELECT * FROM {table_name}").output
            assert '1' in output and '2' not in output, error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Alter_Access_Restrictive("1.0")
)
def restrictive(self, node=None):
    """Check that user is able to see values they have a RESTRICTIVE policy for.
    """

    table_name = f"table_{getuid()}"
    perm_pol_name = f"perm_pol_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):

        with Given("I have a row policy"):
            row_policy(name=pol_name, table=table_name)

        with And("I have a second row policy"):
            row_policy(name=perm_pol_name, table=table_name)

        with And("I alter a row policy to be permissive"):
            node.query(f"ALTER ROW POLICY {perm_pol_name} ON {table_name} FOR SELECT USING y=1 OR y=2 TO default")

        with And("I alter a row policy to be restrictive"):
            node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} AS RESTRICTIVE FOR SELECT USING y=1 TO default")

        with When("The table has some values"):
            node.query(f"INSERT INTO {table_name} (y) VALUES (1), (2)")

        with Then("I try to select from the table"):
            output = node.query(f"SELECT * FROM {table_name}").output
            assert '1' in output and '2' not in output, error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Alter_ForSelect("1.0"),
)
def for_select(self, node=None):
    """Check that user is able to see values allowed by the row policy condition in the FOR SELECT clause.
    """

    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):

        with Given("I have a row policy"):
            row_policy(name=pol_name, table=table_name)

        with And("The table has some values"):
            node.query(f"INSERT INTO {table_name} (y) VALUES (1)")

        with Given("I alter therow policy to use FOR SELECT"):
            node.query(f"Alter ROW POLICY {pol_name} ON {table_name} FOR SELECT USING 1 TO default")

        with Then("I try to select from the table"):
            output = node.query(f"SELECT * FROM {table_name}").output
            assert '1' in output, error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Alter_Condition("1.0")
)
def condition(self, node=None):
    """Check that user is able to see values allowed by the row policy condition.
    """

    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):

        with Given("I have a row policy"):
            row_policy(name=pol_name, table=table_name)

        with And("The table has some values"):
            node.query(f"INSERT INTO {table_name} (y) VALUES (1),(2)")

        with When("I alter a row policy to be permissive"):
            node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} FOR SELECT USING y=1 TO default")

        with Then("I try to select from the table"):
            output = node.query(f"SELECT * FROM {table_name}").output
            assert '1' in output, error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Alter_Condition_None("1.0")
)
def remove_condition(self, node=None):
    """Check that user is able to see the table after row policy condition has been removed.
    """

    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):

        with Given("I have a row policy"):
            row_policy(name=pol_name, table=table_name)

        with And("The row policy has a condition"):
            node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} FOR SELECT USING 1")

        with And("The table has some values"):
            node.query(f"INSERT INTO {table_name} (y) VALUES (1)")

        with When("I alter a row policy to not have a condition"):
            node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} FOR SELECT USING NONE")

        with Then("I try to select from the table"):
            output = node.query(f"SELECT * FROM {table_name}").output
            assert '1' in output, error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Alter_IfExists("1.0")
)
def if_exists(self, node=None):
    """Check that a row policy altered using IF EXISTS restricts rows as expected.
    """

    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):

        with Given("I have a row policy"):
            row_policy(name=pol_name, table=table_name)

        with And("The table has some values"):
            node.query(f"INSERT INTO {table_name} (y) VALUES (1)")

        with When("I have alter a row policy to be permissive using IF EXISTS clause"):
            node.query(f"ALTER ROW POLICY IF EXISTS {pol_name} ON {table_name} FOR SELECT USING 1 TO default")

        with Then("I select from the table"):
            output = node.query(f"SELECT * FROM {table_name}").output
            assert '1' in output, error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Alter_Rename("1.0")
)
def rename(self, node=None):
    """Check that a row policy altered using RENAME restricts rows as expected.
    """

    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"
    pol_new_name = f"pol_new_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):
        try:
            with Given("I have a row policy"):
                row_policy(name=pol_name, table=table_name)

            with And("The table has some values"):
                node.query(f"INSERT INTO {table_name} (y) VALUES (1)")

            with And("The row policy is permissive"):
                node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} FOR SELECT USING y=1 TO default")

            with When("I have alter a row policy by renaming it"):
                node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} RENAME TO {pol_new_name}")

            with Then("I select from the table"):
                output = node.query(f"SELECT * FROM {table_name}").output
                assert '1' in output, error()

        finally:
            with Finally("I drop the row policy"):
                node.query(f"DROP ROW POLICY IF EXISTS {pol_new_name} ON {table_name}")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Alter_OnCluster("1.0")
)
def on_cluster(self, node=None):
    """Check that a row policy altered using ON CLUSTER applies to the nodes of the cluster correctly.
    """

    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node
        node2 = self.context.node2

    try:
        with Given("I have a table on a cluster"):
            node.query(f"CREATE TABLE {table_name} ON CLUSTER sharded_cluster (x UInt64) ENGINE = Memory")

        with And("I have a row policy on a cluster on that table"):
            node.query(f"CREATE ROW POLICY {pol_name} ON CLUSTER sharded_cluster ON {table_name}")

        with And("The table has some values on the first node"):
            node.query(f"INSERT INTO {table_name} (x) VALUES (1)")

        with And("The table has some values on the second node"):
            node2.query(f"INSERT INTO {table_name} (x) VALUES (1)")

        with When("I alter the row policy to have a condition"):
            node.query(f"ALTER ROW POLICY {pol_name} ON CLUSTER sharded_cluster ON {table_name} FOR SELECT USING 1")

        with Then("I select from the table"):
            output = node.query(f"SELECT * FROM {table_name}").output
            assert '' == output, error()

        with And("I select from another node on the cluster"):
            output = node2.query(f"SELECT * FROM {table_name}").output
            assert '' == output, error()

    finally:
        with Finally("I drop the row policy", flags=TE):
            node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON CLUSTER sharded_cluster ON {table_name}")

        with And("I drop the table", flags=TE):
            node.query(f"DROP TABLE {table_name} ON CLUSTER sharded_cluster")

@TestScenario
def diff_policies_on_diff_nodes(self, node=None):
    """Check that a row policy altered on a node, does not effect row policy on a different node.
    """

    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node
        node2 = self.context.node2

    try:
        with Given("I have a table on a cluster"):
            node.query(f"CREATE TABLE {table_name} ON CLUSTER sharded_cluster (x UInt64) ENGINE = Memory")

        with And("I have a row policy on the cluster"):
            node.query(f"CREATE ROW POLICY {pol_name} ON CLUSTER sharded_cluster ON {table_name}")

        with And("The table has some values on the first node"):
            node.query(f"INSERT INTO {table_name} (x) VALUES (1)")

        with And("The table has some values on the second node"):
            node2.query(f"INSERT INTO {table_name} (x) VALUES (1)")

        with When("I alter the row policy on the first node"):
            node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} FOR SELECT USING 1")

        with Then("I select from the table"):
            output = node.query(f"SELECT * FROM {table_name}").output
            assert '' == output, error()

        with And("I select from another node on the cluster"):
            output = node2.query(f"SELECT * FROM {table_name}").output
            assert '1' in output, error()

    finally:
        with Finally("I drop the row policy", flags=TE):
            node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON CLUSTER sharded_cluster ON {table_name}")

        with And("I drop the table", flags=TE):
            node.query(f"DROP TABLE {table_name} ON CLUSTER sharded_cluster")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Alter_Assignment("1.0"),
)
def assignment(self, node=None):
    """Check that user is able to see rows from a table when they have PERMISSIVE policy assigned to them.
    """

    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):

        with Given("I have a row policy"):
            row_policy(name=pol_name, table=table_name)

        with And("The row policy is permissive"):
            node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} FOR SELECT USING 1")

        with And("The table has some values"):
            node.query(f"INSERT INTO {table_name} (y) VALUES (1)")

        with When("I alter a row policy to be assigned to default"):
            node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} TO default")

        with Then("I try to select from the table"):
            output = node.query(f"SELECT * FROM {table_name}").output
            assert '1' in output, error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Alter_Assignment_None("1.0"),
)
def assignment_none(self, node=None):
    """Check that no one is affected when a row policy is altered to be assigned to NONE.
    """

    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):

        with Given("I have a row policy"):
            row_policy(name=pol_name, table=table_name)

        with And("The row policy is permissive"):
            node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} FOR SELECT USING 1")

        with And("The table has some values"):
            node.query(f"INSERT INTO {table_name} (y) VALUES (1)")

        with When("I alter a row policy to be assigned to NONE"):
            node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} TO NONE")

        with Then("I try to select from the table"):
            output = node.query(f"SELECT * FROM {table_name}").output
            assert '' == output, error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Alter_Assignment_All("1.0"),
)
def assignment_all(self, node=None):
    """Check that everyone is effected with a row policy is altered to be assigned to ALL.
    """

    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):

        with Given("I have a row policy"):
            row_policy(name=pol_name, table=table_name)

        with And("The row policy is permissive"):
            node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} FOR SELECT USING 1")

        with And("The table has some values"):
            node.query(f"INSERT INTO {table_name} (y) VALUES (1)")

        with When("I alter a row policy to be assigned to ALL"):
            node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} TO ALL")

        with Then("I try to select from the table"):
            output = node.query(f"SELECT * FROM {table_name}").output
            assert '1' in output, error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Alter_Assignment_AllExcept("1.0"),
)
def assignment_all_except(self, node=None):
    """Check that everyone is except the specified user is effect by a row policy is altered to be assigned to ALL EXCEPT.
    """

    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):

        with Given("I have a row policy"):
            row_policy(name=pol_name, table=table_name)

        with And("The row policy is permissive"):
            node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} FOR SELECT USING 1")

        with And("The table has some values"):
            node.query(f"INSERT INTO {table_name} (y) VALUES (1)")

        with When("I alter a row policy to be assigned to ALL EXCEPT default"):
            node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} TO ALL EXCEPT default")

        with Then("I try to select from the table"):
            output = node.query(f"SELECT * FROM {table_name}").output
            assert '' == output, error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Nesting("1.0")
)
def nested_view(self, node=None):
    """Check that if a user has a row policy on a table and a view is altered to use a condition on that table,
    the user is only able to access the rows specified by the assigned policies.
    """

    table_name = f"table_{getuid()}"
    view_name = f"view_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):

        try:
            with Given("I have a row policy"):
                row_policy(name=pol_name, table=table_name)

            with And("The table has some values"):
                node.query(f"INSERT INTO {table_name} (y) VALUES (1),(2)")

            with And("There is a view on the table"):
                node.query(f"CREATE VIEW {view_name} AS SELECT * FROM {table_name}")

            with When("I alter the row policy to be permissive"):
                node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} FOR SELECT USING y=1 TO default")

            with Then("I try to select from the view"):
                output = node.query(f"SELECT * FROM {view_name}").output
                assert '1' in output and '2' not in output, error()

        finally:
            with Finally("I drop the view", flags=TE):
                node.query(f"DROP VIEW IF EXISTS {view_name}")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Nesting("1.0")
)
def nested_live_view_before_policy(self, node=None):
    """Check that if a live view exists on a table and then a row policy is created,
    the user is only able to select rows specified by the assigned policies from the view.
    """

    table_name = f"table_{getuid()}"
    view_name = f"view_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):
        try:
            with Given("I add allow_experimental_live_view to the default query settings"):
                default_query_settings = getsattr(current().context, "default_query_settings", [])
                default_query_settings.append(("allow_experimental_live_view", 1))

            with And("I have a row policy"):
                row_policy(name=pol_name, table=table_name)

            with And("The table has some values"):
                node.query(f"INSERT INTO {table_name} (y) VALUES (1),(2)")

            with And("There exists a live view on the table"):
                node.query(f"CREATE LIVE VIEW {view_name} AS SELECT * FROM {table_name}")

            with When("I alter the row policy to be permissive"):
                node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} FOR SELECT USING y=1 TO default")

            with Then("I try to select from the view"):
                output = node.query(f"SELECT * FROM {view_name}").output
                assert '1' in output and '2' not in output, error()

        finally:
            with Finally("I drop the live view", flags=TE):
                node.query(f"DROP VIEW IF EXISTS {view_name}")

            with And("I remove allow_experimental_live_view from the default query settings", flags=TE):
                if default_query_settings:
                    try:
                        default_query_settings.pop(default_query_settings.index(("allow_experimental_live_view", 1)))
                    except ValueError:
                        pass

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Nesting("1.0")
)
def nested_live_view_after_policy(self, node=None):
    """Check that if a user has a row policy on a table and a materialized view is created on that table,
    the user is only able to select rows specified by the assigned policies from the view.
    """

    table_name = f"table_{getuid()}"
    view_name = f"view_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):
        try:
            with Given("I add allow_experimental_live_view to the default query settings"):
                default_query_settings = getsattr(current().context, "default_query_settings", [])
                default_query_settings.append(("allow_experimental_live_view", 1))

            with And("I have a row policy"):
                row_policy(name=pol_name, table=table_name)

            with And("The table has some values"):
                node.query(f"INSERT INTO {table_name} (y) VALUES (1),(2)")

            with When("I alter the row policy to be permissive"):
                node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} FOR SELECT USING y=1 TO default")

            with And("I create a live view on the table"):
                node.query(f"CREATE LIVE VIEW {view_name} AS SELECT * FROM {table_name}")

            with Then("I try to select from the view"):
                output = node.query(f"SELECT * FROM {view_name}").output
                assert '1' in output and '2' not in output, error()

        finally:
            with Finally("I drop the live view", flags=TE):
                node.query(f"DROP VIEW IF EXISTS {view_name}")

            with And("I remove allow_experimental_live_view from the default query settings", flags=TE):
                if default_query_settings:
                    try:
                        default_query_settings.pop(default_query_settings.index(("allow_experimental_live_view", 1)))
                    except ValueError:
                        pass

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Nesting("1.0")
)
def nested_mat_view_before_policy(self, node=None):
    """Check that if a materialized view exists on a table and then a row policy is created,
    the user is only able to select rows specified by the assigned policies from the view.
    """

    table_name = f"table_{getuid()}"
    view_name = f"view_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):
        try:
            with Given("I have a row policy"):
                row_policy(name=pol_name, table=table_name)

            with And("There exists a mat view on the table"):
                node.query(f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT * FROM {table_name}")

            with And("The table has some values"):
                node.query(f"INSERT INTO {table_name} (y) VALUES (1),(2)")

            with When("I alter the row policy"):
                node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} FOR SELECT USING y=1 TO default")

            with Then("I try to select from the view"):
                output = node.query(f"SELECT * FROM {view_name}").output
                assert '1' in output and '2' not in output, error()

        finally:
            with Finally("I drop the materialized view", flags=TE):
                node.query(f"DROP VIEW IF EXISTS {view_name}")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Nesting("1.0")
)
def nested_mat_view_after_policy(self, node=None):
    """Check that if a user has a row policy on a table and a materialized view is created on that table,
    the user is only able to select rows specified by the assigned policies from the view.
    """

    table_name = f"table_{getuid()}"
    view_name = f"view_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):
        try:
            with Given("I have a row policy"):
                row_policy(name=pol_name, table=table_name)

            with And("I alter the row policy"):
                node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} FOR SELECT USING y=1 TO default")

            with When("I create a mat view on the table"):
                node.query(f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT * FROM {table_name}")

            with And("The table has some values"):
                node.query(f"INSERT INTO {table_name} (y) VALUES (1),(2)")

            with Then("I try to select from the view"):
                output = node.query(f"SELECT * FROM {view_name}").output
                assert '1' in output and '2' not in output, error()

        finally:
            with Finally("I drop the materialized view", flags=TE):
                node.query(f"DROP VIEW IF EXISTS {view_name}")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Nesting("1.0")
)
def populate_mat_view(self, node=None):
    """Check that if a user has a row policy on a table and a materialized view is created using POPULATE from that table,
    the user can only select the rows from the materialized view specified in the row policy.
    """

    table_name = f"table_{getuid()}"
    view_name = f"view_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):
        try:
            with Given("I have a row policy"):
                row_policy(name=pol_name, table=table_name)

            with And("I alter a row policy on the table"):
                node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} FOR SELECT USING y=1 TO default")

            with And("The table has some values"):
                node.query(f"INSERT INTO {table_name} (y) VALUES (1),(2)")

            with When("I create a mat view populated by the table"):
                node.query(f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory POPULATE AS SELECT * FROM {table_name}")

            with Then("I try to select from the view"):
                output = node.query(f"SELECT * FROM {view_name}").output
                assert '1' in output and '2' not in output, error()

        finally:
            with Finally("I drop the materialized view", flags=TE):
                node.query(f"DROP VIEW IF EXISTS {view_name}")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Nesting("1.0")
)
def dist_table(self, node=None):
    """Check that if a user has a row policy on a table and a distributed table is created on that table,
    the user is only able to access the rows specified by the assigned policies.
    """

    table_name = f"table_{getuid()}"
    dist_table_name = f"dist_table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node
        node2 = self.context.node2

    try:
        with Given("I have a table on a cluster"):
            node.query(f"CREATE TABLE {table_name} ON CLUSTER sharded_cluster (x UInt64) ENGINE = Memory")

        with And("I have a row policy"):
            node.query(f"CREATE ROW POLICY {pol_name} ON CLUSTER sharded_cluster ON {table_name}")

        with And("I have a distributed table"):
            node.query(f"CREATE TABLE {dist_table_name} (x UInt64) ENGINE = Distributed(sharded_cluster, default, {table_name}, rand())")

        with And("The table has some values on the first node"):
            node.query(f"INSERT INTO {table_name} (x) VALUES (1)")

        with When("I alter the row policy to be permissive"):
            node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} ON CLUSTER sharded_cluster FOR SELECT USING 1")

        with Then("I select from the distributed table"):
            output = node.query(f"SELECT * FROM {dist_table_name}").output
            assert '' == output, error()

    finally:
        with Finally("I drop the row policy", flags=TE):
            node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON CLUSTER sharded_cluster ON {table_name}")

        with And("I drop the table", flags=TE):
            node.query(f"DROP TABLE IF EXISTS {table_name} ON CLUSTER sharded_cluster")

        with And("I drop the distributed table", flags=TE):
            node.query(f"DROP TABLE IF EXISTS {dist_table_name}")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Nesting("1.0")
)
def dist_table_diff_policies_on_diff_nodes(self, node=None):
    """Check that user is only able to select from the distributed table what is allowed by the row policies on each node.
    """

    table_name = f"table_{getuid()}"
    dist_table_name = f"dist_table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node
        node2 = self.context.node2

    try:
        with Given("I have a table on a cluster"):
            node.query(f"CREATE TABLE {table_name} ON CLUSTER sharded_cluster (x UInt64) ENGINE = Memory")

        with And("I have a row policy"):
            node.query(f"CREATE ROW POLICY {pol_name} ON CLUSTER sharded_cluster ON {table_name}")

        with And("I have a distributed table"):
            node.query(f"CREATE TABLE {dist_table_name} (x UInt64) ENGINE = Distributed(sharded_cluster, default, {table_name}, rand())")

        with And("The table has some values on the first node"):
            node.query(f"INSERT INTO {table_name} (x) VALUES (1)")

        with And("The table has some values on the second node"):
            node2.query(f"INSERT INTO {table_name} (x) VALUES (2)")

        with When("I alter the row policy to be permissive on the first node"):
            node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} FOR SELECT USING 1")

        with Then("I select from the distributed table"):
            output = node.query(f"SELECT * FROM {dist_table_name}").output
            assert '1' not in output and '2' in output, error()

    finally:
        with Finally("I drop the row policy", flags=TE):
            node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON {table_name} ON CLUSTER sharded_cluster")

        with And("I drop the table", flags=TE):
            node.query(f"DROP TABLE IF EXISTS {table_name} ON CLUSTER sharded_cluster")

        with And("I drop the distributed table", flags=TE):
            node.query(f"DROP TABLE IF EXISTS {dist_table_name}")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Nesting("1.0")
)
def dist_table_on_dist_table(self, node=None):
    """Check that if a user has a row policy on a table and a distributed table is created on that table,
    and another distributed table is created on top of that,
    the user is only able to access rows on any of the tables specified by the assigned policies.
    """
    table_name = f"table_{getuid()}"
    dist_table_name = f"dist_table_{getuid()}"
    dist_table_2_name = f"dist_table_2_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node
        node2 = self.context.node2

    try:
        with Given("I have a table on a cluster"):
            node.query(f"CREATE TABLE {table_name} ON CLUSTER sharded_cluster (x UInt64) ENGINE = Memory")

        with And("I have a row policy"):
            node.query(f"CREATE ROW POLICY {pol_name} ON CLUSTER sharded_cluster ON {table_name}")

        with And("I have a distributed table on a cluster"):
            node.query(f"CREATE TABLE {dist_table_name} ON CLUSTER sharded_cluster (x UInt64) ENGINE = Distributed(sharded_cluster, default, {table_name}, rand())")

        with And("I have a distributed table on the other distributed table"):
            node.query(f"CREATE TABLE {dist_table_2_name} (x UInt64) ENGINE = Distributed(sharded_cluster, default, {dist_table_name}, rand())")

        with And("The table has some values on the first node"):
            node.query(f"INSERT INTO {table_name} (x) VALUES (1)")

        with When("I alter the row policy to be permissive on the first node"):
            node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} FOR SELECT USING 1")

        with Then("I select from the second distributed table"):
            output = node.query(f"SELECT * FROM {dist_table_2_name}").output
            assert '' == output, error()

    finally:
        with Finally("I drop the row policy", flags=TE):
            node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON CLUSTER sharded_cluster ON {table_name}")

        with And("I drop the table", flags=TE):
            node.query(f"DROP TABLE IF EXISTS {table_name} ON CLUSTER sharded_cluster")

        with And("I drop the distributed table", flags=TE):
            node.query(f"DROP TABLE IF EXISTS {dist_table_name} ON CLUSTER sharded_cluster")

        with And("I drop the outer distributed table", flags=TE):
            node.query(f"DROP TABLE IF EXISTS {dist_table_2_name}")

@TestScenario
def policy_before_table(self, node=None):
    """Check that if the policy is created and altered before the table,
    then it is still applied correctly.
    """
    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with Given("I have a row policy"):
        row_policy(name=pol_name, table=table_name)

    with And("I alter the row policy"):
        node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} FOR SELECT USING y=1 TO default")

    with table(node, table_name):
        with When("The table has some values"):
            node.query(f"INSERT INTO {table_name} (y) VALUES (1), (2)")

        with Then("I try to select from the table"):
            output = node.query(f"SELECT * FROM {table_name}").output
            assert '1' in output and '2' not in output, error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Nesting("1.0"),
)
def dict(self, node=None):
    """Check that if a user has a row policy on a table and a dictionary is created on that table,
    the user is only able to access the rows specified by the assigned policies.
    """

    table_name = f"table_{getuid()}"
    dict_name = f"view_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    try:
        with Given("I have a row policy"):
            row_policy(name=pol_name, table=table_name)

        with And("I have a table"):
            node.query(f"CREATE TABLE {table_name} (key UInt64, val UInt64 DEFAULT 5) ENGINE = Memory")

        with And("The table has some values"):
            node.query(f"INSERT INTO {table_name} (key) VALUES (1),(2)")

        with And("I create a dict on the table"):
            node.query(f"CREATE DICTIONARY {dict_name} (key UInt64 DEFAULT 0, val UInt64 DEFAULT 5) PRIMARY KEY key SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE {table_name} PASSWORD '' DB 'default')) LIFETIME(MIN 0 MAX 0) LAYOUT(FLAT())")

        with When("I alter the row policy to be permissive"):
            node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} FOR SELECT USING key=1 TO default")

        with Then("I try to select from the dict"):
            output = node.query(f"SELECT * FROM {dict_name}").output
            assert '1' in output and '2' not in output, error()

    finally:
        with Finally("I drop the materialized view", flags=TE):
            node.query(f"DROP DICTIONARY IF EXISTS {dict_name}")

        with And("I drop the table", flags=TE):
            node.query(f"DROP TABLE IF EXISTS {table_name}")


@TestFeature
@Name("alter row policy")
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AlterRowPolicy("1.0"),
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0")
)
def feature(self, node="clickhouse1"):
    """Check the RBAC functionality of ALTER ROW POLICY.
    """
    self.context.node = self.context.cluster.node(node)
    self.context.node2 = self.context.cluster.node("clickhouse2")

    Suite(run=privileges_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=privileges_granted_via_role, setup=instrument_clickhouse_server_log)

    Scenario(run=no_grants, setup=instrument_clickhouse_server_log)
    Scenario(run=permissive, setup=instrument_clickhouse_server_log)
    Scenario(run=restrictive, setup=instrument_clickhouse_server_log)
    Scenario(run=for_select, setup=instrument_clickhouse_server_log)
    Scenario(run=condition, setup=instrument_clickhouse_server_log)
    Scenario(run=remove_condition, setup=instrument_clickhouse_server_log)
    Scenario(run=if_exists, setup=instrument_clickhouse_server_log)
    Scenario(run=rename, setup=instrument_clickhouse_server_log)
    Scenario(run=on_cluster, setup=instrument_clickhouse_server_log)
    Scenario(run=assignment, setup=instrument_clickhouse_server_log)
    Scenario(run=assignment_none, setup=instrument_clickhouse_server_log)
    Scenario(run=assignment_all, setup=instrument_clickhouse_server_log)
    Scenario(run=assignment_all_except, setup=instrument_clickhouse_server_log)
    Scenario(run=nested_view, setup=instrument_clickhouse_server_log)
    Scenario(run=nested_live_view_before_policy, setup=instrument_clickhouse_server_log)
    Scenario(run=nested_live_view_after_policy, setup=instrument_clickhouse_server_log)
    Scenario(run=nested_mat_view_before_policy, setup=instrument_clickhouse_server_log)
    Scenario(run=nested_mat_view_after_policy, setup=instrument_clickhouse_server_log)
    Scenario(run=populate_mat_view, setup=instrument_clickhouse_server_log)
    Scenario(run=dist_table, setup=instrument_clickhouse_server_log)
    Scenario(run=dist_table_on_dist_table, setup=instrument_clickhouse_server_log)
    Scenario(run=dist_table_diff_policies_on_diff_nodes, setup=instrument_clickhouse_server_log)
    Scenario(run=diff_policies_on_diff_nodes, setup=instrument_clickhouse_server_log)
    Scenario(run=policy_before_table, setup=instrument_clickhouse_server_log)
    Scenario(run=dict, setup=instrument_clickhouse_server_log)
