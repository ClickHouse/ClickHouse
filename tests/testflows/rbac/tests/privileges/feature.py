from testflows.core import *

from rbac.helper.common import *

@TestFeature
@Name("privileges")
def feature(self):

    tasks = []
    pool = Pool(16)

    try:
        try:
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.insert", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.select", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.public_tables", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.distributed_table", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.grant_option", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.truncate", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.optimize", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.kill_query", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.kill_mutation", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.role_admin", "feature"), flags=TE), {})

            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.show.show_tables", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.show.show_dictionaries", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.show.show_databases", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.show.show_columns", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.show.show_users", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.show.show_roles", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.show.show_quotas", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.show.show_settings_profiles", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.show.show_row_policies", "feature"), flags=TE), {})

            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_column", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_index", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_constraint", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_ttl", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_settings", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_update", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_delete", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_freeze", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_fetch", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_move", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_user", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_role", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_row_policy", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_quota", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_settings_profile", "feature"), flags=TE), {})

            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.create.create_database", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.create.create_dictionary", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.create.create_temp_table", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.create.create_table", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.create.create_user", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.create.create_role", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.create.create_row_policy", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.create.create_quota", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.create.create_settings_profile", "feature"), flags=TE), {})

            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.attach.attach_database", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.attach.attach_dictionary", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.attach.attach_temp_table", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.attach.attach_table", "feature"), flags=TE), {})

            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.drop.drop_database", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.drop.drop_dictionary", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.drop.drop_table", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.drop.drop_user", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.drop.drop_role", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.drop.drop_row_policy", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.drop.drop_quota", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.drop.drop_settings_profile", "feature"), flags=TE), {})

            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.detach.detach_database", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.detach.detach_dictionary", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.detach.detach_table", "feature"), flags=TE), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.detach.detach_view", "feature"), flags=TE), {})
        finally:
            join(tasks)
    finally:
        pool.close()
