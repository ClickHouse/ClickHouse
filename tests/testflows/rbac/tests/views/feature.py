from testflows.core import *

from rbac.helper.common import *

@TestFeature
@Name("views")
def feature(self):
    xfail("broken", reason="https://github.com/ClickHouse/ClickHouse/issues/27570")

    with Pool(3) as pool:
        try:
            Feature(run=load("rbac.tests.views.view", "feature"), parallel=True, executor=pool)
            Feature(run=load("rbac.tests.views.live_view", "feature"), parallel=True, executor=pool)
            Feature(run=load("rbac.tests.views.materialized_view", "feature"), parallel=True, executor=pool)
        finally:
            join()
