from testflows.core import *

from rbac.helper.common import *


@TestFeature
@Name("views")
def feature(self):

    with Pool(3) as pool:
        try:
            Feature(
                test=load("rbac.tests.views.view", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                test=load("rbac.tests.views.live_view", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                test=load("rbac.tests.views.materialized_view", "feature"),
                parallel=True,
                executor=pool,
            )
        finally:
            join()
