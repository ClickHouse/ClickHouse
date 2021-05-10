from multiprocessing.dummy import Pool

from testflows.core import *

def join(tasks, test=None):
    """Join all parallel tests.
    """
    exc = None

    if test is None:
        test = current()

    for task in tasks:
        try:
            task.get()
        except Exception as e:
            exc = e

    if exc:
        raise exc

def start(pool, tasks, scenario, kwargs=None, test=None):
    """Start parallel test.
    """
    if test is None:
        test = current()
    if kwargs is None:
        kwargs = {}

    task = pool.apply_async(scenario, [], kwargs)
    tasks.append(task)
    return task

def run_scenario(pool, tasks, scenario, kwargs=None):
    """Run scenario in parallel if parallel flag is set
    in the context.
    """
    if kwargs is None:
        kwargs = {}

    if current().context.parallel:
        start(pool, tasks, scenario, kwargs)
    else:
        scenario(**kwargs)


