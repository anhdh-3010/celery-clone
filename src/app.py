from src.broker.base import BaseBroker
from src.task import Task


class Celery:
    def __init__(self, broker: BaseBroker):
        self.tasks = {}
        self.broker = broker

    def task(self, *dargs, **dkwargs):
        def wrapper(func):
            task = Task(func, app=self, **dkwargs)
            self.tasks[func.__name__] = task
            return task

        if dargs and callable(dargs[0]) and len(dargs) == 1 and not dkwargs:
            return wrapper(dargs[0])

        return wrapper
