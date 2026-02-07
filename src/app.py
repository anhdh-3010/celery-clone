from src.broker.base import BaseBroker
from src.message import Message
from src.task import Task


class Celery:
    def __init__(self, broker: BaseBroker):
        self.tasks = {}
        self.broker = broker

    def task(self, func):
        task = Task(func, app=self)
        self.tasks[func.__name__] = task
        return task

    def send_task(self, task_name: str, args: tuple = (), kwargs: dict | None = None):
        if task_name not in self.tasks:
            raise KeyError(f"Task '{task_name}' not registered")

        msg = Message(task=task_name, args=args, kwargs=kwargs or {})
        self.broker.send(msg)

        return msg
