import json
import time

import uuid6


class Message:
    def __init__(self, task: str, args: tuple, kwargs: dict, retries: int = 0):
        self.id = str(uuid6.uuid7())
        self.task = task
        self.args = args
        self.kwargs = kwargs
        self.retries = retries
        self.ts = time.time()

    def dumps(self) -> str:
        return json.dumps(self.__dict__)

    @staticmethod
    def loads(raw: str) -> Message:
        data = json.loads(raw)
        msg = Message(
            task=data["task"],
            args=tuple(data["args"]),
            kwargs=data["kwargs"],
            retries=data["retries"],
        )
        msg.id = data["id"]
        msg.ts = data["ts"]

        return msg
