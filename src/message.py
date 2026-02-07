import json
import time
from typing import Optional

import uuid6


class Message:
    def __init__(
        self,
        task: str,
        args: tuple,
        kwargs: dict,
        retries: int = 0,
        eta: Optional[float] = None,
    ):
        self.id = str(uuid6.uuid7())
        self.task = task
        self.args = args
        self.kwargs = kwargs
        self.retries = retries
        self.eta = eta
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
            retries=data.get("retries", 0),
            eta=data.get("eta", None),
        )
        msg.id = data["id"]
        msg.ts = data["ts"]

        return msg
