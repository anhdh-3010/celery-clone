import time
from datetime import datetime

from src.message import Message


class Task:
    def __init__(self, func, app, max_retries: int = 3, default_retry_delay: int = 5):
        self.func = func
        self.app = app
        self.name = func.__name__
        self.max_retries = max_retries
        self.default_retry_delay = default_retry_delay

    def delay(self, *args, **kwargs):
        return self.apply_async(args=args, kwargs=kwargs)

    def apply_async(
        self,
        args: tuple = (),
        kwargs: dict | None = None,
        countdown: int | None = None,
        eta: datetime | float | None = None,
    ):
        if kwargs is None:
            kwargs = {}

        eta_ts = None
        if countdown is not None:
            eta_ts = time.time() + countdown
        elif eta is not None:
            if isinstance(eta, datetime):
                eta_ts = eta.timestamp()
            else:
                eta_ts = float(eta)

        msg = Message(
            task=self.name,
            args=args,
            kwargs=kwargs,
            eta=eta_ts,
        )

        if msg.eta:
            self.app.broker.schedule(msg)
        else:
            self.app.broker.send(msg)

        return msg

    __call__ = delay
