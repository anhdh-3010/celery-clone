import time
from typing import List, cast

import redis

from src.broker.base import DEAD, PROCESSING, READY, WORKERS, BaseBroker
from src.delivery import Delivery
from src.message import Message


class RedisBroker(BaseBroker):
    def __init__(self, url: str = "redis://localhost:6379/0", max_retries: int = 3):
        self.redis = redis.Redis.from_url(url)
        self.max_retries = max_retries

    def send_heartbeat(self, worker_name: str, ts: float | None = None) -> None:
        if ts is None:
            ts = time.time()

        self.redis.zadd(WORKERS, {worker_name: ts})

    def live_alive_workers(self, timeout: int) -> List:
        now = time.time()

        workers = cast(List, self.redis.zrangebyscore(WORKERS, now - timeout, now))
        return [worker.decode() for worker in workers]

    def send(self, msg: Message) -> None:
        self.redis.rpush(READY, msg.dumps())

    def reserve(self, timeout: int = 5):
        raw = self.redis.brpoplpush(READY, READY, timeout)
        if not raw:
            return None

        raw_str = str(raw)
        ts = time.time()
        self.redis.zadd(PROCESSING, {raw_str: ts})

        msg = Message.loads(raw_str)
        return Delivery(raw=raw_str, message=msg)

    def ack(self, delivery: Delivery) -> None:
        self.redis.zrem(PROCESSING, 1, delivery.raw)

    def retry(self, delivery: Delivery) -> None:
        self.redis.zrem(PROCESSING, 1, delivery.raw)
        msg = delivery.message
        msg.retries += 1

        if msg.retries > self.max_retries:
            self.redis.rpush(DEAD, msg.dumps())
        else:
            self.redis.rpush(READY, msg.dumps())

    def recover_expired(self, visibility_timeout: int) -> int:
        now = time.time()
        expired = cast(
            List[bytes],
            self.redis.zrangebyscore(PROCESSING, 0, now - visibility_timeout),
        )
        if not expired:
            return 0

        for raw in expired:
            self.redis.zrem(PROCESSING, raw)
            self.redis.rpush(READY, raw)

        return len(expired)
