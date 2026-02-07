from abc import ABC, abstractmethod

from src.delivery import Delivery
from src.message import Message

READY = "celery:ready"
PROCESSING = "celery:processing"
DEAD = "celery:dead"
WORKERS = "celery:workers"


class BaseBroker(ABC):
    @abstractmethod
    def send(self, msg: Message) -> None:
        pass

    @abstractmethod
    def reserve(self, timeout: int = 5) -> Delivery | None:
        pass

    @abstractmethod
    def ack(self, delivery: Delivery) -> None:
        pass

    @abstractmethod
    def retry(self, delivery: Delivery) -> None:
        pass

    @abstractmethod
    def recover_expired(self, visibility_timeout: int) -> int:
        pass

    @abstractmethod
    def send_heartbeat(self, worker_name: str, ts: float | None = None) -> None:
        pass

    @abstractmethod
    def list_alive_workers(self, timeout: int) -> list:
        pass
