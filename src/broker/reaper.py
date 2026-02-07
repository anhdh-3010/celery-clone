from src.broker.base import BaseBroker


class Reaper:
    def __init__(self, broker: BaseBroker, visibility_timeout: int = 30):
        self.broker = broker
        self.visibility_timeout = visibility_timeout

    def reap(self) -> None:
        recovered = self.broker.recover_expired(self.visibility_timeout)
        if recovered:
            print(f"[REAPER] Recovered {recovered} expired tasks")
