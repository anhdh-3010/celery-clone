from src.broker.base import BaseBroker


class Reaper:
    """Component để recover các tasks bị expired trong PROCESSING queue.

    Khi worker bị crash hoặc timeout, tasks trong PROCESSING queue sẽ bị stuck.
    Reaper sẽ tự động phát hiện và chuyển các tasks này về READY queue để retry.
    """

    def __init__(self, broker: BaseBroker, visibility_timeout: int = 30):
        """Khởi tạo Reaper.

        Args:
            broker: BaseBroker instance để interact với message queue
            visibility_timeout: Thời gian (giây) sau đó task được coi là expired
        """
        self.broker = broker
        self.visibility_timeout = visibility_timeout

    def reap(self) -> None:
        """Recover các tasks đã expired từ PROCESSING về READY queue.

        Tasks trong PROCESSING quá lâu (> visibility_timeout) sẽ được
        move về READY queue để worker khác có thể xử lý lại.
        """
        recovered = self.broker.recover_expired(self.visibility_timeout)
        if recovered:
            print(f"[REAPER] Recovered {recovered} expired tasks")
