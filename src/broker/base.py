from abc import ABC, abstractmethod

from src.delivery import Delivery
from src.message import Message

READY = "celery:ready"
PROCESSING = "celery:processing"
DEAD = "celery:dead"
WORKERS = "celery:workers"
SCHEDULED = "celery:scheduled"


class BaseBroker(ABC):
    """Lớp trừu tượng định nghĩa interface cho message broker.

    Cung cấp các phương thức để gửi, nhận, xử lý và quản lý messages trong hệ thống task queue.
    """

    @abstractmethod
    def send(self, msg: Message) -> None:
        """Gửi một message vào queue ready để xử lý.

        Args:
            msg: Message object cần gửi
        """
        pass

    @abstractmethod
    def reserve(self, timeout: int = 5) -> Delivery | None:
        """Lấy một message từ queue ready để xử lý.

        Args:
            timeout: Thời gian chờ tối đa (giây) để lấy message

        Returns:
            Delivery object nếu có message, None nếu không có
        """
        pass

    @abstractmethod
    def ack(self, delivery: Delivery) -> None:
        """Xác nhận đã xử lý thành công một message.

        Args:
            delivery: Delivery object cần acknowledge
        """
        pass

    @abstractmethod
    def dead(self, delivery: Delivery) -> None:
        """Chuyển message thất bại vào dead letter queue.

        Args:
            delivery: Delivery object cần chuyển vào dead queue
        """
        pass

    @abstractmethod
    def recover_expired(self, visibility_timeout: int) -> int:
        """Khôi phục các message đã hết hạn visibility timeout về queue ready.

        Args:
            visibility_timeout: Thời gian timeout (giây) để xác định message hết hạn

        Returns:
            Số lượng messages đã được khôi phục
        """
        pass

    @abstractmethod
    def send_heartbeat(self, worker_name: str, ts: float | None = None) -> None:
        """Gửi tín hiệu heartbeat từ worker để báo hiệu worker đang hoạt động.

        Args:
            worker_name: Tên của worker gửi heartbeat
            ts: Timestamp của heartbeat, mặc định là thời gian hiện tại
        """
        pass

    @abstractmethod
    def list_alive_workers(self, timeout: int) -> list:
        """Liệt kê các workers đang hoạt động.

        Args:
            timeout: Thời gian timeout (giây) để xác định worker còn sống

        Returns:
            Danh sách tên các workers đang hoạt động
        """
        pass

    @abstractmethod
    def schedule(self, msg: Message) -> None:
        """Lên lịch một message để xử lý vào thời gian trong tương lai.

        Args:
            msg: Message object có chứa thông tin ETA (estimated time of arrival)
        """
        pass

    @abstractmethod
    def poll_schedule(self) -> int:
        """Kiểm tra và chuyển các scheduled messages đã đến hạn vào queue ready.

        Returns:
            Số lượng messages đã được chuyển từ scheduled sang ready queue
        """
        pass
