import time
from typing import List, cast

import redis

from src.broker.base import DEAD, PROCESSING, READY, SCHEDULED, WORKERS, BaseBroker
from src.delivery import Delivery
from src.message import Message


class RedisBroker(BaseBroker):
    """Implementation của BaseBroker sử dụng Redis làm message broker.

    Sử dụng Redis data structures để quản lý task queue:
    - List (READY, DEAD): Queue chứa messages
    - Sorted Set (PROCESSING, SCHEDULED, WORKERS): Quản lý messages và workers theo thời gian
    """

    def __init__(self, url: str = "redis://localhost:6379/0", max_retries: int = 3):
        """Khởi tạo RedisBroker.

        Args:
            url: Redis connection URL
            max_retries: Số lần retry tối đa cho tasks
        """
        self.redis = redis.Redis.from_url(url)
        self.max_retries = max_retries

    def send_heartbeat(self, worker_name: str, ts: float | None = None) -> None:
        """Gửi tín hiệu heartbeat từ worker để báo hiệu worker đang hoạt động.

        Lưu timestamp của worker vào sorted set WORKERS để tracking.

        Args:
            worker_name: Tên của worker gửi heartbeat
            ts: Timestamp của heartbeat, mặc định là thời gian hiện tại
        """
        if ts is None:
            ts = time.time()

        # Lưu worker vào sorted set với score là timestamp
        self.redis.zadd(WORKERS, {worker_name: ts})

    def list_alive_workers(self, timeout: int) -> List:
        """Liệt kê các workers đang hoạt động.

        Worker được coi là alive nếu có heartbeat trong khoảng timeout.

        Args:
            timeout: Thời gian timeout (giây) để xác định worker còn sống

        Returns:
            Danh sách tên các workers đang hoạt động
        """
        now = time.time()

        # Lấy workers có timestamp trong khoảng [now - timeout, now]
        workers = cast(List, self.redis.zrangebyscore(WORKERS, now - timeout, now))
        return [worker.decode() for worker in workers]

    def send(self, msg: Message) -> None:
        """Gửi một message vào queue ready để xử lý.

        Args:
            msg: Message object cần gửi
        """
        # Push message vào cuối queue READY (FIFO)
        self.redis.rpush(READY, msg.dumps())

    def reserve(self, timeout: int = 5):
        """Lấy một message từ queue ready để xử lý.

        Blocking operation: chờ cho đến khi có message hoặc timeout.
        Message được move vào PROCESSING sorted set để tracking.

        Args:
            timeout: Thời gian chờ tối đa (giây) để lấy message

        Returns:
            Delivery object nếu có message, None nếu không có
        """
        # Blocking right pop từ queue READY
        result = self.redis.brpop([READY], timeout)
        if not result:
            return None

        _, raw = result  # type: ignore
        raw_str = raw.decode("utf-8")
        ts = time.time()

        # Move message vào PROCESSING với timestamp để track visibility timeout
        self.redis.zadd(PROCESSING, {raw_str: ts})

        msg = Message.loads(raw_str)
        return Delivery(raw=raw_str, message=msg)

    def ack(self, delivery: Delivery) -> None:
        """Xác nhận đã xử lý thành công một message.

        Xóa message khỏi PROCESSING sorted set.

        Args:
            delivery: Delivery object cần acknowledge
        """
        # Xóa message khỏi PROCESSING sau khi xử lý thành công
        self.redis.zrem(PROCESSING, 1, delivery.raw)

    def dead(self, delivery: Delivery) -> None:
        """Chuyển message thất bại vào dead letter queue.

        Message bị lỗi hoặc vượt quá số lần retry sẽ được chuyển vào DEAD queue.

        Args:
            delivery: Delivery object cần chuyển vào dead queue
        """
        # Xóa khỏi PROCESSING
        self.redis.zrem(PROCESSING, delivery.raw)
        # Push vào DEAD queue để lưu trữ messages thất bại
        self.redis.rpush(DEAD, delivery.raw)

    def recover_expired(self, visibility_timeout: int) -> int:
        """Khôi phục các message đã hết hạn visibility timeout về queue ready.

        Messages trong PROCESSING quá lâu (vượt visibility_timeout) có thể do
        worker bị crash. Method này sẽ move chúng về READY để retry.

        Args:
            visibility_timeout: Thời gian timeout (giây) để xác định message hết hạn

        Returns:
            Số lượng messages đã được khôi phục
        """
        now = time.time()

        # Tìm messages có timestamp cũ hơn (now - visibility_timeout)
        expired = cast(
            List[bytes],
            self.redis.zrangebyscore(PROCESSING, 0, now - visibility_timeout),
        )
        if not expired:
            return 0

        # Move các expired messages từ PROCESSING về READY
        for raw in expired:
            self.redis.zrem(PROCESSING, raw)
            self.redis.rpush(READY, raw)

        return len(expired)

    def schedule(self, msg: Message) -> None:
        """Lên lịch một message để xử lý vào thời gian trong tương lai.

        Message được lưu vào SCHEDULED sorted set với score là ETA timestamp.

        Args:
            msg: Message object có chứa thông tin ETA (estimated time of arrival)
        """
        if msg.eta is not None:
            # Lưu vào SCHEDULED với score là ETA timestamp
            self.redis.zadd(SCHEDULED, {msg.dumps(): msg.eta})

    def poll_schedule(self) -> int:
        """Kiểm tra và chuyển các scheduled messages đã đến hạn vào queue ready.

        Scan SCHEDULED sorted set để tìm messages có ETA <= now,
        sau đó move chúng vào READY queue để xử lý.

        Returns:
            Số lượng messages đã được chuyển từ scheduled sang ready queue
        """
        now = time.time()

        # Lấy các messages có ETA <= now (score từ 0 đến now)
        scheduled = cast(
            List[bytes],
            self.redis.zrangebyscore(SCHEDULED, 0, now),
        )
        if not scheduled:
            return 0

        # Move các messages đã đến hạn từ SCHEDULED sang READY
        for raw in scheduled:
            self.redis.zrem(SCHEDULED, raw)
            self.redis.rpush(READY, raw)

        return len(scheduled)
