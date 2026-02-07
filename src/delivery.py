import time

from src.message import Message


class Delivery:
    def __init__(self, raw: str, message: Message):
        self.raw = raw
        self.message = message
        self.reserved_at = time.time()
