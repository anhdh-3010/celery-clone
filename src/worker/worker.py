import signal
import threading
import time
from collections import deque

from src.broker.base import BaseBroker


class Worker:
    def __init__(self, name: str, app, prefetch: int = 1, heartbeat_interval: int = 5):
        self.name = name
        self.app = app
        self.broker: BaseBroker = app.broker
        self.prefetch = prefetch
        self.inflight: deque = deque()
        self.heartbeat_interval = heartbeat_interval
        self._running = True
        self._shutting_down = False
        self._register_signals()

    def _register_signals(self):
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, signum, frame):
        print(f"[WORKER {self.name}] Received signal {signum}. Shutting down...")
        self._shutting_down = True
        self._running = False

    def start(self):
        print(f"[WORKER {self.name}] started")

        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        self._main_loop()

    def _heartbeat_loop(self):
        while self._running:
            self.broker.send_heartbeat(self.name)
            time.sleep(self.heartbeat_interval)

    def _main_loop(self):
        while self._running:
            if self._shutting_down:
                break

            while len(self.inflight) < self.prefetch:
                delivery = self.broker.reserve()
                if not delivery:
                    break

                self.inflight.append(delivery)

            if not self.inflight:
                time.sleep(0.1)
                continue

            delivery = self.inflight.popleft()
            self._process_delivery(delivery)

        self._drain_inflight()
        print(f"[WORKER {self.name}] Shutdown complete")

    def _drain_inflight(self):
        print(f"[WORKER {self.name}] Draining inflight tasks...")
        while self.inflight:
            delivery = self.inflight.popleft()
            self.broker.ack(delivery)

    def _process_delivery(self, delivery):
        msg = delivery.message
        print(f"[WORKER {self.name}] Run: {msg.task}: {msg.id}")

        try:
            task = self.app.tasks[msg.task]
            task.func(*msg.args, **msg.kwargs)
            self.broker.ack(delivery)
            print(f"[WORKER {self.name}] Acknowledged: {msg.id}")

        except Exception as e:
            msg.retries += 1
            if msg.retries > task.max_retries:
                self.broker.dead(delivery)
                print(f"[WORKER {self.name}] Dead: {msg.id} - {e}")
                return

            delay = task.default_retry_delay
            msg.eat = time.time() + delay

            self.broker.schedule(msg)
            self.broker.ack(delivery)

            print(
                f"[WORKER {self.name}] Retry {msg.id} in {delay}s "
                f"(attempt {msg.retries}/{task.max_retries})"
            )
