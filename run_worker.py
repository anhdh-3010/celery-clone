# IMPORTANT: import task để registry được populate
from producer import app  # noqa
from src.worker.worker import Worker

print(f"[WORKER INIT] App tasks registry: {list(app.tasks.keys())}")

worker = Worker("worker 1", app)
worker.start()
