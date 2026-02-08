from src.app import Celery
from src.broker.redis import RedisBroker

broker = RedisBroker()
app = Celery(broker)

print(f"[PRODUCER] Created app with broker: {broker}")


@app.task
def add(x, y):
    return x + y


print(f"[PRODUCER] Registered tasks: {list(app.tasks.keys())}")
result = add.apply_async(args=(10, 20), countdown=5)
print(f"[PRODUCER] Task sent with ID: {result.id}")
