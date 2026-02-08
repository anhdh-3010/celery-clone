# Celery Clone - Distributed Task Queue

Một implementation đơn giản của Celery task queue sử dụng Redis làm message broker. Project này demo các core concepts của distributed task queue system.

## 🏗️ Architecture

```
Producer (producer.py)
    ↓
Celery App (src/app.py)
    ↓
Redis Broker (src/broker/redis.py)
    ↓
Queues:
├── READY (List)         - Tasks sẵn sàng xử lý
├── PROCESSING (ZSet)    - Tasks đang xử lý (với timestamp)
├── SCHEDULED (ZSet)     - Tasks được schedule (với ETA)
└── DEAD (List)          - Tasks thất bại
    ↓
Worker (src/worker/worker.py)
├── Main Loop           - Reserve và execute tasks
├── Heartbeat Loop      - Gửi heartbeat
├── Schedule Poll Loop  - Poll scheduled tasks
└── Reaper Loop         - Recover expired tasks
```

## 📦 Components

### BaseBroker (src/broker/base.py)

Abstract base class định nghĩa interface cho message broker với các methods:

- `send()`, `reserve()`, `ack()`, `dead()`
- `schedule()`, `poll_schedule()`
- `recover_expired()`
- `send_heartbeat()`, `list_alive_workers()`

### RedisBroker (src/broker/redis.py)

Implementation cụ thể sử dụng Redis data structures:

- **List (READY, DEAD)**: FIFO queue cho messages
- **Sorted Set (PROCESSING, SCHEDULED, WORKERS)**: Tracking với timestamp

### Reaper (src/broker/reaper.py)

Component để recover tasks bị expired:

- Phát hiện tasks trong PROCESSING quá lâu (> visibility_timeout)
- Chuyển chúng về READY queue để retry

### Worker (src/worker/worker.py)

Worker process với 3 background threads:

- **Main loop**: Reserve và execute tasks
- **Heartbeat loop**: Gửi heartbeat mỗi 5s
- **Schedule poll loop**: Poll scheduled tasks mỗi 1s
- **Reaper loop**: Recover expired tasks mỗi 10s

### Task (src/task.py)

Task decorator và execution:

- `@app.task` decorator để register tasks
- `.delay()` và `.apply_async()` để enqueue tasks
- Support `countdown` và `eta` cho scheduling

### Message (src/message.py)

Message format với UUID7 ID và JSON serialization

### Delivery (src/delivery.py)

Wrapper cho reserved message với metadata

## 🚀 Usage

### 1. Start Redis

```bash
docker-compose up -d
```

### 2. Define tasks (producer.py)

```python
from src.app import Celery
from src.broker.redis import RedisBroker

broker = RedisBroker()
app = Celery(broker)

@app.task
def add(x, y):
    return x + y

# Enqueue task
result = add.delay(10, 20)

# Schedule task (chạy sau 5 giây)
result = add.apply_async(args=(10, 20), countdown=5)
```

### 3. Run worker

```bash
python run_worker.py
```

## 🔧 Configuration

Worker có thể configure với các parameters:

```python
worker = Worker(
    name="worker-1",
    app=app,
    prefetch=1,                    # Số tasks lấy mỗi lần
    heartbeat_interval=5,          # Gửi heartbeat mỗi 5s
    schedule_poll_interval=1,      # Poll schedule mỗi 1s
    reaper_interval=10,            # Reaper chạy mỗi 10s
    visibility_timeout=30          # Timeout để recover tasks
)
```

Task có thể configure:

```python
@app.task(max_retries=5, default_retry_delay=10)
def my_task():
    pass
```

## 📊 Redis Data Structures

### Lists

- `celery:ready` - Tasks sẵn sàng xử lý (FIFO)
- `celery:dead` - Tasks thất bại sau max retries

### Sorted Sets (với score là timestamp)

- `celery:processing` - Tasks đang được xử lý
- `celery:scheduled` - Tasks được schedule (score = ETA)
- `celery:workers` - Workers còn sống (score = last heartbeat)

## 🎓 Concepts Learned

1. **Message Queue Pattern**: Producer-Consumer với Redis
2. **Distributed Task Queue**: Async task execution
3. **Fault Tolerance**: Retry, reaper, visibility timeout
4. **Graceful Shutdown**: Signal handling và resource cleanup
5. **Background Workers**: Multi-threading cho concurrent operations
6. **Task Scheduling**: Delayed execution với sorted sets
7. **Heartbeat Mechanism**: Worker liveness tracking

## 📝 TODO / Future Enhancements

- [ ] Result backend để lưu kết quả tasks
- [ ] Priority queues
- [ ] Task routing (nhiều queues)
- [ ] Concurrency (multiprocessing/threading)
- [ ] Monitoring dashboard
- [ ] Task chaining và workflows
- [ ] Rate limiting
- [ ] Exponential backoff cho retries

---
