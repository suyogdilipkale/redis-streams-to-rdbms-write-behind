# Redis Write-Behind to RDBMS Pipeline using Redis Streams

## 📌 Purpose

This project demonstrates a **Write-Behind caching strategy** using Redis Streams and RedisJSON as a front-end write buffer, with **eventual consistency** to a target RDBMS such as MySQL, PostgreSQL, or Oracle. It simulates how Redis can be used to improve write performance, durability, and scalability in data ingestion pipelines while ensuring persistence in traditional databases.

---

## 🎯 Business Objectives

- **Improve performance** for high-throughput applications by decoupling write-intensive operations.
- **Enhance scalability** of backend services with Redis-based buffer queues.
- **Ensure eventual consistency** by syncing data from Redis to RDBMS asynchronously.
- **Support zero-loss durability** with Redis WAITAOF and WAIT commands.
- **Enable observability** through Redis-based metrics.

---

## 💼 Key Use Cases

- Event stream processing (e.g., user actions, clickstreams)
- Activity tracking pipelines (e.g., audit trails)
- Transaction log buffers for payment or banking systems
- IoT or telemetry ingestion pipelines
- Data staging layer for analytics

---

## 🔧 How It Works

### Architectural Diagram

| Flow |
|---------|
| `Client / App` |
| `(Writes JSON)` |
|---------|
| `V` |
| `Redis JSON` |
| `(Structured Storage)` |
|---------|
| `V` |
| `Redis Streams` |
| `(Event Tracking)` |
|---------|
| `V` |
| `RedisWriteBehind Class` |
| `(RDBMS Sink Worker)` |
|---------|
| `V` |
| `RDBMS (MySQL, etc)` |
|---------|

---

## 🧠 Key Functions

| Function | Purpose |
|---------|---------|
| `insert_document()` | Inserts data into Redis JSON and emits a record to a Redis Stream |
| `insert_dummy_user_actions()` | Creates sample data for testing |
| `RedisWriteBehind` class | Main worker that reads Redis Streams and writes to RDBMS |
| `_transform_data()` | Placeholder function to transform data before persisting |
| `_write_to_rdbms()` | Writes JSON records from Redis into the configured database |
| `process_stream()` | Core loop for reading stream entries and processing them |
| `load_metrics()` | Prints out Redis counters for success/failure monitoring |

---

## ⚙️ Configuration Files

### `config/config.yaml`
```yaml
redis:
  host: localhost
  port: 6379
  replicas: 1

rdbms:
  type: mysql
  host: localhost
  port: 3306
  database: user_events_db
  user: redis
  password: redis
  max_retries: 3
  retry_delay_sec: 2

app:
  instance_id: instance-001

streams:
  user_action:
    stream_name: stream:user_action
    batch_size: 10
    start_id: "0-0"

log:
  enabled: true
  streams:
    success_insert: stream:success_inserts
    failed_insert: stream:failed_inserts
    success_rdbms: stream:success_rdbms
    failed_rdbms: stream:failed_rdbms
  ```
## 🏃 How to Run the Dummy Pipeline
### 1. Prepare Environment
- Start Redis Stack (with RedisJSON and Streams)
- Start MySQL and create the required database and table:
```sql
-- 1. Create the database (if not already created)
CREATE DATABASE IF NOT EXISTS redisdemo;

USE redisdemo;

CREATE TABLE user_actions (
  user_id VARCHAR(50),
  action VARCHAR(50),
  ts DATETIME
);
Add user credentials as per config.yaml:
-- 2. Create a new user (replace `redispass` with a strong password)
CREATE USER 'redisuser'@'localhost' IDENTIFIED BY 'redispass';
-- 3. Grant full privileges on the specific database
GRANT ALL PRIVILEGES ON redisdemo.* TO 'redisuser'@'localhost';
FLUSH PRIVILEGES;
```
### 2. Install Required Packages
```bash
pip install redis mysql-connector-python PyYAML
```
### 3. Run the Jupyter Notebook

## 📊 Observability
## To view Redis-based counters:
```python
from src.utils import load_metrics
load_metrics(config)
```
## Sample output:
```makefile
metrics:user_action:redis:success: 5
metrics:user_action:rdbms:success: 5
metrics:user_action:rdbms:fail: 0
```
