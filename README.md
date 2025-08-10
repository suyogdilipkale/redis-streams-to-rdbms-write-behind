# Redis Write-Behind to RDBMS Pipeline using Redis Streams

## 📌 Purpose

This project demonstrates a **Write-Behind caching strategy** using Redis Streams and RedisJSON as a front-end write buffer, with **eventual consistency** to a target RDBMS such as MySQL, PostgreSQL, or Oracle. It simulates how Redis can be used to improve write performance, durability, and scalability in data ingestion pipelines while ensuring persistence in traditional databases.

This approach can be used as a high-performance in-memory data store for:
- Serving data with **low-latency read queries**  
- Handling **write-intensive operations**  
- Queuing data changes to be written to a **primary database (PostgreSQL)** at a slower pace

The approach is intended for **use cases where**:
- Some **lag** between Redis writes and PostgreSQL persistence is acceptable
- In the worst case, **data loss is tolerable** (unless using the `WAIT`/`WAITAOF` zero-data-loss flag)
- Primary objective is **sub-millisecond read/write latency**

---

## 🎯 Business Objectives

- Reduce **read query load** on primary databases  
- Handle **burst write traffic** without overloading disk-based RDBMS  
- Enable **asynchronous write-behind** from Redis Streams to PostgreSQL  
- Allow **configurable durability** (best-effort vs zero-data-loss)  
- Provide **metrics & logging** for observability
---

## 💼 Key Use Cases



✅ **Good for**:
- Event stream processing (e.g., user actions, clickstreams)
- Activity tracking pipelines (e.g., audit trails)
- Transaction log buffers for payment or banking systems
- IoT or telemetry ingestion pipelines
- Data staging layer for analytics
- Cache-aside patterns with eventual persistence
- IoT event ingestion and asynchronous processing

⚠️ **Not recommended for**:
- Mission-critical financial transactions requiring guaranteed persistence (but can be used for transaction meta data)  
- Use cases where **strict consistency** is mandatory

---

## 🔧 How It Works
<img width="1536" height="1024" alt="f06a67b5-369b-4ee6-bb48-11b100f0e2e3" src="https://github.com/user-attachments/assets/30f09837-c111-4ab5-8dd6-6371d166eda7" />

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

| Function Name               | Purpose                                                           |
| --------------------------- | ----------------------------------------------------------------- |
| `connect_redis()`           | Create Redis connection object (params from `config.yaml`)        |
| `connect_postgres()`        | Create PostgreSQL connection object                               |
| `insert_document()`         | Insert JSON doc into Redis, generate unique ID                    |
| `update_document()`         | Update doc in Redis                                               |
| `delete_document()`         | Delete doc in Redis                                               |
| `generate_dummy_data()`     | Simulate mobile banking sessions (create/update/delete docs)      |
| `enqueue_event_to_stream()` | Push event (insert/update/delete) to Redis Streams with timestamp |
| `create_redisearch_index()` | Create secondary index in Redis for search queries                |
| `read_documents_by_ids()`   | Fetch list of docs from Redis by IDs                              |
| `search_documents()`        | Run random search queries using RediSearch                        |
| `redis_write_behind()`      | Read from Redis Streams, write to PostgreSQL with retry logic     |
| `dummy_consumer()`          | Periodically trigger write-behind                                 |
| `log_metrics()`             | Store metrics in Redis counters & log streams                     |
| `load_metrics()`            | Fetch current metrics from Redis                                  |

---

## ⚙️ Configuration Files

### `config/config.yaml`
```yaml
postgresql:
  host: "localhost"
  port: 5432
  database: "redisdemo"
  user: "redis"
  password: "redis"

redis:
  host: "localhost"
  port: 6379
  db: 0
  password: null

zero_data_loss:
  enabled: true
  wait_for_replicas: true
  replicas: 1
  wait_timeout_ms: 2000
  wait_for_aof_rewrite: false   # best-effort simulation using BGREWRITEAOF

streams:
  prefix: "stream"
  log_streams:
    redis_success: "log:redis:success"
    redis_fail: "log:redis:fail"
    redis_retry: "log:redis:retry"
    postgres_success: "log:postgres:success"
    postgres_fail: "log:postgres:fail"
  last_id_key_prefix: "stream:lastid"  # used to store last processed id per stream

dummy_data:
  num_records: 5
  create_pct: 0.6
  update_pct: 0.3
  delete_pct: 0.1
  perform_write_behind:
    insert: true
    update: true
    delete: false

write_behind:
  batch_size: 5
  max_retry_attempts: 3
  interval_seconds: 10

search_index:
  create_index: true
  index_name: "idx:sessions"

  fields:
    - { name: "user_id", type: "TEXT" }
    - { name: "device_type", type: "TAG" }
    - { name: "device_model", type: "TAG" }
```
## 🏃 Running in Jupyter Notebook
### Install dependencies
```bash
pip install redis psycopg2-binary pyyaml
```
### Start Redis & PostgreSQL
```bash
redis-server
psql -U postgres
```
### Load notebook
- Open writebehind.ipynb

### Run cells in order
- Configure configs.yaml
- Create indexes
- Generate dummy traffic
- Start write-behind consumer
- Observe metrics
- Check Redis Streams for logs
- Query PostgreSQL for persisted data
```sql
select * from sessions
```

### 1. Prepare Environment
- Start Redis Stack (with RedisJSON and Streams)
- Start MySQL and create the required database and table:
```sql
-- 1. Create the database (if not already created)
CREATE DATABASE IF NOT EXISTS redisdemo;
-- 1. Create the table (if not already created)
CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                payload JSONB,
                created_at TIMESTAMPTZ DEFAULT now()
            )
```
### 2. Install Required Packages
```bash
pip install redis psycopg2 yaml
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

## Disclaimer
This is a demonstration project to highlight Redis in high-read/write workloads.
While it implements retry logic and optional zero-data-loss (WAIT/WAITAOF), edge cases can cause data loss.
Production deployments must consider:
- Multi-AZ Redis deployment
- Redis persistence (AOF/RDB)
- Network fault tolerance
- Consumer failure handling

## Redis Enterprise Features & Benefits
- Active-Active replication for geo-distribution
- High availability with automatic failover
- Integrated modules like RediSearch, RedisJSON, Redis Streams
- Memory tiering for cost optimization
- Scalable clustering for high throughput
- Enterprise-grade persistence & durability

## Possible Enhancements
- Use multiple Redis Streams per document type
- Implement Redis Consumer Groups for parallel write-behind workers
- Add exact-once processing for idempotency
- Alternative option to Integrate with Kafka for downstream systems
- Enhance metrics with Prometheus + Grafana dashboards
