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
<img width="600" height="400" alt="f06a67b5-369b-4ee6-bb48-11b100f0e2e3" src="https://github.com/user-attachments/assets/30f09837-c111-4ab5-8dd6-6371d166eda7" />

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

# Functions and Purpose

### **Configuration & Connection**
- `load_config(path)`: Loads YAML configuration file.
- `connect_redis(cfg)`: Connects to Redis using config.
- `connect_postgres(cfg)`: Connects to PostgreSQL using config.

### **Utility**
- `generate_doc_id(app_id, machine_id)`: Generates unique document ID with app prefix and timestamp.

### **Redis JSON + Streams CRUD**
- `insert_document(redis_client, cfg, entity, document, doc_id)`: Inserts a JSON document into Redis and pushes an `insert` event to Streams.
- `update_document(redis_client, cfg, entity, doc_id, patch)`: Updates an existing JSON document and pushes an `update` event to Streams.
- `delete_document(redis_client, cfg, entity, doc_id)`: Deletes a document and pushes a `delete` event to Streams.

### **Internal Helpers**
- `_push_stream(redis_client, cfg, entity, doc_id, event_type)`: Adds an event entry to the Redis Stream for the entity.
- `_log_event(redis_client, cfg, key, payload)`: Logs an operation into a log stream and increments corresponding metrics.
- `_wait_for_durability(redis_client, cfg)`: Waits for replicas (WAIT) and optionally simulates AOF durability.
- `_get_stream_names_for_entity(cfg, entity)`: Returns the list of stream names for the given entity.
- `_get_last_processed_id(redis_client, cfg, stream_name)`: Retrieves last processed stream ID for consumer.
- `_set_last_processed_id(redis_client, cfg, stream_name, last_id)`: Stores last processed stream ID for consumer.

### **Read + Search**
- `read_by_ids(redis_client, entity, doc_ids)`: Reads documents from Redis by IDs.
- `create_search_index(redis_client, cfg, entity)`: Creates a RediSearch index for the entity.
- `random_search_queries(redis_client, cfg, entity, qcount)`: Runs random FT.SEARCH queries (best-effort).

### **Write-Behind Consumer**
- `RedisWriteBehind(redis_client, pg_conn, cfg, entity)`: Processes Redis Stream events and writes to PostgreSQL with retry logic.

### **Dummy Data**
- `generate_dummy_session()`: Creates a dummy session document.
- `dummy_data_worker(redis_client, cfg, entity)`: Generates dummy create/update/delete events for testing.

### **Consumer Loop & Metrics**
- `consume_periodically(redis_client, pg_conn, cfg, entity, stop_event)`: Runs the write-behind consumer at intervals in a background thread.
- `load_metrics(r, config)`: Retrieves Redis metrics keys for success/fail counts.

### **Main Entry Point**
- `main()`: Orchestrates connections, table creation, index creation, starts the consumer loop, runs dummy data generator, and displays metrics.

---

## Example Search Queries
```bash
FT.SEARCH idx:sessions "@user_id:user_4554" RETURN 4 user_id session_id device_type device_model
FT.SEARCH idx:sessions "@device_type:{android}" RETURN 4 user_id session_id device_type device_model
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
{'metrics:redis_success': '30', 'metrics:redis_fail': 0, 'metrics:redis_retry': 0, 'metrics:postgres_success': '28', 'metrics:postgres_fail': 0}
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
