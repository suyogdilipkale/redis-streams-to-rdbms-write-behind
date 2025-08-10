"""
redis_writebehind_demo.py

Single-file demo implementing:
 - Redis JSON storage (insert/update/delete)
 - Redis Streams for change events
 - Optional zero-data-loss (WAIT replicas + simulated AOF wait)
 - RediSearch index creation (if module present)
 - Dummy data generator (create/update/delete random sessions)
 - Read helpers (by ids and search)
 - RedisWriteBehind consumer which reads streams and writes to PostgreSQL with retry logic
 - Periodic consumer loop and metrics/logging into Redis

Sample search queries:
FT.SEARCH idx:sessions "@user_id:user_4554" RETURN 4 user_id session_id device_type device_model
FT.SEARCH idx:sessions "@device_type:{android}" RETURN 4 user_id session_id device_type device_model

Put a configs.yaml next to this file (example structure below). Example:
-----------------------------------------------------------------------
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

postgresql:
  host: "localhost"
  port: 5432
  database: "redisdemo"
  user: "redis"
  password: "redis"

dummy_data:
  num_records: 100
  create_pct: 0.6
  update_pct: 0.3
  delete_pct: 0.1
  perform_write_behind:
    insert: true
    update: true
    delete: false

write_behind:
  batch_size: 50
  max_retry_attempts: 3
  interval_seconds: 10

search_index:
  create_index: true
  index_name: "idx:sessions"
  fields:
    - { name: "user_id", type: "TEXT" }
    - { name: "device_type", type: "TAG" }
    - { name: "device_model", type: "TAG" }
-----------------------------------------------------------------------
"""

import json
import logging
import random
import threading
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import psycopg2
import redis
import yaml

# ---------------------------
# Logging setup
# ---------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("RedisWriteBehindDemo")

# ---------------------------
# Utility / Config loading
# ---------------------------
def load_config(path: str = "/Users/suyog/Documents/GitHub/redis-streams-to-rdbms-write-behind/configs/configs.yaml") -> Dict[str, Any]:
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)
    return cfg


# ---------------------------
# Connections
# ---------------------------
def connect_redis(cfg: Dict[str, Any]) -> redis.Redis:
    rconf = cfg.get("redis", {})
    client = redis.Redis(
        host=rconf.get("host", "localhost"),
        port=int(rconf.get("port", 6379)),
        password=rconf.get("password", None),
        decode_responses=True,
    )
    # Test connection
    client.ping()
    logger.info("Connected to Redis at %s:%s", rconf.get("host"), rconf.get("port"))
    return client


def connect_postgres(cfg: Dict[str, Any]):
    p = cfg.get("postgresql", {})
    conn = psycopg2.connect(
        host=p.get("host", "localhost"),
        port=p.get("port", 5432),
        dbname=p.get("database"),
        user=p.get("user"),
        password=p.get("password"),
    )
    conn.autocommit = True
    logger.info("Connected to PostgreSQL at %s:%s/%s", p.get("host"), p.get("port"), p.get("database"))
    return conn


# ---------------------------
# ID generation
# ---------------------------
def generate_doc_id(app_id: str = "app", machine_id: Optional[str] = None) -> str:
    if machine_id is None:
        machine_id = uuid.getnode() & 0xFFFFFF
    epoch = int(time.time() * 1000)
    unique = uuid.uuid4().hex[:8]
    return f"{app_id}:{epoch}:{machine_id}:{unique}"


# ---------------------------
# Redis JSON CRUD + Stream push + durability wait helpers
# ---------------------------
def _push_stream(redis_client: redis.Redis, cfg: Dict[str, Any], entity: str, doc_id: str, event_type: str):
    streams_cfg = cfg.get("streams", {})
    prefix = streams_cfg.get("prefix", "stream")
    stream_name = f"{prefix}:{entity}"
    payload = {"ts": str(int(time.time() * 1000)), "doc_id": doc_id, "event_type": event_type}
    redis_client.xadd(stream_name, payload)
    logger.debug("Pushed to stream %s: %s", stream_name, payload)


def _log_event(redis_client: redis.Redis, cfg: Dict[str, Any], key: str, payload: Dict[str, str]):
    """
    Log into a configured Redis log stream and increment counters.
    """
    streams_cfg = cfg.get("streams", {})
    log_streams = streams_cfg.get("log_streams", {})
    stream_name = log_streams.get(key)
    if stream_name:
        redis_client.xadd(stream_name, payload)
    # increment counters
    counter_key = f"metrics:{key}"
    redis_client.incr(counter_key)


def _wait_for_durability(redis_client: redis.Redis, cfg: Dict[str, Any]):
    """
    Implement 'zero data loss' durability best-effort:
     - Wait for replica acknowledgement using WAIT command (replicas, timeout_ms)
     - Optionally trigger BGREWRITEAOF and wait until rewrite completes (simulation of AOF persistence)
    NOTE: There isn't a single immediate 'fsync AOF for a single write' Redis command; this is a best-effort approach.
    """
    zdl = cfg.get("zero_data_loss", {}) or {}
    if not zdl.get("enabled"):
        return

    # Wait for replicas
    if zdl.get("wait_for_replicas"):
        replicas = int(zdl.get("replicas", 1))
        timeout_ms = int(zdl.get("wait_timeout_ms", 2000))
        try:
            # WAIT returns number of replicas that acknowledged
            acked = redis_client.execute_command("WAIT", replicas, timeout_ms)
            logger.debug("WAIT acked replicas: %s (requested %s)", acked, replicas)
            if acked < replicas:
                logger.warning("WAIT: fewer replicas acked (%s) than requested (%s)", acked, replicas)
        except Exception as e:
            logger.exception("Error while calling WAIT: %s", e)

    # Optionally simulate AOF durability by performing BGREWRITEAOF and waiting for completion
    if zdl.get("wait_for_aof_rewrite"):
        try:
            info = redis_client.info(section="persistence")
            rewrite_in_progress = info.get("aof_rewrite_in_progress", 0)
            if rewrite_in_progress == 0:
                redis_client.bgrewriteaof()
                # Poll until rewrite completes or times out
                wait_start = time.time()
                timeout = zdl.get("aof_rewrite_timeout_secs", 30)
                while True:
                    time.sleep(0.5)
                    info = redis_client.info(section="persistence")
                    if not info.get("aof_rewrite_in_progress", 0):
                        break
                    if time.time() - wait_start > timeout:
                        logger.warning("Timeout waiting for AOF rewrite to complete")
                        break
                logger.debug("BGREWRITEAOF finished or timed out")
            else:
                logger.debug("AOF rewrite already in progress")
        except Exception as e:
            logger.exception("Error while triggering/waiting for BGREWRITEAOF: %s", e)


def insert_document(redis_client: redis.Redis, cfg: Dict[str, Any], entity: str, document: Dict[str, Any], doc_id: Optional[str] = None):
    """Insert JSON doc into Redis and push event to stream"""
    if doc_id is None:
        doc_id = generate_doc_id(app_id=entity)
    key = f"{entity}:{doc_id}"
    # JSON.SET key . <json>
    try:
        # Use JSON.SET if module exists
        try:
            redis_client.execute_command("JSON.SET", key, ".", json.dumps(document))
        except redis.ResponseError as e:
            # maybe JSON module not present; fallback to plain set
            logger.debug("JSON.SET failed, falling back to plain SET: %s", e)
            redis_client.set(key, json.dumps(document))
        # durability wait (replica ack / optional AOF)
        _wait_for_durability(redis_client, cfg)
        # push to stream
        if cfg.get("dummy_data", {}).get("perform_write_behind", {}).get("insert", True):
            _push_stream(redis_client, cfg, entity, doc_id, "insert")
        # logging
        _log_event(redis_client, cfg, "redis_success", {"ts": str(int(time.time() * 1000)), "key": key, "op": "insert"})
        return doc_id
    except Exception as e:
        logger.exception("insert_document failed: %s", e)
        _log_event(redis_client, cfg, "redis_fail", {"ts": str(int(time.time() * 1000)), "key": key, "op": "insert", "err": str(e)})
        raise


def update_document(redis_client: redis.Redis, cfg: Dict[str, Any], entity: str, doc_id: str, patch: Dict[str, Any]):
    """Update JSON doc fields (for simplicity we fully replace object)"""
    key = f"{entity}:{doc_id}"
    try:
        # read existing
        try:
            curr = redis_client.execute_command("JSON.GET", key, ".")
            curr_obj = json.loads(curr) if curr else {}
        except redis.ResponseError:
            curr_raw = redis_client.get(key)
            curr_obj = json.loads(curr_raw) if curr_raw else {}
        # apply patch (merge)
        curr_obj.update(patch)
        try:
            redis_client.execute_command("JSON.SET", key, ".", json.dumps(curr_obj))
        except redis.ResponseError:
            redis_client.set(key, json.dumps(curr_obj))
        _wait_for_durability(redis_client, cfg)
        if cfg.get("dummy_data", {}).get("perform_write_behind", {}).get("update", True):
            _push_stream(redis_client, cfg, entity, doc_id, "update")
        _log_event(redis_client, cfg, "redis_success", {"ts": str(int(time.time() * 1000)), "key": key, "op": "update"})
    except Exception as e:
        logger.exception("update_document failed: %s", e)
        _log_event(redis_client, cfg, "redis_fail", {"ts": str(int(time.time() * 1000)), "key": key, "op": "update", "err": str(e)})
        raise


def delete_document(redis_client: redis.Redis, cfg: Dict[str, Any], entity: str, doc_id: str):
    key = f"{entity}:{doc_id}"
    try:
        # Delete JSON
        try:
            redis_client.execute_command("JSON.DEL", key, ".")
        except redis.ResponseError:
            redis_client.delete(key)
        _wait_for_durability(redis_client, cfg)
        if cfg.get("dummy_data", {}).get("perform_write_behind", {}).get("delete", True):
            _push_stream(redis_client, cfg, entity, doc_id, "delete")
        _log_event(redis_client, cfg, "redis_success", {"ts": str(int(time.time() * 1000)), "key": key, "op": "delete"})
    except Exception as e:
        logger.exception("delete_document failed: %s", e)
        _log_event(redis_client, cfg, "redis_fail", {"ts": str(int(time.time() * 1000)), "key": key, "op": "delete", "err": str(e)})
        raise


# ---------------------------
# Read helpers + search
# ---------------------------
def read_by_ids(redis_client: redis.Redis, entity: str, doc_ids: List[str]) -> List[Dict[str, Any]]:
    docs = []
    for did in doc_ids:
        key = f"{entity}:{did}"
        try:
            try:
                raw = redis_client.execute_command("JSON.GET", key, ".")
            except redis.ResponseError:
                raw = redis_client.get(key)
            if raw:
                # if JSON.GET returned a JSON string it may be double-quoted; attempt to parse
                try:
                    obj = json.loads(raw)
                except Exception:
                    # sometimes JSON.GET returns JSON string, try removing quotes
                    obj = json.loads(raw.strip('"'))
                docs.append({"doc_id": did, "doc": obj})
            else:
                docs.append({"doc_id": did, "doc": None})
        except Exception as e:
            logger.exception("read_by_ids error for %s: %s", key, e)
            docs.append({"doc_id": did, "error": str(e)})
    return docs


def create_search_index(redis_client: redis.Redis, cfg: Dict[str, Any], entity: str):
    """
    Very simple create index using RediSearch if present.
    TTL, field types should be adjusted for production.
    """
    sconf = cfg.get("search_index", {}) or {}
    if not sconf.get("create_index"):
        logger.info("Search index creation disabled in config")
        return
    index_name = sconf.get("index_name", f"idx:{entity}")
    # Build basic schema: TEXT fields for provided fields
    fields = sconf.get("fields", [])
    # If FT.CREATE fails, catch and log
    try:
        # Try a JSON index FT.CREATE idx ON JSON PREFIX 1 entity: SCHEMA $.user_id AS user_id TEXT ...
        schema_parts = []
        for field in sconf.get("fields", []):
            name = field.get("name")
            ftype = field.get("type")
            schema_parts.extend([f"$.{name}", "AS", name, ftype])
        # Execute FT.CREATE... (works if RediSearch v2+ with JSON support)
        cmd = ["FT.CREATE", index_name, "ON", "JSON", "PREFIX", "1", f"{entity}:", "SCHEMA"] + schema_parts
        redis_client.execute_command(*cmd)
        logger.info("Created RediSearch index %s for entity %s", index_name, entity)
    except redis.ResponseError as e:
        # Could already exist, or module not available
        logger.warning("Could not create RediSearch index (%s). Error: %s", index_name, e)


def random_search_queries(redis_client: redis.Redis, cfg: Dict[str, Any], entity: str, qcount: int = 5):
    """
    Run a few sample search queries using FT.SEARCH if available.
    This function is best-effort: if RediSearch isn't present it will log and return empty.
    """
    sconf = cfg.get("search_index", {}) or {}
    index_name = sconf.get("index_name", f"idx:{entity}")
    results = []
    try:
        for _ in range(qcount):
            # simple random sample: search by device_type or user_id patterns
            sample_field = random.choice(sconf.get("fields", ["user_id"]))
            term = "*"
            # attempt to run FT.SEARCH
            try:
                resp = redis_client.execute_command("FT.SEARCH", index_name, f"@{sample_field}:{term}", "LIMIT", "0", "10")
                results.append(resp)
                logger.debug(results)
            except Exception as e:
                logger.debug("FT.SEARCH failed: %s", e)
                break
    except Exception as e:
        logger.exception("random_search_queries error: %s", e)
    return results


# ---------------------------
# RedisWriteBehind consumer
# ---------------------------
def _get_stream_names_for_entity(cfg: Dict[str, Any], entity: str) -> List[str]:
    prefix = cfg.get("streams", {}).get("prefix", "stream")
    return [f"{prefix}:{entity}"]


def _get_last_processed_id(redis_client: redis.Redis, cfg: Dict[str, Any], stream_name: str) -> str:
    key = f"{cfg.get('streams',{}).get('last_id_key_prefix','stream:lastid')}:{stream_name}"
    val = redis_client.get(key)
    return val or "0-0"


def _set_last_processed_id(redis_client: redis.Redis, cfg: Dict[str, Any], stream_name: str, last_id: str):
    key = f"{cfg.get('streams',{}).get('last_id_key_prefix','stream:lastid')}:{stream_name}"
    redis_client.set(key, last_id)


def RedisWriteBehind(redis_client: redis.Redis, pg_conn, cfg: Dict[str, Any], entity: str):
    """
    Read events from Redis Streams (range-based) and write to PostgreSQL with retry logic.
    The function uses XRANGE starting from last-processed ID for each configured stream.
    """
    wb_cfg = cfg.get("write_behind", {})
    batch_size = int(wb_cfg.get("batch_size", 50))
    max_retries = int(wb_cfg.get("max_retry_attempts", 3))

    stream_names = _get_stream_names_for_entity(cfg, entity)
    for stream in stream_names:
        last_id = _get_last_processed_id(redis_client, cfg, stream)
        # XRANGE stream last_id + batch_size
        # XRANGE expects start < end; to get > last_id, use (last_id
        try:
            entries = redis_client.xrange(stream, min=last_id, max="+", count=batch_size)
            # Note: xrange returns entries inclusive of start; we need to skip start if equals last_id
            to_process = []
            for eid, fields in entries:
                if eid == last_id:
                    continue
                # fields is dict of bytes->str
                # Standardize fields
                ev = {k: v for k, v in fields.items()}
                to_process.append((eid, ev))
            if not to_process:
                continue

            # Now write each event to PostgreSQL
            cur = pg_conn.cursor()
            for eid, ev in to_process:
                event_type = ev.get("event_type")
                doc_id = ev.get("doc_id")
                ts = ev.get("ts")
                key = f"{entity}:{doc_id}"
                # Fetch current document from Redis if needed (for insert/update)
                doc_obj = None
                if event_type in ("insert", "update"):
                    try:
                        raw = redis_client.execute_command("JSON.GET", key, ".")
                    except redis.ResponseError:
                        raw = redis_client.get(key)
                    if raw:
                        try:
                            doc_obj = json.loads(raw)
                        except Exception:
                            doc_obj = json.loads(raw.strip('"'))
                # Build SQL based on event type
                sql = None
                params = None
                if event_type == "insert":
                    # Assuming a table sessions(id text primary key, payload jsonb, created_at timestamptz)
                    sql = "INSERT INTO sessions (id, payload, created_at) VALUES (%s, %s::jsonb, to_timestamp(%s::double precision / 1000)) ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload"
                    params = (doc_id, json.dumps(doc_obj or {}), ts or int(time.time() * 1000))
                elif event_type == "update":
                    sql = "UPDATE sessions SET payload = %s::jsonb WHERE id = %s"
                    params = (json.dumps(doc_obj or {}), doc_id)
                elif event_type == "delete":
                    sql = "DELETE FROM sessions WHERE id = %s"
                    params = (doc_id,)
                else:
                    logger.warning("Unknown event_type: %s", event_type)
                    _log_event(redis_client, cfg, "redis_fail", {"ts": str(int(time.time() * 1000)), "stream": stream, "id": eid, "op": "unknown_event"})
                    _set_last_processed_id(redis_client, cfg, stream, eid)
                    continue

                # Execute with retry
                success = False
                attempt = 0
                while attempt <= max_retries and not success:
                    try:
                        cur.execute(sql, params)
                        _log_event(redis_client, cfg, "postgres_success", {"ts": str(int(time.time() * 1000)), "id": doc_id, "op": event_type})
                        success = True
                    except Exception as e:
                        attempt += 1
                        logger.exception("Error writing to Postgres (attempt %s/%s): %s", attempt, max_retries, e)
                        _log_event(redis_client, cfg, "postgres_fail", {"ts": str(int(time.time() * 1000)), "id": doc_id, "op": event_type, "err": str(e)})
                        if attempt <= max_retries:
                            _log_event(redis_client, cfg, "redis_retry", {"ts": str(int(time.time() * 1000)), "id": doc_id, "attempt": str(attempt)})
                            time.sleep(1)  # backoff; could be exponential
                        else:
                            logger.error("Exceeded max retries for id %s op %s; skipping", doc_id, event_type)
                # After processing (success or after exhausting retries) advance last_id
                _set_last_processed_id(redis_client, cfg, stream, eid)
            cur.close()

        except Exception as e:
            logger.exception("Error in RedisWriteBehind for stream %s: %s", stream, e)


# ---------------------------
# Dummy data generator
# ---------------------------
def generate_dummy_session():
    """Return a sample session document dict"""
    user_id = f"user_{random.randint(1, 5000)}"
    device_type = random.choice(["android", "ios", "web"])
    device_model = random.choice(["Pixel-4", "iPhone-12", "Galaxy-S10", "OnePlus-8"])
    session_id = uuid.uuid4().hex
    now = int(time.time() * 1000)
    last_activity = now - random.randint(0, 300000)
    login_time = now - random.randint(0, 3600000)
    geo = {"lat": round(12 + random.random(), 6), "lon": round(77 + random.random(), 6)}
    device_logs = [{"ts": now - i * 1000, "evt": random.choice(["click", "tap", "swipe"])} for i in range(random.randint(1, 5))]
    return {
        "user_id": user_id,
        "session_id": session_id,
        "device_logs": device_logs,
        "last_activity": last_activity,
        "login_time": login_time,
        "device_type": device_type,
        "device_model": device_model,
        "geo_location": geo,
    }


def dummy_data_worker(redis_client: redis.Redis, cfg: Dict[str, Any], entity: str):
    """
    Generate dummy records and perform create/update/delete operations randomly based on config percentages.
    """
    dd = cfg.get("dummy_data", {})
    n = int(dd.get("num_records", 100))
    create_pct = float(dd.get("create_pct", 0.6))
    update_pct = float(dd.get("update_pct", 0.3))
    delete_pct = float(dd.get("delete_pct", 0.1))
    created_ids = []

    for i in range(n):
        r = random.random()
        if r <= create_pct or not created_ids:
            # create
            doc = generate_dummy_session()
            doc_id = insert_document(redis_client, cfg, entity, doc)
            created_ids.append(doc_id)
            logger.debug("Dummy created %s", doc_id)
        elif r <= create_pct + update_pct and created_ids:
            # update a random existing
            doc_id = random.choice(created_ids)
            patch = {"last_activity": int(time.time() * 1000), "device_logs": [{"ts": int(time.time() * 1000), "evt": "heartbeat"}]}
            update_document(redis_client, cfg, entity, doc_id, patch)
            logger.debug("Dummy updated %s", doc_id)
        else:
            # delete random
            if created_ids:
                doc_id = created_ids.pop(random.randrange(len(created_ids)))
                delete_document(redis_client, cfg, entity, doc_id)
                logger.debug("Dummy deleted %s", doc_id)

        # slight pause to emulate traffic
        time.sleep(random.uniform(0.01, 0.1))


# ---------------------------
# Periodic consumer loop
# ---------------------------
def consume_periodically(redis_client: redis.Redis, pg_conn, cfg: Dict[str, Any], entity: str, stop_event: threading.Event):
    interval = int(cfg.get("write_behind", {}).get("interval_seconds", 10))
    logger.info("Starting periodic consumer for entity %s, interval %s seconds", entity, interval)
    while not stop_event.is_set():
        try:
            RedisWriteBehind(redis_client, pg_conn, cfg, entity)
        except Exception as e:
            logger.exception("Error in periodic RedisWriteBehind: %s", e)
        # sleep in small increments so stop_event is responsive
        for _ in range(interval):
            if stop_event.is_set():
                break
            time.sleep(1)
    logger.info("Stopped periodic consumer for entity %s", entity)


# ---------------------------
# Main
# ---------------------------
def main():
    cfg = load_config("/Users/suyog/Documents/GitHub/redis-streams-to-rdbms-write-behind/configs/configs.yaml")
    redis_client = connect_redis(cfg)
    pg_conn = connect_postgres(cfg)

    entity = "session"  # can be config-driven

    # Create Postgres table if not exists (simple schema)
    try:
        cur = pg_conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                payload JSONB,
                created_at TIMESTAMPTZ DEFAULT now()
            )
            """
        )
        cur.close()
    except Exception as e:
        logger.exception("Error creating sessions table: %s", e)
        return

    # Create search index if configured
    logger.info("Creating search index")
    create_search_index(redis_client, cfg, entity)
    logger.info("Starting dummy searches")
    random_search_queries(redis_client, cfg, entity, 5)
    # Start periodic consumer in background thread
    stop_event = threading.Event()
    consumer_thread = threading.Thread(target=consume_periodically, args=(redis_client, pg_conn, cfg, entity, stop_event), daemon=True)
    consumer_thread.start()

    # Run dummy data generator in main thread (or could be separate)
    try:
        logger.info("Starting dummy data generation")
        dummy_data_worker(redis_client, cfg, entity)
        logger.info("Dummy data generation complete")
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        # Allow one last pass for consumer
        logger.info("Waiting briefly for consumer to process...")
        time.sleep(2)
        stop_event.set()
        consumer_thread.join(timeout=10)
        pg_conn.close()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    main()
