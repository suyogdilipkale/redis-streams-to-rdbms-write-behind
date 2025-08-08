import redis
import json
import time
import random
import yaml
import logging
import mysql.connector
from datetime import datetime
from redis.exceptions import ResponseError

# --- Logging Setup ---
logger = logging.getLogger("RedisWriteBehind")
logger.setLevel(logging.DEBUG)

# Console handler
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

# --- Redis Client ---
def get_redis_client(redis_config):
    return redis.Redis(
        host=redis_config['host'],
        port=redis_config['port'],
        db=redis_config.get('db', 0),
        decode_responses=True
    )

# --- MySQL Client ---
def get_mysql_connection(mysql_config):
    return mysql.connector.connect(
        host=mysql_config['host'],
        port=mysql_config.get('port', 3306),
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )

# --- Insert JSON + Stream Entry ---
def insert_document(config, entity_type, key, data, zero_loss=False, debug=False):
    redis_client = get_redis_client(config['redis'])
    stream_key = config['entity_streams'][entity_type]['stream']
    retries = config.get('retry_attempts', 3)

    for attempt in range(retries):
        try:
            # Insert JSON document
            redis_client.json().set(key, '$', data)

            # WAIT/AOF commands for zero-loss if enabled
            if zero_loss:
                if debug:
                    logger.debug(f"[{entity_type}] Using WAITAOF and WAIT for durability")
                redis_client.execute_command("WAITAOF", 1, 1000)
                redis_client.execute_command("WAIT", 1, 1000)

            # Add key to Redis Stream
            redis_client.xadd(stream_key, {'key': key, 'entity': entity_type})

            # Log success
            if debug:
                logger.debug(f"Inserted key {key} into Redis and stream {stream_key}")
            redis_client.incr(f"metrics:{entity_type}:redis:success")
            return

        except Exception as e:
            logger.warning(f"[Attempt {attempt+1}] Failed to insert {key}: {e}")
            redis_client.incr(f"metrics:{entity_type}:redis:fail")
            time.sleep(1)

    # Final failure
    if debug:
        logger.error(f"Failed to insert {key} after {retries} attempts")

# --- Redis Write-Behind Class ---
class RedisWriteBehind:
    def __init__(self, config):
        self.config = config
        self.redis = get_redis_client(config['redis'])
        self.rdbms_type = config['rdbms']['type']
        self.retry_attempts = config.get('retry_attempts', 3)
        self.debug = config.get('debug_logs', False)

    def _transform_data(self, entity_type, json_data):
        # Placeholder transform
        transform = self.config['entity_streams'][entity_type].get('transform')
        return json_data if transform is None else json_data  # apply your transformation logic here

    def _write_to_rdbms(self, entity_type, records):
        rdbms_cfg = self.config['rdbms']
        conn = get_mysql_connection(rdbms_cfg)
        cursor = conn.cursor()

        for record in records:
            try:
                key = record['key']
                json_data = self.redis.json().get(key)
                transformed = self._transform_data(entity_type, json_data)

                # For demo, assume table user_actions(user_id, action, ts)
                cursor.execute(
                    "INSERT INTO user_actions (user_id, action, ts) VALUES (%s, %s, %s)",
                    (transformed['user_id'], transformed['action'], transformed['timestamp'])
                )
                self.redis.incr(f"metrics:{entity_type}:rdbms:success")

            except Exception as e:
                self.redis.incr(f"metrics:{entity_type}:rdbms:fail")
                if self.debug:
                    logger.warning(f"Failed to write key {key} to RDBMS: {e}")

        conn.commit()
        cursor.close()
        conn.close()

    def process_stream(self, entity_type):
        stream_key = self.config['entity_streams'][entity_type]['stream']
        count = self.config['entity_streams'][entity_type].get('batch_size', 10)
        last_id = '0'

        while True:
            try:
                entries = self.redis.xrange(stream_key, min=last_id, count=count)
                if not entries:
                    break

                records = []
                for entry_id, data in entries:
                    last_id = entry_id
                    records.append({k: v for k, v in data.items()})

                self._write_to_rdbms(entity_type, records)

            except Exception as e:
                logger.error(f"Error processing stream {stream_key}: {e}")
                break

# --- Dummy Inserter ---
def insert_dummy_user_actions(config, num_records=10):
    import uuid
    for _ in range(num_records):
        key = f"user_action:{datetime.utcnow().isoformat()}:{uuid.uuid4()}"
        doc = {
            "user_id": f"user{random.randint(1, 5)}",
            "action": random.choice(["login", "logout", "purchase", "view"]),
            "timestamp": datetime.utcnow().isoformat()
        }
        insert_document(config, "user_action", key, doc, zero_loss=True, debug=True)

# --- Load Metrics from Redis ---
def load_metrics(config):
    redis_client = get_redis_client(config['redis'])
    keys = redis_client.keys("metrics:*")
    for key in keys:
        print(f"{key}: {redis_client.get(key)}")
