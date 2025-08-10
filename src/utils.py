# Required Imports
import redis
import yaml
import json
import time
import random
import logging
from datetime import datetime
from typing import Dict, Any
from redis.exceptions import ConnectionError
import mysql.connector
from mysql.connector import Error

# Setup Logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("RedisWriteBehind")

# Load Config
def load_config(path: str) -> Dict[str, Any]:
    with open(path, 'r') as f:
        return yaml.safe_load(f)

# Initialize Redis client from config
def init_redis_client(config: Dict[str, Any]) -> redis.Redis:
    redis_cfg = config['redis']
    return redis.Redis(
        host=redis_cfg['host'],
        port=redis_cfg['port'],
        db=redis_cfg.get('db', 0),
        username=redis_cfg.get('username'),
        password=redis_cfg.get('password'),
        decode_responses=redis_cfg.get('decode_responses', True)
    )

# Insert JSON Document & Queue Stream
def insert_document(r, entity: str, key: str, data: Dict, config: Dict, ensure_persistence: bool = False):
    metrics = config['metrics']
    try:
        r.json().set(key, '$', data)

        if ensure_persistence and config.get('redis_wait', {}).get('enabled', False):
            replicas = config['redis_wait']['replicas']
            timeout = config['redis_wait']['timeout_ms']
            if r.wait(replicas, timeout) < replicas:
                logger.warning(f"WAIT timeout for key {key}")
                raise ConnectionError("WAIT failed")
            if r.execute_command('WAITAOF', replicas, timeout) < replicas:
                logger.warning(f"WAITAOF timeout for key {key}")
                raise ConnectionError("WAITAOF failed")

        stream_key = f"{config['stream_prefixes'][entity]}:{entity}"
        r.xadd(stream_key, {'key': key, 'timestamp': str(datetime.utcnow())})
        r.xadd(metrics['insert_success_stream'], {'key': key, 'entity': entity, 'timestamp': str(datetime.utcnow())})
        r.incr(metrics['insert_success_counter'])

    except Exception as e:
        r.xadd(metrics['insert_failure_stream'], {'key': key, 'entity': entity, 'error': str(e), 'timestamp': str(datetime.utcnow())})
        r.incr(metrics['insert_failure_counter'])
        if config.get('debug', False):
            logger.error(f"Insert failed for key {key}: {e}")
        if ensure_persistence:
            retry_insert(r, entity, key, data, config)

# Retry Logic
def retry_insert(r, entity: str, key: str, data: Dict, config: Dict, max_attempts: int = 3):
    metrics = config['metrics']
    for attempt in range(1, max_attempts + 1):
        try:
            r.json().set(key, '$', data)
            r.xadd(metrics['insert_retry_stream'], {'key': key, 'attempt': attempt, 'timestamp': str(datetime.utcnow())})
            r.incr(metrics['insert_retry_counter'])
            return
        except Exception as e:
            if config.get('debug', False):
                logger.warning(f"Retry {attempt} failed for key {key}: {e}")
    r.xadd(metrics['insert_failure_stream'], {'key': key, 'entity': entity, 'error': 'Final Retry Failed', 'timestamp': str(datetime.utcnow())})
    r.incr(metrics['insert_failure_counter'])

# Write Behind Processor
def RedisWriteBehind(r, entity: str, config: Dict):
    metrics = config['metrics']
    stream_key = f"{config['stream_prefixes'][entity]}:{entity}"
    last_id = '0-0'
    batch_size = config['stream_read_batch']
    attempts = config['rdbms']['retry_attempts']

    while True:
        try:
            records = r.xrange(stream_key, min=last_id, count=batch_size)
            if not records:
                break
            for record_id, record in records:
                key = record['key']
                data = r.json().get(key)
                transformed = apply_transform(entity, data, config)
                success = write_to_rdbms(entity, transformed, config, attempts)
                if success:
                    r.xadd(metrics['rdbms_success_stream'], {'key': key, 'timestamp': str(datetime.utcnow())})
                    r.incr(metrics['rdbms_success_counter'])
                else:
                    r.xadd(metrics['rdbms_failure_stream'], {'key': key, 'timestamp': str(datetime.utcnow())})
                    r.incr(metrics['rdbms_failure_counter'])
            last_id = records[-1][0]
        except Exception as e:
            if config.get('debug', False):
                logger.error(f"WriteBehind failed for {entity}: {e}")
            break

# Transformation Logic
def apply_transform(entity: str, data: Dict, config: Dict) -> Dict:
    mappings = config['transformations'][entity]['mappings']
    return {tgt_key: data.get(src_key) for src_key, tgt_key in mappings.items()}

# Write to MySQL
def write_to_rdbms(entity: str, data: Dict, config: Dict, attempts: int) -> bool:
    rdbms_cfg = config['rdbms']
    transform_cfg = config['transformations'][entity]
    table = transform_cfg['table']
    columns = list(data.keys())
    values = list(data.values())
    insert_query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(values))})"

    for attempt in range(attempts):
        try:
            conn = mysql.connector.connect(
                host=rdbms_cfg['jdbc']['host'],
                port=rdbms_cfg['jdbc']['port'],
                user=rdbms_cfg['jdbc']['username'],
                password=rdbms_cfg['jdbc']['password'],
                database=rdbms_cfg['jdbc']['database']
            )
            cursor = conn.cursor()
            cursor.execute(insert_query, tuple(values))
            conn.commit()
            cursor.close()
            conn.close()
            return True
        except Error as e:
            if config.get('debug', False):
                logger.error(f"MySQL insert attempt {attempt+1} failed: {e}")
            time.sleep(1)
    return False

# Dummy Data Generator
def load_dummy_data(r, config):
    for i in range(10):
        entity = "user_action"
        timestamp = datetime.utcnow().isoformat()
        instance_id = random.randint(1000, 9999)
        key = f"user_action:{timestamp}:{instance_id}"
        data = {
            "user_id": random.randint(1, 100),
            "action": random.choice(["login", "logout", "view", "click"]),
            "timestamp": timestamp
        }
        insert_document(r, entity, key, data, config, ensure_persistence=config.get('redis_wait', {}).get('enabled', False))

# Load Metrics
def load_metrics(r, config):
    metric_keys = [
        config['metrics']['insert_success_counter'],
        config['metrics']['insert_failure_counter'],
        config['metrics']['insert_retry_counter'],
        config['metrics']['rdbms_success_counter'],
        config['metrics']['rdbms_failure_counter']
    ]
    return {key: r.get(key) or 0 for key in metric_keys}

# Main
def main():
    config = load_config("config.yaml")
    r = init_redis_client(config)
    load_dummy_data(r, config)
    RedisWriteBehind(r, "user_action", config)
    print(load_metrics(r, config))

if __name__ == "__main__":
    main()
