import redis
import yaml
import json
import random
import logging
import time
from datetime import datetime
from redis.exceptions import ResponseError

# Setup logger
logger = logging.getLogger("redis_pipeline")
logger.setLevel(logging.DEBUG)
console = logging.StreamHandler()
logger.addHandler(console)

def get_redis_connection(config):
    return redis.Redis(host=config['redis']['host'], port=config['redis']['port'], decode_responses=True)

def insert_to_redis(config, entity_type, data, ensure_persistence=False):
    r = get_redis_connection(config)
    key_name = f"{entity_type}:{datetime.utcnow().isoformat()}:{config['app']['instance_id']}"
    try:
        r.json().set(key_name, '$', data)
        r.xadd(config['streams'][entity_type]['stream_name'], {'key': key_name})
        if ensure_persistence:
            replicas = config['redis'].get('replicas', 1)
            if not r.wait(replicas, 5000):
                raise Exception("WAIT failed")
            if not r.execute_command('WAITAOF', '5', '1000'):
                raise Exception("WAITAOF failed")
        if config['log']['enabled']:
            r.xadd(config['log']['streams']['success_insert'], {'key': key_name, 'ts': datetime.utcnow().isoformat()})
            r.incr('metrics:success_inserts')
    except Exception as e:
        if config['log']['enabled']:
            r.xadd(config['log']['streams']['failed_insert'], {'key': key_name, 'error': str(e), 'ts': datetime.utcnow().isoformat()})
            r.incr('metrics:failed_inserts')
        if ensure_persistence:
            logger.error(f"Retrying insert due to error: {e}")
            insert_to_redis(config, entity_type, data, ensure_persistence=False)

def RedisWriteBehind(config):
    for entity, stream_cfg in config['streams'].items():
        r = get_redis_connection(config)
        stream_name = stream_cfg['stream_name']
        start_id = stream_cfg.get('start_id', '0-0')
        try:
            entries = r.xrange(stream_name, start=start_id, count=stream_cfg['batch_size'])
            for entry_id, entry in entries:
                key = entry['key']
                json_data = r.json().get(key)
                # Dummy transform and RDBMS write simulation
                print(f"Writing to DB: {json_data}")
                if config['log']['enabled']:
                    r.xadd(config['log']['streams']['success_rdbms'], {'key': key, 'ts': datetime.utcnow().isoformat()})
                    r.incr('metrics:success_rdbms')
        except Exception as e:
            logger.error(f"Failed to write to RDBMS: {e}")
            r.xadd(config['log']['streams']['failed_rdbms'], {'error': str(e), 'ts': datetime.utcnow().isoformat()})
            r.incr('metrics:failed_rdbms')

def insert_dummy_user_actions(config):
    actions = ["login", "logout", "view", "click", "purchase"]
    for _ in range(5):
        data = {
            "user_id": random.randint(1, 100),
            "action": random.choice(actions),
            "timestamp": datetime.utcnow().isoformat()
        }
        insert_to_redis(config, "user_action", data, ensure_persistence=True)

def log_metrics(config):
    r = get_redis_connection(config)
    print("Current Metrics:")
    for metric in ['success_inserts', 'failed_inserts', 'success_rdbms', 'failed_rdbms']:
        print(f"{metric}: {r.get(f'metrics:{metric}') or 0}")