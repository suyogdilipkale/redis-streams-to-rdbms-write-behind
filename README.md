# Redis Write-Behind to RDBMS Pipeline

## Overview

This project demonstrates a Redis-based write-behind caching mechanism that:
- Inserts JSON data into Redis with zero data loss guarantee (optional).
- Queues keys to Redis Streams per entity type.
- Pulls from Redis Streams and writes to an RDBMS using batch processing.

## Business Objectives

- Enable zero data loss ingestion to Redis.
- Use Redis Streams to track data changes asynchronously.
- Transform and load data into RDBMS systems (PostgreSQL, MySQL, Oracle).
- Maintain logs and metrics of pipeline activities.

## Use Cases

- Event logging pipelines
- User activity tracking
- Real-time caching and durable storage
- Async ingestion from microservices

## Functions

### insert_to_redis
Inserts a JSON object into Redis and Redis Streams, supports WAIT/WAITAOF for persistence.

### RedisWriteBehind
Reads batch entries from Redis Streams and writes them to RDBMS.

### insert_dummy_user_actions
Generates random user action events and inserts them.

### log_metrics
Logs current success/failure metrics stored in Redis.

## YAML Configurations

- `redis`: Connection details.
- `app.instance_id`: Used for generating unique keys.
- `streams`: Defines stream names and read settings.
- `log`: Logging enable flag and stream names for tracking events.

## Running the Project

1. Start Redis server.
2. Update `configs/config.yaml` as needed.
3. Run `data_pipeline_demo.ipynb` in Jupyter.