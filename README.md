# Inventory CDC Streaming Pipeline

Real-time Change Data Capture pipeline built with PostgreSQL, Debezium, Apache Kafka and Apache Spark Structured Streaming.

The project replicates changes from an operational PostgreSQL database into a separate PostgreSQL target database while validating incoming records and routing invalid events to Kafka Dead Letter Queue topics.

## Architecture

```text
PostgreSQL Source
       |
       | WAL / Logical Replication
       v
    Debezium
       |
       v
     Kafka
       |
       v
Spark Structured Streaming
       |
       +--------------------+
       |                    |
       v                    v
PostgreSQL Target       Kafka DLQ
```

The pipeline processes three entities:

- `products`
- `warehouses`
- `inventory`

Each source table is captured by Debezium and published to its own Kafka topic.

## Tech Stack

- PostgreSQL 17
- Apache Kafka 4.3.1
- Debezium 3.6
- Apache Spark 4.1.3
- PySpark
- Python
- psycopg
- pytest
- Docker
- Docker Compose

## CDC Flow

PostgreSQL is configured for logical replication.

Debezium reads changes from PostgreSQL WAL and publishes them to Kafka topics:

```text
inventory.public.products
inventory.public.warehouses
inventory.public.inventory
```

Spark Structured Streaming consumes these topics, parses Debezium events, validates records and routes them either to PostgreSQL or to a Dead Letter Queue.

Supported Debezium operations:

```text
r - snapshot read
c - create
u - update
d - delete
```

## Kafka Source Processing

Kafka source handling is centralized in `src/sources/kafka.py`.

Each CDC stream follows the same processing path:

```text
Kafka value
    |
    v
CAST(value AS STRING)
    |
    v
raw_value
    |
    v
from_json(...)
    |
    v
before / after / op
```

The original Kafka payload is preserved as `raw_value` so invalid records can be written to the DLQ together with the exact message that caused the validation failure.

Kafka metadata is also preserved:

```text
topic
partition
offset
timestamp
```

## Data Validation

Incoming events are validated before being written to the target database.

Validation includes cases such as:

- malformed payload
- unsupported CDC operation
- missing entity identifier
- invalid product price
- invalid timestamp
- missing product identifier in inventory
- missing warehouse identifier in inventory
- missing inventory quantity
- negative inventory quantity

Invalid events are excluded from the PostgreSQL sink and sent to dedicated DLQ topics:

```text
inventory.products.dlq
inventory.warehouses.dlq
inventory.inventory.dlq
```

DLQ messages contain:

```text
raw_payload
source_topic
partition
offset
timestamp
error_type
error_message
```

This allows invalid records to be inspected without stopping the main streaming pipeline.

## PostgreSQL Sink

Valid records are processed using Spark `foreachBatch`.

For every micro-batch:

1. The latest event for each entity ID is selected.
2. Create, snapshot and update events are separated from delete events.
3. Delete events remove corresponding rows from the target table.
4. Upsert records are written to a staging table.
5. PostgreSQL `INSERT ... ON CONFLICT DO UPDATE` merges staging data into the final table.

The upsert operation makes repeated processing of the same final state idempotent.

PostgreSQL write logic is centralized in `src/sinks/postgres.py`.

The sink includes:

- staging-table writes
- parameterized delete operations
- safe SQL identifier handling with `psycopg.sql.Identifier`
- retry handling for transient PostgreSQL connection errors
- final upsert execution

## Retry Handling

Transient PostgreSQL connection errors are retried automatically.

The retry mechanism:

- retries only `psycopg.OperationalError`
- performs up to 3 attempts by default
- waits between attempts
- logs retry attempts
- re-raises the final exception if all attempts fail

Errors such as invalid SQL or constraint violations are not retried automatically because repeating the same operation would not resolve them.

## SQL Safety

Dynamic table and column names are handled with:

```python
psycopg.sql.Identifier
```

Values are passed separately through parameterized queries.

This keeps SQL identifiers and data values separate and avoids unsafe string interpolation.

## Logging

The project uses Python's built-in `logging` module.

Logs include:

- timestamp
- log level
- module name
- batch information
- delete counts
- PostgreSQL retry warnings and errors

Example:

```text
2026-09-18 20:36:25,893 | INFO | processors.products | Processing products batch 8
2026-09-18 20:36:30,042 | INFO | processors.products | Products batch 8 contains 1 deletes
2026-09-18 20:36:30,311 | INFO | processors.inventory | Inventory batch 6 contains 9 deletes
```

This also makes cascade-delete processing visible across independent streaming queries.

## Referential Integrity and Eventual Consistency

The source PostgreSQL database enforces foreign keys:

```text
inventory.product_id
    -> products.product_id

inventory.warehouse_id
    -> warehouses.warehouse_id
```

Both relationships use `ON DELETE CASCADE`.

Deleting a product or warehouse on the source therefore also removes related inventory rows.

Debezium captures those changes as separate CDC events. These events are processed by independent Kafka topics and independent Spark Streaming queries.

Because separate streams may be processed at different speeds, the target database intentionally does not enforce foreign keys between `inventory`, `products` and `warehouses`.

This allows temporary inconsistencies during processing while the system converges toward the correct final state.

This is an example of eventual consistency.

## Kafka Ordering Assumption

Kafka offsets are ordered only within a partition.

The current implementation uses the highest offset to choose the latest event for a given entity within a micro-batch.

This assumes that events for the same entity key remain in the same Kafka partition.

The current project uses one partition per CDC topic, so this assumption is satisfied.

## Kafka Persistence

Kafka data is stored in a Docker named volume.

A regular shutdown:

```powershell
docker compose down
```

preserves Kafka data.

Removing Docker volumes:

```powershell
docker compose down -v
```

removes Kafka history as well.

Spark checkpoints are stored locally in:

```text
./checkpoints
```

Checkpoint files are excluded from Git.

Each streaming query uses its own checkpoint directory:

```text
checkpoints/products
checkpoints/products-dlq
checkpoints/warehouses
checkpoints/warehouses-dlq
checkpoints/inventory
checkpoints/inventory-dlq
```

## Automated Tests

The project includes pytest-based Spark tests.

Tests currently cover:

- selecting the latest event by Kafka offset
- update/delete ordering within a micro-batch
- valid inventory records
- negative inventory quantity
- delete events using IDs from `before`
- invalid timestamps
- invalid CDC operations
- malformed payload routing
- invalid product prices
- warehouse validation

Tests run inside the Spark Docker environment.

Run them with:

```powershell
.\scripts\run-tests.ps1
```

## Project Structure

```text
inventory-cdc-streaming-pipeline/
├── config/
│   └── debezium-postgres-source-config.json
│
├── scripts/
│   ├── connect-source.ps1
│   ├── connect-target.ps1
│   ├── run-stream.ps1
│   └── run-tests.ps1
│
├── sql/
│   ├── source_schema.sql
│   └── target_schema.sql
│
├── src/
│   ├── processors/
│   │   ├── products.py
│   │   ├── warehouses.py
│   │   └── inventory.py
│   │
│   ├── sinks/
│   │   ├── postgres.py
│   │   └── kafka.py
│   │
│   ├── sources/
│   │   └── kafka.py
│   │
│   ├── config.py
│   ├── logger.py
│   ├── schemas.py
│   ├── validation.py
│   ├── transformations.py
│   └── streaming_job.py
│
├── tests/
│   ├── conftest.py
│   ├── test_transformations.py
│   └── test_validation.py
│
├── checkpoints/
├── Dockerfile.spark
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

## Running the Project

### 1. Start the infrastructure

```powershell
docker compose up -d
```

### 2. Run Spark Structured Streaming

```powershell
.\scripts\run-stream.ps1
```

### 3. Run tests

```powershell
.\scripts\run-tests.ps1
```

### 4. Connect to the source PostgreSQL database

```powershell
.\scripts\connect-source.ps1
```

### 5. Connect to the target PostgreSQL database

```powershell
.\scripts\connect-target.ps1
```

## Example CDC Scenario

Create or update a row in the source PostgreSQL database.

Debezium reads the WAL entry and publishes the change to Kafka.

Spark consumes the event, validates it and writes the new state to the target PostgreSQL database.

For a delete:

```text
DELETE on source
      |
      v
PostgreSQL WAL
      |
      v
Debezium op = d
      |
      v
Kafka
      |
      v
Spark Structured Streaming
      |
      v
DELETE on target
```

With `ON DELETE CASCADE`, deleting a product or warehouse can additionally produce delete events for related inventory rows.

For example, deleting one product can result in one `products` delete event and multiple `inventory` delete events processed by separate streaming queries.

## Current Status

Implemented:

- PostgreSQL logical replication
- Debezium CDC connector
- Kafka in KRaft mode
- persistent Kafka storage
- Spark Structured Streaming
- products CDC pipeline
- warehouses CDC pipeline
- inventory CDC pipeline
- snapshot, create, update and delete processing
- PostgreSQL staging tables and upserts
- cascade delete handling
- record validation
- malformed payload detection
- Kafka Dead Letter Queues
- Spark checkpointing
- Docker-based local environment
- eventual consistency handling
- transient PostgreSQL retry handling
- safe dynamic SQL identifier handling
- structured application logging
- automated Spark validation and transformation tests

## Purpose

The project was created as a practical Data Engineering portfolio project focused on understanding and implementing:

- Change Data Capture
- PostgreSQL logical replication
- event streaming
- Kafka
- Debezium
- Spark Structured Streaming
- micro-batch processing
- idempotent database writes
- data validation
- Dead Letter Queues
- checkpointing
- retry handling
- SQL safety
- logging
- automated testing
- eventual consistency
- Docker-based data infrastructure
