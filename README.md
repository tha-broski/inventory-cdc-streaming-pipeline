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

The pipeline currently processes three entities:

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

## Data Validation

Incoming events are validated before being written to the target database.

Validation includes cases such as:

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

DLQ messages contain information such as:

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

This allows temporary inconsistencies during processing while the system converges toward the correct final state. This is an example of eventual consistency.

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

## Project Structure

```text
inventory-cdc-streaming-pipeline/
├── config/
│   └── debezium-postgres-source-config.json
├── scripts/
│   ├── connect-source.ps1
│   ├── connect-target.ps1
│   └── run-stream.ps1
├── sql/
│   ├── source_schema.sql
│   └── target_schema.sql
├── src/
│   ├── processors/
│   │   ├── products.py
│   │   ├── warehouses.py
│   │   └── inventory.py
│   ├── sinks/
│   │   ├── postgres.py
│   │   └── kafka.py
│   ├── config.py
│   ├── schemas.py
│   ├── validation.py
│   ├── transformations.py
│   └── streaming_job.py
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

### 3. Connect to the source PostgreSQL database

```powershell
.\scripts\connect-source.ps1
```

### 4. Connect to the target PostgreSQL database

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
- snapshot processing
- create processing
- update processing
- delete processing
- PostgreSQL staging tables
- PostgreSQL upserts
- cascade delete handling
- record validation
- Kafka Dead Letter Queues
- Spark checkpointing
- Docker-based local environment
- eventual-consistency handling between related streams

Planned:

- automated validation tests
- Spark transformation tests
- micro-batch ordering tests
- integration tests
- monitoring and observability improvements

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
- eventual consistency
- Docker-based data infrastructure
