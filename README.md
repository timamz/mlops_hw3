# Kafka-to-ClickHouse Fraud Pipeline

## Overview

This project demonstrates loading a fraud dataset into Kafka, sinking the stream into ClickHouse, and running the query that finds each state's highest-volume transaction category.

## Prerequisites

- Docker & Docker Compose (20+ or compatible)
- Python 3.8+ (use a virtual environment if desired)
- `kafka-python` (installed via `pip install -r requirements.txt`)
- Download train.csv from [here](https://www.kaggle.com/competitions/teta-ml-1-2025/data?select=train.csv) and put it in the root of this repo

## Getting started

1. Install the Python dependencies so the CSV publisher script can reach Kafka:

   ```bash
   python -m pip install -r requirements.txt
   ```

2. Start the required services:

   ```bash
   docker compose up -d
   ```

3. Create the Kafka table, MergeTree table, and the materialized view in ClickHouse before producing data (the SQL file lives on the host, so pipe it into the container's client):

   ```bash
   docker exec -i clickhouse clickhouse-client --multiquery < sql/create_tables.sql
   ```

4. Push the CSV into Kafka (see script header for other flags):

   ```bash
   python scripts/load_transactions_to_kafka.py train.csv
   ```

   - The script defaults to `localhost:9092` and the `transactions_topic` topic.
   - Use `--broker` and `--topic` flags to point at alternative endpoints.
   - If Kafka is still starting the script will keep retrying; tweak `--retries`/`--retry-wait` if needed.

5. Run the stored SQL query inside ClickHouse and persist the result as CSV:

   ```bash
   mkdir -p results
   docker exec -i clickhouse clickhouse-client --multiquery --format CSVWithNames < sql/top_category_per_state.sql > results/top_category_per_state.csv
   ```

   The command above generates `results/top_category_per_state.csv` straight from ClickHouse.

## Storage optimizations (point 4)

1. The MergeTree table is partitioned by `toYYYYMM(transaction_time)` and ordered by `(us_state, cat_id, transaction_time)` so queries that aggregate by state and category scan fewer parts.
2. High-cardinality strings (`cat_id`, `us_state`) use `LowCardinality` wrappers plus `ZSTD(3)` codecs for efficient dictionary compression and faster comparisons.
3. Numeric columns are typed (`Float64`, `UInt32`, `UInt8`) ahead of ingestion in the materialized view, so ClickHouse stores compact binary values from the start.
