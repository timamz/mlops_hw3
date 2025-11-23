#!/usr/bin/env python3
"\"\"\"Send transaction records from CSV into a Kafka topic.\"\"\""

from __future__ import annotations

import argparse
import csv
import json
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


NUMERIC_CASTERS: Dict[str, Callable[[str], Any]] = {
    "amount": float,
    "lat": float,
    "lon": float,
    "merchant_lat": float,
    "merchant_lon": float,
    "population_city": int,
    "target": int,
}


def parse_value(raw: str, caster: Callable[[str], Any]) -> Optional[Any]:
    """Convert a CSV field to the expected numeric type when possible."""
    if raw is None:
        return None

    raw = raw.strip()
    if raw == "":
        return None

    try:
        return caster(raw)
    except ValueError:
        return None


def build_record(row: Dict[str, str]) -> Dict[str, Any]:
    """Transform a CSV row into the payload that will go to Kafka."""
    payload: Dict[str, Any] = {}
    for key, value in row.items():
        if key in NUMERIC_CASTERS:
            payload[key] = parse_value(value, NUMERIC_CASTERS[key])
        else:
            payload[key] = value.strip() if value is not None else None

    return payload


def create_producer(broker: str, attempts: int, wait_seconds: float) -> KafkaProducer:
    """Try to create a KafkaProducer with retries while the broker starts up."""
    last_error: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=[broker],
                value_serializer=lambda payload: json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            )
        except NoBrokersAvailable as exc:
            last_error = exc
            print(f"broker {broker} unavailable (attempt {attempt}/{attempts}); retrying in {wait_seconds}s")
            time.sleep(wait_seconds)

    raise SystemExit(f"Kafka broker {broker} unreachable after {attempts} attempts: {last_error}")


def load_transactions(
    file_path: Path,
    broker: str,
    topic: str,
    maximum: Optional[int],
    retries: int,
    retry_wait: float,
) -> int:
    """Stream records from CSV file into Kafka topic."""
    producer = create_producer(broker, retries, retry_wait)

    sent = 0
    try:
        with file_path.open("r", newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if maximum and sent >= maximum:
                    break
                record = build_record(row)
                producer.send(topic, value=record)
                sent += 1
                if sent % 250 == 0:
                    producer.flush()
                    print(f"sent {sent} records so far")
            producer.flush()
    finally:
        producer.close()

    return sent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish transactions CSV rows to Kafka.")
    parser.add_argument(
        "source",
        nargs="?",
        default="train.csv",
        help="CSV file with transaction records (default: train.csv)",
    )
    parser.add_argument(
        "--broker",
        "-b",
        default="localhost:9092",
        help="Address of the Kafka broker (default: localhost:9092)",
    )
    parser.add_argument(
        "--topic",
        "-t",
        default="transactions_topic",
        help="Kafka topic to produce into (default: transactions_topic)",
    )
    parser.add_argument(
        "--max-records",
        "-m",
        type=int,
        default=None,
        help="Optional cap on records to send (useful for initial smoke tests)",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=10,
        help="Number of times to retry connecting to Kafka before giving up (default: 10)",
    )
    parser.add_argument(
        "--retry-wait",
        type=float,
        default=3.0,
        help="Seconds to wait between connection retries (default: 3)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source_path = Path(args.source).expanduser()

    if not source_path.exists():
        raise SystemExit(f"{source_path} is not a readable file")

    print(f"pushing data from {source_path} -> {args.topic} on {args.broker}")
    total = load_transactions(
        source_path,
        args.broker,
        args.topic,
        args.max_records,
        args.retries,
        args.retry_wait,
    )
    print(f"finished sending {total} records")


if __name__ == "__main__":
    main()
