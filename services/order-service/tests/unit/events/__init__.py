"""Kafka event layer unit tests — topics, schemas, producer, consumer, handlers.

Covers ``src/events/{topics, schemas, producer, consumer, handlers}``.
All tests are hermetic — Confluent Kafka and Schema Registry are mocked.
"""

from __future__ import annotations
