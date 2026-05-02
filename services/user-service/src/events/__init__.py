"""Kafka event integration package for the User Service.

Submodules:
    * :mod:`src.events.producer`            — :class:`EventProducer` that
      serializes payloads via Schema Registry and produces to Kafka.
    * :mod:`src.events.consumer`            — :class:`KafkaConsumerRunner`
      that polls ``user.registered`` (and retry siblings), routes failures
      to retry / DLQ topics, and dispatches to handlers.
    * :mod:`src.events.outbox_dispatcher`   — :class:`OutboxDispatcher`
      background worker that publishes ``events_outbox`` rows to Kafka,
      closing the loop on the transactional outbox pattern.
    * :mod:`src.events.correlation`         — Helpers that bridge HTTP
      contextvars ↔ Kafka ``correlation_id`` headers per AAP R-13.
    * ``src/events/schemas/``               — JSON-Schema definitions
      mirroring the canonical schemas in
      ``infrastructure/kafka/schemas/`` (registered with the Schema
      Registry out-of-band by the deployment pipeline).

Importers should import the SPECIFIC SUBMODULE they need (this package
exposes no shortcut symbols at the package level by design — importing
``src.events.producer`` does NOT pull in the consumer, dispatcher, or
schemas, keeping startup-time imports minimal and circular-import risk
zero).

This package marker is intentionally side-effect-free:

    * No logging configuration here (see ``src/observability/logger.py``).
    * No metric registration here (each submodule registers its own).
    * No Kafka client construction here (see ``src/container.py``).
    * No submodule imports — pull what you need where you need it.
"""

from __future__ import annotations

__all__: list[str] = []
