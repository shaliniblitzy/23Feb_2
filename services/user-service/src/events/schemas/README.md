# User Service · Event Schemas

JSON Schema definitions for the Kafka events that the User Service consumes (`user.registered`) and
produces (`user.updated`, `user.deleted`). The schemas are registered with the Schema Registry
**out-of-band by the deployment pipeline** (`infrastructure/kafka/schemas/` is the canonical source
of truth); this folder holds **byte-identical local copies** so the service is self-documenting and
so unit tests can validate Pydantic models offline.

## Files

| File | Subject (Schema Registry) | Direction | Topic | Description |
|------|---------------------------|-----------|-------|-------------|
| [`user.registered.schema.json`](./user.registered.schema.json) | `user.registered-value` | **Consumed** | `user.registered` | Auth Service registration event; idempotently provisions the User aggregate keyed on `external_auth_id`. |
| [`user.updated.schema.json`](./user.updated.schema.json) | `user.updated-value` | **Produced** | `user.updated` | Profile, preferences, or address change; downstream consumers (Notification Service, Recommendation Engine) refresh their projections. |
| [`user.deleted.schema.json`](./user.deleted.schema.json) | `user.deleted-value` | **Produced** | `user.deleted` | Soft-delete of a user (typically GDPR right-to-be-forgotten); downstream consumers suppress further sends and purge derived state. |

## Mirroring Policy

These files are **byte-identical mirrors** (modulo formatting) of the canonical schemas in
[`infrastructure/kafka/schemas/`](../../../../../infrastructure/kafka/schemas/). The canonical
schemas are registered with the Schema Registry **out-of-band by the deployment pipeline** — never
at runtime by the service. The User Service holds local copies for two reasons:

1. **Self-documenting** — anyone reading `services/user-service/` can understand the wire contracts
   without leaving the folder.
2. **Offline unit-test validation** — `tests/unit/events/` references these local files to validate
   Pydantic domain-event payloads without a running Schema Registry.

If a schema in this folder ever drifts from its canonical counterpart, the deployment pipeline's
pre-flight check fails the build. To evolve a schema, edit the canonical file in
`infrastructure/kafka/schemas/` and copy the result here in the same commit.

## Subject Naming

Per AAP R-30 and the platform convention, every Kafka topic carries a value subject named
`<topic>-value`:

| Topic | Subject |
|-------|---------|
| `user.registered` | `user.registered-value` |
| `user.updated`    | `user.updated-value`    |
| `user.deleted`    | `user.deleted-value`    |

The same subject is reused for the primary topic and its `<topic>.retry` and `<topic>.dlq` siblings
— retry and DLQ messages MUST remain re-deserializable with the same schema. Because the User
Service uses plain string keys (the `user_id`), no key subject is registered; only the
`<topic>-value` subjects above exist.

## Schema Evolution (AAP R-31)

Every schema includes a top-level `version` field (integer ≥ 1, default 1). Backward-compatible
evolution is enforced by Schema Registry's `BACKWARD` compatibility rule, applied by the deployment
pipeline. To evolve a schema:

1. Add new fields as **optional** — do NOT append to `required`.
2. Keep `additionalProperties: false` so unexpected producer fields don't break older consumers
   (the Pydantic side uses `extra="ignore"` to cover the symmetric case on the consumer).
3. Bump the registered subject's Schema Registry version when registering the new shape; bump the
   in-payload `version` field only when the producer starts emitting the new shape.
4. NEVER remove or rename a required field. NEVER change a field's type or its constraints in a way
   that rejects previously-valid payloads.

## Runtime Configuration

The producer ([`../producer.py`](../producer.py)) and consumer ([`../consumer.py`](../consumer.py))
configure the Confluent `JSONSerializer` / `JSONDeserializer` with:

```yaml
schema.registry.url: ${SCHEMA_REGISTRY_URL}
auto.register.schemas: false   # AAP R-14 — schemas must already be registered
use.latest.version: true       # use the latest backward-compatible version
```

This guarantees that schema management is centralized (in the deployment pipeline + Schema
Registry) and that the runtime path always validates against the registered schema, never against
these local files.

## Cross-References

- **Canonical source of truth:** [`infrastructure/kafka/schemas/`](../../../../../infrastructure/kafka/schemas/)
  — schemas registered with Schema Registry by the deployment pipeline.
- **Topic catalog:** [`infrastructure/kafka/topics.yaml`](../../../../../infrastructure/kafka/topics.yaml)
  — declares the `user.registered`, `user.updated`, `user.deleted` topics plus their `.retry` and
  `.dlq` siblings.
- **Producer:** [`../producer.py`](../producer.py) — uses `JSONSerializer` to validate every
  produced event against `user.updated-value` / `user.deleted-value`.
- **Consumer:** [`../consumer.py`](../consumer.py) — uses `JSONDeserializer` to validate every
  consumed `user.registered` event against `user.registered-value`.
- **Domain events (Pydantic):** [`../../domain/events/`](../../domain/events/) — application-layer
  Pydantic models that mirror these JSON Schema payloads.
- **AAP Section 0.4.4** — Database/schema updates per service.
- **AAP Section 0.5.2.3** — Messaging backbone (Kafka).
- **AAP R-14** — Schema Registry validation mandatory.
- **AAP R-30** — Topic names mirror event names.
- **AAP R-31** — Versioning for backward-compatible evolution.
- **AAP R-32** — Producers don't know consumers.
- **AAP R-33** — Events are self-contained.
