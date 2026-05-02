"""User Service — repository (PostgreSQL data access) package.

Implements the PostgreSQL data access layer for the User Service's
private ``user_db`` (AAP Section 0.4.4). This package is the SOLE
owner of raw SQL touching the five tables:

    * ``users``             — root user identity record
                               (id, external_auth_id UNIQUE, email UNIQUE,
                               status, version, created_at, updated_at,
                               deleted_at)
    * ``user_profiles``     — 1:1 profile attributes
                               (first_name, last_name, display_name, phone,
                               avatar_url, date_of_birth, gender, locale,
                               timezone)
    * ``user_preferences``  — 1:1 notification + regional preferences
                               (email_opt_in, sms_opt_in, marketing_opt_in,
                               preferred_language, preferred_currency,
                               quiet_hours_start, quiet_hours_end,
                               channel_overrides JSONB)
    * ``user_addresses``    — 1:N address book
                               (type, is_default, line1..postal_code,
                               country ISO-3166, phone, is_verified)
    * ``events_outbox``     — transactional outbox for atomic
                               write-then-publish of `user.updated` /
                               `user.deleted` Kafka events

Database-per-service (AAP R-6) is STRICTLY ENFORCED. No other service
reads from this database directly; ``external_auth_id``, ``user_id``,
and product/order IDs that may appear in upstream events are opaque
UUIDs with NO foreign-key constraints to other services' tables.

Submodule layout
----------------
This package is intentionally an EMPTY package marker per the user-service
folder spec. Consumers import submodules DIRECTLY using absolute paths::

    from src.repository.models import (
        UserRow, UserProfileRow, UserPreferencesRow,
        UserAddressRow, OutboxEventRow,
    )
    from src.repository.user_repository import UserRepository
    from src.repository.address_repository import AddressRepository
    from src.repository.outbox_repository import OutboxRepository

This convention matches ``src.container.build_container``'s import
style and avoids any side effects at package import time.

Implementation contract
-----------------------
All three repositories use **psycopg v3** with **psycopg_pool.AsyncConnectionPool**
(NOT SQLAlchemy ORM). The pool is constructed once in ``src.container.build_container``
and passed to each repository's ``__init__`` as the ``pool`` keyword:

    user_repository = UserRepository(pool=pg_pool)
    address_repository = AddressRepository(pool=pg_pool)
    outbox_repository = OutboxRepository(pool=pg_pool)

Write methods accept an optional ``conn: AsyncConnection`` parameter so
callers (the application services in ``src/services/``) can wrap a
domain write + an outbox row write in the SAME Postgres transaction —
this is the **transactional outbox pattern** that guarantees atomic
write-then-publish (AAP R-32 / R-33 reliable event delivery).

Authority
---------
- AAP Section 0.4.4 — 4-table schema for user_db + events_outbox
- AAP R-6  — Database-per-service (strictly enforced)
- AAP R-7  — Polyglot persistence (PostgreSQL for user_db)
- AAP R-9  — Migration scripts in ``../../migrations/``
- AAP R-13 — Correlation IDs propagated through every log line
- AAP R-19 — Fail-fast on missing dependencies (smoke test in pool factory)
- AAP R-22 — JWKS bounded TTL caching (consumed elsewhere; documented here for completeness)
- AAP R-25 — Secrets (DB URL/password) in environment variables only
- AAP R-26 — Structured JSON logs
"""
