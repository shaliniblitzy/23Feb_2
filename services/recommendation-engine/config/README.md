# Recommendation Engine — Configuration

Non-secret, declarative configuration for the Recommendation Engine service; loaded at startup by pydantic-settings and merged by environment.

## File Layout

```
config/
├── default.yaml        # Base defaults for ALL environments (loaded first)
├── local.yaml          # Local-development overrides (ENVIRONMENT=local)
├── log_config.json     # stdlib logging.dictConfig schema (JSON log output)
└── README.md           # This file
```

- `default.yaml` — the authoritative base layer. Every typed setting consumed by the service has a default here, including service identity, retry policy (AAP R-15), circuit-breaker thresholds (AAP R-16), startup readiness gates (AAP R-19), fallback behavior (AAP R-20), JWKS TTL cache (AAP R-22), and observability fields (AAP R-26).
- `local.yaml` — minimal overrides for laptop/dev: relaxed timeouts, verbose logs, a dev Kafka bootstrap address, and a stub-friendly fallback mode. Selected when `ENVIRONMENT=local`.
- `log_config.json` — Python `logging.dictConfig` schema. Defines a structured-JSON formatter so every log line ships the fields required by AAP R-26 (timestamp, level, service, correlation_id, message). The service entrypoint passes this file to `--log-config`.
- `README.md` — this navigation document.

## Precedence & Loading Order

Values are merged from lowest to highest precedence:

1. `default.yaml` — ALWAYS loaded first as the base layer.
2. `<ENVIRONMENT>.yaml` — loaded when the `ENVIRONMENT` env var is set (e.g., `local.yaml` is selected for `ENVIRONMENT=local`); deep-merged on top of `default.yaml`, overriding only the keys it declares.
3. Environment variables — applied LAST and override any YAML value. Any YAML key whose name ends in `_env` does not contain a literal value; instead it names the environment variable to resolve at runtime (for example, `base_url_env: PRODUCT_SERVICE_URL` means "read the `PRODUCT_SERVICE_URL` env var on startup").

One-line summary: **`default.yaml → <env>.yaml → environment variables (highest precedence)`**.

Loading and merging are implemented in [`../src/config/settings.py`](../src/config/settings.py) using `pydantic-settings`.

## Secrets Policy (AAP R-25)

**No secrets in YAML or JSON files in this folder.** This rule is non-negotiable per AAP R-25; any change that introduces a secret value into a tracked file in this folder must be reverted before merge.

The following classes of values are FORBIDDEN in any file under `config/`:

- No passwords (any `*_password` key value).
- No API tokens, client secrets, or private keys (any `*_token`, `*_secret`, `*_key` value).
- No database URLs with embedded credentials (e.g., `postgresql://user:pass@host/db`).
- No JWT signing keys — this service only validates JWTs via the Auth Service JWKS endpoint (AAP R-22) and never mints tokens.

When a setting MUST reference a secret, use the `_env` suffix convention so the value is resolved from the process environment at startup rather than persisted in source. For example:

```yaml
product_service_client:
  base_url_env: PRODUCT_SERVICE_URL    # resolved from env var, not stored here
```

The complete catalog of environment variable names lives in the sibling [`../.env.example`](../.env.example). In production, these env vars are sourced from Kubernetes Secrets or a Vault-backed secret manager; in local development, from a gitignored `.env` file.

## Cross-File Coupling Constraints

The embedding dimension `128` is referenced in **three places** that MUST agree exactly. A drift between any two will either crash the model loader or corrupt the pgvector index:

| Location | Key / Token |
|---|---|
| [`./default.yaml`](./default.yaml) | `database.vector.embedding_dim: 128` |
| [`./default.yaml`](./default.yaml) | `model.embedding_dim: 128` |
| [`../migrations/V002__create_embeddings_table.sql`](../migrations/V002__create_embeddings_table.sql) | `vector(128)` column type |

[`../.env.example`](../.env.example) additionally exposes `MODEL_EMBEDDING_DIM=128` as a runtime override; on any given deployment this env var must agree with the YAML value.

Changing the embedding dimension is a coordinated, breaking operation: edit all three sites, retrain or re-export the model with the new dimension, drop and rebuild the pgvector index, and roll the service forward only after the new index is populated.

## Adding a New Environment

To introduce a new deployment environment (e.g., `stage`):

1. Create `config/<env>.yaml` in this folder (e.g., `config/stage.yaml`).
2. Include ONLY the keys that differ from `default.yaml` — the loader applies deep-merge semantics, so unset keys inherit from the base layer.
3. Set the deployment's `ENVIRONMENT` env var to the new name (e.g., `ENVIRONMENT=stage`).

**Production caveat: `prod.yaml` is NOT committed to this repo.** Production values are delivered as a Kubernetes ConfigMap mounted at the service's runtime config path by the deployment manifest under [`../../../deploy/k8s/`](../../../deploy/k8s/). The ConfigMap template is maintained in a separate operator repository, not here, so that production tuning is decoupled from application source review.

## Validation

[`../src/config/settings.py`](../src/config/settings.py) mirrors every key declared in `default.yaml` as a typed Pydantic model. Malformed YAML, missing required keys, or values that fail type or range validation cause the service to fail fast on startup, satisfying AAP R-19.

Quick local sanity checks:

```bash
# Confirm default.yaml parses as YAML
python -c "import yaml; yaml.safe_load(open('default.yaml'))"

# Confirm local.yaml parses as YAML
python -c "import yaml; yaml.safe_load(open('local.yaml'))"

# Confirm log_config.json is valid JSON
python -c "import json; json.load(open('log_config.json'))"

# Confirm log_config.json is accepted by logging.dictConfig
python -c "import json, logging.config as lc; lc.dictConfig(json.load(open('log_config.json')))"
```

If any check exits non-zero, the service will refuse to start; fix the file before deploying.

## Related Files and References

- [`../.env.example`](../.env.example) — Full environment variable catalog (companion file).
- [`../migrations/V002__create_embeddings_table.sql`](../migrations/V002__create_embeddings_table.sql) — pgvector schema that must agree on `vector(128)`.
- [`../src/config/settings.py`](../src/config/settings.py) — pydantic-settings loader (reads files in this folder).
- [`../Dockerfile`](../Dockerfile) — runtime `CMD` references `config/log_config.json` via `--log-config`.
- [`../../../docs/architecture/system-diagram.md`](../../../docs/architecture/system-diagram.md) — Canonical architecture diagram (service context).
- [`../../../docs/architecture/service-catalog.md`](../../../docs/architecture/service-catalog.md) — Per-service responsibility matrix.

AAP citations:

- AAP R-15 — retry policy defaults.
- AAP R-16 — circuit breaker defaults.
- AAP R-19 — startup fail-fast, warmup.
- AAP R-20 — fallback configuration.
- AAP R-22 — JWKS TTL cache.
- AAP R-25 — no secrets in config (CRITICAL).
- AAP R-26 — structured JSON log fields.
