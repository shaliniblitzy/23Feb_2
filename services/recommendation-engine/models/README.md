# Recommendation Engine — Model Artifacts

Serialized machine-learning model artifacts loaded by the Recommendation Engine inference runtime at service startup.

## Purpose

This directory holds the **deployable model artifact and its metadata** — not training code,
training datasets, or training pipelines (those are explicitly out of scope per AAP Section 0.6.2
and live in a separate repository).
[`../src/inference/model_loader.py`](../src/inference/model_loader.py) deserializes these files
at startup and exposes them through an in-memory adapter. The runtime reads the directory
identified by `MODEL_PATH` (default `./models/latest`, see
[`../.env.example`](../.env.example)). The service [`../Dockerfile`](../Dockerfile) bakes this
directory into the image via `COPY --chown=${APP_USER}:${APP_USER} models/ ./models/`. Operators
who need to override the baked-in artifact use a Kubernetes volume mount or an `initContainer`
that fetches from object storage — see
[Fetching the Real Artifact](#fetching-the-real-artifact-operator-runbook).

## Directory Layout

```
models/
├── README.md                     # This file
├── latest/                       # Currently-promoted production model (symbolic convention)
│   ├── model.joblib              # Main serialized model (sklearn NearestNeighbors + metadata)
│   ├── embeddings_matrix.npz     # Precomputed embedding matrix (cold-start fallback reference)
│   ├── vocabulary.json           # Feature-to-index mapping (TF-IDF / categorical encoding)
│   ├── model_metadata.json       # Version, training date, dim, top_k default, provenance
│   └── CHECKSUMS                 # SHA-256 of every file in this directory
├── .gitattributes                # LFS declarations for *.joblib, *.npz, *.pt, *.pb, *.onnx, *.h5
└── .gitkeep                      # Ensures empty directory is committed before first model
```

`latest/` is the canonical mount point pointed at by `MODEL_PATH=./models/latest`. Older versions
are kept as sibling directories (`v1/`, `v2/`, `v2025-01-15/`, ...) so a rollback is a one-line
env-var change — see [Versioning and Promotion](#versioning-and-promotion).

## Artifact Format

- **Default framework:** scikit-learn — pinned in [`../requirements.txt`](../requirements.txt) at
  `scikit-learn>=1.4.0,<2.0.0` alongside `joblib>=1.3.0,<2.0.0` and `numpy>=1.26.0,<2.0.0`.
- **Serialization format:** `joblib.dump(...)` output — a compressed pickle optimized for NumPy
  arrays. Loaded via `joblib.load(...)`.
- **Expected in-memory structure:** a `dict` of shape
  `{"model": <sklearn estimator>, "dim": <int>, "version": <str>}`. The estimator is typically a
  `sklearn.neighbors.NearestNeighbors` instance with `metric="cosine"`.
- **Loader interface contract** — the estimator must support one of:
  - `model.predict(user_embedding: np.ndarray, top_k: int) -> list[tuple[uuid.UUID, float]]`, **or**
  - `model.kneighbors(query: np.ndarray, n_neighbors: int) -> (distances, indices)` — the
    `NearestNeighbors`-compatible signature.
- **Framework-agnostic extension** — the same folder layout holds non-scikit-learn artifacts;
  only the loader adapter changes:

| Framework | Primary artifact | Loader entry point |
|-----------|------------------|--------------------|
| scikit-learn (default) | `model.joblib` | `joblib.load(...)` |
| TensorFlow SavedModel | `saved_model.pb` + `variables/` | `tf.saved_model.load(...)` |
| PyTorch | `*.pt` | `torch.load(...)` |
| ONNX | `*.onnx` | `onnxruntime.InferenceSession(...)` |
| Keras / H5 | `*.h5` | `keras.models.load_model(...)` |

## Model Metadata

`model_metadata.json` is the manifest read by the loader before the model is deserialized.

```json
{
  "version": "1.0.0",
  "trained_at": "2025-01-01T00:00:00Z",
  "framework": "scikit-learn",
  "framework_version": "1.4.0",
  "embedding_dim": 128,
  "top_k_default": 20,
  "metric": "cosine",
  "training_dataset_hash": "sha256:...",
  "checksum": "sha256:...",
  "notes": "Baseline model; TF-IDF + NearestNeighbors."
}
```

Required fields:

- `version` — semantic version string (e.g., `1.0.0`).
- `trained_at` — ISO-8601 / RFC 3339 UTC timestamp.
- `framework` — one of `scikit-learn` | `tensorflow` | `pytorch` | `onnx`.
- `framework_version` — version of the training framework, used for compatibility checks.
- `embedding_dim` — embedding dimensionality. **MUST** match all locations in
  [Dimension Coupling](#dimension-coupling). Project-wide canonical value: **128**.
- `top_k_default` — default `top_k` when the inference caller omits it (project default: **20**,
  matches `MODEL_TOP_K=20` in [`../.env.example`](../.env.example)).
- `metric` — `cosine` | `l2` | `dot_product`. Must be pgvector-compatible.
- `training_dataset_hash` — SHA-256 of the training dataset manifest (optional if the external
  pipeline manages provenance separately).
- `checksum` — SHA-256 of `model.joblib` for integrity verification on load.
- `notes` — free-text provenance note.

## Dimension Coupling

`embedding_dim` is **byte-for-byte coupled** across these five locations — all must stay in sync:

1. [`../config/default.yaml`](../config/default.yaml) — key `database.vector.embedding_dim`.
2. [`../config/default.yaml`](../config/default.yaml) — key `model.embedding_dim`.
3. [`../migrations/V002__create_embeddings_table.sql`](../migrations/V002__create_embeddings_table.sql)
   — the `vector(N)` column on the `embeddings` table.
4. [`../.env.example`](../.env.example) — `MODEL_EMBEDDING_DIM` default.
5. The example JSON above (`"embedding_dim": 128`).

**Canonical project-wide value: `128`.** Changing it requires an atomic five-file edit plus a
migration that re-creates the `embeddings` table at the new dimension (pgvector column dimensions
are immutable after creation). Two operator-level approaches:

- **Static (current).** Hard-code `128` in every location — explicit and immune to runtime drift,
  but every change is a coordinated five-file edit. Chosen approach for the greenfield build.
- **Dynamic (future).** Read `embedding_dim` from `model_metadata.json` at startup and validate
  it against the live `vector(N)` column type, refusing to start on mismatch. Documented as a
  future improvement; **out of scope** for the greenfield build.

## Checksum Verification

`CHECKSUMS` is in standard `sha256sum` output format — one line per artifact, two spaces between
hash and filename:

```
<sha256-hex>  model.joblib
<sha256-hex>  embeddings_matrix.npz
<sha256-hex>  vocabulary.json
<sha256-hex>  model_metadata.json
```

Generate (run from the artifact directory):

```bash
cd latest/
sha256sum model.joblib embeddings_matrix.npz vocabulary.json model_metadata.json > CHECKSUMS
```

Verify (returns non-zero on any mismatch):

```bash
cd latest/
sha256sum -c CHECKSUMS
```

At service startup, [`../src/inference/model_loader.py`](../src/inference/model_loader.py)
recomputes each SHA-256 and compares it against `CHECKSUMS`. Any mismatch is a fatal startup
failure: the process exits non-zero and `/health/ready` returns 503 (per AAP R-19).

## Versioning and Promotion

- `latest/` is a **convention** — not a filesystem symlink. The currently-promoted model's files
  physically live there.
- New models are introduced as **sibling** directories (`v1/`, `v2/`, `v2025-01-15/`, ...).
- Promotion is one of:
  1. Copy the new version's files over `latest/` and regenerate `CHECKSUMS` — keeps
     `MODEL_PATH=./models/latest` stable; suitable for image-rebuild flows.
  2. Change `MODEL_PATH` in the deployment manifest (e.g., `MODEL_PATH=/app/models/v2`) and
     trigger a rolling restart — preferred zero-downtime path in Kubernetes.
- Keep at least **2 previous versions** on disk (per
  [`../config/default.yaml`](../config/default.yaml) `model.versions_to_keep: 2`) so rollback is
  a one-line `kubectl set env`.

## Rollback Procedure

1. Identify the last known-good version (e.g., `v2/`); inspect `models/v2/model_metadata.json`
   to confirm framework, version, and `trained_at`.
2. **Option A — env-var rollback (zero-downtime, preferred):**

   ```bash
   kubectl set env deployment/recommendation-engine MODEL_PATH=/app/models/v2
   # Readiness probes flap; traffic drains to the new path during the rolling update.
   ```

3. **Option B — file-system rollback (requires image rebuild):**

   ```bash
   cp -r models/v2/* models/latest/
   cd models/latest && sha256sum model.joblib embeddings_matrix.npz vocabulary.json model_metadata.json > CHECKSUMS
   # Rebuild image and redeploy.
   ```

Option A is preferred whenever the previous version is still present on the running image.

## Hot Reload (Future)

A planned-but-disabled-by-default capability allows in-place model swaps:
[`../config/default.yaml`](../config/default.yaml) exposes `model.reload_endpoint_enabled: false`
(the safe default). When enabled, the service exposes `POST /admin/model/reload`, which re-runs
the loader pipeline (checksum verification, metadata validation, warmup inference) and atomically
swaps the in-memory model behind a write lock. Protected by:

- a JWT bearing an admin scope (per AAP R-21 — Auth Service is the sole token issuer);
- a Kubernetes `NetworkPolicy` allow-list that restricts ingress to internal namespaces;
- a per-endpoint rate limit of 1 request per minute.

This is a **future enhancement**; **not** implemented in the greenfield build. The current
promotion path is `kubectl set env` + rolling restart.

## Fetching the Real Artifact (Operator Runbook)

For environments that do not track binaries in Git LFS, fetch the artifact at deploy time. Three
supported workflows:

- **Option A — object storage (preferred for production):**

  ```bash
  aws s3 cp s3://<bucket>/models/recommendation-engine/latest/ ./models/latest/ --recursive
  # or
  gcloud storage cp gs://<bucket>/models/recommendation-engine/latest/ ./models/latest/ --recursive
  ```

- **Option B — Git LFS (for repos that already use LFS):** the sibling `.gitattributes` declares
  `*.joblib`, `*.npz`, `*.pt`, `*.pb`, `*.onnx`, `*.h5` as LFS-tracked.

  ```bash
  git lfs install
  git lfs pull
  ```

- **Option C — HTTP download in a Kubernetes initContainer:** an init container runs `curl` /
  `wget` against an internal artifact registry, places files under `/app/models/latest/`, and the
  main container starts after the init completes. Simplest pattern when no cloud-provider
  credentials are available.

For the **greenfield initial commit**, `latest/` may ship as zero-byte placeholder files plus
`.gitkeep`. Operators **must** fetch the real artifact via one of the workflows above before
starting the service in any non-test environment. Integration tests at
[`../tests/integration/test_recommendation_api.py`](../tests/integration/test_recommendation_api.py)
mock the model loader, so CI does not require a real binary.

## Fail-Fast Validation (AAP R-19)

Per AAP R-19, the loader fails fast on any of the following — process exits non-zero and
`/health/ready` returns 503 until a valid artifact is loaded:

- `MODEL_PATH` directory does not exist.
- `model.joblib` is missing **or** zero bytes (unless the test-mode mock flag is set).
- `model_metadata.json` is missing, malformed JSON, or missing any required field listed in
  [Model Metadata](#model-metadata).
- `CHECKSUMS` is missing **or** any listed file's recomputed SHA-256 does not match.
- The metadata `embedding_dim` does not match the loaded estimator's actual dimension.
- The metadata `framework` does not correspond to a loader adapter compiled into the image.

Probe semantics — `/health/ready` returns 503 until the artifact has loaded **and** one synthetic
warmup inference has completed within the configured latency budget; `/health/live` is
independent of model state and returns 200 while the process is responsive (so Kubernetes does
not kill a pod that is merely warming up).

## Seed Model Generation

The following snippet produces a placeholder seed `model.joblib` for local development and
startup-validation tests. **Not a production model** — it is fitted on random vectors.

```python
"""
Minimal seed model for local development and startup validation.
NOT a production model — replace with a properly trained artifact.
"""
import joblib
import numpy as np
from sklearn.neighbors import NearestNeighbors

# Seed: 100 random 128-dim embeddings — matches project-wide embedding_dim = 128
np.random.seed(42)
X = np.random.randn(100, 128).astype(np.float32)

model = NearestNeighbors(n_neighbors=20, metric="cosine")
model.fit(X)

joblib.dump(
    {"model": model, "dim": 128, "version": "1.0.0-seed"},
    "latest/model.joblib",
)
```

The recommended default for the **initial commit** is to ship zero-byte placeholder files plus
`.gitkeep` and rely on the documented fetch procedure above; integration tests mock the loader.

## Git LFS Configuration

The sibling `.gitattributes` declares `*.joblib`, `*.npz`, `*.pt`, `*.pb`, `*.onnx`, `*.h5` as
LFS-tracked. Initial enablement (run once at the repository level):

```bash
git lfs install
git lfs migrate import --include="*.joblib,*.npz,*.pt,*.pb,*.onnx,*.h5"
```

If operators prefer **not** to use LFS, gitignore these extensions instead and use the
object-storage fetch pattern from
[Fetching the Real Artifact](#fetching-the-real-artifact-operator-runbook). CI does not block on
missing binaries because integration tests mock the loader.

## Out-of-Scope Clarifications

Per AAP Section 0.6.2, the following are **explicitly out of scope** for this directory:

- **Training code** — lives in a separate repository / pipeline.
- **Training datasets** — live in a data lake or feature store.
- **ML experiment tracking artifacts** — MLflow runs, Weights & Biases artifacts.
- **Evaluation notebooks** — Jupyter notebooks demonstrating model performance.
- **Feature store infrastructure** — Feast, Tecton, or similar.

Only the **deployable artifact, its metadata, and integrity checksums** live here.

## Cross-References

- [`../README.md`](../README.md) — Service-level README.
- [`../requirements.txt`](../requirements.txt) — Python dependency manifest (scikit-learn,
  joblib, numpy).
- [`../Dockerfile`](../Dockerfile) — Container image definition that copies this directory.
- [`../.env.example`](../.env.example) — `MODEL_PATH`, `MODEL_EMBEDDING_DIM`, `MODEL_TOP_K`,
  `MODEL_VERSION`, `MODEL_WARMUP_ON_STARTUP`.
- [`../config/default.yaml`](../config/default.yaml) — `model.*` and `database.vector.*`
  configuration keys.
- [`../migrations/V002__create_embeddings_table.sql`](../migrations/V002__create_embeddings_table.sql)
  — `vector(N)` column dimension coupling.
- [`../src/inference/model_loader.py`](../src/inference/model_loader.py) — Deserializer
  (validates checksums, parses metadata, runs warmup, gates the readiness probe).
- [`../../../docs/architecture/data-stores.md`](../../../docs/architecture/data-stores.md) —
  Polyglot persistence rationale (pgvector + Redis).
- [`../../../docs/architecture/system-diagram.md`](../../../docs/architecture/system-diagram.md)
  — Canonical complex Mermaid architecture diagram.

## Security Posture

- This directory **MUST NOT** contain secrets, credentials, API keys, customer PII, or any other
  sensitive data (per AAP R-25).
- Model weights are intellectual property but **not** secrets in the AAP's sense — they are
  deployable artifacts and follow the standard artifact-distribution workflow (Git LFS or object
  storage).
- Integrity of `model.joblib` is protected by `CHECKSUMS` plus the fail-fast loader behavior in
  [Fail-Fast Validation](#fail-fast-validation-aap-r-19). Chain-of-custody is the operator's
  responsibility; high-assurance environments should verify the artifact against a cosign /
  notation-signed bundle (or equivalent supply-chain attestation) before placing it here.
