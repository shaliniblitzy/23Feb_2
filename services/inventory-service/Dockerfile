# syntax=docker/dockerfile:1.6
# ============================================================================
# Inventory Service — Dockerfile
# ----------------------------------------------------------------------------
# Stock reservation engine and warehouse-state owner for the e-commerce
# platform. Owns inventory_db (PostgreSQL); produces inventory.reserved /
# inventory.released / inventory.low-stock; consumes order.created /
# order.cancelled / order.fulfilled.
#
# Architecture diagram: docs/architecture/system-diagram.md
# AAP refs: 0.1.1 #5, 0.4.2, 0.4.4, 0.5.2.2 b5, 0.5.2.5, 0.6.1
# Pattern: Multi-stage builder/runtime with non-root user (UID/GID 10001)
#
# This Dockerfile is intentionally verbose with comments — Dockerfiles are
# read by operators under stress (incident response); the cost of an extra
# line of comment is negligible compared to the cost of an opaque image
# during a pager-driven debug session at 3 a.m.
#
# This Dockerfile depends on the following peer artifacts living in the
# inventory-service folder, all created by sibling sub-agents:
#   - requirements.txt                  (Python runtime + transitive deps)
#   - src/main.py                       (FastAPI `app` object — referenced by CMD)
#   - config/log_config.json            (uvicorn structured-JSON log config — AAP R-26)
#   - migrations/                       (Alembic / SQL DDL — AAP R-9)
#
# A `.dockerignore` at the repository root or this folder MUST exclude
# `tests/`, `.env`, `.env.*`, `.git/`, `Dockerfile`, `README.md`, and any
# build / test caches from the build context. This Dockerfile assumes such
# a `.dockerignore` is in place; without it the build context will be larger
# than necessary but image content remains correct because the runtime stage
# only `COPY`s explicit paths (src/, config/, migrations/, requirements.txt).
# ============================================================================


# ============================================================================
# Phase 2 — Builder Stage
# ----------------------------------------------------------------------------
# Compiles wheels for all Python dependencies, including those that need
# C compilation: psycopg (libpq), confluent-kafka (librdkafka), and
# cryptography (libssl + libffi). The builder stage carries the full
# toolchain (gcc, build-essential, *-dev headers) so the runtime stage can
# remain compiler-free and minimal.
#
# Pinning the base to `python:3.11-slim-bookworm` (Debian 12 slim variant)
# matches the monorepo Python convention and provides multi-arch images
# (linux/amd64 + linux/arm64) so CI buildx can emit both architectures
# without changing this Dockerfile.
# ============================================================================
FROM python:3.11-slim-bookworm AS builder

# Builder-stage environment: deterministic, fast, non-interactive, and with a
# generous pip timeout so transient mirror slowness does not break the build.
# PIP_NO_CACHE_DIR keeps the resulting wheel directory minimal — pip's own
# cache is not needed once wheels are produced.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_DEFAULT_TIMEOUT=120 \
    DEBIAN_FRONTEND=noninteractive

# System-package toolchain. We deliberately keep this list minimal — only
# what is strictly required to compile native extensions. The runtime stage
# below installs the matching shared-library counterparts (libpq5, librdkafka1,
# libssl3) without any compilers.
#
#   - build-essential / gcc / python3-dev:     C toolchain for native wheels
#   - libpq-dev:                                psycopg / libpq build-time headers
#   - librdkafka-dev:                           confluent-kafka build-time headers
#   - libssl-dev / libffi-dev:                  cryptography build-time headers
#   - ca-certificates:                          TLS for pip/PyPI fetches
#
# `--no-install-recommends` keeps the layer lean and predictable.
# hadolint ignore=DL3008 — apt versions are intentionally unpinned: Debian
# stable's apt point-releases provide ABI-stable updates within a release.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        libpq-dev \
        librdkafka-dev \
        libssl-dev \
        libffi-dev \
        python3-dev \
        ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Use a dedicated /build directory so the builder context is isolated from
# the future /app runtime layout. This makes layer-cache reasoning easier
# and avoids accidental host-path leakage.
WORKDIR /build

# Cache-friendly layer ordering: requirements.txt is copied before any source
# code so the expensive `pip wheel` layer is reused on every build that does
# not change the dependency manifest. A dependency change invalidates the
# wheel cache; an application code change does NOT.
COPY requirements.txt /build/requirements.txt

# Build wheels for ALL declared dependencies into /wheels. The runtime stage
# will install from this wheelhouse exclusively (`--no-index`), which:
#   1. Eliminates the network call during runtime image construction.
#   2. Guarantees the runtime image installs exactly the resolved set the
#      builder produced (reproducible builds).
#   3. Permits offline / air-gapped image construction in the runtime stage.
#
# `pip install --upgrade pip setuptools wheel` ensures the builder pip is
# new enough to honor PEP 660 / PEP 517 build backends used by modern deps.
RUN pip install --upgrade pip setuptools wheel \
 && pip wheel --wheel-dir /wheels -r /build/requirements.txt


# ============================================================================
# Phase 3 — Runtime Stage
# ----------------------------------------------------------------------------
# The runtime stage is the image actually shipped to production. It contains:
#   - Python 3.11 (slim bookworm)
#   - Runtime shared libraries ONLY (libpq5, librdkafka1, libssl3, ca-certs,
#     curl for HEALTHCHECK, tini for PID 1)
#   - The wheel-installed Python packages
#   - The application source under /app
#   - A non-root user `inventory` (UID/GID 10001)
#
# The runtime stage does NOT contain:
#   - Compilers or *-dev headers
#   - The wheelhouse (deleted after install)
#   - pip caches
#   - apt lists
# ============================================================================
FROM python:3.11-slim-bookworm AS runtime

# OCI image labels — discoverability and provenance. The CD pipeline rewrites
# `org.opencontainers.image.source` (and adds `revision` / `created` / `version`
# labels) at build time via `--label`, so the placeholder URL below is
# expected to be overridden in production builds.
LABEL org.opencontainers.image.title="inventory-service" \
      org.opencontainers.image.description="Stock reservation engine and warehouse-state owner for the e-commerce platform" \
      org.opencontainers.image.source="https://example.com/repo" \
      org.opencontainers.image.licenses="UNLICENSED" \
      org.opencontainers.image.vendor="E-Commerce Platform" \
      org.opencontainers.image.documentation="docs/architecture/system-diagram.md"

# Runtime-stage environment defaults. Per AAP R-25 these are NON-SECRET
# defaults only. ALL secret values (POSTGRES_URL, POSTGRES_USER,
# POSTGRES_PASSWORD, KAFKA_SASL_*, JWT_PUBLIC_KEY_URL, etc.) MUST be
# injected at runtime via Kubernetes Secrets / HashiCorp Vault / cloud
# secret managers. Baking secrets into the image is forbidden — they would
# be permanently visible in the layer history.
#
# The values below mirror `.env.example` non-secret defaults so the image
# starts cleanly in local docker-compose flows even when the operator has
# not yet exported every variable.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DEBIAN_FRONTEND=noninteractive \
    SERVICE_NAME=inventory-service \
    SERVICE_PORT=8000 \
    LOG_LEVEL=INFO \
    LOG_FORMAT=json \
    HEALTH_LIVENESS_PATH=/health/live \
    HEALTH_READINESS_PATH=/health/ready \
    METRICS_PORT=9090 \
    METRICS_PATH=/metrics

# Runtime shared-library install. Strictly NO compilers here:
#   - libpq5:        shared libpq for psycopg (built in the builder stage)
#   - librdkafka1:   shared librdkafka for confluent-kafka
#   - libssl3:       OpenSSL runtime for TLS + cryptography wheel
#   - ca-certificates: trusted CA bundle for outbound HTTPS (JWKS, OTLP, etc.)
#   - curl:          used by the HEALTHCHECK directive below
#   - tini:          minimal PID 1 init that reaps zombies and forwards SIGTERM
#                    correctly. Critical for graceful shutdown of saga
#                    participants — see ENTRYPOINT comment.
# hadolint ignore=DL3008
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        libpq5 \
        librdkafka1 \
        libssl3 \
        ca-certificates \
        curl \
        tini \
 && rm -rf /var/lib/apt/lists/* \
 && apt-get clean

# AAP R-25 — running as a non-root user reduces blast radius if the
# container is compromised. UID/GID 10001 is the monorepo convention shared
# by sibling services (orders, notifier, payments, reco). The username
# `inventory` is intentionally domain-aligned so operator introspection
# (e.g., `ps aux`, `top`) immediately reveals which service owns the
# process. `--shell /usr/sbin/nologin` blocks interactive logins as defense
# in depth.
RUN groupadd --system --gid 10001 inventory \
 && useradd  --system --uid 10001 --gid inventory \
             --home-dir /app --shell /usr/sbin/nologin inventory

# Set the application working directory and pre-create the writable
# subdirectories the application expects at runtime:
#   - /app/logs          rotating log files (when not shipping to stdout)
#   - /app/tmp           ephemeral scratch space, including the
#                        prometheus_multiproc directory referenced by
#                        PROMETHEUS_MULTIPROC_DIR in .env.example
# Ownership is set to the non-root `inventory` user so the application
# can write without escalation.
WORKDIR /app
RUN mkdir -p /app /app/logs /app/tmp \
 && chown -R inventory:inventory /app

# Install Python packages from the builder's wheelhouse. The
# `--no-index --find-links=/wheels` combination guarantees that pip resolves
# every package from local wheels only — no PyPI access at runtime image
# construction time, no surprise upstream pulls, and a fully reproducible
# install. The wheelhouse and pip cache are removed in the same RUN to
# avoid bloating the final layer.
COPY --from=builder /wheels /wheels
COPY --chown=inventory:inventory requirements.txt /app/requirements.txt
RUN pip install --no-index --find-links=/wheels -r /app/requirements.txt \
 && rm -rf /wheels /root/.cache

# Copy the application source, configuration, and database migrations into
# the image with proper ownership. These directories are produced by sibling
# sub-agents within services/inventory-service/:
#   - src/         FastAPI application, Kafka consumers/producers, domain
#                  logic, the saga participant code
#   - config/      default.yaml + log_config.json (the latter referenced by
#                  the CMD's --log-config flag for AAP R-26 structured logs)
#   - migrations/  Alembic env / versioned migration scripts for the
#                  inventory_db schema (AAP Section 0.4.4: stock_items,
#                  reservations, warehouses, stock_movements; AAP R-9)
#
# Tests, env templates, the Dockerfile itself, and READMEs are NOT copied
# into the runtime image — they are excluded by `.dockerignore`.
COPY --chown=inventory:inventory src/         /app/src/
COPY --chown=inventory:inventory config/      /app/config/
COPY --chown=inventory:inventory migrations/  /app/migrations/

# Drop privileges. Every command from this point onward (HEALTHCHECK,
# ENTRYPOINT, CMD) runs as the unprivileged `inventory` user. Placing the
# `USER` directive AFTER the `COPY` instructions is intentional: pip
# install + chown require root, while runtime execution must not.
USER inventory:inventory

# Network ports. EXPOSE is documentation only — actual port publishing is
# done at `docker run -p` / Kubernetes Service time.
#   - 8000  Application HTTP (REST + readiness/liveness)
#   - 9090  Prometheus /metrics scrape endpoint (AAP R-27, R-28)
EXPOSE 8000 9090

# AAP R-19 — Docker-level healthcheck that calls the *readiness* probe so
# the container reports unhealthy when its critical downstream dependencies
# (PostgreSQL, Kafka, Schema Registry, JWKS endpoint) are unreachable. This
# is the right semantics for traffic gating in `docker-compose` and ad-hoc
# `docker run` cases. Kubernetes deployments typically rely on their own
# `livenessProbe` / `readinessProbe` settings on the Pod spec; the Docker
# HEALTHCHECK is harmless duplication there and provides parity for local
# development. `curl` is invoked with strict failure semantics so any
# non-2xx response, network timeout (4s), or DNS failure flips the
# container state to unhealthy.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl --fail --silent --show-error \
        --max-time 4 \
        "http://127.0.0.1:${SERVICE_PORT}${HEALTH_READINESS_PATH}" \
        || exit 1

# tini is the minimal init process: it reaps zombie children (avoiding
# defunct-process accumulation) and — crucially for this service —
# forwards SIGTERM to uvicorn so FastAPI executes its shutdown event
# (which gracefully drains in-flight HTTP requests, commits any pending
# Kafka consumer offsets, and flushes the producer). A `kill -9` on this
# container is unsafe because it would skip the consumer commit and could
# leave a reservation in an indeterminate state — making a saga step
# either lost or duplicated. With tini the only acceptable shutdown path
# is: SIGTERM → uvicorn graceful → consumer commit → producer flush → exit.
ENTRYPOINT ["/usr/bin/tini", "--"]

# Runtime command: launch uvicorn pointing at the FastAPI application.
#   - src.main:app                     module path of the FastAPI `app` object
#   - --host 0.0.0.0                   bind on all interfaces inside the container
#   - --port 8000                      mirrors EXPOSE 8000 / SERVICE_PORT
#   - --workers 2                      sensible default for hybrid HTTP +
#                                      Kafka-consumer workload; CD pipeline
#                                      may override per environment via the
#                                      Kubernetes Deployment spec
#   - --loop uvloop --http httptools   high-performance event loop and
#                                      HTTP parser shipped in
#                                      uvicorn[standard] (see requirements.txt)
#   - --access-log                     emit per-request access logs
#   - --log-config config/log_config.json
#                                      structured JSON log configuration —
#                                      AAP R-26 mandates JSON logs with
#                                      timestamp, level, service,
#                                      correlation_id, etc.
CMD ["uvicorn", "src.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "2", \
     "--loop", "uvloop", \
     "--http", "httptools", \
     "--access-log", \
     "--log-config", "config/log_config.json"]


# ============================================================================
# Build & operational notes
# ----------------------------------------------------------------------------
# Build (from repo root):
#   docker build -f services/inventory-service/Dockerfile \
#                -t ecommerce/inventory-service:local .
#
# Run (local sanity check — supply credentials externally per AAP R-25):
#   docker run --rm -p 8000:8000 -p 9090:9090 \
#       --env POSTGRES_URL=postgres://user:pass@host:5432/inventory_db \
#       --env KAFKA_BOOTSTRAP=broker:9092 \
#       --env SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
#       --env JWT_PUBLIC_KEY_URL=https://auth.example.com/.well-known/jwks.json \
#       ecommerce/inventory-service:local
#
# Multi-arch CI builds emit linux/amd64 + linux/arm64 via `docker buildx`;
# this Dockerfile is platform-agnostic because the upstream
# `python:3.11-slim-bookworm` and Debian shared libraries are multi-arch.
#
# Image signing (Cosign) and SBOM generation (syft) happen in the CI
# pipeline (.github/workflows/ci.yml) — they are NOT part of this
# Dockerfile so the local-build path stays simple.
#
# Expected runtime image size: roughly 200–400 MB. A significantly larger
# image indicates leftover build artifacts (wheelhouse, pip cache, apt
# lists) — re-check the cleanup steps in the runtime stage.
# ============================================================================
