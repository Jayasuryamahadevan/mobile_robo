# Phone Robot — Temporal Cognitive Memory Fabric

A Go-based, provenance-first memory service for long-running personal and robotic agents. The authoritative store is PostgreSQL; Redis accelerates working memory; Qdrant and Neo4j are optional, reconstructible search projections. Writes use a transactional outbox. No LLM is required on the critical memory read path.

## Features

- User- and tenant-scoped authenticated memory API (HS256 JWT)
- Bitemporal semantic facts with a full revision history and source event references
- Episodic and lexical retrieval with optional vector enrichment
- Idempotent event ingestion and outbox-backed asynchronous indexing
- Optional Qdrant vectors and Neo4j graph projections
- Explicit forgetting and deletion tombstones
- Simple reviewed procedural memory registry
- Operational health endpoints, trace-friendly request IDs, bounded timeouts
- No robot motion control through historical memory

## Local setup

1. Copy `.env.example` to `.env`, set a unique `JWT_SECRET` (at least 32 random bytes), and load it into your shell.
2. Run `docker compose up --build -d`.
3. Create an access token: `go run ./cmd/devtoken -tenant demo -user alice -scopes memory:read,memory:write,memory:delete,procedure:approve` (requires the same `JWT_SECRET`).
4. Use the bearer token with `POST /v1/events`, `PUT /v1/facts`, and `GET /v1/context?query=...`.

The Compose stack is for development. Production requires TLS, dedicated database roles, secret management, backups, and deployment-specific latency benchmarking. See [Architecture](docs/ARCHITECTURE.md).

## API overview

- `GET /healthz` — process liveness
- `GET /readyz` — database readiness
- `POST /v1/events` — log an interaction
- `PUT /v1/facts` — explicitly assert or correct a fact
- `GET /v1/context?query=...&session_id=...` — retrieve a bounded, evidence-linked context
- `GET /v1/facts/history?subject=...&predicate=...` — recorded-time fact history
- `POST /v1/procedures` and `POST /v1/procedures/{id}/approve`
- `DELETE /v1/memories` — initiate a scoped memory purge; the authoritative store is updated synchronously, projections asynchronously

## Engineering scope

This is an initial implementation of the architecture, not a claim of independently certified production readiness or a measured 100 ms P95. See the CI tests, threat model and acceptance gates before deploying with real personal data.
