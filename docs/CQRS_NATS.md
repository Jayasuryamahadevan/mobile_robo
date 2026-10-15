# CQRS + NATS JetStream — ADR 002

Status: implemented on feature branch; **requires CI and operational validation before merging**.

## Boundaries

- **Command side:** `POST /v1/events`, `PUT /v1/facts`, procedure management, and deletion. Mutations are transactional PostgreSQL writes.
- **Query side:** `GET /v1/context`, history, and projection health. Reads use PostgreSQL as their factual authority and may consult scoped derived indexes.
- **Outbox relay:** scans committed PostgreSQL outbox entries, sends broker messages, waits for server PubAck, then records publication.
- **Projector:** durable NATS JetStream pull consumer with explicit ACK after PostgreSQL records the terminal result; read projection jobs by ID from PostgreSQL.
- **Optional projections:** Redis, Qdrant, Neo4j. The authoritative fact timeline is never constructed from a broker replay.

## Delivery contract

Write transaction:
1. Commit source event/fact AND `memory_outbox` row in the same transaction.
2. Return success from the API only after the authoritative transaction succeeds.
3. Relay claims publication with a lease (or reclaims a still-unprocessed notice after 10 minutes) and publishes `{"schema_version":1,"outbox_id":123}` on `memory.projections.v1`.
4. Relay waits for JetStream PubAck then sets `published_at`. A crash after PubAck and before this UPDATE can produce duplicates by design.
5. Projector claims the outbox row atomically using status + lease. Repeated delivery of completed/deleted work is acknowledged without duplicate application.
6. Projector performs bounded indexing, transitions row to `done` or retry state, then ACKs the broker. The relay can republish a notice with the same outbox ID after 10 minutes if processing has not completed, recovering from broker retention or consumer loss. After 12 processing attempts, `dead` is persisted and the broker message terminated. Inspect alerting before manual replay.

**Guarantees:** at-least-once messaging; effect-level idempotency for projections identified by immutable event/fact IDs. This is not end-to-end exactly-once transport. JetStream message IDs dedupe within a bounded window, but PostgreSQL state handles duplicates beyond that window.

**Races and deletion:** workers and deletion share a tenant/user PostgreSQL advisory lock. If a deletion removes a pending outbox row, a stale JetStream notification is harmless. Published messages contain no memory text or user identifiers. Projection deletes are eventually consistent, and authoritative memory is removed transactionally.

**Read-your-writes:** `PUT /v1/facts` is immediately visible in PostgreSQL-backed queries. Qdrant/Neo4j are eventually consistent. `GET /v1/projections/status` allows callers to observe backlog and dead jobs. There is currently no global atomic snapshot across PostgreSQL, Redis, Qdrant and Neo4j.

## Deploy

For a fresh developer instance:
- Set `JWT_SECRET`, `POSTGRES_PASSWORD` and optional `NATS_URL` in `.env`.
- Start `docker compose up --build -d`.
- New databases run 001, 002 and 003 automatically. Existing volumes must run 002 and 003 in order through a versioned migration runner before starting relay/projector.
- The relay and projector are separate long-running processes (`cmd/memoryrelay` and `cmd/memoryprojector`).
- `memoryworker` is a legacy, PostgreSQL-polling fallback. Never run it concurrently with the JetStream projector except during a controlled migration.

For a private NATS cluster, configure NATS account permissions restricting the relay to publish only `memory.projections.v1` and the projector to consume the durable `MEMORY_PROJECTIONS` stream. Use TLS and NATS credentials outside development.


## Database identity and row-level security

Migration 003 creates non-login roles `memory_api` and `memory_projector`, grants their minimum table access, and enables row-level policies for the outbox and deletion ledger. It does **not** ship embedded passwords.

Before deployment, a DBA must grant LOGIN using securely provisioned, rotated credentials. Run the command/query API with `memory_api`; run the relay and projector with `memory_projector`. The trusted projector role has `BYPASSRLS` because it processes jobs for all tenants, so it must never be exposed to untrusted clients.

The API role is not the table owner. Every query of user-owned rows must run in a PostgreSQL transaction that sets `app.tenant_id` and `app.user_id` from the verified JWT. Integration tests exercise this with a true non-owner API role. The developer Compose setup continues to use the owner role for convenience and is **not** a production credential arrangement.

Never connect production API processes with the database owner, postgres superuser, or projector role.

## Failure matrix

| Failure | Outcome / response |
|---|---|
| API crashes before DB commit | Neither fact nor outbox event becomes visible |
| Relay crashes before publish | Lease expires, retry |
| Relay crashes after PubAck | Duplicate notice, DB state prevents duplicate processing |
| NATS unavailable | Commands remain available; backlog increases; query service serves authoritative facts |
| Consumer crashes before DB status update | Lease expires, broker redelivers |
| Consumer crashes after status update | Duplicate is acknowledged without rerunning |
| Projection corrupt/stale | Query revalidates candidates against scoped authoritative data |
| Delete during projection | Shared user advisory lock serializes external projection writes and purge |
| Terminal indexing error | Dead row retained for inspection and explicit replay |

## SLOs and alerts

- Measured `GET /v1/context` P95 under 100ms is a **target**, not a benchmark result.
- Alert on oldest unpublished outbox age, oldest pending process age, dead job count, relay disconnections, projector redeliveries, and failed projection purge.
- Separate API latency from the background projection completion SLO.
- Before production: tests with a real multi-tenant, non-owner DB role; replay/delete fault injection; retention cleanup; encryption, key rotation, backups and restore, network isolation, PII handling.

## Constraints and future improvements

This release does not magically provide fact extraction from arbitrary language. The deterministic extraction path from the first iteration remains. A robust LLM-assisted extraction pipeline must write proposed assertions with evidence spans and explicit confidence; it must not silently override verified user facts. CQRS is a consistency/transport architecture, not an accuracy model.

Consider migrating Qdrant and Neo4j projection payloads to include a **generation/epoch** to make partial purges and new writes perfectly separable. Current broad user purge plus replay is safe for authoritative reads but can temporarily reduce derived-index recall after a deletion.
