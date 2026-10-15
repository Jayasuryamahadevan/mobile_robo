# Temporal Cognitive Memory Fabric — Phone Robot

## Consistency model

PostgreSQL is the authoritative store for personal facts and source episodes. Facts use bitemporal history: `valid_from/valid_until` represent when the claim applies in the world, and `recorded_from/recorded_until` represent when the system held that version. Correcting a fact closes its previous recorded-time interval and inserts a new version; prior recorded-time queries can still inspect what the system formerly believed. The baseline assumes each `(tenant,user,subject,predicate)` has one current value. Multi-valued relations require a separate relation assertion table or a cardinality-aware predicate schema.

Qdrant and Neo4j are optional materialized projections; they are **never** trusted to authorize a memory read. All external candidates must be hydrated by tenant/user-scoped authoritative queries, with deletion and time checks. Working memory in Redis is advisory, expires quickly, and must not be treated as verified factual evidence. Long-lived personal data should not be cached in mutable public clients.

## Read flow

1. JWT auth with audience, issuer, expiry and least-privilege scopes.
2. Bind tenant/user scope in a transaction. Fail closed if binding fails.
3. Resolve relevant current semantic facts in PostgreSQL using valid and recorded times.
4. Retrieve episodic lexical results through PostgreSQL FTS.
5. Optionally retrieve dense vector matches, always validating the candidate IDs against PostgreSQL.
6. Optionally expand graph relations, always rechecking current facts against PostgreSQL.
7. Assemble at most 12 records with source references, never a fabricated memory.
8. The consumer LLM may use returned evidence; the memory service does not invoke an LLM on the synchronous fast path.

Latency objective: 100 ms P95 for retrieval under a measured regional deployment; the application LLM response time is **excluded**. This repository contains no fabricated benchmarks. Remote query embedding and multi-hop graph traversal use bounded optional budgets, not a mandatory dependency.

## Write flow

1. Validate source, identity, and request size.
2. Within one PostgreSQL transaction, insert event/episode or replace a semantic fact's current recorded-time version.
3. Insert an outbox job in the *same* transaction and commit.
4. Respond to the caller with committed ID. Do not wait for extraction/embedding/indexes.
5. A worker leases queued jobs via `FOR UPDATE SKIP LOCKED`, performs idempotent projection calls, acknowledges success, and retries transient failures with bounded exponential delay.
6. After repeated failure, a job becomes `dead` and requires operational intervention. Monitor `memory_outbox` by state/age.

The deterministic extraction grammar `remember: predicate = value` is deliberately narrow. A general LLM extractor can be added behind an asynchronous worker with cited source spans and a confirmation policy; unverified inferred facts must never auto-promote to trusted facts.

## Robot-specific safety

The agent memory API is not a motion controller. Avoid injecting historical observations directly into actuator commands. Live sensors and an independent safety controller override remembered state. Procedural memories begin `proposed`; an explicit permission is required for approval. This repository does not implement motor control.

## Threat model and requirements

- All tenant and user IDs come from cryptographically verified JWTs, never untrusted body fields.
- Protect `JWT_SECRET`, database credentials and TLS termination using a secret manager. Rotate signing keys and use an identity-provider JWKS verifier in production.
- Current Compose uses a PostgreSQL owner role (which bypasses RLS). Real deployments must provision restricted API roles and worker roles; enforce RLS on all user tables and test it with those roles. Never use the admin database role in a live multi-tenant environment.
- Event content may include prompt-injection text. Treat it as untrusted data. Do not allow it to create executable procedures or override tool permissions.
- Never log memory text, raw authorization tokens, or high-sensitivity memory payloads.
- Deletion is immediate for authoritative records and eventually consistent in optional projections. The deletion response is `202` until projection cleanup succeeds. A purge tombstone remains to support retries and auditing.
- Backups, exports, object storage, graph and vector replicas require coordinated retention/deletion policies. This service cannot guarantee external backups are purged.
- The Redis cache is a best-effort accelerator, not authoritative. Deletion barriers prevent Redis content older than a deletion from being included in a normal context response.
- Add authorization tests for negative cross-tenant queries and penetration tests before Internet-facing exposure.
- Use TLS and separate network policies for Postgres, Redis, Qdrant, and Neo4j. Compose binds database ports to loopback for development only.
- Implement request rate limiting, tenant quotas, encryption-at-rest, audit retention, and privacy consent gating at the deployment boundary.

## Phased hardening

Phase 1: measure PostgreSQL/Redis latency, run database-backed tests, calibrate retrieval ranking, add idempotency tests and structured tracing.

Phase 2: enable local embedding and Qdrant. Benchmark query embedding P95 and measure recall@k; add vector reindex and repair jobs.

Phase 3: enable Neo4j only when relationship queries demonstrably benefit. Benchmark traversal and check graph facts against the authoritative store.

Phase 4: add event-driven NATS JetStream notification for worker wakeups while retaining PostgreSQL transactional outbox for durability; expose standardized consumer/producer interfaces.

Phase 5: add consent policies, LLM extraction of candidate memories with evidence spans, procedural skill evaluation, LoCoMo/LongMemEval tests, and a complete backup deletion workflow.

## Example memory

`PUT /v1/facts`:
```json
{"session_id":"s-001","subject":"self","predicate":"preferred_language","value":"Tamil"}
```

`POST /v1/events`:
```json
{"session_id":"s-001","source":"user","content":"Remember: preferred_language = Tamil"}
```

`GET /v1/context?query=preferred_language&session_id=s-001` returns versioned semantic evidence and matching episodes, not a synthesized answer.

## Migration rule

`db/migrations/001_init.sql` initializes a new database automatically in Compose. Docker's PostgreSQL init scripts **do not** rerun on an existing volume. Use an independently reviewed migration tool, backups, and rollout tests for subsequent migrations. Never rerun schema mutation scripts indiscriminately in production.
