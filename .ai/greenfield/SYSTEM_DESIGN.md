# System Design Specification: Kafka Monitoring Service

Architecture for a **single-process** Spring Boot service plus a **single-node** Kafka broker. This is not a multi-datacenter or Kafka Streams deployment.

v1 **does not** include: authentication, a durable user-status store, Schema Registry, or an application image in Compose. Those remain future re-plans.

## 1. Component Data Flow

```
[ Client ]
    │
    ├── PUT /api/users/{userId}/status ──> [ UserStatusController ]
    │                                         │  202 Accepted
    │                                         ▼
    │                                   [ UserStatusPublisher ]
    │                                    key = userId
    │                                         │
    │                                         ▼
    │                              [ user-status-events | 12 ]
    │                                    │              │
    │                    processor group │              │ audit group
    │                                    ▼              ▼
    │                          [ UserStatusStore ]   [ audit log ]
    │                           version check
    │                                    │
    │                                    ▼
    │                         [ user-status-changelog | 6, compacted ]
    │
    └── GET /api/users/{userId}/status ──> [ UserStatusStore ] ── 200 or 404

[ MetricsPublisher @ 1s ]
    │  key = instance id
    ▼
[ system-metrics | 24 ]
    │                         │
    │ aggregator (batch)      │ alerting (batch)
    ▼                         ▼
[ MetricsAggregator ]    [ threshold + 30s cooldown ]
    gauges + e2e timer        │
                              ▼
                    [ AlertPublisher ]
                     CRITICAL -> partition 0
                     WARNING  -> partition 1..n-1
                              │
                              ▼
                    [ system-alerts | 3 ]
                              │
                              ▼
                    [ alert-notifier ]
                     p0 = page, else ticket

[ ConsumerLagMonitor @ 15s ] ──> app.kafka.group.lag
[ listener failure ] ──> retry or immediate DLT ──> {topic}.DLT
```

## 2. Topic Catalog

`KafkaAdmin.NewTopics` builds every topic from `app.kafka.topics`. Missing topics are created at startup. If a topic already exists and the configured partition count is **larger**, KafkaAdmin may increase it. Kafka never decreases partitions. Increasing remaps keys and breaks per-key ordering across the old and new data, so the numbers below are the v1 baseline.

| Key in yml | Name | Partitions | Cleanup | Retention |
|:-----------|:-----|:-----------|:--------|:----------|
| `user-status` | `user-status-events` | 12 | delete | 7 days (`604800000` ms) |
| `user-status-changelog` | `user-status-changelog` | 6 | compact | n/a (compacted) |
| `metrics` | `system-metrics` | 24 | delete | 1 day (`86400000` ms) |
| `alerts` | `system-alerts` | 3 | delete | 7 days |
| `user-status-dlt` | `user-status-events.DLT` | 3 | delete | broker default |
| `metrics-dlt` | `system-metrics.DLT` | 3 | delete | broker default |

Replicas: `${KAFKA_REPLICAS:1}`. When replicas are greater than 1, `min.insync.replicas` is `2`; otherwise it is `1`.

Idempotency of user status is **application logic** (compare `version`), not a unique Kafka key constraint. The changelog is the rebuild source for the latest state per user. v1 does not start a changelog consumer; the live read model is the in-memory map filled by the processor group.

### 2.1 Listener containers

| Factory | Mode | Ack | Used by |
|:--------|:-----|:----|:--------|
| `recordFactory` | one record | `RECORD` | processor, audit, alert-notifier |
| `batchFactory` | whole poll | `BATCH` | metrics-aggregator, metrics-alerting |

Concurrency is **not** set on the factory. Each `@KafkaListener` sets `concurrency` from `app.kafka.groups.<id>.concurrency`.

| Group id | Concurrency | Topic | Why this number |
|:---------|:------------|:------|:----------------|
| `user-status-processor` | 6 | `user-status-events` | 12 / 6 = 2 partitions per thread; a second instance can take the rest |
| `user-status-audit` | 3 | `user-status-events` | lighter work; 4 partitions per thread |
| `metrics-aggregator` | 12 | `system-metrics` | 24 / 12 = 2 partitions per thread |
| `metrics-alerting` | 4 | `system-metrics` | isolated from aggregation; 6 partitions per thread |
| `alert-notifier` | 3 | `system-alerts` | 3 partitions, so 3 is the useful maximum |

Assignment strategy: `CooperativeStickyAssignor`.

## 3. Processing Rules

### 3.1 User status

* **Publish:** `UserStatusEvent(userId, status, reason, occurredAt, version)` with `version = occurredAt.toEpochMilli()`. Key is `userId`.
* **Apply:** `store.apply` returns true only when no row exists or `event.version > stored.version`. Equal or older versions are ignored.
* **Changelog write:** only after a successful apply, key still `userId`.
* **Invalid:** null status, or null/blank userId → `IllegalArgumentException`.

### 3.2 Metrics and alerts

* Sample key is `instance` (`auth-1` … `notifications-4`).
* Gauge identity is `service|metric`. EWMA uses `0.2 * value + 0.8 * previous`. `max` is the high-water mark since process start.
* End-to-end timer: `Duration.between(sample.ts, now)` on meter `app.kafka.e2e.latency` with tag `topic=system-metrics`.
* Alert cooldown key is `instance|metric`. A second breach inside 30 seconds is dropped.
* Warning partition: `1 + floorMod(service.hashCode(), partitions - 1)`. Critical partition: `0`.

### 3.3 Dead letter

`DeadLetterPublishingRecoverer` publishes to `record.topic() + ".DLT"` with partition `-1` so the producer selects the DLT partition. The DLT may have fewer partitions than the source. Backoff: `ExponentialBackOffWithMaxRetries(3)`, initial interval 500 ms, multiplier 2. `IllegalArgumentException` is not retryable.

### 3.4 Lag

Every 15 seconds (initial delay 20 seconds), for each configured group, lag is the sum of (latest offset − committed offset) across partitions the group has committed. Empty groups are skipped. Poll failures are logged and do not fail the process. The gauge name is `app.kafka.group.lag`.

## 4. Error Handling Matrix

| Situation | Result |
|:----------|:-------|
| `PUT` of a known status enum | **202**; event queued |
| `PUT` body that cannot bind to `UserStatus` | **400** (framework message conversion) |
| `GET` of a user the processor has not applied | **404** |
| `GET` after a newer event was applied | **200** and that event |
| Invalid status record inside the listener | no retry; `user-status-events.DLT` |
| Other listener exception | up to 3 backoff retries, then `{topic}.DLT` |
| Lag admin call fails | warning log; previous gauge value stands |

There is no global JSON error document in v1. Do not invent a second error schema.

## 5. Failure Modes

| Risk | v1 mitigation |
|:-----|:--------------|
| Per-user reordering | Key `user-status-events` and the changelog by `userId`. Do not raise partition counts without a re-plan. |
| Stale replay | Ignore events whose `version` is not strictly greater. |
| Alert starvation | Critical alerts are pinned to partition 0. |
| Poison messages | `ErrorHandlingDeserializer` plus a non-retryable `IllegalArgumentException` path to the DLT. |
| Idle consumers | Concurrency stays a divisor of the partition count and does not exceed it. |
| Read-model loss on restart | Accepted in v1. The compacted changelog is the rebuild source; a startup rebuild consumer is out of v1. |
| Single broker | Compose runs one KRaft node. Replica default is 1 so that node can start. |
