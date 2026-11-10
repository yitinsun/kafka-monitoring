# Product Requirement Document (PRD): Kafka Monitoring Service

## 1. Executive Summary & Objective

Build a runnable **Spring Boot 3** service that uses Kafka for two coupled problems:

* **User status management.** A caller changes a user's status. Events are ordered per user, stale replays are ignored, and the latest state is readable.
* **System performance monitoring.** Service instances emit metric samples. One consumer group aggregates them. Another raises alerts. Critical alerts are isolated from warning floods.

Partition counts and consumer thread counts are configuration. The service must be complicated enough to show independent consumer groups, a compacted changelog, a dead-letter path, and Prometheus metrics.

**Out of v1 (re-plan if requested later):** user accounts and authentication, a durable status database or Redis, Schema Registry, Kafka Streams, multi-broker production clusters, click-style analytics, and an application container image.

The behavioral reference is `.notes/kafka-demo.zip`, with the topic table in `.notes/overview.docx` and `.notes/ABOUTME.md`.

## 2. Functional Requirements

### 2.1 User status API

* **Change:** `PUT /api/users/{userId}/status`
* **Input payload:**
  ```json
  { "status": "ONLINE", "reason": "login" }
  ```
  `status` is one of `ONLINE`, `AWAY`, `OFFLINE`, `SUSPENDED`, `DELETED`.
* **Behavior:** Publish a `UserStatusEvent` to `user-status-events` with key `userId`. `version` is the event time in epoch millis. This call is asynchronous.
* **Success:** **202 Accepted** with an empty body. 202 means the event was handed to the Kafka producer, not that the read model has already caught up.

* **Read:** `GET /api/users/{userId}/status`
* **Success (`200`):** the current `UserStatusEvent` from the in-memory read model.
* **Missing user:** **404**.

### 2.2 User status pipeline

* Events are keyed by `userId`, so one user's changes stay on one partition and stay in order.
* Group `user-status-processor` applies the event only when `version` is strictly newer than the stored version. Stale or replayed events are ignored. A newer event is written to the read model and published to the compacted topic `user-status-changelog` with the same key.
* Group `user-status-audit` reads `user-status-events` independently and records the event. It does not update the read model.
* A record with a null status, or a null/blank `userId`, is invalid. It is not retried. It is published to `user-status-events.DLT`.

### 2.3 Metrics, alerts, and monitoring

* While `app.demo.generate-metrics` is true (the default), a scheduler emits `cpu.usage` and `latency.p99.ms` samples every second for five services (`auth`, `orders`, `payments`, `search`, `notifications`), four instances each. The key is the instance id (`{service}-{n}`).
* Group `metrics-aggregator` is a batch listener. It folds samples into Micrometer gauges `app.service.metric.ewma` and `app.service.metric.max`, and records `app.kafka.e2e.latency` from sample timestamp to consume time.
* Group `metrics-alerting` is a separate batch listener on the same topic. Thresholds:
  * `cpu.usage` above **0.90**
  * `latency.p99.ms` above **1000**
  * any other metric name does not alert
  * severity is `CRITICAL` when `value > threshold * 1.2`, otherwise `WARNING`
  * cooldown is **30 seconds** per `instance|metric`
* `AlertPublisher` chooses the partition explicitly. `CRITICAL` always uses partition **0**. `WARNING` uses a partition in `1 .. partitions-1` derived from the service name. A flood of warnings cannot occupy the critical partition.
* Group `alert-notifier` logs partition 0 as a page, and other partitions as a ticket.
* `ConsumerLagMonitor` publishes `app.kafka.group.lag` (tag `group`) about every 15 seconds: sum over the group's committed partitions of (log end offset − committed offset).
* Actuator exposes `/actuator/health`, `/actuator/metrics`, and `/actuator/prometheus`.

### 2.4 Topic and consumer layout

All of these values live in `application.yml`. `KafkaTopicConfig` creates the topics from them at startup. Each `@KafkaListener` sets its own concurrency from the config, which overrides the container factory default.

| Topic | Partitions | Key | Consumer groups (threads) |
|:------|:-----------|:----|:--------------------------|
| `user-status-events` | 12 | userId | `user-status-processor` (6), `user-status-audit` (3) |
| `user-status-changelog` (compacted) | 6 | userId | none; latest state per user |
| `system-metrics` | 24 | service-instance | `metrics-aggregator` (12), `metrics-alerting` (4) |
| `system-alerts` | 3 | service; partition 0 is CRITICAL | `alert-notifier` (3) |
| `user-status-events.DLT` | 3 | producer-selected | none; inspect or replay |
| `system-metrics.DLT` | 3 | producer-selected | none; inspect or replay |

## 3. Non-Functional Requirements

### 3.1 Parallelism

* Effective consumers in a group = `min(concurrency × app instances, partitions)`. Extra threads or instances sit idle.
* With 12 partitions, 6 threads on 1 instance gives 2 partitions per thread. A second instance rebalances toward 12 consumers. A third instance has nothing left to take.
* Concurrency must divide the partition count so load is even: 12/6, 12/3, 24/12, 24/4, 3/3.
* Partition counts can only increase. Raising them changes which partition a key maps to. Size with headroom from the start. Do not decrease.

### 3.2 Local run

* `docker compose up -d` starts one Kafka broker on `localhost:9092`.
* `mvn spring-boot:run` starts the app on port **8080**.
* Local replica count is **1**. Production replica count is **3**, supplied with `KAFKA_REPLICAS`.

### 3.3 Failure handling

* Listener errors retry with exponential backoff (initial 500 ms, multiplier 2, max 3 retries), then publish to `{sourceTopic}.DLT`.
* `IllegalArgumentException` skips retries and goes straight to the DLT.
* Producer acknowledgements are `all`, with idempotence enabled.

## 4. Technical Grounding

* **Runtime:** Java 21.
* **Framework:** Spring Boot 3.3.x.
* **Broker:** Apache Kafka 3.8 (single-node KRaft in compose).
* **Encoding:** JSON values, string keys.
* **Reference module:** `.notes/kafka-demo.zip` (`com.example.kafkademo`).
