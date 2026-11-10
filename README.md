# kafka-monitoring

Gated SDLC workspace for a Spring Boot **Kafka monitoring** service (Java 21, single-node Kafka). The service covers user status management and system performance monitoring. Partition counts and consumer concurrency live in `application.yml`.

Agents follow `.ai/greenfield/` (build from spec) and `.ai/brownfield/` (later small fixes). The running process is ordinary Spring Boot: no LLM inside the JAR.

Gate 2 is approved (`GATE2_APPROVAL.txt` = `[APPROVED: yitin]`). The Spring Boot module is `com.example:kafka-demo`. On this machine `JAVA_HOME` may still point at JDK 8; Maven needs JDK 21 (`C:\Program Files\Java\jdk-21.0.12.1`).

## Requirements

* JDK **21** (`JAVA_HOME`)
* Apache Maven 3.9+
* Docker Desktop (broker compose)

## Tests

```bash
mvn test
```

JaCoCo fails the build if line coverage is below **80%**. Do not use `@Disabled` / `@Ignore`. Last green run: `mvn clean test` on JDK 21, 30 tests, **190 / 205 lines (92.68%)**.

## Run

```bash
docker compose up -d          # single-node Kafka on localhost:9092
mvn spring-boot:run
```

```bash
curl -X PUT localhost:8080/api/users/u42/status -H "Content-Type: application/json" -d "{\"status\":\"ONLINE\",\"reason\":\"login\"}"
curl localhost:8080/api/users/u42/status
curl localhost:8080/actuator/prometheus | grep -E "app_kafka|app_service|kafka_consumer_fetch_manager_records_lag"
```

Verify partitions and assignments:

```bash
docker exec -it <kafka-container> /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
docker exec -it <kafka-container> /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups
```

## Layout

| Topic | Partitions | Key | Groups (threads) |
|:------|:-----------|:----|:-----------------|
| user-status-events | 12 | userId | user-status-processor (6), user-status-audit (3) |
| user-status-changelog (compacted) | 6 | userId | none (latest state per user) |
| system-metrics | 24 | service-instance | metrics-aggregator (12), metrics-alerting (4) |
| system-alerts | 3 | service (partition 0 = CRITICAL) | alert-notifier (3) |
| `*.DLT` | 3 | — | none (inspect or replay failed records) |

## Rules of thumb

1. **Partitions cap parallelism.** Effective consumers per group = `min(concurrency × app instances, partitions)`. Idle extras get nothing.
2. **Partitions can only grow.** Raising the number later changes key-to-partition mapping. Size for peak throughput plus headroom.
3. **Same key, same partition.** Key by the entity whose ordering matters (`userId`, instance).
4. **Concurrency = threads per instance.** Pick a divisor of the partition count so the load is even (12 partitions / 6 threads = 2 each).
5. **One group per independent concern** (processing vs audit, aggregating vs alerting).
6. **Watch lag** (`app.kafka.group.lag`) and end-to-end latency (`app.kafka.e2e.latency`). Rising lag means more concurrency (up to the partition count), more instances, or a faster handler.

Spec index: `.ai/greenfield/KAFKA_MONITORING_SUMMARY.md`.
