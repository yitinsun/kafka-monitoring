# Kafka + Spring Boot: partitions, consumer concurrency, user status & system monitoring

## Run
```bash
docker compose up -d          # single-node Kafka on localhost:9092
mvn spring-boot:run

curl -X PUT localhost:8080/api/users/u42/status -H 'Content-Type: application/json' \
     -d '{"status":"ONLINE","reason":"login"}'
curl localhost:8080/api/users/u42/status
curl localhost:8080/actuator/prometheus | grep -E 'app_kafka|app_service|kafka_consumer_fetch_manager_records_lag'
```
Verify partitions/assignments:
```bash
docker exec -it <kafka-container> /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
docker exec -it <kafka-container> /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups
```

## Layout
| Topic | Partitions | Key | Groups (threads) |
|---|---|---|---|
| user-status-events | 12 | userId | user-status-processor (6), user-status-audit (3) |
| user-status-changelog (compacted) | 6 | userId | - (latest state per user; rebuild source) |
| system-metrics | 24 | service-instance | metrics-aggregator (12), metrics-alerting (4) |
| system-alerts | 3 | service (p0 = CRITICAL) | alert-notifier (3) |
| *.DLT | 3 | - | - (inspect/replay failed records) |

## Rules of thumb
1. **Partitions cap parallelism.** Effective consumers per group = `min(concurrency x app instances, partitions)`. Idle extras get nothing.
2. **Partitions can only grow.** Raising the number later changes key->partition mapping, so per-key ordering is only guaranteed within old/new data separately. Size for peak throughput / per-consumer throughput, plus 2-3x headroom.
3. **Same key, same partition.** Key by the entity whose ordering matters (userId, instance).
4. **Concurrency = threads per instance.** Pick a divisor of the partition count so load is even (12 partitions / 6 threads = 2 each).
5. **One group per independent concern** (processing vs audit, aggregating vs alerting) so they scale and fail independently.
6. **Watch lag** (`app.kafka.group.lag`) and end-to-end latency (`app.kafka.e2e.latency`); rising lag = add concurrency (<= partitions), add instances, or speed up the handler.
