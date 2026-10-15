# Automated Test Verification Suite Plan

Mandatory tests and the agent loop for Gate 4. The suite must pass `mvn clean test` at **100%** and JaCoCo **line coverage >= 80%**. Never use `@Ignore` or `@Disabled`.

Coverage is enforced by the JaCoCo plugin in `TECH_STACK.md`, not by a manual percentage in `KAFKA_MONITORING_SUMMARY.md`.

The reference zip has no tests. This plan is the contract for the workspace module. Prefer unit tests with fakes for pure rules. Use `@EmbeddedKafka` only where a listener, topic declaration, or producer metadata has to be real.

## 1. Unit Testing Matrix

### 1.1 `UserStatusStoreTest`

No Spring context.

| Case | Setup | Expect |
|:-----|:------|:-------|
| First event | empty store | `apply` returns true; `get` returns that event |
| Newer version | stored version lower | `apply` returns true; stored event replaced |
| Stale version | stored version higher | `apply` returns false; stored event unchanged |
| Equal version | same millis | `apply` returns false |
| Unknown user | `get` | empty |

### 1.2 `AlertPublisherTest`

Mock `KafkaTemplate` and a properties object whose alerts topic has 3 partitions.

| Case | Expect |
|:-----|:-------|
| `Severity.CRITICAL` | `send` to partition **0** |
| `Severity.WARNING` | `send` to a partition in **1..2**, never 0 |

### 1.3 `MetricsListenersTest`

Mock `MetricsAggregator` and `AlertPublisher`.

| Case | Setup | Expect |
|:-----|:------|:-------|
| Under threshold | `cpu.usage` 0.5, `latency.p99.ms` 100 | no alert |
| CPU breach | `cpu.usage` 0.95 | one `WARNING` (`0.95 <= 0.90 * 1.2`) |
| CPU critical | `cpu.usage` 1.0, or any value `> 1.08` | one `CRITICAL` |
| Latency breach | `latency.p99.ms` 1001 | one alert |
| Unknown metric | name `disk.used` at a high value | no alert |
| Cooldown | two breaches for the same instance and metric inside 30 seconds | one `publish` |

### 1.4 `UserStatusListenersTest`

Mock the store, template, and properties.

| Case | Setup | Expect |
|:-----|:------|:-------|
| Invalid | null status, or blank `userId` | `IllegalArgumentException`; no changelog send |
| Applied | `apply` returns true | changelog send with key = userId |
| Stale | `apply` returns false | no changelog send |

### 1.5 `KafkaTopicConfigTest`

Call `appTopics` with an in-memory `AppKafkaProperties` that includes one compacted topic and one retained topic.

| Case | Expect |
|:-----|:-------|
| Compacted changelog | cleanup policy `compact`; partition count from the property |
| Retained events | `retention.ms` set; partition count from the property |
| Replicas = 1 | `min.insync.replicas` = `1` |

## 2. Web and Embedded Kafka

### 2.1 `UserStatusControllerTest`

`@WebMvcTest` (or equivalent standalone MVC) with a mocked publisher and store. Do not start Kafka.

| Case | Assert |
|:-----|:-------|
| `PUT /api/users/u42/status` with `ONLINE` / `login` | **202**; publisher called with `u42`, `ONLINE`, `login` |
| `GET` when the store is empty | **404** |
| `GET` when the store has an event | **200** and the event body |

### 2.2 `UserStatusPipelineIT`

`@SpringBootTest` with `@EmbeddedKafka` and `app.demo.generate-metrics=false`.

| Case | Assert |
|:-----|:-------|
| Publish one status for `u42` | processor applies it; `GET /api/users/u42/status` becomes **200** with `ONLINE` |
| Publish an older `version` after that | `GET` still returns the newer event |
| Second publish of a newer `ONLINE`→`AWAY` | `GET` returns `AWAY` |

Do not assert a wall-clock SLA. Poll the read model with a short bounded wait.

## 3. Automated Agent Workflow Cycle

JaCoCo `check` runs as part of `mvn clean test`. Do not skip tests (`-DskipTests`) except when an operator explicitly runs the app against a local broker.

```
[ Compile ] ──> [ mvn clean test  (Surefire + JaCoCo 80%) ]
                      │
         ┌────────────┴────────────┐
         ▼                         ▼
[ All tests pass + coverage ]   [ Failures or coverage < 80% ]
         │                         │
         ▼                         ▼
[ Gate 5: broker compose ]    [ Read stack trace ]
         (Stage 6)                 │
                                   ▼
                          [ Patch failing class ]
                                   │
                                   ▼
                          [ Retry; max 3 times ]
                                   │
                    exhausted ──> [ Safe-Stop / rollback to Gate 2 ]
```

This loop **is** the Bounded Retry in `ORCHESTRATION_MANIFEST.md`. Passing tests do **not** rewrite classes. Gate 4C (property-driven topology, no hardcoded production brokers, DLT policy) is a compliance review of the same tree, not a JUnit case — still must pass before Stage 6.
