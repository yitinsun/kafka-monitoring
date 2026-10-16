# Engineering Summary: Kafka Monitoring Service

## 1. Status

| Item | State |
|:-----|:------|
| Lifecycle | Gate 2 approved (`GATE2_APPROVAL.txt` = `[APPROVED: yitin]`). Stages 3–5 complete. Stage 6 compose file is present; **compose was not started**. Stage 7 summary matches this run. |
| Java / `pom.xml` / tests | Present. `mvn clean test` green on JDK 21 (`C:\Program Files\Java\jdk-21.0.12.1`): **30 tests, 0 failures**. |
| `docker-compose.yml` | Present (broker only, `apache/kafka:3.8.0`, port 9092). Not executed. |
| `SDLC_AUDIT_LOG.json` / `SDLC_METRICS.json` | Written. `stage1_epoch_ms` is the first audit timestamp. `stage7_epoch_ms` is the successful `mvn clean test` finish (`2026-10-15T21:02:06-05:00`). |
| Measured coverage | **190 / 205 lines (92.68%)** from `target/site/jacoco/jacoco.csv`. JaCoCo check ≥ 80% passed. |

## 2. Plan & Engineering Rationale

**Product:** A Spring Boot 3 process that publishes user-status events and metric samples to Kafka, consumes them with several groups, and exposes the latest user status plus Prometheus gauges. Partition counts and listener concurrency are data in `application.yml`.

**Lifecycle:** Specs live in `.ai/greenfield/`. Agents follow `ORCHESTRATION_MANIFEST.md` and `AGENTS.md`. Gate 2 is a human file. Stages 3A (topic catalog), 3B (user status), and 3C (metrics, alerts, lag) must exist before `mvn test`. Brownfield playbooks are for the small fixes the overview says will come later. None are written yet.

**Ordering:** User events are keyed by `userId`. The processor drops a record unless its `version` (event-time millis) is strictly newer. The compacted changelog keeps the latest state per user. v1 does not rebuild the in-memory map from that topic on startup.

**Critical alerts:** `AlertPublisher` writes `CRITICAL` to partition 0 instead of hashing the key, so warning traffic on the other partitions cannot fill the critical partition.

**Ambiguous choices** (recorded, not invented later): see `.ai/scenarios/AMBIGUOUS.md`. The read model stays in memory. Orchestration is policy files plus an agent, not an LLM inside the JAR.

## 3. Artifact Manifest

| Artifact | Present in this workspace? |
|:---------|:---------------------------|
| `GATE2_APPROVAL.txt` | Yes (`[APPROVED: yitin]`) |
| `.ai/greenfield/` PRD, design, stack, tests, manifest, instructions | Yes |
| `.ai/greenfield/ORCHESTRATION_ARCHITECTURE.md` | Yes |
| `.ai/greenfield/KAFKA_MONITORING_SUMMARY.md` | Yes (this file) |
| `.ai/scenarios/{GREENFIELD,BROWNFIELD,AMBIGUOUS}.md` | Yes |
| `.ai/brownfield/` playbooks | No playbook yet |
| `AGENTS.md` | Yes |
| `README.md` | Yes (run steps; module is in this workspace) |
| `.notes/overview.docx`, `ABOUTME.md`, `application.yml`, `kafka-demo.zip` | Yes (inputs, not the Stage 3 tree) |
| `pom.xml`, `src/**` | Yes |
| `docker-compose.yml` | Yes (not executed) |
| JaCoCo report | Yes (`target/site/jacoco/jacoco.csv` after `mvn clean test`) |

## 4. Risks, Trade-offs, & Validation

* In-memory status is lost on restart. The changelog is the recovery source and is not consumed at boot in v1.
* Sequential per-user ordering holds only while the partition count stays 12. Increasing it is a re-plan.
* Demo metric generation is on by default and will publish about 40 samples per second once the app and broker are up.
* Single Kafka node in compose. Replica default is 1 so that node can form a cluster of one.
* Default `JAVA_HOME` on this machine is JDK 8. The green run used `C:\Program Files\Java\jdk-21.0.12.1`.
* **Validation:** `mvn clean test` passed (30 tests, JaCoCo 190/205 lines, 92.68%). Two repairs before that green run: a test helper named `put` collided with MockMvc's static `put`, then `KafkaAdmin.NewTopics` stores a `Collection` rather than a `NewTopic[]`. Docker Compose was not started.

## 5. Assumptions & Limitations

* Java **21** and Spring Boot **3.3.5**, taken from the reference `pom.xml`. The overview names Spring Boot 3 and does not name the JDK.
* Package `com.example.kafkademo` so `spring.json.trusted.packages` matches the zip.
* Local broker `localhost:9092`. Production replica count is an environment value, not a checked-in password.
* No auth, no rate limiter, no Schema Registry, no JDBC.
* Thresholds (`cpu.usage` 0.90, `latency.p99.ms` 1000, critical at 1.2×, cooldown 30 seconds) come from the reference listeners. The overview does not restate the numbers.

## 6. Human vs agent

| Action | Agent | Human |
|:-------|:------|:------|
| Method-body test repair | Yes, after Gate 2 | Review |
| New `pom` dependency / topic / partition count | Halt | Signature file |
| Coverage bar &lt; 80% or `@Disabled` | Forbidden | — |
| Merge / release | — | Operator |
| Start Stage 3 | Done under `[APPROVED: yitin]` | Signature file already present |

## 7. Lineage (how to read the audit log)

`SDLC_AUDIT_LOG.json` is append-only. Grep `REPAIR` for the two Gate 4 fixes (MockMvc `put` name clash, `NewTopics` collection cast). New entries carry the SHA-256 of the governing markdown.

A PRD or topic-catalog change after Gate 2 invalidates downstream hashes and requires a re-plan plus a new human signature for high-impact items.

## 8. Scenario and architecture pointers

* Greenfield path: `.ai/scenarios/GREENFIELD.md`
* Brownfield path: `.ai/scenarios/BROWNFIELD.md`
* Ambiguous decisions: `.ai/scenarios/AMBIGUOUS.md`
* DAG tied to files: `.ai/greenfield/ORCHESTRATION_ARCHITECTURE.md`
* Overview inputs: `.notes/overview.docx`, `.notes/ABOUTME.md`, `.notes/application.yml`, `.notes/kafka-demo.zip`
