# Engineering Summary: Kafka Monitoring Service

## 1. Status

| Item | State |
|:-----|:------|
| Lifecycle | Stages 1–2 documents written from `.notes/overview.docx`, `.notes/ABOUTME.md`, `.notes/application.yml`, and `.notes/kafka-demo.zip`. **Gate 2 is not approved.** `GATE2_APPROVAL.txt` is absent. Stages 3–7 have not run. |
| Java / `pom.xml` / tests | **Not in this workspace.** The reference module is inside `.notes/kafka-demo.zip` only. |
| `docker-compose.yml` | **Not in this workspace.** The zip contains a broker-only compose file. |
| `SDLC_AUDIT_LOG.json` / `SDLC_METRICS.json` | **Not written.** `stage1_epoch_ms`, `stage7_epoch_ms`, and `end_to_end_latency_ms` are unset. |
| Measured coverage | **None.** Do not cite a percentage until `target/site/jacoco/jacoco.csv` exists after `mvn test`. |

## 2. Plan & Engineering Rationale

**Product:** A Spring Boot 3 process that publishes user-status events and metric samples to Kafka, consumes them with several groups, and exposes the latest user status plus Prometheus gauges. Partition counts and listener concurrency are data in `application.yml`.

**Lifecycle:** Specs live in `.ai/greenfield/`. Agents follow `ORCHESTRATION_MANIFEST.md` and `AGENTS.md`. Gate 2 is a human file. Stages 3A (topic catalog), 3B (user status), and 3C (metrics, alerts, lag) must exist before `mvn test`. Brownfield playbooks are for the small fixes the overview says will come later. None are written yet.

**Ordering:** User events are keyed by `userId`. The processor drops a record unless its `version` (event-time millis) is strictly newer. The compacted changelog keeps the latest state per user. v1 does not rebuild the in-memory map from that topic on startup.

**Critical alerts:** `AlertPublisher` writes `CRITICAL` to partition 0 instead of hashing the key, so warning traffic on the other partitions cannot fill the critical partition.

**Ambiguous choices** (recorded, not invented later): see `.ai/scenarios/AMBIGUOUS.md`. The read model stays in memory. Orchestration is policy files plus an agent, not an LLM inside the JAR.

## 3. Artifact Manifest

| Artifact | Present in this workspace? |
|:---------|:---------------------------|
| `GATE2_APPROVAL.txt` | No |
| `.ai/greenfield/` PRD, design, stack, tests, manifest, instructions | Yes |
| `.ai/greenfield/ORCHESTRATION_ARCHITECTURE.md` | Yes |
| `.ai/greenfield/KAFKA_MONITORING_SUMMARY.md` | Yes (this file) |
| `.ai/scenarios/{GREENFIELD,BROWNFIELD,AMBIGUOUS}.md` | Yes |
| `.ai/brownfield/` playbooks | No playbook yet |
| `AGENTS.md` | Yes |
| `README.md` | Yes (run steps; module not built here) |
| `.notes/overview.docx`, `ABOUTME.md`, `application.yml`, `kafka-demo.zip` | Yes (inputs, not the Stage 3 tree) |
| `pom.xml`, `src/**` | No |
| `docker-compose.yml` | No |
| JaCoCo report | No |

## 4. Risks, Trade-offs, & Validation

* In-memory status is lost on restart. The changelog is the recovery source and is not consumed at boot in v1.
* Sequential per-user ordering holds only while the partition count stays 12. Increasing it is a re-plan.
* Demo metric generation is on by default and will publish about 40 samples per second once the app and broker are up.
* Single Kafka node in compose. Replica default is 1 so that node can form a cluster of one.
* **Validation:** not run. `mvn test` and `docker compose up` are not claimed.

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
| Start Stage 3 | Halt until `GATE2_APPROVAL.txt` | Write `[APPROVED: …]` |

## 7. Lineage (how to read the audit log)

`SDLC_AUDIT_LOG.json` does not exist yet. When implementation starts it is append-only. Grep `Gate 2` / `GATE2` for the approval that unlocks `src/**`. Grep `REPAIR` for test-loop fixes. New entries carry the SHA-256 of the governing markdown.

A PRD or topic-catalog change after Gate 2 invalidates downstream hashes and requires a re-plan plus a new human signature for high-impact items.

## 8. Scenario and architecture pointers

* Greenfield path: `.ai/scenarios/GREENFIELD.md`
* Brownfield path: `.ai/scenarios/BROWNFIELD.md`
* Ambiguous decisions: `.ai/scenarios/AMBIGUOUS.md`
* DAG tied to files: `.ai/greenfield/ORCHESTRATION_ARCHITECTURE.md`
* Overview inputs: `.notes/overview.docx`, `.notes/ABOUTME.md`, `.notes/application.yml`, `.notes/kafka-demo.zip`
