# Agentic Orchestration & Lifecycle Governance Manifest

This manifest defines the stateful execution graph, gate criteria, and governance constraints for the multi-agent system automating the Kafka monitoring SDLC.

## 1. Lifecycle State Machine Graph (DAG)

```
[1. REQUIREMENTS] ──(Gate 1)──> [2. ARCHITECTURE] ──(Gate 2: HITL)──> [3. IMPLEMENTATION]
       ▲                                    │                              │
       │                                    │                         (Gate 3: sync)
       │                                    ▼                              ▼
       │                         [FALLBACK & REPAIR] <── (Gate 4 fail)── [4 & 5. TEST & COMPLIANCE]
       │                                                                   │
       │                                                              (Gate 4 pass)
       │                                                                   ▼
[7. RELEASE READY] <──(Gate 6)── [6. OBSERVABILITY / CONTAINERS] <──(Gate 5)┘
       ▲
       └── (Dynamic Re-plan Trigger: return to Stage 1)
```

## 2. Lifecycle Stage Specifications & Gate Governance

### Stage 1: Requirements Analysis (`PRD.md`)
* **Agent Context:** Product Manager Agent.
* **Exit Gate 1 (Automated):** Every functional requirement maps to a named API, topic, or consumer group. Core constraints (Java 21, Spring Boot 3.3.x, Kafka topic partition counts, listener concurrency) are explicit in `PRD.md`.

### Stage 2: Architecture & Design (`SYSTEM_DESIGN.md` / `TECH_STACK.md`)
* **Agent Context:** System Architect Agent.
* **Dependencies:** Wait until Gate 1 passes.
* **Exit Gate 2 (CRITICAL — Human-in-the-Loop):**
  * Halt Stage 3. Emit a structural delta of topics, partition counts, consumer groups, and `pom.xml` dependency footprint versus the previous approved baseline.
  * Human writes `GATE2_APPROVAL.txt` in the workspace root with `[APPROVED: HUMAN_ENGINEER_ID]` (replace with the engineer identifier).
  * On approval, the orchestrator records a **Gate 2 snapshot** (git tag `gate2-approved` or a documented workspace snapshot) used as the Safe-Stop rollback point. Then unlock Stage 3.

### Stage 3: Implementation (Parallel Path Execution)
* **Agent Context:** Software Engineer Agents (parallel).
  * *Sub-Path 3A:* `src/main/resources/application.yml` plus `KafkaTopicConfig` / `AppKafkaProperties` from `SYSTEM_DESIGN.md` (partition catalog, compaction, retention, DLT topics).
  * *Sub-Path 3B:* User status path (`UserStatusPublisher`, `UserStatusStore`, `UserStatusListeners`, `UserStatusController`) and the record listener factory.
  * *Sub-Path 3C:* Metrics, alerts, and monitoring (`MetricsPublisher`, `MetricsListeners`, `AlertPublisher`, `AlertListener`, `MetricsAggregator`, `ConsumerLagMonitor`) and the batch listener factory.
* **Exit Gate 3 (Synchronization Barrier):** Paths 3A, 3B, and 3C must all complete. The orchestrator merges file tokens into one workspace context before Stage 4. Partial delivery (listeners without the topic catalog, or a controller without `pom.xml`) does not pass Gate 3.

### Stage 4 & 5: Testing, Validation & Policy Compliance (`TESTING_PLAN.md`)
* **Agent Context:** QA Engineer Agent and Compliance/Security Guardrail Agent.
* **Exit Gate 4 (Automated):**
  * Check 4A: `mvn clean test` completes with **100%** test success (zero failures, zero `@Disabled` / `@Ignore`).
  * Check 4B: JaCoCo fails the build unless **line coverage >= 80%**.
  * Check 4C: Partition counts and concurrency are property-driven; no production broker credentials hardcoded; invalid status events are non-retryable; dead-letter destination is `{topic}.DLT`.

### Stage 6: Observability & Containers
* **Agent Context:** Release / Platform Agent.
* **Exit Gate 5 (Automated):**
  * `SDLC_AUDIT_LOG.json` and `SDLC_METRICS.json` exist and contain Stage 3–5 entries.
  * `docker-compose.yml` starts a single-node Kafka broker reachable at `localhost:9092` (image `apache/kafka:3.8.0`).
  * The Spring Boot process is started with Maven (`mvn spring-boot:run`), not as a second compose image, unless a later playbook adds one.
  * App health: the process listens on **8080**. Actuator exposes `health`, `metrics`, and `prometheus`.

### Stage 7: Release Ready
* **Agent Context:** Orchestrator + human operator.
* **Exit Gate 6:** `KAFKA_MONITORING_SUMMARY.md` is updated to match **actual** artifacts and measured coverage (no invented percentages). Gate 6 fails if the summary claims files or metrics that are not in the workspace.

## 3. Resilience, Self-Correction & Fault Management

### Bounded Retries & Fallback Strategy
* **Scope:** Compilation or test failures in Stage 4.
* **Maximum Loop Count:** **3 retries**.
* **Self-Correction Policy:** On failure, the QA Agent pipes Maven logs and stack traces to the Software Engineer Agent. The Engineer Agent patches the failing class, appends `SDLC_AUDIT_LOG.json`, increments `retry_rollback_frequency`, and records MTTR when the suite passes.

### Fallback, Rollback & Safe-Stop Control
* **Trigger:** Three retries exhausted without a green `mvn clean test`.
* **Rollback Protocol:**
  1. Halt all code write streams immediately.
  2. Reset the workspace to the **Gate 2 snapshot** (`gate2-approved` tag, or the recorded pre-Stage-3 tree). Do not reset `.ai/greenfield` specs unless they changed mid-run.
  3. Enter **Safe-Stop Mode:** freeze background agent loops, write a high-severity entry to `SDLC_AUDIT_LOG.json`, and emit the full error trace. Do not start Stage 6.

## 4. Dynamic Re-Planning Loop
* **Trigger:** An upstream spec changes during a run (for example `PRD.md` adds a durable user-status database, or a topic's partition count changes). Those are a re-plan, not a silent patch.
* **Behavior:** Invalidate downstream file hashes recorded in `SDLC_AUDIT_LOG.json`, halt generation, keep the historical log, and restart at **Stage 1**.

## 5. Controlled Autonomy Rules & Approval Gates

```
[Agent Action Pipeline] ──> [Impact Evaluation]
                                   │
         ┌─────────────────────────┴─────────────────────────┐
         ▼ (High Impact)                                     ▼ (Low Impact)
[HALT Execution Loop]                               [Execute Autonomously]
         │                                                   │
         ▼                                                   ▼
[Verify GATE2 / human signature]                    [Log Action & Telemetry]
         │                                                   │
         └─────────────────────────┬─────────────────────────┘
                                   ▼
                       [Commit State Changes]
```

### 5.1 High-Impact Threshold Actions (Human Intervention Required)
Pause workers and wait for a human signature file for:
* **Topic topology changes:** any add/modify/remove of topic names, partition counts, replica policy, or compaction after Gate 2.
* **Group contract changes:** group id or concurrency that no longer divides the partition count as specified in `SYSTEM_DESIGN.md`.
* **Dependency modifiers:** packages in `pom.xml` outside the baseline in `TECH_STACK.md`.
* **Test-bar reduction:** lowering coverage below 80% or deleting mandatory cases in `TESTING_PLAN.md`.

### 5.2 Low-Impact Autonomous Actions (Machine Execution Approved)
Allowed without halt if logged in `SDLC_AUDIT_LOG.json`:
* Refactoring method bodies to fix compile or test failures.
* Adding tests **above** the mandatory matrix.
* Adding log or metric hooks that do not change API, topic, or group contracts.
* Small post-build fixes that follow an approved `.ai/brownfield/` playbook and do not cross §5.1.
