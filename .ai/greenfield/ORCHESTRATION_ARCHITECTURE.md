# Orchestration architecture (this repo)

`ORCHESTRATION_MANIFEST.md` is the **policy**. This page maps that DAG onto **files that exist**.

The monitoring JVM is not an LLM. Agents write the workspace; Maven and Docker execute deterministic artifacts.

```
operator
   │  GATE2_APPROVAL.txt  (HITL)
   ▼
agent (Cursor)  ──reads──►  .ai/greenfield  .ai/brownfield  AGENTS.md
   │                         writes src/, pom, tests, logs
   ▼
Maven (JDK 21) / Docker Compose (Kafka broker only)
```

## DAG and what each gate produces

```
[1 REQUIREMENTS] ── Gate 1 ──► [2 ARCHITECTURE] ── Gate 2 HITL ──► [3 IMPLEMENTATION]
                                                                      3A ║ 3B ║ 3C
                                                                      └── Gate 3 sync ──►
[4–5 TEST / COMPLIANCE] ── Gate 4 ──► [6 CONTAINERS] ── Gate 5 ──► [7 RELEASE]
        │
        └── fail ──► FALLBACK (max 3) ──► Safe-Stop
upstream spec change ──► re-plan at Stage 1 (audit log kept)
```

| Gate | Meaning here | Artifact |
|:-----|:-------------|:---------|
| 1 | APIs, topics, and groups named | `.ai/greenfield/PRD.md` |
| 2 | Human unlock | `GATE2_APPROVAL.txt` (`[APPROVED: HUMAN_ENGINEER_ID]`) |
| 3 | Topic catalog + user status + metrics/alerts together | `application.yml`, `KafkaTopicConfig`, user-status types, metrics/alert types, `pom.xml` |
| 4 | Tests + coverage + property-driven topology | `mvn test`, JaCoCo check ≥ 0.80 |
| 5 | Broker compose | `docker-compose.yml` starts Kafka on 9092. The app is `mvn spring-boot:run` on 8080. |
| 6 | Honest summary | `.ai/greenfield/KAFKA_MONITORING_SUMMARY.md` |

Stage 3 is specified to run **in parallel** (3A catalog, 3B user status, 3C metrics and alerts) then sync. That implementation has **not** been generated in this workspace. The behavioral reference is `.notes/kafka-demo.zip`.

## Lineage

`SDLC_AUDIT_LOG.json` is append-only. Fields: `timestamp`, `stage`, `action_taken`, `upstream_dependency`, `upstream_dependency_hash`, `rationale`.

A change to `PRD.md` should invalidate downstream hashes and restart at Stage 1. Grep the log for `Gate 2`, `REPAIR`, `brownfield`, `REFACTOR`, `ENHANCE`.

New entries should SHA-256 the governing markdown.

## Human vs agent

| Action | Agent | Human |
|:-------|:------|:------|
| Method-body test repair | Yes | Review |
| New `pom` dependency / topic / partition count | Halt | Signature file |
| Coverage bar &lt; 80% or `@Disabled` | Forbidden | — |
| Merge / release | — | Operator |

High-impact: topic or partition changes after Gate 2, undeclared `pom` deps, lowering the coverage bar.

## Bounded repair

Max **3** test-repair loops, then Safe-Stop (freeze writes, do not start Stage 6). No repair loop has run in this workspace.

## Re-plan example (brownfield)

`.notes/overview.docx` says later small fixes are expected on the first build. Those land as playbooks in `.ai/brownfield/` after Gate 4. Changing a partition count or adding a durable status store is a re-plan plus a new human signature, not a path-only patch.

## Metrics

`SDLC_METRICS.json`: compile success vs syntax failure, `retry_rollback_frequency`, `mttr_ms`, stage timestamps, coverage notes. Stage 7 end-to-end latency stays **unset** until a recorded release pass. Neither file exists until Stage 3 starts.
