# AI Agent Instructions: Spring Boot Kafka Monitoring with Lifecycle Governance

You are an advanced, context-aware AI software engineering node operating inside a stateful orchestration graph. You will build and test a Kafka monitoring service under the specs in this folder. The product is a runnable Spring Boot 3 application for user status management and system performance monitoring. Partition counts and consumer concurrency come from configuration, not from hardcoded listener values.

Source of the first-build behavior (do not contradict them, and do not contradict the governing documents below):

* `.notes/overview.docx`
* `.notes/ABOUTME.md`
* `.notes/application.yml`
* `.notes/kafka-demo.zip` (reference Spring Boot coding example)

Governing documents (do not contradict them):

* Product: `PRD.md`
* Architecture: `SYSTEM_DESIGN.md`
* Toolchain: `TECH_STACK.md`
* Tests: `TESTING_PLAN.md`
* Lifecycle gates: `ORCHESTRATION_MANIFEST.md`

## 1. Observability, Lineage & Decision Logging

Maintain a continuous file named `SDLC_AUDIT_LOG.json` in the **workspace root**. The file is a JSON array. Append one object for every file generation, class modification, test execution, or self-correction run:

```json
{
  "timestamp": "2026-10-15T21:00:00Z",
  "stage": "IMPLEMENTATION",
  "action_taken": "Created KafkaTopicConfig.java to implement SYSTEM_DESIGN.md section 2.",
  "upstream_dependency": "SYSTEM_DESIGN.md",
  "upstream_dependency_hash": "<SHA-256 of the governing markdown file>",
  "rationale": "Topics, partition counts, and cleanup policy are declared once from application.yml."
}
```

Allowed `stage` values: `REQUIREMENTS`, `ARCHITECTURE`, `IMPLEMENTATION`, `TESTING`, `COMPLIANCE`, `OBSERVABILITY`, `RELEASE`, `REPAIR`, `REPLAN`.

Preserve historical entries. Never overwrite the log on re-plan; only append.

## 2. Reliability Metrics Tracking

Write aggregated telemetry to `SDLC_METRICS.json` in the workspace root after every execution loop. Do not leave metrics as unnamed context variables.

```json
{
  "success_rate": {
    "compiled_ok": 0,
    "syntax_failures": 0
  },
  "retry_rollback_frequency": 0,
  "mttr_ms": null,
  "stage1_epoch_ms": null,
  "stage7_epoch_ms": null,
  "end_to_end_latency_ms": null,
  "updated_at": "2026-10-15T21:00:00Z"
}
```

* **Success Rate:** successful compiled code blocks vs syntax failures generated.
* **Retry/Rollback Frequency:** increment on each Gate 4 fallback loop.
* **MTTR:** milliseconds from a failing `mvn test` line to a successful re-run of that suite.
* **End-to-End Latency:** `stage7_epoch_ms - stage1_epoch_ms` when Stage 7 completes.

`app.kafka.e2e.latency` in the running service is a product metric. It is not a substitute for `end_to_end_latency_ms` in this file.

## 3. Hard Policy Compliance Guardrails

* **Config is the catalog.** Topic names, partition counts, replica counts, retention, compaction, group ids, and listener concurrency are read from `application.yml` (`app.kafka.topics.*`, `app.kafka.groups.*`). Do not hardcode those numbers inside `@KafkaListener` or `NewTopic` beans.
* **Partitions only grow.** Do not lower a topic's partition count after Gate 2. Raising it remaps keys and is high-impact (new human signature).
* **Secret isolation.** Do not hardcode production broker addresses or credentials. Map the broker and replica count through `${KAFKA_BOOTSTRAP:localhost:9092}` and `${KAFKA_REPLICAS:1}` as defined in `TECH_STACK.md`.
* **No test bypass.** Never add `@Ignore` or `@Disabled` (JUnit 4 or 5) to make a suite pass.
* **Poison messages.** Invalid user-status records throw `IllegalArgumentException` and go to the dead-letter topic without retry. Other listener failures use exponential backoff, then `{topic}.DLT`.

## 4. Work Execution Cycle (State Gate Constrained)

1. **Gate 2 lock:** Do not generate functional Java sources, `pom.xml` application modules, or `src/**` until Gate 2 is approved. Approval is a workspace file `GATE2_APPROVAL.txt` (or an injected orchestrator token) containing a line of the form `[APPROVED: HUMAN_ENGINEER_ID]`. Documentation, this `.ai/greenfield` folder, and `docker-compose.yml` may exist before Gate 2; they do not unlock Stage 3.
2. **Bounded retry:** If compilation or tests fail, follow the 3-retry protocol in `ORCHESTRATION_MANIFEST.md`. Fix production or test code so assertions pass. Do not weaken coverage below 80%.
3. **Lineage:** After each mutation, append to `SDLC_AUDIT_LOG.json` with the SHA-256 of the markdown file that justified the change (`PRD.md`, `SYSTEM_DESIGN.md`, `TECH_STACK.md`, or `TESTING_PLAN.md`).
4. **Later fixes:** Small corrections after the first green build are brownfield playbooks under `.ai/brownfield/`. They are not silent edits to these specs.
