# Greenfield scenario

**Intent:** build the Kafka monitoring service from specs. Behavior is defined by `.notes/overview.docx` and the reference project in `.notes/kafka-demo.zip`. The Java module is now in this workspace.

**Decomposition**

1. Requirements → `.ai/greenfield/PRD.md` (Gate 1: `PUT`/`GET` user status, topic table, Java 21, Spring Boot 3, Kafka).
2. Architecture → `SYSTEM_DESIGN.md`, `TECH_STACK.md` (Gate 2: human `[APPROVED: HUMAN_ENGINEER_ID]` in `GATE2_APPROVAL.txt`).
3. Implementation in parallel (Gate 3 sync):
   - **3A** `application.yml` + `KafkaTopicConfig` / `AppKafkaProperties`
   - **3B** user status publisher, store, listeners, controller
   - **3C** metrics publisher, alerting, alert partition pin, lag and e2e gauges
4. Tests / compliance (Gate 4): `mvn test`, JaCoCo ≥ 80%, concurrency and partition counts only from configuration.

**Orchestration path:** Stages 1→2→HITL→3A/3B/3C→4. Two repairs, then a green `mvn clean test`.

**Validation:** `UserStatusStoreTest`, `AlertPublisherTest`, `MetricsListenersTest`, `UserStatusListenersTest`, `UserStatusControllerTest`, `UserStatusPipelineIT`. Command: `mvn test` on JDK 21.

**Outputs:** `pom.xml`, `application.yml`, `src/main/java/**`, `docker-compose.yml`, `GATE2_APPROVAL.txt`, audit/metrics files.

Policy: `.ai/greenfield/ORCHESTRATION_MANIFEST.md`. File mapping: `.ai/greenfield/ORCHESTRATION_ARCHITECTURE.md`.
