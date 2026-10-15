# Ambiguous requirements — decisions

Intent: `.notes/overview.docx` names the two features and the topic table, and points at `.notes/kafka-demo.zip` for the code. Several behaviors exist only in that zip. This file records the v1 pick. Topic topology and new public APIs stay halted until a human signature.

Lifecycle: `REQUIREMENTS` / `REPLAN` → log in `SDLC_AUDIT_LOG.json` once that file exists. No silent partition changes.

---

## Read model

**Vague phrase:** "user status management" with no database.

| Option | Risk |
|:-------|:-----|
| A. In-memory map, changelog written but not read on startup | Status disappears on restart |
| B. Rebuild from `user-status-changelog` at boot | Extra consumer; still not a system of record |
| C. Redis or JDBC as the current status | New dependency and schema (high impact) |

**v1 pick: A.** `UserStatusStore` is a `ConcurrentHashMap`. The processor writes the compacted changelog. A boot rebuild is a re-plan.

---

## JDK

**Vague phrase:** "Spring Boot 3" with no Java version. The zip's `pom.xml` sets Java **21** and Boot **3.3.5**.

**v1 pick:** Java 21 and Boot 3.3.5. Do not retarget Java 17 unless the human changes `TECH_STACK.md`.

---

## HTTP status of `PUT`

**Vague phrase:** the overview shows a `curl -X PUT` and does not name the status code. The reference controller returns **202** because the read model catches up after the produce.

**v1 pick:** **202 Accepted** on `PUT`, **200** or **404** on `GET`.

---

## Alert numbers

**Vague phrase:** "checks thresholds, with a 30-second cooldown". The overview does not name the thresholds. The reference listener uses `cpu.usage` **0.90**, `latency.p99.ms` **1000**, and `CRITICAL` when the value is above **1.2×** the threshold.

**v1 pick:** those three numbers. Changing them is a product change to `PRD.md`.

---

## Demo traffic

**Vague phrase:** system performance monitoring, without saying whether the process fabricates samples.

**v1 pick:** `app.demo.generate-metrics` defaults to **true**, matching the zip. Tests that boot the application set it to **false**.

---

## Orchestration runtime

**Vague phrase:** lifecycle documents versus files on disk.

**v1 pick:** Cursor agent + `ORCHESTRATION_MANIFEST.md` + `GATE2_APPROVAL.txt` + append-only `SDLC_AUDIT_LOG.json` / `SDLC_METRICS.json`. Not an LLM inside the Spring process.

---

## Containers

**Vague phrase:** "run docker compose up -d and mvn spring-boot:run".

**v1 pick:** Compose starts Kafka only (`apache/kafka:3.8.0`, port 9092). The app stays a host Maven process on port 8080. An app `Dockerfile` is a later playbook.

---

## Follow-through not taken

A startup consumer that rebuilds `UserStatusStore` from `user-status-changelog` was considered. It changes the failure mode of the read model and needs a new signature. Not in v1.
