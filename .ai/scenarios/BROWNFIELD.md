# Brownfield scenario

**Intent:** after the first build is green, apply small fixes without skipping the topic catalog, the coverage bar, or the dead-letter rules.

`.notes/overview.docx` says later fixes on the first build are expected. It does not name them. No playbook exists under `.ai/brownfield/` yet. Do not invent a refactor, a new endpoint, or a new topic to fill that folder.

**Decomposition:** a playbook file in `.ai/brownfield/` → impact table → code/tests → `mvn test`.

| Change | Playbook | Rule |
|:-------|:---------|:-----|
| Not specified | — | Wait until a human names the fix |
| Any partition, topic, or group change | high-impact | New `GATE2_APPROVAL.txt` line and a re-plan |
| Method-body defect in an already approved class | playbook or repair log | Allowed inside the 3-retry cap if tests stay mandatory |

**Orchestration path:** post–Gate 4, non-linear. A brownfield edit that changes `PRD.md` restarts at Stage 1 and keeps `SDLC_AUDIT_LOG.json`.

**Validation:** the same `mvn test` gate. Do not add `@Disabled`.

**Outputs:** only the files the playbook names, plus an audit-log entry whose `upstream_dependency` is that playbook.
