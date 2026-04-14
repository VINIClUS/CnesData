---
allowed-tools: Read, Grep, Glob, Bash(pytest:*), Bash(ruff:*), Bash(git:*), Write, Edit, Task
description: |
  Full feature implementation lifecycle: research → plan → TDD → security review → commit.
  Use when implementing a new feature, work package, or audit rule from scratch.
  Triggers: "implement feature", "new work package", "WP-", "build the", "full lifecycle".
  Does NOT activate for: bug fixes (use /tdd directly), doc changes, config tweaks,
  or exploratory investigation (use /research).
---

# Full Feature Lifecycle

Implement the following feature using the complete development lifecycle:

**Feature:** $ARGUMENTS

---

## Stage 1 — RESEARCH (Subagent: Explore)

Spawn a read-only subagent to investigate the codebase:

1. Find existing patterns related to this feature in `src/` and `tests/`.
2. Identify files that will need creation or modification.
3. Map dependencies and integration points with existing modules.
4. Check `docs/data-dictionary-firebird-bigquery.md` for any schema/column requirements.
5. Check if Firebird quirks apply (consult `.claude/agents/cnes-domain-expert.md` mentally).

**Output:** A summary with file paths, key components, and how they connect. Under 500 words.

---

## Stage 2 — PLAN (confirm with user)

Based on the research summary, produce a written plan:

### 2a. Test cases (behaviors to verify)
- Happy paths — expected inputs → expected outputs.
- Edge cases — empty DataFrames, null values, boundary conditions.
- Error cases — invalid input, missing dependencies.

### 2b. Files to create or modify
- New test file(s): `tests/<layer>/test_<module>.py`
- New/modified source file(s): `src/<layer>/<module>.py`
- Config changes (if any): `src/config.py`, `.env.example`
- Integration in `src/main.py` (if the feature produces new exports)

### 2c. Risks and open questions
- Flag anything ambiguous. Ask the user before proceeding.

**STOP HERE.** Present the plan and wait for user confirmation before coding.

---

## Stage 3 — IMPLEMENT (TDD — strict Red/Green/Refactor)

Follow `.claude/skills/tdd-workflow/SKILL.md` exactly:

### Phase 1 — RED
Write ALL failing tests FIRST. Import from modules that don't exist yet.
Run: `pytest tests/test_<module>.py -x --tb=short -q`
All tests MUST fail. Commit: `git add tests/ && git commit -m "test(<scope>): red — failing tests for <feature>"`

### Phase 2 — GREEN
Write implementation to make every test pass.
Do NOT modify or delete any test from Phase 1.
Run after each change: `pytest tests/test_<module>.py -x --tb=short -q`
Then full suite: `pytest --tb=short -q`
Commit: `git add . && git commit -m "feat(<scope>): green — implementation for <feature>"`

### Phase 3 — REFACTOR
Clean up without changing behavior. Run tests after each refactoring step.
If any test turns red, UNDO immediately.
Commit: `git add . && git commit -m "refactor(<scope>): clean up <feature>"`

### Phase 4 — VERIFY
```bash
ruff check . --fix && ruff format .
pytest --cov=app --cov-report=term-missing --cov-fail-under=80 -v
```

---

## Stage 4 — SECURITY REVIEW (Subagent: security-reviewer)

Spawn the security-reviewer agent to audit the changes:

```
Review the changes from the last 3 commits for security issues.
Focus on: input validation, SQL injection, secrets exposure, access control.
```

If CRITICAL or HIGH findings are reported:
1. Fix them immediately (write a failing test first, then fix).
2. Re-run the security review on the fix.
3. Do not proceed to Stage 5 until findings are resolved.

MEDIUM findings: note them but proceed. LOW/INFO: acknowledge in commit.

---

## Stage 5 — FINAL COMMIT & REPORT

1. Squash WIP commits if any exist.
2. Final commit message: `feat(<scope>): <concise description of feature>`
3. Report to user:
   - Tests passed (count + coverage %).
   - Security review result (clean / findings addressed).
   - Files created or modified.
   - Any follow-up items noted.