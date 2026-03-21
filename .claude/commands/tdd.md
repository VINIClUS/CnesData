---
allowed-tools: Bash(pytest:*), Bash(ruff:*), Bash(git:*), Write, Edit
description: Implement a feature using strict TDD (Red → Green → Refactor)
---

Implement the following using strict Test-Driven Development:

$ARGUMENTS

Follow the <workflow> TDD protocol from .claude\skills\tdd-workflow\SKILL.md exactly:
1. Phase 0: Analyze the requirement and plan test cases — present the plan for confirmation.
2. Phase 1 (RED): Write ALL failing tests FIRST. Run them. Confirm all fail. Commit.
3. Phase 2 (GREEN): Write minimal implementation. Do NOT modify tests. Run until all pass. Commit.
4. Phase 3 (REFACTOR): Clean up. Run tests after each change. Commit.
5. Phase 4 (VERIFY): Run lint, type check, full suite with coverage. Report results.

For each new behavior or bugfix after initial implementation, repeat from Phase 1.
A bugfix ALWAYS starts with a failing test that reproduces the bug.

CRITICAL: Do NOT write implementation before tests. Do NOT modify tests to make them pass.