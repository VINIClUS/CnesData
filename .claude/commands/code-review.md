---
allowed-tools: Read, Grep, Glob, Bash(ruff:*), Bash(pytest:*), Bash(git:*)
description: Review code changes using the code-reviewer agent before committing
---

Review the following using the code-reviewer agent:

$ARGUMENTS

If no arguments are given, review all uncommitted changes (`git diff HEAD`).

Delegate fully to the **code-reviewer** agent. The agent will:
1. Run `ruff check` on changed Python files and report violations.
2. Run the targeted test suite for changed modules.
3. Check each changed file against CLAUDE.md hard limits (line length, function size, nesting depth, complexity).
4. Flag anti-patterns, security issues, and convention mismatches specific to this CNES pipeline.
5. Output a structured report: PASS / WARN / FAIL per file, with actionable fix instructions.

Do NOT fix issues directly — report them for the developer to address.
