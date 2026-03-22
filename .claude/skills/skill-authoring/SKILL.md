---
name: skill-authoring
description: >
  Guide for creating and modifying Claude Code skills, agents, and slash commands.
  Use when user says "create a skill", "new agent", "add a command", "modify skill",
  "improve the security-reviewer", or asks about skill/agent architecture.
  Does NOT activate for normal coding tasks, bug fixes, or feature implementation.
invocation: user
---

# Skills, Agents & Commands — Authoring Guide

## 1 · When to Use What

| Mechanism      | What It Is                                       | When To Use                                                    |
|----------------|--------------------------------------------------|----------------------------------------------------------------|
| **CLAUDE.md**  | Persistent context loaded every session           | Universal rules, style guides, project conventions              |
| **Skill**      | Directory with SKILL.md + optional resources      | Reusable procedural knowledge loaded on-demand                  |
| **Slash Command** | Single .md file in `.claude/commands/`         | User-invoked parameterized procedures                           |
| **Subagent**   | Isolated Claude instance with own context window  | Focused tasks that would bloat main context                     |

## 2 · Skill Architecture — Progressive Disclosure

Context window is a public good. Only `name` + `description` are pre-loaded.
SKILL.md loaded on activation. Reference files on demand. Scripts executed, never read.

```
.claude/skills/<skill-name>/
├── SKILL.md              # Core instructions (≤500 lines, ≤2000 words)
├── references/           # Deep docs read on-demand
├── scripts/              # Executable utilities (output enters context, code does NOT)
└── assets/               # Templates, schemas, configs
```

## 3 · Writing Effective Descriptions

| Quality                       | Activation Rate |
|-------------------------------|-----------------|
| Generic, vague                | ~20%            |
| Specific with trigger phrases | ~50%            |
| Specific + examples           | ~72-90%         |

Rules:
- First sentence: WHAT the skill does.
- Second sentence: WHEN Claude should use it.
- Include 3-5 trigger phrases.
- Include anti-triggers ("Does NOT...") to prevent false activations.

## 4 · SKILL.md Body — Content Principles

Include only what Claude does NOT already know. Challenge every line:
- Claude already knows this? → Delete.
- Will become outdated? → Move to reference file or use `!`command`` for live data.
- Style preference? → Put in CLAUDE.md instead.
- Executable logic? → Move to `scripts/`.

Structure: Quick Start → Workflow → Rules → Reference Files → Scripts.

## 5 · Subagent Design

File: `.claude/agents/<agent-name>.md` with YAML frontmatter.

System prompt structure:
```markdown
[1-2 sentence persona and expertise]

## Protocol
[Numbered steps]

## Rules
[Non-negotiable constraints]

## Output Format
[Headers, sections, severity labels]

## Weakness Awareness
[Known limitations and how to handle them]
```

Model selection: `haiku` (fast/cheap), `sonnet` (default), `opus` (complex reasoning), `inherit`.

Tool permissions by role:
- Reviewer/Auditor: Read, Grep, Glob (read-only)
- Researcher: Read, Grep, Glob, WebFetch, WebSearch
- Implementer: Read, Write, Edit, Bash, Glob, Grep
- Doc writer: Read, Write, Edit, Glob, Grep

## 6 · Testing Skills — Claude A/B Pattern

1. Write the skill with Claude A.
2. Test with Claude B on a REAL task.
3. Observe failures and wrong paths.
4. Diagnose with Claude A.
5. Fix and repeat until Claude B handles correctly on first attempt.

## 7 · Anti-Patterns

| Anti-Pattern                        | Fix                                              |
|-------------------------------------|--------------------------------------------------|
| Everything in SKILL.md              | ≤500 lines; detail in reference files             |
| Duplicating code in skills          | Point to actual source file with relative path    |
| Skill for what Claude already knows | Only add context Claude DOESN'T have              |
| Generic subagent persona            | Specific persona, protocol, output format         |
| Subagent with all tools             | Minimum tools for the role                        |
| No anti-triggers                    | Add "Does NOT..." to description                  |
| Testing on toy examples only        | Use Claude A/B with real project tasks            |