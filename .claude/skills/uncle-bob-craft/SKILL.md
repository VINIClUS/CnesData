---
name: uncle-bob-craft
description: Apply Robert C. Martin (Uncle Bob) criteria for code review and production — Clean Code, Clean Architecture, SOLID, design patterns, and professional craft.
risk: unknown
source: community
date_added: '2026-02-27'
---

Apply Robert C. Martin (Uncle Bob) criteria for **code review and production**: Clean Code, Clean Architecture, The Clean Coder, Clean Agile, and design-pattern discipline.

## When to Use This Skill

- **Code review**: Apply Dependency Rule, boundaries, SOLID, and smell heuristics; suggest concrete refactors.
- **Refactoring**: Decide what to extract, where to draw boundaries, and whether a design pattern is justified.
- **Architecture discussion**: Check layer boundaries, dependency direction, and separation of concerns.
- **Design patterns**: Assess correct use vs cargo-cult or overuse before introducing a pattern.
- **Do not use** to replace or override the project's linter, formatter, or automated tests.

## Source Aggregators

| Source | Focus |
|--------|--------|
| **Clean Code** | Names, functions, comments, formatting, tests, classes, smells |
| **Clean Architecture** | Dependency Rule, layers, boundaries, SOLID in architecture |
| **The Clean Coder** | Professionalism, estimation, saying no, sustainable pace |
| **Clean Agile** | Values, Iron Cross, TDD, refactoring, pair programming |
| **Design patterns** | When to use, misuse, cargo cult |

## Design Patterns: Use vs Misuse

- **Use patterns** when they solve a real design problem (variation in behavior, lifecycle, or cross-cutting concern), not to look "enterprise."
- **Avoid cargo cult**: Do not add Factory/Strategy/Repository just because the codebase "should" have them; add them when duplication or rigidity justifies the abstraction.
- **Signs of misuse**: Pattern name in every class name, layers that only delegate without logic, patterns that make simple code harder to follow.
- **Rule of thumb**: Introduce a pattern when you feel the third duplication or the second reason to change.

## Smells and Heuristics

| Smell / Heuristic | Meaning |
|-------------------|--------|
| **Rigidity** | Small change forces many edits. |
| **Fragility** | Changes break unrelated areas. |
| **Immobility** | Hard to reuse in another context. |
| **Viscosity** | Easy to hack, hard to do the right thing. |
| **Needless complexity** | Speculative or unused abstraction. |
| **Needless repetition** | DRY violated; same idea in multiple places. |
| **Opacity** | Code is hard to understand. |

## Review vs Production

| Context | Apply |
|---------|--------|
| **Code review** | Dependency Rule and boundaries; SOLID in context; list smells; suggest one or two concrete refactors; check tests present. |
| **Writing new code** | Prefer small functions and single responsibility; depend inward; write tests first when doing TDD; avoid patterns until duplication justifies them. |
| **Refactoring** | Identify one smell at a time; refactor in small steps with tests green; improve names and structure before adding behavior. |

## How to Review Code

1. **Boundaries and Dependency Rule**: Check that dependencies point inward (use cases do not depend on UI or DB details).
2. **SOLID in context**: Check SRP, OCP, LSP, ISP, DIP where they apply to the changed code.
3. **Smells**: Scan for rigidity, fragility, immobility, viscosity, needless complexity/repetition, opacity; list them with file/area.
4. **Concrete suggestions**: Propose one or two refactors (e.g., "Extract this into a function named X," "Introduce an interface so this layer does not depend on the concrete DB client").
5. **Tests and craft**: Note if tests exist and if the change respects sustainable pace (no obvious "fix later" markers).

## Example: Review Prompt

```markdown
Review this change using Uncle Bob craft criteria:
1. Dependency Rule and boundaries — do dependencies point inward?
2. SOLID in context — any violations in the touched code?
3. Smells — list rigidity, fragility, immobility, viscosity, needless complexity/repetition, or opacity.
4. Suggest one or two concrete refactors (extract function, invert dependency).
Do not duplicate lint/format; focus on structure and design.
```

## Before/After Example

**Before (opacity, does more than one thing):**

```python
def process(d):
    if d.get("t") == 1:
        d["x"] = d["a"] * 1.1
    elif d.get("t") == 2:
        d["x"] = d["a"] * 1.2
    return d
```

**After (clear intent, single level of abstraction):**

```python
def apply_discount(amount: float, discount_type: int) -> float:
    if discount_type == 1:
        return amount * 1.1
    if discount_type == 2:
        return amount * 1.2
    return amount

def process(order: dict) -> dict:
    order["x"] = apply_discount(order["a"], order.get("t", 0))
    return order
```

## Best Practices

- Use this skill for architecture, boundaries, SOLID, smells, and process — not syntax or style.
- In review, name the smell or principle (e.g., "Dependency Rule violation: use case imports from the web framework").
- Suggest at least one concrete refactor per review (extract, rename, invert dependency).
- Run the project linter and formatter separately; this skill does not replace them.
- Do not add design patterns without a clear duplication or variation reason.

## Limitations

- Does not replace the project linter or formatter.
- Does not replace automated tests.
- Focuses on structure, dependencies, smells, and professional practice — not brace style or line length.
