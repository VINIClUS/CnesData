# Worktree ownership policy

This policy governs CND task dispatch, branch creation, shared-file ownership, and
integration order.

## Concurrency

At most three feature worktrees may run at once. One controller lane is retained for
integration, review, and full-suite verification.

Each worktree owns one independently reviewable task. Tasks that edit the same aggregate or
bootstrap file stay in one worktree or run sequentially. Parallel tasks must not consume unstable
interfaces or compete for paths.

## Branch naming and dependencies

Branches must match `^(feat|fix|test|docs)/cnd-[0-9]{3}-[a-z0-9-]+$`.

Valid examples cover every permitted prefix:

- `feat/cnd-020-sqlite-control-plane`
- `fix/cnd-021-control-plane-retry`
- `test/cnd-022-control-plane-race`
- `docs/cnd-003-worktree-ownership`

Every branch starts from the latest green `develop` commit containing all declared dependencies.
Dependent work never starts from an unmerged branch, and dependent branches are not pre-created
from stale integration heads.

## Controller-lane ownership

The following surfaces belong to the controller lane unless an issue explicitly grants ownership:

| Surface | Controller-owned scope |
|---|---|
| Dependency manifests | Root and package `pyproject.toml` files |
| Locks | `uv.lock`, Go module lock changes spanning another task, and frontend lockfiles |
| Shared package exports | Package `__init__.py` exports used by multiple tasks |
| Composition roots | Application bootstrap and dependency-injection composition roots |
| Generated contracts | Generated OpenAPI and JSON Schema artifacts |
| Root planning docs | Root documentation indexes and roadmap |
| Delivery configuration | Docker Compose, CI workflows, and deployment-wide configuration |

Feature work exposes new modules through direct imports in its tests. A serial integration task
updates controller-owned surfaces after feature review.

## Controller queue

The fixed integration order is `CND-064` → AWS Task 8 → Billing Task 6 → Source Task 4 →
Billing Task 13 → Billing Task 17. Each queued task starts only after the previous item is on
green `develop` and must preserve every previously composed profile.

## Definition of ready

A task is ready for dispatch only when:

- all `Depends on` tasks are merged into `develop`;
- consumed interfaces exist at the documented signatures;
- allowed paths do not overlap another active task;
- the baseline test command is known and runnable;
- fixtures or external emulators required by the task are available;
- the acceptance criteria can be verified without another unmerged branch.

## Definition of done

A task is done only when:

- behavior and negative tests pass;
- relevant contract, property, race, security, and recovery tests pass;
- lint, type, coverage, and build gates pass;
- no forbidden dependency or legacy coupling was introduced;
- generated artifacts are updated by the integration lane when applicable;
- the PR documents consumed and produced interfaces;
- the wave-level suite remains green after integration.

A phase is done only when its final gate is green on integrated `develop`, not when
individual worktrees pass in isolation.
