# web_dashboard — Bun + React + TypeScript SPA

## Executive Summary

SPA Bun-built que provê login OIDC, página /activate (P4 device flow) e
status dos edge agents do tenant. Consumida via mesmo origin que central_api
em produção. Stack: Bun 1.3, Vite 5, React 18, TS strict, Tailwind 3.4,
shadcn/ui, TanStack Router/Query, oidc-client-ts, Zod, ESLint, Prettier,
OxLint (replaces Biome), Vitest, Playwright, msw.

## Role

**Frontend único do CnesData.** Persona primária: gestor saúde municipal.
Persona secundária: técnico hospitalar redimindo `user_code` na rota
/activate. Sem backend próprio — toda lógica em central_api (FastAPI).

## Functionalities

- `/login` — OIDC Auth Code + PKCE
- `/auth/callback` — redirect handler
- `/agentes` — status edge agents do tenant + últimas 20 execuções (Task 24)
- `/activate` — RFC 8628 device code redemption (Task 20)
- Auto-refresh 30s em /agentes via TanStack Query

## Objectives

- LCP < 1.5s, TTI < 2s
- Bundle main ≤ 200KB gzipped
- Coverage 80% line / 70% branch
- Zero contract drift (CI gate via openapi-typescript)

## Limitations

- pt-BR único locale v1.0
- Desktop primary; mobile best-effort
- Sem dark mode v1.0
- Sem WebSocket — apenas polling
- Sem charts em v1.0 (Tremor lazy em rotas v1.1)

## Requirements

**Bun 1.1+ (instalar via https://bun.sh).** Backend: central_api rodando :8000.

**Env vars (`.env.local`, prefix `VITE_`):**

| Var                      | Obrigatória | Descrição                                                    |
| ------------------------ | ----------- | ------------------------------------------------------------ |
| `VITE_API_BASE_URL`      | sim         | Default `/api/v1`                                            |
| `VITE_OIDC_AUTHORITY`    | opcional    | URL do issuer (ex.: `http://localhost:8080/realms/cnesdata`) |
| `VITE_OIDC_CLIENT_ID`    | opcional    | Default `cnesdata-dashboard`                                 |
| `VITE_OIDC_REDIRECT_URI` | opcional    | Default `http://localhost:5173/auth/callback`                |

Se `VITE_OIDC_AUTHORITY` estiver ausente o `oidc.ts` faz fail-soft (lazy
manager) — SPA carrega mas login falha controladamente.

## Module Map

| Path                        | Responsabilidade                                                                                                    |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `src/main.tsx`              | render React root                                                                                                   |
| `src/App.tsx`               | providers (Query + Auth + Router)                                                                                   |
| `src/routes/`               | TanStack Router file-based                                                                                          |
| `src/api/client.ts`         | fetch wrapper, anexa Bearer + X-Tenant-Id                                                                           |
| `src/api/generated.ts`      | types gerados via openapi-typescript (gitignored)                                                                   |
| `src/api/hooks/`            | TanStack Query hooks                                                                                                |
| `src/auth/oidc.ts`          | UserManager config (lazy)                                                                                           |
| `src/auth/AuthProvider.tsx` | context user                                                                                                        |
| `src/components/ui/`        | shadcn primitives                                                                                                   |
| `src/components/layout/`    | Shell + Sidebar + TenantPill                                                                                        |
| `src/lib/env.ts`            | Zod-validated env                                                                                                   |
| `src/lib/format.ts`         | BRL, datas pt-BR, lag                                                                                               |
| `src/i18n/pt-BR.ts`         | strings                                                                                                             |
| `tests/unit/`               | Vitest                                                                                                              |
| `tests/e2e/`                | Playwright                                                                                                          |
| `tests/mocks/`              | msw setup                                                                                                           |
| `eslint.config.js`          | ESLint flat config (typescript-eslint, react-hooks, react-refresh, jsx-a11y, import-x; eslint-config-prettier last) |
| `.prettierrc`               | Prettier 3 config + tailwindcss plugin (`endOfLine: auto` for cross-OS)                                             |
| `.prettierignore`           | mirror of common ignores                                                                                            |
| `.oxlintrc.json`            | OxLint pre-commit config (correctness=error, suspicious=warn)                                                       |

## Commands

```bash
cd apps/web_dashboard
bun install
bun run codegen     # regen src/api/generated.ts
bun run dev         # vite :5173
bun run test
bun run e2e
bun run build
bun run lint
bun run typecheck
```

## Gotchas

- **Tokens em memória apenas.** Nunca localStorage (XSS risk).
- **Codegen drift:** CI roda `git diff --exit-code src/api/generated.ts`.
  Sempre rode `bun run codegen` após mudar openapi.json.
- **TanStack Router pathless layout:** `_app.tsx` é pathless; rotas filhas
  ficam em `/agentes`, `/activate`, etc. (não `/_app/agentes`).
- **Test names em pt-BR:** convenção do projeto.
- **Função ≤ 50 linhas, file ≤ 500 linhas.**
- **Coverage 80%/70%** (Vitest).
- **Bundle ≤ 200KB main gzipped** (CI gate em Task 17).
- **Tremor lazy:** carregar só em rotas v1.1+ via `lazy()` import.
- **ESLint/Prettier/OxLint ignoram `routeTree.gen.ts` e `src/api/generated.ts`**
  — gerados automaticamente, fora do escopo de lint/format.
- **Pre-commit hook installed via `bun install` → `prepare`**: re-run if hook missing.
  In a worktree on Windows, the hook lives at `<repo-root>/.git/hooks/pre-commit`
  and runs `cd apps/web_dashboard && bunx lint-staged` so binaries resolve via
  the app's local `node_modules`.
- **OxLint pre-commit + ESLint CI**: OxLint is a subset of ESLint rules — fast
  guardrail in dev (~50ms), ESLint runs full coverage with React plugins in CI.
  Don't run both in CI; ESLint passing implies OxLint passing.
- **Prettier formats; ESLint lints**. Don't enable ESLint formatting rules —
  they're disabled by `eslint-config-prettier` (LAST entry in flat config).
- **`prettier-plugin-tailwindcss`** auto-sorts Tailwind classes; idempotent.
- **`endOfLine: "auto"`** in `.prettierrc` lets Windows + Linux both pass
  format:check despite different line endings (CRLF vs LF).
- **Keycloak dev seed** em `docker-compose.keycloak/realm.json` é apenas
  para desenvolvimento local (usuário `gestor@local`, senha `dev`). Não usar
  em produção; em prod o IdP é externo (Keycloak gerenciado pelo município).
