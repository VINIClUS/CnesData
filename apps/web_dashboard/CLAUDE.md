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

- `/` — landing pública (hero + recursos + benefícios + CTA)
- `/recursos` — capacidades com estágio explícito ("Em desenvolvimento") + fluxo coleta → processamento → consulta
- `/sobre` — página editorial: motivação, público, abordagem, estágio atual
- `/contato?interesse=acesso-antecipado|piloto|contato` — formulário de interesse (`LeadForm`); valor desconhecido cai em `contato`
- `/privacidade`, `/termos`, `/ajuda` — páginas legais/ajuda (`LegalPage`), conteúdo em `i18n/legal.ts`
- `/precos` — planos, trust strip e FAQ. Sem divulgação interna (fora de menu, rodapé e CTAs); acessível por URL direta; `noindex` via `head()` + `X-Robots-Tag` (nginx)
- `/login` — OIDC Auth Code + PKCE; formulário visual, e-mail vira `login_hint`
- `/auth/callback` — redirect handler
- `/agentes` — status edge agents do tenant + últimas 20 execuções (Task 24)
- `/activate` — RFC 8628 device code redemption (Task 20)
- `/overview` — KPIs do tenant + faturamento area chart 12m por estabelecimento (v1.1)
- `/access-pending` — solicitação de acesso a um município, fluxo JIT (v1.1)
- Dark mode 3-state (light/dark/system) — toggle no header, persiste em localStorage (v1.1)
- Auto-refresh 30s em /agentes via TanStack Query

## Objectives

- LCP < 1.5s, TTI < 2s
- Bundle main ≤ 200KB gzipped
- Coverage 80% line / 70% branch
- Zero contract drift (CI gate via openapi-typescript)

## Limitations

- pt-BR único locale v1.0
- Desktop primary; mobile best-effort
- Sem WebSocket — apenas polling
- Aprovação de signup via SQL admin manual em v1.1 (UI em v1.2)
- Tremor v3 lazy-loaded só em /overview; outras rotas seguem shadcn nativo

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

| Path                            | Responsabilidade                                                                                                               |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `src/main.tsx`                  | render React root                                                                                                              |
| `src/App.tsx`                   | providers (Theme + Query + Auth + Router)                                                                                      |
| `src/routes/`                   | TanStack Router file-based                                                                                                     |
| `src/routes/access-pending.tsx` | top-level (fora `_app` guard) — formulário de signup                                                                           |
| `src/routes/_app.overview.tsx`  | /overview KPIs + lazy faturamento chart (v1.1)                                                                                 |
| `src/api/client.ts`             | fetch wrapper, anexa Bearer + X-Tenant-Id                                                                                      |
| `src/api/generated.ts`          | types gerados via openapi-typescript (gitignored)                                                                              |
| `src/api/hooks/`                | TanStack Query hooks (inclui useOverview, useFaturamentoChart, useAccessRequests, useSubmitAccessRequest, useAvailableTenants) |
| `src/auth/oidc.ts`              | UserManager config (lazy)                                                                                                      |
| `src/auth/AuthProvider.tsx`     | context user                                                                                                                   |
| `src/theme/ThemeProvider.tsx`   | 3-state (light/dark/system) + localStorage + matchMedia (v1.1)                                                                 |
| `src/theme/useTheme.ts`         | hook export                                                                                                                    |
| `src/components/ui/`            | shadcn primitives                                                                                                              |
| `src/components/layout/`        | Shell + Sidebar + TenantPill + ThemeToggle                                                                                     |
| `src/components/brand/`         | Logo / LogoMark (SVG inline)                                                                                                   |
| `src/components/marketing/`     | Navbar, Footer, HeroSurface (dark forçado), CtaBand, FeatureItem, DashboardPreview estático                                    |
| `src/components/landing/`       | LandingPage (`/`)                                                                                                              |
| `src/components/pricing/`       | PricingPage (`/precos`) + PricingCard + FaqList                                                                                |
| `src/components/resources/`     | ResourcesPage (`/recursos`) — blocos alternados + fluxo, visuais ilustrativos reaproveitando `preview/*`                        |
| `src/components/about/`         | AboutPage (`/sobre`) — corpo editorial `max-w-[720px]`                                                                        |
| `src/components/contact/`       | ContactPage (`/contato`) + LeadForm/useLeadForm/leadSchema; `contactInterest.ts` valida `?interesse`                          |
| `src/components/login/`         | LoginPage + LoginForm (senha renderizada, nunca enviada)                                                                       |
| `src/components/signup/`        | AccessRequestForm + PendingRequestsList (v1.1)                                                                                 |
| `src/components/overview/`      | KpiCard + KpiGrid + FaturamentoAreaChart (Tremor lazy) (v1.1)                                                                  |
| `src/lib/env.ts`                | Zod-validated env                                                                                                              |
| `src/lib/format.ts`             | BRL, datas pt-BR, lag                                                                                                          |
| `src/i18n/pt-BR.ts`             | strings do painel; `marketing.ts`, `landing.ts`, `pricing.ts`, `login.ts` para páginas públicas                                |
| `tests/unit/`                   | Vitest                                                                                                                         |
| `tests/e2e/`                    | Playwright                                                                                                                     |
| `tests/mocks/`                  | msw setup                                                                                                                      |
| `eslint.config.js`              | ESLint flat config (typescript-eslint, react-hooks, react-refresh, jsx-a11y, import-x; eslint-config-prettier last)            |
| `.prettierrc`                   | Prettier 3 config + tailwindcss plugin (`endOfLine: auto` for cross-OS)                                                        |
| `.prettierignore`               | mirror of common ignores                                                                                                       |
| `.oxlintrc.json`                | OxLint pre-commit config (correctness=error, suspicious=warn)                                                                  |

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
- **Hard limits e convenção pt-BR de test names:** ver `CLAUDE.md` raiz —
  não duplicar aqui.
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
- **Signup aprovação manual v1.1**: `dashboard.access_requests` row criada
  via POST /api/v1/access-requests; admin executa SQL em
  `docs/runbooks/access-request-approval.md` para approve/reject (UI em v1.2).
- **`@tremor/react` lazy-loaded**: importar via `lazy(() => import(...))`
  apenas em rotas que usam charts (hoje só /overview). Tremor entra em chunk
  próprio no `manualChunks` do `vite.config.ts`; bundle main fica fora.
- **Páginas públicas (`/`, `/recursos`, `/sobre`, `/contato`, `/precos`, `/login`) usam `HeroSurface`** com classe `dark`
  literal: hero/footer/CTA ficam navy em qualquer tema; seções claras seguem o toggle.
  Cores de marca via `--navy*`/`--brand*` em `styles.css` (aliases `bg-navy`, `text-brand`).
- **`HeadContent` vai para `document.head` via `createPortal` em `__root.tsx`**: React 18 não hoista
  `<title>`/`<meta>`. `index.html` não tem `<title>` estático; o root route define o fallback e cada
  rota pública define o seu em `head()`. Só `/precos` emite `robots: noindex`.
- **Navegação pública é a lista estática `marketingNavigation.ts`** (Início, Recursos, Sobre, Contato).
  Não derivar do route tree; não reintroduzir `/precos` em header, rodapé ou CTAs.
- **Leads: `POST ${VITE_API_BASE_URL}/public/leads`** (`src/api/marketingLeads.ts`) → `central_api`
  `routes/public_leads.py`. Falha real (422/429/5xx/rede) sem simular sucesso; "Tentar novamente"
  só para indisponibilidade/rede; "Enviar e-mail" sempre. Hosts de API vêm só de
  `VITE_API_BASE_URL` (build-arg do Dockerfile; dev = `https://api.dev.vinisantana.com/api/v1`,
  prod = `https://api.vinisantana.com/api/v1`, local = `/api/v1` via proxy do Vite/nginx).
- **CSP `connect-src`** recebe `${API_ORIGIN}` em runtime (`nginx/entrypoint.sh`); o compose de
  cada ambiente define `API_ORIGIN`. Sem ele, chamadas cross-origin à API são bloqueadas.
- **`noindex` de `/precos` é pré-lançamento e configurável**: meta via build-arg
  `VITE_PRECOS_NOINDEX` (default `true`), header via env de runtime `PRECOS_NOINDEX` (default
  `true`; `false` remove o header). Não é mecanismo de segurança.
- **Termos/Privacidade/Ajuda são rotas** (`/termos`, `/privacidade`, `/ajuda`, `components/legal/`).
  Seções com `pending: true` em `i18n/legal.ts` marcam pontos de negócio ainda não definidos.
- **Fonte Inter self-hosted** (`@fontsource-variable/inter` em `main.tsx`) — CSP não permite
  Google Fonts.
- **Dark mode FOUC script** inline em `index.html` `<head>` aplica classe
  `.dark` antes do React montar — evita flash branco em system/dark.
  Mantém em sync com a chave `localStorage["cnesdata-theme"]`.
- **`/access-pending` fica FORA do `_app` guard**: usuário sem tenant pode
  acessar mesmo após login (caso JIT user sem aprovação). `_app.tsx`
  redireciona para lá quando `tenant_ids` é vazio.
- **Per-chunk bundle budget**: main ≤ 200KB gzipped, tremor ≤ 100KB,
  recharts ≤ 100KB, qualquer rota ≤ 100KB (gate em `scripts/bundle-check.ts`).
