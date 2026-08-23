# CnesData Billing and Entitlements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Entregar `BIL-010` a `BIL-024`: billing local desativado, projeção de entitlements e quotas transacionais no perfil AWS, fluxos Stripe hospedados, webhooks seguros e recuperáveis, revogação com fencing e testes E2E de lifecycle.

**Architecture:** O domínio puro em `cnes_domain.billing` define modelos imutáveis, políticas, gates e ports sem importar Stripe, FastAPI, boto3 ou storage. Adapters em `cnes_infra.billing` usam a tabela DynamoDB de control plane como projeção runtime e inbox transacional, enquanto a API apenas autentica, valida raw webhook body e delega; um worker separado processa projeção, recovery, reconciliação e revogação. Gates críticos fazem leitura consistente e bypass de cache; operações de UI podem usar cache local de no máximo 60 segundos.

**Tech Stack:** Python 3.13, dataclasses/Enums/PEP 544, FastAPI, Pydantic 2.10+, Stripe Python SDK oficial, boto3/DynamoDB transactions and Streams, Step Functions, OpenTelemetry/CloudWatch EMF, pytest 9, pytest-asyncio, Hypothesis, Stripe test clocks, uv, Ruff.

**Spec:** `docs/superpowers/specs/2026-08-16-stripe-billing-entitlements-design.md`; `docs/superpowers/specs/2026-08-16-parquet-data-plane-orchestration-design.md`; `docs/superpowers/specs/2026-08-23-cnesdata-redesign-execution-design.md`

## Global Constraints

- A base de integração é `develop`; o baseline observado para este plano é `f1ca71bb4277e9b1354fa11d8997a00871fa6c36`, mas cada branch começa no último `develop` verde contendo todas as dependências declaradas.
- O perfil local usa exatamente `BILLING_MODE=disabled`, não exige secrets e não faz chamadas à Stripe, Secrets Manager ou qualquer validador remoto.
- O perfil AWS usa exatamente `BILLING_MODE=stripe`; Stripe é a fonte financeira e `EntitlementSnapshot` no DynamoDB é a autoridade runtime.
- Stripe Entitlements representa features booleanas; quotas numéricas pertencem a uma `PlanVersion` interna, publicada e imutável.
- O redirect de Checkout nunca concede acesso; somente webhook assinado e projetado pode alterar o snapshot.
- Gates críticos — criar run, registrar agent, reservar budget e publicar — usam base key com leitura consistente ou DynamoDB transaction e nunca cache, GSI ou TTL para correção.
- Cache local só atende leitura de UI ou emissão de URL/cookie, tem TTL máximo de 60 segundos e chave `(billing_account_id, entitlement_version)`.
- `admin_revoked` bloqueia imediatamente novo compute, serving e publicação; cancelamento normal em fim de período preserva acesso até `period_end`.
- Revogação invalida fences antes de tentar cancelar Step Functions; falha do cancelamento não permite publicação por worker antigo.
- Reserva de quota é atômica e idempotente; conclusão consome, cancelamento libera saldo não consumido e reconciliation recupera reservas abandonadas.
- Eventos Stripe duplicados não reaplicam efeitos; eventos fora de ordem convergem consultando Subscription e active entitlements atuais na Stripe.
- Webhook valida `Stripe-Signature` sobre bytes raw com o SDK oficial antes de persistir ou responder sucesso.
- Dados de cartão não entram em logs, DynamoDB ou S3; metadata Stripe contém apenas IDs opacos.
- Secrets Stripe ficam no AWS Secrets Manager e URLs de retorno são escolhidas de allowlist server-side.
- Logs operacionais são estruturados; toda mutação financeira ou de autorização grava audit event via outbox durável.
- Não introduzir Redis, CloudFront invalidation como autorização, quotas em JWT, múltiplos payment providers, preços comerciais ou infraestrutura AWS de produção.
- Python exige 100% de branch coverage nos packages já cobertos e 90% de line coverage nos apps; function body ≤ 50 linhas, complexidade ciclomática ≤ 10, linha ≤ 100 caracteres, arquivo ≤ 500 linhas, ≤ 4 parâmetros e nesting ≤ 3.
- O limite de quatro parâmetros vale para toda função e construtor novos, desconsiderando `self`; entradas de mutação com mais campos usam os command value objects imutáveis deste plano, e serviços com mais de quatro colaboradores recebem um único frozen dependency value object, nunca uma lista crescente de dependências escalares.
- Testes recebem nomes descritivos em Português; código e comentários ficam em Inglês; mensagens de erro usam `key=value`.
- Commits usam `<type>(<scope>): <description>` e nunca vão diretamente para `main`.
- Cada mutação auditável preserva ator, reason code e contexto de negócio; perda de assinatura não exclui datasets automaticamente.

---

## Repository Baseline and Entry Gates

O branch `develop` foi inspecionado via conector GitHub, sem mutação remota. Os paths existentes relevantes são:

- `packages/cnes_domain/src/cnes_domain/{config.py,models,ports}`;
- `packages/cnes_infra/src/cnes_infra/{config.py,storage,telemetry.py}`;
- `apps/central_api/src/central_api/{app.py,deps.py,middleware.py,routes}`;
- `packages/{cnes_domain,cnes_infra}/tests`, `apps/central_api/tests`, `tests/{property,chaos}`;
- `pyproject.toml`, `pytest.ini`, `.env.example`, `.github/workflows/python-quality.yml`;
- `docs/contracts/openapi.json` e `scripts/export_openapi.py`.

Os módulos alvo de control plane, run e publisher ainda não existem nesse SHA. O core `BIL-010`/`BIL-011` começa quando `CND-045` estiver integrado. `BIL-012`/`BIL-013` também exige o adapter DynamoDB de `CND-021`, mas não espera `AWS-010`–`AWS-014`. A Task 6 espera o gate local `CND-064` e a Task 8 AWS já integrados, pois é o item seguinte na fila controller-owned de composition roots. A lane Stripe respeita os gates do backlog: `BIL-020` espera `AWS-011` e `BIL-010`; `BIL-021` espera `AWS-010` e `BIL-012`; `BIL-022` espera `CND-044`, `BIL-011` e `BIL-021`. A Task 13 espera ainda o checkpoint de integração das fontes retidas para preservar o registry final, e também é controller-serial.

No gate `CND-060` exigido pela Task 6, os contratos canônicos consumidos por billing são os contratos de orquestração sem tipos de billing:

```python
class ProcessorExecutorPort(Protocol):
    def start(self, request: StartRunExecution) -> str: ...
    def cancel(self, request: CancelRunExecution) -> None: ...
    def status(self, execution_ref: str) -> ExecutionStatus: ...

# Imported unchanged from the CND registry:
# ExecutionPermit, RunDispatch, DispatchState, DispatchOutcome,
# ExecutionCallbacks, ExecutionPolicyConfig, and PublicationPermit.

class ClaimRunUnit(BaseModel):
    tenant_id: str
    run_id: str
    unit_id: str
    dispatch_id: str
    owner: str
    now: datetime
    lease_seconds: int

class ControlPlanePort(Protocol):
    def put_run(self, run: Run) -> None: ...
    def get_run(self, tenant_id: str, run_id: str) -> Run | None: ...
    def transition_run(self, command: TransitionRun, event: OutboxEvent) -> Run: ...
    def publish_dataset(self, command: PublishDataset) -> DatasetPointer: ...
    def reserve_run_dispatch(self, command: ReserveRunDispatch) -> RunDispatch: ...
    def bind_run_dispatch(self, command: BindRunDispatch) -> RunDispatch: ...
    def finish_run_dispatch(self, command: FinishRunDispatch) -> RunDispatch: ...
    def get_active_run_dispatch(
        self, tenant_id: str, run_id: str,
    ) -> RunDispatch | None: ...

```

The same CND gate requires `dispatch_id: str` on `CommitRunUnit` and `FailRunUnit`; Billing never adds a parallel dispatch field or command family.

No gate `CND-060`, o contrato canônico já fornece `RunDispatch`, `ReserveRunDispatch`, `BindRunDispatch`, `FinishRunDispatch`, `reserve_run_dispatch`, `bind_run_dispatch`, `finish_run_dispatch`, `get_active_run_dispatch`, `ExecutionPermit` e `PublicationPermit`; `ClaimRunUnit` já carrega `dispatch_id`. Task 6 usa esses nomes sem criar equivalentes e estende `packages/cnes_domain/src/cnes_domain/ports/control_plane.py` somente com `reserve_and_create_run(command: ReserveRunCommand) -> RunAuthorization`, `create_unmetered_run(command: AuthorizedRunCommand) -> Run`, `consume_reservation(command: ConsumeReservationCommand) -> QuotaReservation`, `release_reservation(command: ReleaseReservationCommand) -> QuotaReservation`, `get_run_billing_state(tenant_id: str, run_id: str) -> RunBillingState | None` e `bind_run_execution(command: RunExecutionBindingCommand) -> RunBillingState`. Task 17 mantém o `PublishRequest` canônico sem campos de billing fornecidos pelo caller; `BillingPublicationPolicy` lê o companion/snapshot por base key e retorna o único CND `PublicationPermit` com `PublicationGuard` em `binding_context`. Essas são extensões seriais do único contrato; o capability Protocol usado pelas Tasks 1–5 tem essas mesmas assinaturas e é satisfeito pelo `ControlPlanePort`, não implementado como um segundo repositório canônico. Se os nomes CND acima divergirem no `develop` integrado, corrige-se primeiro o PR upstream ou este plano; nenhum branch cria uma segunda interface equivalente.

## Canonical Billing Interfaces

Todas as tarefas usam estes nomes e tipos; não os renomeiam localmente.

```python
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Mapping, Protocol

class BillingMode(StrEnum):
    DISABLED = "disabled"
    STRIPE = "stripe"

class SubscriptionStatus(StrEnum):
    TRIALING = "trialing"
    ACTIVE = "active"
    PAST_DUE = "past_due"
    INCOMPLETE = "incomplete"
    INCOMPLETE_EXPIRED = "incomplete_expired"
    UNPAID = "unpaid"
    PAUSED = "paused"
    CANCELED = "canceled"
    ADMIN_REVOKED = "admin_revoked"

class EntitlementAction(StrEnum):
    CREATE_RUN = "create_run"
    REGISTER_AGENT = "register_agent"
    ANALYTICS_QUERY = "analytics_query"
    SERVING_ACCESS = "serving_access"
    TENANT_CREATION = "tenant_creation"
    PUBLISH_RUN = "publish_run"

@dataclass(frozen=True, slots=True)
class RunAuthorization:
    billing_account_id: str
    plan_version_id: str
    entitlement_version: int
    max_concurrency: int
    budget_reservation_id: str | None
    authorized_at: datetime

@dataclass(frozen=True, slots=True)
class RunExecutionPermit:
    billing_account_id: str
    wave_id: str
    dispatch_id: str
    generation: int
    expected_previous_dispatch_id: str | None
    expected_previous_execution_ref: str | None
    expected_entitlement_version: int
    expected_fencing_token: int
    authorized_at: datetime

class EntitlementGate:
    def authorize_create_run(self, request: CreateRunRequest) -> RunAuthorization: ...
    def authorize_register_agent(self, request: GateRequest) -> EntitlementDecision: ...
    def authorize_analytics_query(self, request: AnalyticsRequest) -> AnalyticsAuthorization: ...
    def authorize_serving_access(self, request: GateRequest) -> EntitlementDecision: ...
    def authorize_tenant_creation(self, request: GateRequest) -> EntitlementDecision: ...
    def authorize_publish_run(self, request: PublishGateRequest) -> EntitlementDecision: ...
```

### Exact Domain Model Catalog

Task 1 implements these field names and types exactly. `RunDependency` is the canonical CND-010
type. Constructors validate UTC-aware datetimes and non-empty identifiers; serialization uses the
enum string values. `CreateRunRequest` requires `competencia` in `YYYY-MM`, a non-empty
`dataset_name`, and non-empty unique dependencies. Those values come from the server-selected
CND-060 `SourcePipeline`, are included in `request_hash`, and are never accepted as dependency
overrides from a browser.

```python
class BillingAccountStatus(StrEnum):
    ACTIVE = "active"
    TRANSFER_PENDING = "transfer_pending"
    CLOSED = "closed"

class BillingEnforcementMode(StrEnum):
    OFF = "off"
    SHADOW = "shadow"
    ENFORCE = "enforce"

class AccessLevel(StrEnum):
    FULL = "full"
    READ_ONLY = "read_only"
    BLOCKED = "blocked"

class ReservationStatus(StrEnum):
    RESERVED = "reserved"
    CONSUMED = "consumed"
    RELEASED = "released"

class ReservationKind(StrEnum):
    RUN = "run"
    ANALYTICS = "analytics"

class CapacityKind(StrEnum):
    TENANT = "tenant"
    AGENT = "agent"

@dataclass(frozen=True, slots=True)
class BillingAccount:
    billing_account_id: str
    stripe_customer_id: str | None
    owner_user_id: str
    status: BillingAccountStatus
    created_at: datetime
    updated_at: datetime

@dataclass(frozen=True, slots=True)
class BillingAccountPage:
    accounts: tuple[BillingAccount, ...]
    next_cursor: str | None

@dataclass(frozen=True, slots=True)
class QuotaLimits:
    max_tenants: int | None
    max_agents: int | None
    max_runs_per_period: int | None
    max_concurrency: int | None
    retention_days: int | None
    athena_scan_budget_bytes: int | None

@dataclass(frozen=True, slots=True)
class PlanVersion:
    plan_version_id: str
    plan_key: str
    stripe_product_id: str | None
    stripe_price_ids: tuple[str, ...]
    features: frozenset[str]
    quotas: QuotaLimits
    grace_period_days: int
    effective_from: datetime

@dataclass(frozen=True, slots=True)
class GateRequest:
    billing_account_id: str
    tenant_id: str

@dataclass(frozen=True, slots=True)
class CreateRunRequest:
    billing_account_id: str
    tenant_id: str
    run_id: str
    competencia: str
    dataset_name: str
    dependencies: tuple[RunDependency, ...]
    idempotency_key: str
    request_hash: str
    requested_concurrency: int
    estimated_scan_bytes: int

@dataclass(frozen=True, slots=True)
class AnalyticsRequest:
    billing_account_id: str
    tenant_id: str
    query_id: str
    idempotency_key: str
    request_hash: str
    estimated_scan_bytes: int

@dataclass(frozen=True, slots=True)
class AnalyticsAuthorization:
    billing_account_id: str
    entitlement_version: int
    budget_reservation_id: str | None
    max_scan_bytes: int
    authorized_at: datetime

@dataclass(frozen=True, slots=True)
class PublishGateRequest:
    billing_account_id: str
    tenant_id: str
    run_id: str
    expected_entitlement_version: int
    expected_fencing_token: int

@dataclass(frozen=True, slots=True)
class EntitlementDecision:
    action: EntitlementAction
    allowed: bool
    access_level: AccessLevel
    reason: str
    entitlement_version: int
    quota_limit: int | None

@dataclass(frozen=True, slots=True)
class SnapshotWrite:
    expected_version: int
    snapshot: EntitlementSnapshot
    audit_events: tuple[BillingAuditEvent, ...]

@dataclass(frozen=True, slots=True)
class ReserveRunCommand:
    request: CreateRunRequest
    snapshot: EntitlementSnapshot
    deployment_max_concurrency: int
    reservation_id: str
    expires_at: datetime

@dataclass(frozen=True, slots=True)
class ReserveAnalyticsCommand:
    request: AnalyticsRequest
    snapshot: EntitlementSnapshot
    reservation_id: str
    expires_at: datetime

@dataclass(frozen=True, slots=True)
class AuthorizedRunCommand:
    request: CreateRunRequest
    authorization: RunAuthorization

@dataclass(frozen=True, slots=True)
class RunBillingState:
    billing_account_id: str
    tenant_id: str
    run_id: str
    authorization: RunAuthorization
    execution_generation: int
    execution_wave_id: str | None
    execution_dispatch_id: str | None
    execution_ref: str | None
    execution_unit_ids: tuple[str, ...]
    execution_status: DispatchState | None
    execution_terminal_outcome: DispatchOutcome | None
    fencing_token: int
    cancel_requested: bool
    updated_at: datetime

@dataclass(frozen=True, slots=True)
class RunExecutionBindingCommand:
    tenant_id: str
    run_id: str
    wave_id: str
    dispatch_id: str
    generation: int
    execution_ref: str
    unit_ids: tuple[str, ...]
    expected_previous_dispatch_id: str | None
    expected_previous_execution_ref: str | None
    expected_entitlement_version: int
    expected_fencing_token: int
    bound_at: datetime

@dataclass(frozen=True, slots=True)
class PublicationGuard:
    billing_account_id: str
    expected_entitlement_version: int
    expected_run_fencing_token: int
    checked_at: datetime

@dataclass(frozen=True, slots=True)
class CapacityReservationCommand:
    billing_account_id: str
    tenant_id: str
    resource_id: str
    kind: CapacityKind
    idempotency_key: str
    request_hash: str
    entitlement_version: int
    limit: int | None

@dataclass(frozen=True, slots=True)
class CapacityReservation:
    reservation_id: str
    billing_account_id: str
    resource_id: str
    kind: CapacityKind
    status: ReservationStatus
    created_at: datetime
    expires_at: datetime

@dataclass(frozen=True, slots=True)
class ReleaseCapacityCommand:
    billing_account_id: str
    reservation_id: str
    released_at: datetime
    reason_code: str

@dataclass(frozen=True, slots=True)
class ConsumeCapacityCommand:
    billing_account_id: str
    reservation_id: str
    consumed_at: datetime

@dataclass(frozen=True, slots=True)
class QuotaReservation:
    reservation_id: str
    billing_account_id: str
    resource_id: str
    kind: ReservationKind
    period_start: datetime
    reserved_runs: int
    reserved_scan_bytes: int
    consumed_runs: int
    consumed_scan_bytes: int
    status: ReservationStatus
    created_at: datetime
    expires_at: datetime

@dataclass(frozen=True, slots=True)
class ConsumeReservationCommand:
    billing_account_id: str
    reservation_id: str
    actual_scan_bytes: int
    consumed_at: datetime

@dataclass(frozen=True, slots=True)
class ReleaseReservationCommand:
    billing_account_id: str
    reservation_id: str
    released_at: datetime
    reason_code: str

@dataclass(frozen=True, slots=True)
class CheckoutCommand:
    billing_account_id: str
    stripe_customer_id: str
    plan_version: PlanVersion
    idempotency_key: str

@dataclass(frozen=True, slots=True)
class CreateStripeCustomerCommand:
    billing_account_id: str
    idempotency_key: str

@dataclass(frozen=True, slots=True)
class StripeCustomer:
    stripe_customer_id: str

@dataclass(frozen=True, slots=True)
class AttachStripeCustomerCommand:
    billing_account_id: str
    stripe_customer_id: str
    expected_updated_at: datetime

@dataclass(frozen=True, slots=True)
class PortalCommand:
    billing_account_id: str
    stripe_customer_id: str
    idempotency_key: str

@dataclass(frozen=True, slots=True)
class HostedSession:
    session_id: str
    url: str

@dataclass(frozen=True, slots=True)
class StripeBillingState:
    stripe_customer_id: str
    stripe_subscription_id: str
    subscription_status: SubscriptionStatus
    cancel_at_period_end: bool
    stripe_price_id: str
    active_features: frozenset[str]
    period_start: datetime
    period_end: datetime
    latest_invoice_id: str | None

@dataclass(frozen=True, slots=True)
class StripeStateRequest:
    stripe_customer_id: str
    stripe_subscription_id: str | None

@dataclass(frozen=True, slots=True)
class StripeEvent:
    event_id: str
    event_type: str
    created_at: datetime
    stripe_customer_id: str | None
    stripe_subscription_id: str | None
    payload_sha256: str

@dataclass(frozen=True, slots=True)
class StripeEventListRequest:
    created_gte: datetime
    ending_before: str | None
    limit: int

@dataclass(frozen=True, slots=True)
class BillingAuditEvent:
    event_id: str
    event_type: str
    aggregate_id: str
    actor_id: str
    reason_code: str
    occurred_at: datetime
    attributes: Mapping[str, str | int | bool | None]

@dataclass(frozen=True, slots=True)
class BillingMetric:
    name: str
    value: float
    unit: str
    dimensions: Mapping[str, str]
    occurred_at: datetime

class InboxDisposition(StrEnum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    IGNORED = "ignored"

@dataclass(frozen=True, slots=True)
class InboxAcceptResult:
    event_id: str
    disposition: InboxDisposition

@dataclass(frozen=True, slots=True)
class InboxClaim:
    event_id: str
    event_type: str
    customer_id: str
    subscription_id: str | None
    acquired: bool

@dataclass(frozen=True, slots=True)
class ProjectionResult:
    event_id: str
    applied: bool
    entitlement_version: int | None

@dataclass(frozen=True, slots=True)
class RecoveryRequest:
    lookback_hours: int
    batch_size: int

@dataclass(frozen=True, slots=True)
class RecoveryResult:
    scanned: int
    imported: int
    reprocessed: int
    failed: int
    next_cursor: str | None

@dataclass(frozen=True, slots=True)
class ReservationRecoveryRequest:
    now: datetime
    limit: int
    cursor: str | None

@dataclass(frozen=True, slots=True)
class ReservationRecoveryResult:
    examined: int
    released: int
    next_cursor: str | None

@dataclass(frozen=True, slots=True)
class ReconciliationRequest:
    limit: int
    cursor: str | None

@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    examined: int
    drift_found: int
    corrected: int
    failed: int
    next_cursor: str | None

class SecretProviderPort(Protocol):
    def get_secret(self, secret_arn: str) -> str: ...

class WebhookInboxPort(Protocol):
    def accept(self, event: StripeEvent) -> InboxAcceptResult: ...
    def claim(self, event_id: str, now: datetime) -> InboxClaim: ...
    def mark_processed(self, event_id: str, entitlement_version: int) -> None: ...
    def mark_failed(self, event_id: str, error_code: str, retryable: bool) -> None: ...
    def list_stale(self, now: datetime, limit: int) -> tuple[StripeEvent, ...]: ...
```

`EntitlementSnapshot` and `RunAuthorization` retain the exact definitions in Task 1 and Canonical Billing Interfaces. `BillingAuditEvent.attributes` and `BillingMetric.dimensions` are copied to immutable mappings during construction.

## File Ownership Map

| Surface | Responsibility | Ownership |
|---|---|---|
| `packages/cnes_domain/src/cnes_domain/billing/models.py` | Immutable accounts, plans, snapshots, decisions and authorizations | `BIL-010` |
| `packages/cnes_domain/src/cnes_domain/billing/errors.py` | Stable domain error codes | `BIL-010` |
| `packages/cnes_domain/src/cnes_domain/billing/ports.py` | Projection, quota, Stripe, audit, metrics and clock protocols | `BIL-010` |
| `packages/cnes_domain/src/cnes_domain/billing/policy.py` | Status/grace/action decision table | `BIL-011` |
| `packages/cnes_domain/src/cnes_domain/billing/gate.py` | `EntitlementGate` and critical/cache routing | `BIL-011` |
| `packages/cnes_infra/src/cnes_infra/billing/disabled.py` | Local unmetered no-network adapter | `BIL-010` |
| `packages/cnes_infra/src/cnes_infra/billing/dynamodb_projection.py` | Base-key snapshot projection and CAS | `BIL-012` |
| `packages/cnes_infra/src/cnes_infra/billing/dynamodb_catalog.py` | BillingAccount and immutable PlanVersion persistence | `BIL-012` |
| `packages/cnes_infra/src/cnes_infra/billing/cache.py` | 60-second local read cache and invalidation | `BIL-012` |
| `packages/cnes_infra/src/cnes_infra/billing/dynamodb_quota.py` | Atomic reservation/consume/release/recovery | `BIL-013` |
| `packages/cnes_infra/src/cnes_infra/billing/secrets_manager.py` | Text-only, fail-closed Secrets Manager adapter | `BIL-020` |
| `packages/cnes_infra/src/cnes_infra/billing/stripe_gateway.py` | Checkout, Portal, current state and Events API | `BIL-020` |
| `apps/central_api/src/central_api/routes/billing.py` | Owner-authorized billing endpoints | `BIL-020` |
| `apps/central_api/src/central_api/routes/stripe_webhook.py` | Raw-body signature ingress | `BIL-021` |
| `packages/cnes_infra/src/cnes_infra/billing/webhook_inbox.py` | Inbox lease/dedupe | `BIL-021` |
| `packages/cnes_infra/src/cnes_infra/billing/projector.py` | Reorder-safe state refresh and projection | `BIL-021` |
| `packages/cnes_infra/src/cnes_infra/billing/recovery.py` | Stale processing and undelivered-event recovery | `BIL-021` |
| `packages/cnes_domain/src/cnes_domain/billing/revocation.py` | Revocation command/result service | `BIL-022` |
| `packages/cnes_infra/src/cnes_infra/billing/reconciliation.py` | Stripe/snapshot drift correction | `BIL-023` |
| `packages/cnes_infra/src/cnes_infra/billing/metrics.py` | Structured billing metrics | `BIL-023` |
| `apps/billing_worker/src/billing_worker/main.py` | Async inbox/recovery/reconciliation entrypoint | `BIL-021`, `BIL-023` |
| `tests/e2e/billing/test_stripe_test_clock.py` | Lifecycle test-clock E2E | `BIL-024` |
| Shared manifests, exports, bootstrap, run/publisher integration, OpenAPI, CI and root docs | Composition and cross-plan wiring | serial `integration-owned` tasks only |

## Delivery Order

1. Após `CND-045`: Tasks 1–3 (`BIL-010`, `BIL-011`) podem iniciar; Tasks 4–5 iniciam quando `CND-021` também estiver integrado.
2. Após Tasks 1–5 e `CND-060`: serial core integration Task 6; nenhuma tarefa core espera `AWS-010`–`AWS-014`. O controller adquire o lock dos arquivos de composição e nunca executa Task 6 em paralelo com a Task 8 AWS: se AWS Task 8 já tiver integrado, Task 6 faz rebase sobre ela; caso contrário, AWS Task 8 faz rebase sobre Task 6.
3. Após `AWS-011` + Task 1: Tasks 7–8 (`BIL-020`). Após `AWS-010` + Task 4: Tasks 9–12 (`BIL-021`).
4. Após Tasks 7–12, Task 6 e a Task 8 integration-owned do plano AWS: serial Stripe integration Task 13, com lock exclusivo sobre composition roots, manifests e testes compartilhados.
5. Após `CND-044` + Tasks 3 e 9–13: Task 14 (`BIL-022`); Tasks 15–16 seguem para `BIL-023`.
6. Após Tasks 14–16: serial enforcement integration Task 17.
7. Após Tasks 7–17: Tasks 18–19 close `BIL-024`.

### Task 1: BIL-010 — Immutable Billing Domain

**Files:**
- Create: `packages/cnes_domain/src/cnes_domain/billing/models.py`
- Create: `packages/cnes_domain/src/cnes_domain/billing/errors.py`
- Create: `packages/cnes_domain/src/cnes_domain/billing/ports.py`
- Test: `packages/cnes_domain/tests/billing/test_models.py`
- Test: `packages/cnes_domain/tests/billing/test_ports.py`

**Interfaces:**
- Consumes: Python `datetime`, `Decimal`, `UUID`, `Protocol`; no infra import.
- Produces: `BillingMode`, `BillingAccount`, `PlanVersion`, `QuotaLimits`, `EntitlementSnapshot`, `SubscriptionStatus`, `EntitlementAction`, `EntitlementDecision`, `RunAuthorization`, `RunExecutionPermit`, `RunBillingState`, `RunExecutionBindingCommand`, `PublicationGuard`, the remaining request/command dataclasses and the ports listed below. It imports the single canonical CND `ExecutionPermit` and never redefines it.

- [ ] **Step 1: Write failing immutability and invariant tests**

```python
def test_plan_version_e_snapshot_sao_immutaveis() -> None:
    plan = make_plan_version()
    snapshot = make_snapshot(plan_version_id=plan.plan_version_id)
    with pytest.raises(FrozenInstanceError):
        plan.max_tenants = 9
    with pytest.raises(FrozenInstanceError):
        snapshot.entitlement_version = 2

def test_snapshot_rejeita_periodo_invertido() -> None:
    with pytest.raises(ValueError, match="period_end_before_start"):
        make_snapshot(period_start=NOW, period_end=NOW - timedelta(seconds=1))
```

- [ ] **Step 2: Run tests to verify RED**

Run: `uv run pytest packages/cnes_domain/tests/billing/test_models.py -q`

Expected: FAIL during collection with `ModuleNotFoundError: No module named 'cnes_domain.billing'`.

- [ ] **Step 3: Implement the exact immutable models**

```python
@dataclass(frozen=True, slots=True)
class QuotaLimits:
    max_tenants: int | None
    max_agents: int | None
    max_runs_per_period: int | None
    max_concurrency: int | None
    retention_days: int | None
    athena_scan_budget_bytes: int | None

@dataclass(frozen=True, slots=True)
class PlanVersion:
    plan_version_id: str
    plan_key: str
    stripe_product_id: str | None
    stripe_price_ids: tuple[str, ...]
    features: frozenset[str]
    quotas: QuotaLimits
    grace_period_days: int
    effective_from: datetime

@dataclass(frozen=True, slots=True)
class EntitlementSnapshot:
    billing_account_id: str
    stripe_subscription_id: str | None
    subscription_status: SubscriptionStatus
    cancel_at_period_end: bool
    plan_version_id: str
    features: frozenset[str]
    quotas: QuotaLimits
    period_start: datetime
    period_end: datetime
    grace_until: datetime | None
    valid_until: datetime
    entitlement_version: int
    updated_at: datetime
    source_event_id: str
```

Implement `__post_init__` checks for non-empty opaque IDs, timezone-aware datetimes, positive `entitlement_version`, non-negative quota values and fencing tokens, `period_end >= period_start`, `valid_until >= updated_at`, and `grace_until is None or grace_until >= period_start`. Execution bindings require lowercase 16-hex `wave_id` and `dispatch_id`, positive persisted `generation`, non-empty unique `unit_ids`, and either both previous dispatch/ref values or neither. `PlanVersion` accepts `None` quotas only for the reserved `plan_key="local-unmetered"`; Stripe plans require integers.

- [ ] **Step 4: Define exact command and port signatures and contract-test runtime shape**

```python
class ReadConsistency(StrEnum):
    EVENTUAL = "eventual"
    STRONG = "strong"

class EntitlementProjectionPort(Protocol):
    def get_snapshot(
        self, billing_account_id: str, consistency: ReadConsistency,
    ) -> EntitlementSnapshot | None: ...
    def compare_and_set_snapshot(self, command: SnapshotWrite) -> bool: ...

class BillingCatalogPort(Protocol):
    def create_account(self, account: BillingAccount) -> BillingAccount: ...
    def get_account(self, billing_account_id: str) -> BillingAccount | None: ...
    def get_account_by_customer(self, stripe_customer_id: str) -> BillingAccount | None: ...
    def list_stripe_accounts(self, limit: int, cursor: str | None) -> BillingAccountPage: ...
    def attach_customer(self, command: AttachStripeCustomerCommand) -> BillingAccount: ...
    def transfer_owner(self, command: TransferOwnerCommand) -> BillingAccount: ...
    def publish_plan(self, plan: PlanVersion) -> PlanVersion: ...
    def get_plan(self, plan_version_id: str) -> PlanVersion | None: ...
    def get_plan_by_price(self, stripe_price_id: str) -> PlanVersion | None: ...

class QuotaReservationPort(Protocol):
    def reserve_and_create_run(self, command: ReserveRunCommand) -> RunAuthorization: ...
    def reserve_analytics(self, command: ReserveAnalyticsCommand) -> AnalyticsAuthorization: ...
    def reserve_capacity(self, command: CapacityReservationCommand) -> CapacityReservation: ...
    def consume_capacity(self, command: ConsumeCapacityCommand) -> CapacityReservation: ...
    def release_capacity(self, command: ReleaseCapacityCommand) -> CapacityReservation: ...
    def consume(self, command: ConsumeReservationCommand) -> QuotaReservation: ...
    def release(self, command: ReleaseReservationCommand) -> QuotaReservation: ...

class StripeGatewayPort(Protocol):
    def create_customer(self, command: CreateStripeCustomerCommand) -> StripeCustomer: ...
    def create_checkout(self, command: CheckoutCommand) -> HostedSession: ...
    def create_portal(self, command: PortalCommand) -> HostedSession: ...
    def get_current_state(self, request: StripeStateRequest) -> StripeBillingState: ...
    def list_events(self, request: StripeEventListRequest) -> tuple[StripeEvent, ...]: ...

class BillingAuditPort(Protocol):
    def append(self, event: BillingAuditEvent) -> None: ...

class BillingMetricsPort(Protocol):
    def emit(self, metric: BillingMetric) -> None: ...

class ClockPort(Protocol):
    def now(self) -> datetime: ...
```

Define `TransferOwnerCommand(billing_account_id: str, expected_owner_user_id: str, new_owner_user_id: str, actor_id: str, reason_code: str, transferred_at: datetime)` in `models.py`. Tests instantiate every model from the Exact Domain Model Catalog and this transfer command with explicit values.

- [ ] **Step 5: Run focused tests and coverage**

Run: `uv run pytest packages/cnes_domain/tests/billing/test_models.py packages/cnes_domain/tests/billing/test_ports.py --cov=cnes_domain.billing --cov-branch --cov-fail-under=100 -q`

Expected: PASS and `100%` branch coverage for `cnes_domain.billing.models`, `errors`, and `ports`.

- [ ] **Step 6: Commit**

```bash
git add packages/cnes_domain/src/cnes_domain/billing \
  packages/cnes_domain/tests/billing
git commit -m "feat(billing): add immutable billing domain"
```

### Task 2: BIL-010 — Disabled Local Billing Mode

**Files:**
- Create: `packages/cnes_infra/src/cnes_infra/billing/disabled.py`
- Test: `packages/cnes_infra/tests/billing/test_disabled.py`
- Test: `tests/negative/test_local_billing_has_no_remote_dependency.py`

**Interfaces:**
- Consumes: Task 1 models and `EntitlementProjectionPort`, `QuotaReservationPort`, `ClockPort`.
- Produces: `DisabledEntitlementProjection`, `DisabledQuotaReservations`, `disabled_snapshot(billing_account_id: str, now: datetime) -> EntitlementSnapshot`.

- [ ] **Step 1: Write the failing no-network acceptance test**

```python
@pytest.mark.negative
def test_local_disabled_nao_le_secrets_nem_abre_rede(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BILLING_MODE", "disabled")
    monkeypatch.delenv("STRIPE_SECRET_KEY", raising=False)
    monkeypatch.delenv("STRIPE_WEBHOOK_SECRET", raising=False)
    with patch("socket.create_connection", side_effect=AssertionError("network_called")):
        gate = build_disabled_gate(FixedClock(NOW))
        result = gate.authorize_create_run(make_create_run_request())
    assert result.plan_version_id == "local-unmetered-v1"
    assert result.budget_reservation_id is None
```

- [ ] **Step 2: Run test to verify RED**

Run: `uv run pytest tests/negative/test_local_billing_has_no_remote_dependency.py -q`

Expected: FAIL with import error for `cnes_infra.billing.disabled`.

- [ ] **Step 3: Implement the disabled adapters without Stripe imports**

```python
def disabled_snapshot(
    billing_account_id: str, now: datetime,
) -> EntitlementSnapshot:
    unmetered = QuotaLimits(None, None, None, None, None, None)
    return EntitlementSnapshot(
        billing_account_id=billing_account_id,
        stripe_subscription_id=None,
        subscription_status=SubscriptionStatus.ACTIVE,
        cancel_at_period_end=False,
        plan_version_id="local-unmetered-v1",
        features=frozenset({"*"}),
        quotas=unmetered,
        period_start=now,
        period_end=datetime.max.replace(tzinfo=UTC),
        grace_until=None,
        valid_until=datetime.max.replace(tzinfo=UTC),
        entitlement_version=1,
        updated_at=now,
        source_event_id="local-disabled",
    )
```

`DisabledEntitlementProjection.get_snapshot()` returns that snapshot for every non-empty account ID. `compare_and_set_snapshot()` raises `BillingDisabledError("billing_mode=disabled operation=write_snapshot")`. `reserve_and_create_run` returns a `RunAuthorization` with `max_concurrency` equal to the deployment limit carried by `ReserveRunCommand` and no reservation ID; Task 6 persists that unmetered run through the canonical control plane. Capacity reserve/consume/release return immutable local records without counters or I/O, and run consume/release remain idempotent no-ops.

- [ ] **Step 4: Prove local mode never imports or constructs Stripe/boto clients**

```python
def test_disabled_module_nao_importa_sdk_remoto() -> None:
    source = inspect.getsource(disabled)
    assert "import stripe" not in source
    assert "import boto3" not in source
    assert "secretsmanager" not in source
```

- [ ] **Step 5: Run focused tests**

Run: `uv run pytest packages/cnes_infra/tests/billing/test_disabled.py tests/negative/test_local_billing_has_no_remote_dependency.py -q`

Expected: PASS; no network mock is called and no Stripe environment variable is required.

- [ ] **Step 6: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/billing/disabled.py \
  packages/cnes_infra/tests/billing/test_disabled.py \
  tests/negative/test_local_billing_has_no_remote_dependency.py
git commit -m "feat(billing): keep local profile unmetered"
```

### Task 3: BIL-011 — Subscription Policy and EntitlementGate

**Files:**
- Create: `packages/cnes_domain/src/cnes_domain/billing/policy.py`
- Create: `packages/cnes_domain/src/cnes_domain/billing/gate.py`
- Test: `packages/cnes_domain/tests/billing/test_policy.py`
- Test: `packages/cnes_domain/tests/billing/test_gate.py`

**Interfaces:**
- Consumes: Task 1 models/ports; Task 2 disabled implementations.
- Produces: `EntitlementPolicy.evaluate(snapshot, action, now) -> EntitlementDecision` and all six exact `EntitlementGate` methods from Canonical Billing Interfaces.

- [ ] **Step 1: Write a parameterized RED matrix for every Stripe status**

```python
@pytest.mark.parametrize(
    ("status", "action", "allowed", "access"),
    [
        (SubscriptionStatus.TRIALING, EntitlementAction.CREATE_RUN, True, "full"),
        (SubscriptionStatus.ACTIVE, EntitlementAction.CREATE_RUN, True, "full"),
        (SubscriptionStatus.PAST_DUE, EntitlementAction.CREATE_RUN, True, "full"),
        (SubscriptionStatus.INCOMPLETE, EntitlementAction.CREATE_RUN, False, "blocked"),
        (SubscriptionStatus.INCOMPLETE_EXPIRED, EntitlementAction.SERVING_ACCESS, False, "blocked"),
        (SubscriptionStatus.UNPAID, EntitlementAction.SERVING_ACCESS, True, "read_only"),
        (SubscriptionStatus.PAUSED, EntitlementAction.CREATE_RUN, False, "read_only"),
        (SubscriptionStatus.CANCELED, EntitlementAction.SERVING_ACCESS, True, "read_only"),
        (SubscriptionStatus.ADMIN_REVOKED, EntitlementAction.SERVING_ACCESS, False, "blocked"),
    ],
)
def test_politica_status(status, action, allowed, access) -> None:
    decision = EntitlementPolicy().evaluate(make_snapshot(status), action, NOW)
    assert (decision.allowed, decision.access_level.value) == (allowed, access)
```

Add cases proving `PAST_DUE` is denied after `grace_until`, `cancel_at_period_end=True` stays full through `period_end`, an expired `valid_until` fails closed for critical actions, missing feature denies analytics, and `ADMIN_REVOKED` denies every action except explicit billing/support routes outside this gate.

- [ ] **Step 2: Run tests to verify RED**

Run: `uv run pytest packages/cnes_domain/tests/billing/test_policy.py -q`

Expected: FAIL with missing `EntitlementPolicy`.

- [ ] **Step 3: Implement the decision table as data, not nested route logic**

```python
_READ_ONLY = frozenset({
    EntitlementAction.SERVING_ACCESS,
})
_CRITICAL = frozenset({
    EntitlementAction.CREATE_RUN,
    EntitlementAction.REGISTER_AGENT,
    EntitlementAction.ANALYTICS_QUERY,
    EntitlementAction.TENANT_CREATION,
    EntitlementAction.PUBLISH_RUN,
})

def evaluate(
    self, snapshot: EntitlementSnapshot, action: EntitlementAction, now: datetime,
) -> EntitlementDecision:
    if snapshot.subscription_status is SubscriptionStatus.ADMIN_REVOKED:
        return EntitlementDecision.denied(action, "admin_revoked", "blocked")
    if action in _CRITICAL and now > snapshot.valid_until:
        return EntitlementDecision.denied(action, "snapshot_expired", "blocked")
    return self._evaluate_status(snapshot, action, now)
```

Keep each helper under 50 lines. `features={"*"}` grants every feature only for disabled mode. Analytics requires `analytics_query`; serving requires `serving_history`; create/register/tenant use quota values rather than feature strings.

- [ ] **Step 4: Implement gate routing and immutable run authorization**

```python
def authorize_create_run(self, request: CreateRunRequest) -> RunAuthorization:
    snapshot = self._critical_snapshot(request.billing_account_id)
    decision = self._policy.evaluate(snapshot, EntitlementAction.CREATE_RUN, self._clock.now())
    decision.require_allowed()
    return self._quotas.reserve_and_create_run(
        ReserveRunCommand.from_request(request, snapshot),
    )

def _critical_snapshot(self, billing_account_id: str) -> EntitlementSnapshot:
    snapshot = self._projection.get_snapshot(
        billing_account_id, ReadConsistency.STRONG,
    )
    if snapshot is None:
        raise EntitlementDenied("reason=snapshot_missing")
    return snapshot
```

`authorize_register_agent`, `authorize_analytics_query`, `authorize_tenant_creation`, and `authorize_publish_run` always call `_critical_snapshot`. `authorize_serving_access` accepts `allow_cached: bool`; only `True` may call a cache facade, and `ADMIN_REVOKED` callers pass `False` after an invalidation event.

`authorize_analytics_query` is a pure policy operation: after the feature decision succeeds, it returns the immutable plan limit and decision metadata with `budget_reservation_id=None`, without calling an executor or creating a reservation. `athena_scan_budget_bytes`, `ReserveAnalyticsCommand`, and `reserve_analytics` remain a dormant, future-optional billing capability exercised only through domain/adapter contract tests in this plan; a direct `reserve_analytics` success returns a non-empty reservation ID. No Athena adapter, executor, HTTP route, or request-path integration is created.

- [ ] **Step 5: Run contract and coverage tests**

Run: `uv run pytest packages/cnes_domain/tests/billing/test_policy.py packages/cnes_domain/tests/billing/test_gate.py --cov=cnes_domain.billing --cov-branch --cov-fail-under=100 -q`

Expected: PASS, including the entire status/action/grace table.

- [ ] **Step 6: Commit**

```bash
git add packages/cnes_domain/src/cnes_domain/billing/policy.py \
  packages/cnes_domain/src/cnes_domain/billing/gate.py \
  packages/cnes_domain/tests/billing/test_policy.py \
  packages/cnes_domain/tests/billing/test_gate.py
git commit -m "feat(billing): authorize operations through entitlement gate"
```

### Task 4: BIL-012 — DynamoDB Projection and Local Cache

**Files:**
- Create: `packages/cnes_infra/src/cnes_infra/billing/dynamodb_projection.py`
- Create: `packages/cnes_infra/src/cnes_infra/billing/dynamodb_catalog.py`
- Create: `packages/cnes_infra/src/cnes_infra/billing/cache.py`
- Test: `packages/cnes_infra/tests/billing/test_dynamodb_projection.py`
- Test: `packages/cnes_infra/tests/billing/test_dynamodb_catalog.py`
- Test: `packages/cnes_infra/tests/billing/test_cache.py`
- Test: `tests/property/test_entitlement_projection_cas.py`

**Interfaces:**
- Consumes: `EntitlementProjectionPort`, `BillingCatalogPort`, `SnapshotWrite`, `ReadConsistency`; canonical DynamoDB client/table factory from `CND-021`.
- Produces: `DynamoEntitlementProjection(table: DynamoTablePort)`, `DynamoBillingCatalog(table: DynamoTablePort)`, `LocalEntitlementCache(max_ttl_seconds: int, clock: ClockPort)`, `handle_entitlement_changed(record: StreamRecord, cache: LocalEntitlementCache) -> None`.

- [ ] **Step 1: Write RED tests for physical keys and strong reads**

```python
def test_critical_read_usa_chave_base_e_consistent_read(table_spy) -> None:
    repo = DynamoEntitlementProjection(table_spy)
    repo.get_snapshot("ba_01", ReadConsistency.STRONG)
    table_spy.get_item.assert_called_once_with(
        Key={"PK": "BILLING#ba_01", "SK": "ENTITLEMENT"},
        ConsistentRead=True,
    )

def test_snapshot_cas_rejeita_versao_concorrente(projection) -> None:
    assert projection.compare_and_set_snapshot(make_write(expected_version=4)) is False

def test_plan_version_publicada_nao_pode_ser_sobrescrita(catalog) -> None:
    catalog.publish_plan(make_plan_version(plan_version_id="plan_v1"))
    with pytest.raises(ImmutablePlanConflict, match="plan_version_id=plan_v1"):
        catalog.publish_plan(make_plan_version(plan_version_id="plan_v1", max_agents=99))
```

- [ ] **Step 2: Run tests to verify RED**

Run: `uv run pytest packages/cnes_infra/tests/billing/test_dynamodb_projection.py packages/cnes_infra/tests/billing/test_dynamodb_catalog.py tests/property/test_entitlement_projection_cas.py -q`

Expected: FAIL with missing `DynamoEntitlementProjection`.

- [ ] **Step 3: Implement base-key serialization and conditional CAS**

```python
def entitlement_key(billing_account_id: str) -> dict[str, str]:
    return {"PK": f"BILLING#{billing_account_id}", "SK": "ENTITLEMENT"}

def compare_and_set_snapshot(self, command: SnapshotWrite) -> bool:
    try:
        self._table.update_item(
            Key=entitlement_key(command.snapshot.billing_account_id),
            UpdateExpression=_SNAPSHOT_UPDATE,
            ConditionExpression="attribute_not_exists(entitlement_version) OR entitlement_version = :expected",
            ExpressionAttributeValues=encode_snapshot_write(command),
        )
    except self._conditional_failure:
        return False
    return True
```

The serialized item includes every `EntitlementSnapshot` field, `entity_type="EntitlementSnapshot"`, and no card/payment-method fields. `compare_and_set_snapshot` increments to exactly `expected_version + 1`; identical current state may retain content but still increments when access, quota, or fencing semantics changed.

`DynamoBillingCatalog` uses these base keys and conditional writes:

```text
BillingAccount  PK=BILLING#<billing_account_id>       SK=ACCOUNT
AccountList     PK=BILLING_ACCOUNTS                   SK=ACCOUNT#<billing_account_id>
CustomerMap     PK=STRIPE_CUSTOMER#<stripe_customer_id> SK=BILLING_ACCOUNT
PlanVersion     PK=PLAN_VERSION#<plan_version_id>     SK=META
PriceMapping    PK=STRIPE_PRICE#<stripe_price_id>     SK=PLAN_VERSION
```

`publish_plan` transactionally puts the immutable plan and each Price mapping with `attribute_not_exists(PK)`. `get_plan_by_price` reads the Price mapping and then strongly reads the PlanVersion base key. `attach_customer` requires `stripe_customer_id` to be absent and `updated_at` to match the command, and conditionally creates the unique CustomerMap plus AccountList entry, preventing one Customer from attaching to two accounts. `get_account_by_customer` reads that mapping then strongly reads the account base key. `list_stripe_accounts` pages AccountList candidates and revalidates every account base key. `transfer_owner` conditions on `expected_owner_user_id`, updates `updated_at`, and appends `billing_account.transferred`; `create_account` appends `billing_account.created` in the same transaction.

- [ ] **Step 4: Write RED cache tests and implement the 60-second ceiling**

```python
def test_cache_rejeita_ttl_acima_de_sessenta() -> None:
    with pytest.raises(ValueError, match="cache_ttl_gt_60"):
        LocalEntitlementCache(61, FixedClock(NOW))

def test_critical_gate_ignora_snapshot_em_cache(stale_cache, gate) -> None:
    stale_cache.put(make_snapshot(status=SubscriptionStatus.ACTIVE))
    gate.authorize_register_agent(make_gate_request())
    assert gate.projection.last_consistency is ReadConsistency.STRONG
```

Cache key is `CacheKey(billing_account_id, entitlement_version)`. A separate latest-version index is local only. `handle_entitlement_changed` removes all entries for the account whose version is lower than `new_entitlement_version`; missing stream delivery is bounded by TTL.

- [ ] **Step 5: Run projection, cache, race and lint checks**

Run: `uv run pytest packages/cnes_infra/tests/billing/test_dynamodb_projection.py packages/cnes_infra/tests/billing/test_dynamodb_catalog.py packages/cnes_infra/tests/billing/test_cache.py tests/property/test_entitlement_projection_cas.py -m "not postgres" -q`

Expected: PASS; the race test reports exactly one successful CAS for a shared expected version.

Run: `uv run ruff check packages/cnes_infra/src/cnes_infra/billing packages/cnes_infra/tests/billing tests/property/test_entitlement_projection_cas.py`

Expected: PASS with no diagnostics.

- [ ] **Step 6: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/billing/dynamodb_projection.py \
  packages/cnes_infra/src/cnes_infra/billing/dynamodb_catalog.py \
  packages/cnes_infra/src/cnes_infra/billing/cache.py \
  packages/cnes_infra/tests/billing/test_dynamodb_projection.py \
  packages/cnes_infra/tests/billing/test_dynamodb_catalog.py \
  packages/cnes_infra/tests/billing/test_cache.py \
  tests/property/test_entitlement_projection_cas.py
git commit -m "feat(billing): project and cache entitlements"
```

### Task 5: BIL-013 — Transactional Quota and Budget Reservations

**Files:**
- Create: `packages/cnes_infra/src/cnes_infra/billing/dynamodb_quota.py`
- Test: `packages/cnes_infra/tests/billing/test_dynamodb_quota.py`
- Test: upstream `packages/cnes_infra/tests/control_plane/test_dynamodb_adapter.py`
- Test: `tests/property/test_quota_last_unit_race.py`
- Test: `tests/chaos/test_quota_reservation_recovery.py`

**Interfaces:**
- Consumes: `QuotaReservationPort`, `ReserveRunCommand`, current snapshot key from Task 4, upstream `AuthorizedRunCommand`, and CND-021 canonical `run_key`, `run_dependency_key`, and Run encoder from `cnes_infra.control_plane.dynamodb_keys`/`dynamodb_codec`.
- Produces: `DynamoQuotaReservations`, `reconcile_expired_reservations(request: ReservationRecoveryRequest) -> ReservationRecoveryResult`.

- [ ] **Step 1: Write RED contract tests for idempotency and lifecycle**

```python
def test_reserva_repetida_retorna_mesma_autorizacao(quota_repo) -> None:
    first = quota_repo.reserve_and_create_run(make_reserve_command(idempotency_key="req-01"))
    second = quota_repo.reserve_and_create_run(make_reserve_command(idempotency_key="req-01"))
    assert second == first

def test_mesma_chave_payload_diferente_conflita(quota_repo) -> None:
    quota_repo.reserve_and_create_run(make_reserve_command(idempotency_key="req-01", request_hash="a"))
    with pytest.raises(IdempotencyConflict, match="key=req-01"):
        quota_repo.reserve_and_create_run(make_reserve_command(idempotency_key="req-01", request_hash="b"))
```

Add tests for `RESERVED -> CONSUMED`, partial consumption, `RESERVED -> RELEASED`, double consume/release idempotency, expired logical reservation still physically present, and deployment concurrency winning over a higher plan limit.

Add two capacity races: two agents competing for the last `max_agents` slot and two tenants competing for the last `max_tenants` slot. Exactly one capacity reservation and one counter increment may commit.

Add an analytics race in which two estimated scans compete for the final bytes; exactly one `AnalyticsAuthorization` may reserve the remaining `athena_scan_budget_bytes`.

Add an adapter contract proving a Run created by `reserve_and_create_run` is byte-equivalent to the canonical CND codec, is returned by `get_run`, and is discovered through every exact `list_waiting_runs_for_dependency` key. Inject a conditional failure on the second dependency `Put` and assert the Run, all dependency indexes, companion, idempotency record, usage mutation, reservation and outbox are all absent after rollback.

- [ ] **Step 2: Run tests to verify RED**

Run: `uv run pytest packages/cnes_infra/tests/billing/test_dynamodb_quota.py -q`

Expected: FAIL with missing `DynamoQuotaReservations`.

- [ ] **Step 3: Implement one atomic create-run transaction**

Use these exact keys:

```text
Snapshot     PK=BILLING#<account>                     SK=ENTITLEMENT
Usage        PK=BILLING#<account>#PERIOD#<start>      SK=USAGE
Reservation  PK=BILLING#<account>#PERIOD#<start>      SK=RESERVATION#<reservation_id>
Run          PK=TENANT#<tenant_id>                    SK=RUN#<run_id>
Run deps     PK=RUN_DEP#<tenant>#<source>#<subtype>#<competencia> SK=RUN#<run_id>
Run billing  PK=TENANT#<tenant_id>                    SK=RUN_BILLING#<run_id>
Run lookup   PK=BILLING#<account>#RUNS                SK=RUN#<tenant_id>#<run_id>
Idempotency  PK=BILLING#<account>#IDEMPOTENCY#RUN     SK=KEY#<idempotency_key>
Outbox       PK=TENANT#<tenant_id>#OUTBOX#<shard>     SK=<created_at>#<event_id>
```

`reserve_and_create_run` sends one `TransactWriteItems` containing: snapshot `ConditionCheck` for expected version/status/`valid_until`; usage update conditioned on `consumed_runs < max_runs_per_period` that increments `consumed_runs` exactly once and reserves only estimated scan/compute bytes; a reservation `Put` with `consumed_runs=1`, `reserved_runs=0` and the scan/compute component still reserved; canonical CND `Run` Put encoded by the shared CND codec plus one immutable Run-dependency lookup Put for every canonical dependency, each with PK exactly `run_dependency_key(tenant_id, source_type, file_subtype, competencia)` and separate codec-owned SK `RUN#<run_id>`; `RunBillingState` Put holding the immutable authorization/fence with `execution_generation=0`, `execution_wave_id=None`, `execution_dispatch_id=None`, `execution_ref=None`, `execution_unit_ids=()`, `execution_status=None`, and `execution_terminal_outcome=None`; billing-account run lookup Put; idempotency Put; and `quota.reserved` outbox Put. Retry reads the idempotency item by base key, verifies `request_hash`, and returns its recorded authorization without incrementing `consumed_runs` again. TTL is only a cleanup attribute. The companion state avoids changing the frozen CND `Run` shape while giving revocation/publication one base-key fence. Never pass `run_id` as a fifth helper argument or duplicate the CND Run/dependency encoding inside `dynamodb_quota.py`.

`reserve_analytics` uses the same Snapshot/Usage/Reservation/Idempotency/Outbox keys without creating a Run. Its transaction conditions `reserved_scan_bytes + consumed_scan_bytes + estimated_scan_bytes <= athena_scan_budget_bytes`; retry returns the original `AnalyticsAuthorization`, and a reused key with another query hash conflicts.

`reserve_capacity` sends a transaction with snapshot version/status condition, account usage update conditioned on `tenant_count < max_tenants` or `agent_count < max_agents`, a capacity reservation Put, idempotency Put and `quota.reserved` outbox. The integration task creates the canonical Tenant/Agent using the reservation ID as idempotency key, then marks it consumed; failure releases the reservation. Recovery releases expired capacity reservations whose canonical resource is absent. `release_capacity` decrements once using the reservation's billing account; an already-released reservation is a stable no-op.

- [ ] **Step 4: Prove two callers cannot reserve the final unit**

```python
@pytest.mark.race
def test_duas_reservas_disputam_ultima_unidade(quota_repo) -> None:
    results = run_concurrently(
        lambda: quota_repo.reserve_and_create_run(make_reserve_command(idempotency_key=uuid4().hex)),
        count=2,
    )
    assert sum(isinstance(item, RunAuthorization) for item in results) == 1
    assert sum(isinstance(item, QuotaExceeded) for item in results) == 1

@pytest.mark.race
def test_dois_agents_disputam_ultima_vaga(quota_repo) -> None:
    results = reserve_agents_concurrently(quota_repo, count=2, max_agents=1)
    assert results.count("created") == 1
    assert results.count("max_agents_exceeded") == 1
```

- [ ] **Step 5: Implement settlement and abandoned-reservation recovery**

`consume` conditionally changes only the reserved scan/compute component to settled, decrements `reserved_scan_bytes`, increments `consumed_scan_bytes` by `actual_scan_bytes`, never increments `consumed_runs`, and appends `quota.consumed`. `release` releases only the unconsumed scan/compute component; it never decrements the already-consumed Run count. Successful publication and terminal failure/cancellation both call `consume` with the measured scan value (zero only when no processor work began), so every terminal Run settles its reservation. Recovery strongly reloads both reservation and canonical Run: an existing Run is never allowed to release or decrement its consumed Run unit; terminal Runs are settled idempotently, active Runs renew only the scan/compute lease, and only a transactionally absent Run may release the whole pre-creation reservation. TTL disappearance is never correctness.

- [ ] **Step 6: Run race, chaos, coverage and lint**

Run: `uv run pytest packages/cnes_infra/tests/billing/test_dynamodb_quota.py packages/cnes_infra/tests/control_plane/test_dynamodb_adapter.py tests/property/test_quota_last_unit_race.py tests/chaos/test_quota_reservation_recovery.py -q`

Expected: PASS; exactly one final-unit reservation wins, Run/dependency writes roll back as one unit, terminal settlement leaves counters balanced, and an existing Run never loses its consumed Run unit.

Run: `uv run ruff check packages/cnes_infra/src/cnes_infra/billing/dynamodb_quota.py packages/cnes_infra/tests/billing/test_dynamodb_quota.py tests/property/test_quota_last_unit_race.py tests/chaos/test_quota_reservation_recovery.py`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/billing/dynamodb_quota.py \
  packages/cnes_infra/tests/billing/test_dynamodb_quota.py \
  packages/cnes_infra/tests/control_plane/test_dynamodb_adapter.py \
  tests/property/test_quota_last_unit_race.py \
  tests/chaos/test_quota_reservation_recovery.py
git commit -m "feat(billing): reserve quotas transactionally"
```

### Task 6: Integration-Owned — Core Exports, Configuration and Run Gate

**Serial entry gate:** Start only after Tasks 1–5, `CND-064`, and AWS plan Task 8 are merged. This
is the next fixed item in the controller-owned composition queue; rebase onto the AWS integration
commit and preserve both local and AWS runtime bundles.

**Files:**
- Create: `packages/cnes_domain/src/cnes_domain/billing/__init__.py`
- Create: `packages/cnes_infra/src/cnes_infra/billing/__init__.py`
- Modify: `packages/cnes_domain/src/cnes_domain/__init__.py`
- Modify: `packages/cnes_domain/src/cnes_domain/ports/__init__.py`
- Modify: `packages/cnes_infra/src/cnes_infra/__init__.py`
- Modify: `packages/cnes_infra/src/cnes_infra/config.py`
- Modify: `apps/central_api/src/central_api/deps.py`
- Create: `apps/central_api/src/central_api/services/run_authorization.py`
- Modify: upstream `apps/central_api/src/central_api/composition.py`
- Modify: upstream `apps/data_processor/src/data_processor/composition.py`
- Modify: upstream `packages/cnes_domain/src/cnes_domain/ports/control_plane.py`
- Modify: upstream `packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_adapter.py`
- Modify: upstream `packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_claims.py`
- Modify: upstream `packages/cnes_infra/src/cnes_infra/control_plane/sqlite_adapter.py`
- Modify: `.env.example`
- Test: `packages/cnes_infra/tests/test_infra_config.py`
- Create: `packages/cnes_infra/tests/billing/test_config.py`
- Create: `packages/cnes_infra/tests/billing/test_control_plane_extensions.py`
- Create: `apps/central_api/tests/services/test_run_authorization.py`
- Create: `tests/integration/billing/test_execution_binding.py`
- Test: upstream `apps/central_api/tests/services/test_run_planning.py`
- Test: upstream `apps/data_processor/tests/orchestration/test_coordinator.py`
- Test: upstream `apps/data_processor/tests/orchestration/test_unit_worker.py`
- Test: upstream `packages/cnes_domain/tests/orchestration/test_planner.py`
- Test: upstream `packages/cnes_infra/tests/executor/test_local_pool.py`
- Test: upstream `packages/cnes_infra/tests/executor/test_step_functions.py`
- Test: upstream `packages/cnes_infra/tests/control_plane/test_dynamodb_adapter.py`
- Test: upstream `packages/cnes_infra/tests/control_plane/test_sqlite_adapter.py`
- Test: `apps/central_api/tests/test_app_wiring.py`

**Interfaces:**
- Consumes: merged Tasks 1–5, `CND-014` profile config, `CND-021` adapters, `CND-041` `Run`/planner contracts, and CND-060 `RunPlanningService` plus stable local bootstrap/composition.
- Produces: `BillingSettings(mode: BillingMode, enforcement_mode: BillingEnforcementMode, cache_ttl_seconds: int)`, `build_entitlement_gate(settings: BillingSettings) -> EntitlementGate`, `RunAuthorizationService(entitlement_gate, control_plane, run_planning).authorize_and_create(command: CreateRunRequest) -> Run`; `BillingConcurrencyPolicy.__call__(run: Run, dispatch: RunDispatch, requested_limit: int) -> ExecutionPermit`; `BillingExecutionStarted.__call__(run: Run, request: StartRunExecution, execution_ref: str, permit: ExecutionPermit) -> None`; dispatch-aware `StartRunExecution`; `ControlPlanePort.get_run_billing_state` and `bind_run_execution` layered on the canonical CND dispatch lifecycle.

- [ ] **Step 1: Write RED config and wiring tests**

```python
def test_local_defaulta_billing_disabled(monkeypatch) -> None:
    monkeypatch.setenv("PROFILE", "local")
    monkeypatch.delenv("BILLING_MODE", raising=False)
    assert load_billing_settings().mode is BillingMode.DISABLED

def test_aws_exige_stripe(monkeypatch) -> None:
    monkeypatch.setenv("PROFILE", "aws")
    monkeypatch.setenv("BILLING_MODE", "disabled")
    with pytest.raises(OSError, match="profile=aws billing_mode=disabled"):
        load_billing_settings()
```

Add orchestration tests proving: the coordinator first obtains/reuses the persisted CND `RunDispatch`, then calls `BillingConcurrencyPolicy(run, dispatch, requested_limit)` and receives the imported canonical `ExecutionPermit(tenant_id, run_id, max_concurrency=min(requested_limit, authorization.max_concurrency), policy_version, fencing_token, binding_context=RunExecutionPermit(...same dispatch_id/generation...))`; after executor start the CND coordinator binds the canonical dispatch, then passes that exact permit object instance (`seen is permit`) to `BillingExecutionStarted`, which binds only the billing companion; normalize, reconcile and materialize produce three sequential bindings with distinct persisted `(wave_id, dispatch_id, generation, execution_ref)`; no successor starts before `finish_run_dispatch` makes the current dispatch terminal. The same pending start/recovery reuses one dispatch; recovery after a terminal dispatch or failed canonical/companion bind advances generation and obtains a new dispatch even if no unit claim increment occurred. Add SQLite fault injection after each statement and assert rollback includes the Run, every `run_dependencies` row, `RunBillingState`, idempotency row and outbox row.

- [ ] **Step 2: Run tests to verify RED**

Run: `uv run pytest packages/cnes_infra/tests/billing/test_config.py packages/cnes_infra/tests/billing/test_control_plane_extensions.py apps/central_api/tests/services/test_run_authorization.py tests/integration/billing/test_execution_binding.py -q`

Expected: FAIL because billing settings/wiring, companion binding and atomic authorized Run creation are absent; the stable CND dispatch/callback tests remain unchanged.

- [ ] **Step 3: Apply serial exports and composition**

Add only re-exports needed by callers. Extend the canonical `ControlPlanePort` with the exact billing signatures while reusing the CND `RunDispatch` methods unchanged. Have `DynamoDBControlPlane` in `packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_adapter.py` delegate atomic billing writes to Task 5's focused helper, and have `SQLiteControlPlane` in `packages/cnes_infra/src/cnes_infra/control_plane/sqlite_adapter.py` implement `create_unmetered_run` as one `BEGIN IMMEDIATE` transaction containing the canonical Run, all dependency indexes, local `RunBillingState`, idempotency record and outbox. Any statement failure rolls back every row. Both adapters expose `get_run_billing_state` and the companion `bind_run_execution`, while dispatch allocation/terminalization remains exclusively `reserve_run_dispatch`/`finish_run_dispatch`. Extend config with `BILLING_MODE`, `BILLING_ENFORCEMENT_MODE` default `off`, `BILLING_CACHE_TTL_SECONDS` default `60`, validation `0..60`, and profile compatibility. `off` records no denials and preserves pre-billing behavior; `shadow` evaluates and audits the would-be denial but returns allow; `enforce` applies the decision. In `deps.py`, local mode constructs `DisabledEntitlementProjection` and `DisabledQuotaReservations`; AWS mode injects the integrated canonical `DynamoDBControlPlane(client, table_name, clock)`. Do not read Stripe secrets in local mode.

- [ ] **Step 4: Close execution authorization and dispatch binding atomically**

Consume the stable CND callback contracts exactly as merged; this task does not edit their owners:

```python
ConcurrencyPolicy = Callable[[Run, RunDispatch, int], ExecutionPermit]
ExecutionStarted = Callable[[Run, StartRunExecution, str, ExecutionPermit], None]
```

`RunPlanningService` and `PipelineCoordinator` compute the ready wave and `requested_limit=min(len(ready_unit_ids), deployment_limit)`, then call canonical `reserve_run_dispatch(ReserveRunDispatch(...))` before authorization or executor start. The CND transaction persists `RunDispatch(wave_id, dispatch_id, generation, unit_ids, state, terminal_outcome, execution_ref, lease_until)`: an identical pending reserve returns the same dispatch/generation; only a terminal prior dispatch permits CAS to `generation + 1`, including recovery after a bind failure before any unit was claimed. `dispatch_id` is the deterministic lowercase 16-hex encoding of Run identity plus persisted generation, never a derivation from mutable unit attempts. The service next calls `BillingConcurrencyPolicy(run, dispatch, requested_limit)`, which returns `ExecutionPermit.max_concurrency=min(requested_limit, authorization.max_concurrency)`, then calls canonical `execution_request(plan, dispatch, permit.max_concurrency)`, `executor.start(request)`, `bind_run_dispatch(BindRunDispatch(...))`, and finally `BillingExecutionStarted(run, request, execution_ref, permit)`. `execution_request` copies `dispatch.dispatch_id` into required `StartRunExecution.dispatch_id`; both executors use tenant/run/wave/dispatch in idempotency and AWS execution names.

After reserve, the services call `BillingConcurrencyPolicy` exactly once with that `RunDispatch` and `requested_limit`, retain the returned immutable canonical permit, and pass the same instance to `BillingExecutionStarted` after `executor.start` and the canonical dispatch bind; they never reload a replacement permit between authorization and binding. The policy clamps only against billing entitlement at this boundary, sets `ExecutionPermit.policy_version` to the entitlement version, `ExecutionPermit.fencing_token` to the billing fence, and `binding_context` to `RunExecutionPermit` containing billing account, dispatch/generation, exact previous dispatch/ref, entitlement version and fence. `BillingExecutionStarted` rejects any context that is not `RunExecutionPermit`, verifies tenant/run/policy-version/fencing-token agreement, strongly verifies that `get_active_run_dispatch` is already `DispatchState.STARTED` with the same dispatch/ref, and calls only companion `bind_run_execution`; it never calls `bind_run_dispatch`. Only the companion command carries `expected_previous_dispatch_id`/ref. Same dispatch/ref is idempotent; same dispatch/another ref conflicts; a new generation replaces the previous companion binding only by CAS against `expected_previous_dispatch_id`/ref with `cancel_requested=False`, unchanged entitlement version and unchanged fence. Revocation between reserve, start, canonical bind or companion bind increments the fence/sets cancel and therefore prevents the new companion bind.

If canonical binding or `BillingExecutionStarted` fails, the CND coordinator cancels only the just-started ref, calls `finish_run_dispatch(FinishRunDispatch(..., outcome=DispatchOutcome.FAILED, ...))` to persist canonical `DispatchState.TERMINAL`/`terminal_outcome=FAILED`, records sanitized `reason_code="bind_failed"` only in the billing audit event, and propagates the original durable error. The billing callback does not duplicate that cleanup. Recovery reserves the next persisted generation; it never reuses a terminal execution name. Before a successor wave or retry, the coordinator verifies the current dispatch's unit IDs are terminal and calls `finish_run_dispatch(..., outcome=DispatchOutcome.SUCCEEDED|FAILED)` according to their aggregate result; only then may reserve create the next generation. Concurrent resumes produce one reserve CAS winner, establishing one active dispatch per Run.

`ClaimRunUnit` carries required `dispatch_id`. In Stripe mode its DynamoDB claim transaction, and in disabled mode the equivalent SQLite transaction, require both canonical `get_active_run_dispatch` to match that ID and the `RunBillingState` companion to have the same bound dispatch/ref/fence with `cancel_requested=False`; therefore ECS/local workers cannot begin compute in the interval between `executor.start` and companion binding. A bounded worker retry waits for binding within the dispatch lease; if it never appears, the dispatch fails and recovery advances generation. Wire these callbacks and claim checks in both API and processor composition roots in this task; local disabled mode returns a real unmetered `ExecutionPermit`, not a no-op callback. No earlier or later task is responsible for making initial-wave quota enforcement/binding functional.

Both composition roots inject the callbacks through the stable CND dependency object `ExecutionPolicyConfig(deployment_limit, dispatch_lease_seconds, ExecutionCallbacks(BillingConcurrencyPolicy, BillingExecutionStarted))`; no callback becomes a separate constructor parameter. If AWS Task 8 merged first, replace its default `ExecutionPolicyConfig` in this controller-serial task. If it merges later, its rebase preserves this configured hook. Each constructor remains within the four-parameter gate because the nested immutable dependency value objects carry the related policy settings and callbacks.

- [ ] **Step 5: Gate canonical run creation before persistence**

```python
authorization = entitlement_gate.authorize_create_run(command)
if authorization.budget_reservation_id is None:
    run = control_plane.create_unmetered_run(
        AuthorizedRunCommand.from_request(command, authorization),
    )
else:
    run = control_plane.get_run(command.tenant_id, command.run_id)
    if run is None:
        raise RuntimeError(f"run_id={command.run_id} missing_after_reservation")
if run.state in {RunState.PLANNED, RunState.WAITING_INPUTS}:
    return run_planning.launch(run.tenant_id, run.run_id).run
return run
```

`AuthorizedRunCommand.from_request` copies `tenant_id`, `run_id`, `competencia`, `dataset_name`,
and `dependencies` into the canonical `Run` and stores the full immutable `RunAuthorization`.
The command is internal orchestration input built after CND-060 registry resolution; no public
request model exposes its dependency tuple. Replay requires the same canonical hash over those
fields. Run planning resolves only control-plane-indexed immutable manifests, persists the complete
three-stage DAG, and starts its initial wave; replay of a Run already in `PROCESSING`, `PUBLISHING`
or a terminal state returns the canonical Run without calling `launch` again. The orchestration
layer supplies `requested_limit=min(ready_units, deployment_limit)` and the permit policy clamps it
to `min(requested_limit, authorization.max_concurrency)` on every dispatch.

- [ ] **Step 6: Run core integration gates**

Run: `uv run pytest packages/cnes_domain/tests/billing packages/cnes_infra/tests/billing packages/cnes_domain/tests/orchestration packages/cnes_infra/tests/control_plane/test_dynamodb_adapter.py packages/cnes_infra/tests/control_plane/test_sqlite_adapter.py packages/cnes_infra/tests/executor/test_local_pool.py packages/cnes_infra/tests/executor/test_step_functions.py apps/central_api/tests/services/test_run_authorization.py apps/central_api/tests/services/test_run_planning.py apps/data_processor/tests/orchestration/test_coordinator.py apps/data_processor/tests/orchestration/test_unit_worker.py apps/central_api/tests/test_app_wiring.py tests/integration/billing/test_execution_binding.py tests/negative/test_local_billing_has_no_remote_dependency.py -q`

Expected: PASS, including three sequential wave bindings, pending-start dispatch reuse, generation advance after terminal/bind failure without a claim, exact permit-object identity, one active dispatch per Run, claim denial before matching companion bind, bind-failure cancellation and full local transaction rollback.

Run: `uv run ruff check packages/cnes_domain packages/cnes_infra apps/central_api`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add packages/cnes_domain/src/cnes_domain/__init__.py \
  packages/cnes_domain/src/cnes_domain/billing/__init__.py \
  packages/cnes_domain/src/cnes_domain/ports/__init__.py \
  packages/cnes_infra/src/cnes_infra/__init__.py \
  packages/cnes_infra/src/cnes_infra/billing/__init__.py \
  packages/cnes_infra/src/cnes_infra/config.py \
  apps/central_api/src/central_api/deps.py \
  apps/central_api/src/central_api/services/run_authorization.py \
  apps/central_api/src/central_api/composition.py \
  apps/data_processor/src/data_processor/composition.py \
  packages/cnes_domain/src/cnes_domain/ports/control_plane.py \
  packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_adapter.py \
  packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_claims.py \
  packages/cnes_infra/src/cnes_infra/control_plane/sqlite_adapter.py \
  packages/cnes_infra/tests/billing/test_config.py \
  packages/cnes_infra/tests/billing/test_control_plane_extensions.py \
  apps/central_api/tests/services/test_run_authorization.py \
  tests/integration/billing/test_execution_binding.py \
  .env.example
git commit -m "feat(billing): wire core entitlement enforcement"
```

### Task 7: BIL-020 — Stripe Gateway for Checkout and Portal

**Files:**
- Create: `packages/cnes_infra/src/cnes_infra/billing/secrets_manager.py`
- Create: `packages/cnes_infra/src/cnes_infra/billing/stripe_gateway.py`
- Test: `packages/cnes_infra/tests/billing/test_secrets_manager.py`
- Test: `packages/cnes_infra/tests/billing/test_stripe_gateway.py`

**Interfaces:**
- Consumes: `SecretProviderPort`, `StripeGatewayPort`, `CreateStripeCustomerCommand`, `CheckoutCommand`, `PortalCommand`, `HostedSession`, `StripeBillingState`; Stripe client is injected as `StripeClientProtocol`. Entry gate: `AWS-011` is integrated.
- Produces: `SecretsManagerSecretProvider(client: SecretsManagerClient)`, `SecretProviderError(code: str, retryable: bool)`, and `StripeGateway(client: StripeClientProtocol, config: StripeGatewayConfig)`.

- [ ] **Step 1: Write RED request-shape tests**

```python
def test_checkout_usa_hosted_session_metadata_opaca_e_idempotencia(stripe_spy) -> None:
    gateway = make_gateway(stripe_spy)
    result = gateway.create_checkout(make_checkout_command())
    stripe_spy.checkout.sessions.create.assert_called_once_with(
        mode="subscription",
        customer="cus_01",
        line_items=[{"price": "price_01", "quantity": 1}],
        metadata={"billing_account_id": "ba_01", "plan_version_id": "plan_v1"},
        success_url="https://app.example.test/billing/success?session_id={CHECKOUT_SESSION_ID}",
        cancel_url="https://app.example.test/billing/cancel",
        idempotency_key="checkout:req_01",
    )
    assert result.url.startswith("https://checkout.stripe.com/")

def test_customer_usa_apenas_id_opaco(stripe_spy) -> None:
    customer = make_gateway(stripe_spy).create_customer(
        CreateStripeCustomerCommand("ba_01", "req_01"),
    )
    stripe_spy.customers.create.assert_called_once_with(
        metadata={"billing_account_id": "ba_01"},
        idempotency_key="customer:req_01",
    )
    assert customer.stripe_customer_id == "cus_01"
```

Add a test that rejects a price not mapped to the requested immutable `PlanVersion`, Portal uses server-selected return URL, and logged arguments never contain email, municipality name, card, CNPJ or CPF.

Add focused adapter tests with a botocore-compatible fake: `get_secret("arn:aws:secretsmanager:us-east-1:123456789012:secret:cnes/stripe")` sends exactly that value as `SecretId` and returns a non-empty `SecretString`; blank ARN, missing/empty `SecretString`, or any `SecretBinary` fails closed; `ThrottlingException`, Secrets Manager `InternalServiceError`, `ServiceUnavailableException`, and transport `BotoCoreError` are retryable, while every other `ClientError` is non-retryable. Assert the exception string and captured logs contain neither the ARN nor a returned secret value.

- [ ] **Step 2: Run test to verify RED**

Run: `uv run pytest packages/cnes_infra/tests/billing/test_secrets_manager.py packages/cnes_infra/tests/billing/test_stripe_gateway.py -q`

Expected: FAIL during collection because `SecretsManagerSecretProvider` and `StripeGateway` do not exist.

- [ ] **Step 3: Implement the Secrets Manager boundary and SDK wrapper**

```python
_RETRYABLE_SECRET_CODES = frozenset({
    "ThrottlingException",
    "InternalServiceError",
    "ServiceUnavailableException",
})

class SecretProviderError(RuntimeError):
    def __init__(self, code: str, retryable: bool) -> None:
        self.code = code
        self.retryable = retryable
        super().__init__(
            f"secret_provider_error code={code} retryable={str(retryable).lower()}",
        )

class SecretsManagerSecretProvider:
    def __init__(self, client: SecretsManagerClient) -> None:
        self._client = client

    def get_secret(self, secret_arn: str) -> str:
        if not secret_arn.strip():
            raise SecretProviderError("secret_arn_empty", retryable=False)
        try:
            response = self._client.get_secret_value(SecretId=secret_arn)
        except ClientError as error:
            code = str(error.response.get("Error", {}).get("Code", "client_error"))
            raise SecretProviderError(
                code, retryable=code in _RETRYABLE_SECRET_CODES,
            ) from error
        except BotoCoreError as error:
            raise SecretProviderError("transport_error", retryable=True) from error
        value = response.get("SecretString")
        if not isinstance(value, str) or not value:
            raise SecretProviderError("secret_not_text", retryable=False)
        return value

@dataclass(frozen=True, slots=True)
class StripeGatewayConfig:
    success_url: str
    cancel_url: str
    portal_return_url: str

def create_portal(self, command: PortalCommand) -> HostedSession:
    session = self._client.billing_portal.sessions.create(
        customer=command.stripe_customer_id,
        return_url=self._config.portal_return_url,
        idempotency_key=f"portal:{command.idempotency_key}",
    )
    return HostedSession(session.id, session.url)
```

`SecretsManagerClient` is a narrow Protocol exposing `get_secret_value(*, SecretId: str) -> Mapping[str, object]`; production accepts the boto3 client without importing a generated service type. Never catch `SecretProviderError` to substitute a key, log the response, accept `SecretBinary`, or cache the secret in a model/setting. Task 13 owns the mode-dependent factory and is the first task allowed to create a Secrets Manager client. URLs are exact configuration values validated as HTTPS and members of `BILLING_RETURN_ORIGINS`.

- [ ] **Step 4: Implement current-state retrieval used by reorder-safe projection**

`get_current_state(request)` retrieves the named Subscription when present; otherwise it lists the Customer's current CnesData subscription, then retrieves latest Invoice reference and active entitlement summary. It maps the Stripe Price to one internal immutable `PlanVersion` and returns `StripeBillingState`. Zero or multiple current subscriptions raises `StripeMappingError("customer_id=<id> subscription_count=<n>")`; unknown Price raises `StripeMappingError("price_id=<id>")`. Both make the event retryable rather than granting a default plan.

- [ ] **Step 5: Run tests and lint**

Run: `uv run pytest packages/cnes_infra/tests/billing/test_secrets_manager.py packages/cnes_infra/tests/billing/test_stripe_gateway.py --cov=cnes_infra.billing.secrets_manager --cov=cnes_infra.billing.stripe_gateway --cov-branch -q`

Expected: PASS and 100% branch coverage for both modules, including retryable/permanent error classification and redaction assertions.

Run: `uv run ruff check packages/cnes_infra/src/cnes_infra/billing/secrets_manager.py packages/cnes_infra/src/cnes_infra/billing/stripe_gateway.py packages/cnes_infra/tests/billing/test_secrets_manager.py packages/cnes_infra/tests/billing/test_stripe_gateway.py`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/billing/secrets_manager.py \
  packages/cnes_infra/src/cnes_infra/billing/stripe_gateway.py \
  packages/cnes_infra/tests/billing/test_secrets_manager.py \
  packages/cnes_infra/tests/billing/test_stripe_gateway.py
git commit -m "feat(billing): create Stripe checkout and portal sessions"
```

### Task 8: BIL-020 — Billing Owner API

**Files:**
- Create: `apps/central_api/src/central_api/routes/billing.py`
- Test: `apps/central_api/tests/routes/test_billing.py`

**Interfaces:**
- Consumes: `StripeGatewayPort`, `BillingCatalogPort`, AWS-011 `OidcPrincipal`/`AuthorizedTenant`, `EntitlementProjectionPort`.
- Produces: `POST /api/v1/billing/accounts`, `POST /api/v1/billing/accounts/{billing_account_id}/transfer`, `POST /api/v1/billing/checkout`, `POST /api/v1/billing/portal`, `GET /api/v1/billing/status`.

- [ ] **Step 1: Write RED authorization and redirect-proof tests**

```python
def test_checkout_exige_billing_owner(client, billing_catalog, stripe_gateway) -> None:
    billing_catalog.get_account.return_value = make_account(owner_user_id="other")
    response = client.post(
        "/api/v1/billing/checkout",
        json={
            "billing_account_id": "ba_01",
            "plan_version_id": "plan_v1",
            "idempotency_key": "checkout-request-01",
        },
    )
    assert response.status_code == 403
    stripe_gateway.create_checkout.assert_not_called()

def test_redirect_de_sucesso_nao_altera_entitlement(client, projection) -> None:
    response = client.get("/api/v1/billing/status?checkout_session_id=cs_01")
    assert response.json()["state"] == "pending"
    projection.compare_and_set_snapshot.assert_not_called()

def test_criacao_de_conta_anexa_customer_idempotente(client, catalog, stripe_gateway) -> None:
    response = client.post(
        "/api/v1/billing/accounts",
        json={"idempotency_key": "account-request-0001"},
    )
    assert response.status_code == 201
    assert catalog.attach_customer.call_count == 1
    assert stripe_gateway.create_customer.call_count == 1
```

- [ ] **Step 2: Run tests to verify RED**

Run: `uv run pytest apps/central_api/tests/routes/test_billing.py -q`

Expected: FAIL because the router does not exist.

- [ ] **Step 3: Implement exact request/response models and owner checks**

```python
class CheckoutCreate(BaseModel):
    billing_account_id: str = Field(min_length=1, max_length=128)
    plan_version_id: str = Field(min_length=1, max_length=128)
    idempotency_key: str = Field(min_length=16, max_length=128)

class BillingAccountCreate(BaseModel):
    idempotency_key: str = Field(min_length=16, max_length=128)

class BillingAccountTransfer(BaseModel):
    new_owner_user_id: str = Field(min_length=1, max_length=128)
    reason_code: str = Field(min_length=1, max_length=128)

class HostedSessionOut(BaseModel):
    session_id: str
    url: HttpUrl

def require_billing_owner(
    account: BillingAccount,
    principal: OidcPrincipal,
    authorized_tenant: AuthorizedTenant | None,
) -> None:
    is_admin = authorized_tenant is not None and authorized_tenant.role == "admin"
    if principal.subject != account.owner_user_id and not is_admin:
        raise HTTPException(status_code=403, detail="billing_owner_required")
```

Account creation generates an opaque internal ID, conditionally persists it, calls `create_customer` with the same idempotency key and conditionally attaches the returned Customer; replay after a crash returns the same account/customer. Transfer requires current owner or admin, validates the target user server-side and writes actor/reason audit. Checkout resolves the requested account by base key, validates ownership and `PlanVersion`, and calls Task 7. Portal accepts `billing_account_id` plus idempotency key. Status accepts `billing_account_id`, returns projection fields safe for UI, never calls Stripe and never treats a supplied session ID as payment evidence.

In `BILLING_MODE=disabled`, account, transfer, Checkout and Portal endpoints return `404 {"detail":"billing_disabled"}` without constructing a Stripe dependency; status returns `{"state":"disabled","plan_version_id":"local-unmetered-v1"}` from the local adapter.

- [ ] **Step 4: Add retryable and audit behavior**

Stripe unavailability returns `503` with `Retry-After: 5`; it does not create a local subscription. Successful session creation appends `checkout.session_created` containing actor, billing account, plan version, opaque Stripe session ID and idempotency key hash.

- [ ] **Step 5: Run route tests**

Run: `uv run pytest apps/central_api/tests/routes/test_billing.py -q`

Expected: PASS, including owner/admin/non-owner and pending redirect cases.

- [ ] **Step 6: Commit**

```bash
git add apps/central_api/src/central_api/routes/billing.py \
  apps/central_api/tests/routes/test_billing.py
git commit -m "feat(api): expose owner-authorized billing sessions"
```

### Task 9: BIL-021 — Signed Raw-Body Webhook Inbox

**Files:**
- Create: `apps/central_api/src/central_api/routes/stripe_webhook.py`
- Create: `packages/cnes_infra/src/cnes_infra/billing/webhook_inbox.py`
- Test: `apps/central_api/tests/routes/test_stripe_webhook.py`
- Test: `packages/cnes_infra/tests/billing/test_webhook_inbox.py`

**Interfaces:**
- Consumes: Stripe SDK signature construction, Task 7 `SecretProviderPort`/`SecretsManagerSecretProvider`, and the DynamoDB table interface from integrated `AWS-010`; Task 4 projection exists. Concrete client construction remains deferred to serial Task 13.
- Produces: `StripeWebhookVerifier.verify(payload: bytes, signature: str) -> StripeEvent`, `WebhookInbox.accept(event: StripeEvent) -> InboxAcceptResult`, `POST /api/v1/billing/webhooks/stripe`.

- [ ] **Step 1: Write RED signature and raw-byte tests**

```python
def test_webhook_valida_assinatura_sobre_body_raw(client, verifier, inbox) -> None:
    payload = b'{"id":"evt_01","type":"invoice.paid"}'
    response = client.post(
        "/api/v1/billing/webhooks/stripe",
        content=payload,
        headers={"Stripe-Signature": sign(payload, "whsec_test")},
    )
    verifier.verify.assert_called_once_with(payload, ANY)
    inbox.accept.assert_called_once()
    assert response.status_code == 200

def test_webhook_assinatura_invalida_nao_persiste(client, inbox) -> None:
    response = client.post(
        "/api/v1/billing/webhooks/stripe",
        content=b"{}",
        headers={"Stripe-Signature": "invalid"},
    )
    assert response.status_code == 400
    inbox.accept.assert_not_called()
```

- [ ] **Step 2: Run tests to verify RED**

Run: `uv run pytest apps/central_api/tests/routes/test_stripe_webhook.py packages/cnes_infra/tests/billing/test_webhook_inbox.py -q`

Expected: FAIL because route and inbox are absent.

- [ ] **Step 3: Implement raw body ingress and supported event allowlist**

```python
@router.post("/webhooks/stripe", status_code=200)
async def stripe_webhook(request: Request) -> dict[str, bool]:
    payload = await request.body()
    signature = request.headers.get("Stripe-Signature", "")
    event = request.app.state.stripe_webhook_verifier.verify(payload, signature)
    request.app.state.webhook_inbox.accept(event)
    return {"received": True}
```

`StripeWebhookVerifier.verify` calls `stripe.Webhook.construct_event(payload, signature, webhook_secret)` and maps only the safe `StripeEvent` fields from the verified object. The Stripe import is local to the verifier method so branch tests inject a fake SDK before serial Task 13 updates the dependency manifest; production composition always supplies the official SDK and Secrets Manager value.

Allow exactly: `checkout.session.completed`, `customer.subscription.created`, `customer.subscription.updated`, `customer.subscription.deleted`, `customer.subscription.paused`, `customer.subscription.resumed`, `invoice.paid`, `invoice.payment_failed`, `invoice.payment_action_required`, `entitlements.active_entitlement_summary.updated`. A valid unsupported event is recorded as `IGNORED` and returns 200.

- [ ] **Step 4: Implement conditional inbox lease/dedupe**

Use `PK=STRIPE_EVENT#<event_id>`, `SK=EVENT`. First accept writes `PENDING`, `event_type`, `created_at`, `payload_sha256`, opaque object IDs and `received_at`. A duplicate `PROCESSED` or unexpired `PROCESSING` returns `DUPLICATE`; stale `PROCESSING` can be claimed later. Store no payment method or card data. DynamoDB failure returns `503` so Stripe retries; duplicates return 200.

- [ ] **Step 5: Run signature, duplicate and concurrent retry tests**

Run: `uv run pytest apps/central_api/tests/routes/test_stripe_webhook.py packages/cnes_infra/tests/billing/test_webhook_inbox.py -q`

Expected: PASS; duplicate payload produces one inbox item and HTTP 200 on every delivery.

- [ ] **Step 6: Commit**

```bash
git add apps/central_api/src/central_api/routes/stripe_webhook.py \
  apps/central_api/tests/routes/test_stripe_webhook.py \
  packages/cnes_infra/src/cnes_infra/billing/webhook_inbox.py \
  packages/cnes_infra/tests/billing/test_webhook_inbox.py
git commit -m "feat(billing): verify and deduplicate Stripe webhooks"
```

### Task 10: BIL-021 — Reorder-Safe Entitlement Projector

**Files:**
- Create: `packages/cnes_infra/src/cnes_infra/billing/projector.py`
- Test: `packages/cnes_infra/tests/billing/test_projector.py`
- Test: `tests/property/test_stripe_event_reordering.py`

**Interfaces:**
- Consumes: `WebhookInbox.claim/mark_processed/mark_failed`, `StripeGatewayPort.get_current_state`, `DynamoEntitlementProjection.compare_and_set_snapshot`, `BillingAuditPort`.
- Produces: `StripeEventProjector.process(event_id: str) -> ProjectionResult`.

- [ ] **Step 1: Write RED duplicate/reorder/retry tests**

```python
@pytest.mark.race
def test_eventos_fora_de_ordem_convergem_ao_estado_atual(projector, stripe) -> None:
    stripe.get_current_state.return_value = make_state(status="active", version="plan_v2")
    projector.process("evt_newer")
    projector.process("evt_older")
    snapshot = projector.projection.get_snapshot("ba_01", ReadConsistency.STRONG)
    assert snapshot.subscription_status is SubscriptionStatus.ACTIVE
    assert snapshot.plan_version_id == "plan_v2"

def test_retry_concorrente_aplica_evento_uma_vez(projector) -> None:
    results = run_concurrently(lambda: projector.process("evt_01"), count=2)
    assert sum(result.applied for result in results) == 1
```

- [ ] **Step 2: Run tests to verify RED**

Run: `uv run pytest packages/cnes_infra/tests/billing/test_projector.py tests/property/test_stripe_event_reordering.py -q`

Expected: FAIL with missing `StripeEventProjector`.

- [ ] **Step 3: Implement lease, refresh-current-state and CAS loop**

```python
def process(self, event_id: str) -> ProjectionResult:
    claimed = self._inbox.claim(event_id, self._clock.now())
    if not claimed.acquired:
        return ProjectionResult(event_id, False, None)
    try:
        account = self._catalog.get_account_by_customer(claimed.customer_id)
        if account is None:
            raise RetryableBillingError("stripe_customer_mapping_missing")
        state = self._stripe.get_current_state(
            StripeStateRequest(claimed.customer_id, claimed.subscription_id),
        )
        result = self._project_current_state(claimed, state)
    except RetryableBillingError as error:
        self._inbox.mark_failed(event_id, str(error), retryable=True)
        raise
    self._inbox.mark_processed(event_id, result.entitlement_version)
    return result
```

`_project_current_state` maps current Subscription, active entitlements and internal `PlanVersion`, derives grace and access status, then retries snapshot CAS on a concurrent version. It never trusts event delivery order or checkout redirect. Every applied change emits `subscription.*` and `entitlement.changed` audit/outbox events with `source_event_id`.

- [ ] **Step 4: Test every lifecycle event mapping**

Add cases for all ten allowlisted event types, including `cancel_at_period_end`, paid renewal period shift, payment failure/grace, paused/resumed and deleted subscription. Invoice events still refresh Subscription and entitlements before projection.

- [ ] **Step 5: Run tests, property suite and coverage**

Run: `uv run pytest packages/cnes_infra/tests/billing/test_projector.py tests/property/test_stripe_event_reordering.py --hypothesis-show-statistics -q`

Expected: PASS for arbitrary event permutations and duplicate counts.

- [ ] **Step 6: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/billing/projector.py \
  packages/cnes_infra/tests/billing/test_projector.py \
  tests/property/test_stripe_event_reordering.py
git commit -m "feat(billing): project current Stripe state safely"
```

### Task 11: BIL-021 — Webhook Recovery

**Files:**
- Create: `packages/cnes_infra/src/cnes_infra/billing/recovery.py`
- Test: `packages/cnes_infra/tests/billing/test_recovery.py`

**Interfaces:**
- Consumes: Task 9 inbox, Task 10 projector, `StripeGatewayPort.list_events`, `ClockPort`.
- Produces: `WebhookRecovery.run(request: RecoveryRequest) -> RecoveryResult`.

- [ ] **Step 1: Write RED recovery tests**

```python
def test_recovery_reclama_processing_vencido(recovery, inbox) -> None:
    inbox.list_stale.return_value = (make_inbox_event("evt_01"),)
    result = recovery.run(RecoveryRequest(lookback_hours=72, batch_size=100))
    assert result.reprocessed == 1

def test_recovery_importa_evento_nao_entregue(recovery, stripe, inbox) -> None:
    stripe.list_events.return_value = (make_stripe_event("evt_missing"),)
    recovery.run(RecoveryRequest(lookback_hours=72, batch_size=100))
    inbox.accept.assert_called_once()
```

- [ ] **Step 2: Run tests to verify RED**

Run: `uv run pytest packages/cnes_infra/tests/billing/test_recovery.py -q`

Expected: FAIL with missing recovery module.

- [ ] **Step 3: Implement bounded, cursor-safe recovery**

Use exact defaults `STRIPE_RECOVERY_LOOKBACK_HOURS=72`, `STRIPE_RECOVERY_BATCH_SIZE=100`, `STRIPE_PROCESSING_LEASE_SECONDS=300`. `WebhookRecovery.run` first reclaims expired `PROCESSING`, then pages Stripe Events backward within the lookback, conditionally inserts unseen supported events, and invokes projector. Cursor and last-success timestamp persist at `PK=BILLING#SYSTEM`, `SK=RECOVERY#STRIPE`; a crash resumes without dropping or duplicating effects.

- [ ] **Step 4: Add crash/resume and pagination coverage**

```python
def test_crash_apos_import_reprocessa_sem_duplicar(recovery, projector) -> None:
    projector.process.side_effect = [
        RuntimeError("worker_crash"),
        ProjectionResult("evt_01", True, 2),
    ]
    with pytest.raises(RuntimeError, match="worker_crash"):
        recovery.run(RecoveryRequest(lookback_hours=72, batch_size=100))
    result = recovery.run(RecoveryRequest(lookback_hours=72, batch_size=100))
    assert result.reprocessed == 1
```

Scheduling and the executable entrypoint are added in serial Task 13 after the app manifest and workspace membership exist.

- [ ] **Step 5: Run tests**

Run: `uv run pytest packages/cnes_infra/tests/billing/test_recovery.py -q`

Expected: PASS; crash/restart test processes every event once.

- [ ] **Step 6: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/billing/recovery.py \
  packages/cnes_infra/tests/billing/test_recovery.py
git commit -m "feat(billing): recover Stripe webhook delivery"
```

### Task 12: BIL-021 — Projection Contract and Failure Matrix

**Files:**
- Create: `tests/integration/billing/test_webhook_projection.py`
- Create: `tests/chaos/test_stripe_projection_failures.py`

**Interfaces:**
- Consumes: Tasks 4, 9–11 with the DynamoDB Local fixture from `CND-025` and a fake Stripe HTTP boundary.
- Produces: integrated evidence for signature, dedupe, reordering, retry, stale lease and recovery.

- [ ] **Step 1: Write the failing integrated scenario**

```python
def test_webhook_confirmado_e_unica_prova_de_acesso(billing_stack) -> None:
    billing_stack.checkout.complete_redirect("cs_01")
    assert billing_stack.gate.create_run_denial() == "snapshot_missing"
    billing_stack.webhooks.deliver_signed("checkout.session.completed", "evt_01")
    billing_stack.worker.drain()
    assert billing_stack.gate.authorize_create_run().plan_version_id == "plan_v1"
```

- [ ] **Step 2: Run test to verify RED against any missing composition**

Run: `uv run pytest tests/integration/billing/test_webhook_projection.py -q`

Expected: FAIL at the first unintegrated boundary, identifying the missing adapter fixture or mapping.

- [ ] **Step 3: Complete fixtures and failure cases without changing production interfaces**

Add cases for duplicate deliveries, reversed delivery, simultaneous retries, Stripe 503, DynamoDB 503, invalid signature, unknown Price, stale processing lease and recovery import. Assert critical gates fail closed while DynamoDB is unavailable and the last valid snapshot governs only until `valid_until`.

- [ ] **Step 4: Run integration and chaos suites**

Run: `uv run pytest tests/integration/billing/test_webhook_projection.py tests/chaos/test_stripe_projection_failures.py -q`

Expected: PASS; all failures either retry safely or deny access without losing the inbox item.

- [ ] **Step 5: Commit**

```bash
git add tests/integration/billing/test_webhook_projection.py \
  tests/chaos/test_stripe_projection_failures.py
git commit -m "test(billing): cover webhook projection failures"
```

### Task 13: Integration-Owned — Stripe Dependencies, App Composition and Contracts

**Serial entry gate:** Start only after Tasks 6–12 and Source plan Task 4 are merged; AWS plan Task
8 is already an ancestor through the fixed composition queue. The controller holds an exclusive
composition lock for this entire task and preserves every retained-source registry entry.

**Files:**
- Modify: `pyproject.toml`
- Modify: `packages/cnes_infra/pyproject.toml`
- Create: `apps/billing_worker/pyproject.toml`
- Create: `apps/billing_worker/CLAUDE.md`
- Create: `apps/billing_worker/src/billing_worker/__init__.py`
- Create: `apps/billing_worker/src/billing_worker/main.py`
- Create: `apps/billing_worker/src/billing_worker/worker.py`
- Create: `apps/billing_worker/tests/test_worker.py`
- Modify: `uv.lock`
- Modify: `packages/cnes_infra/src/cnes_infra/__init__.py`
- Create: `packages/cnes_infra/src/cnes_infra/billing/composition.py`
- Create: `packages/cnes_infra/tests/billing/test_composition.py`
- Modify: `apps/central_api/src/central_api/app.py`
- Modify: `apps/central_api/src/central_api/deps.py`
- Modify: `.env.example`
- Modify: `pytest.ini`
- Modify: `docs/contracts/openapi.json`
- Test: `apps/central_api/tests/test_app_wiring.py`

**Interfaces:**
- Consumes: merged Tasks 6–12; AWS Task 8 `RuntimeComponents`, `build_runtime(profile, values, session)`, and its integrated table/session interfaces; Task 7 `SecretsManagerSecretProvider`. It does not modify `create_aws_clients`, `AwsClients`, `build_aws_runtime`, or any AWS core factory.
- Produces: `build_secret_provider(mode: BillingMode, session: SessionProtocol) -> SecretProviderPort | None`, installed official Stripe dependency, live routers and worker composition, documented env contract and generated OpenAPI.

- [ ] **Step 1: Write RED app-wiring tests**

```python
def test_app_inclui_billing_e_webhook_routes() -> None:
    app = _make_app()
    paths = {route.path for route in app.routes}
    assert "/api/v1/billing/accounts" in paths
    assert "/api/v1/billing/accounts/{billing_account_id}/transfer" in paths
    assert "/api/v1/billing/checkout" in paths
    assert "/api/v1/billing/portal" in paths
    assert "/api/v1/billing/status" in paths
    assert "/api/v1/billing/webhooks/stripe" in paths
```

In `packages/cnes_infra/tests/billing/test_composition.py`, lock down the billing-owned factory independently of the AWS core factory:

```python
def test_disabled_nao_cria_client_secrets_manager(session_spy) -> None:
    assert build_secret_provider(BillingMode.DISABLED, session_spy) is None
    session_spy.client.assert_not_called()

def test_stripe_cria_provider_com_client_secrets_manager(session_spy) -> None:
    provider = build_secret_provider(BillingMode.STRIPE, session_spy)
    session_spy.client.assert_called_once_with("secretsmanager")
    assert isinstance(provider, SecretsManagerSecretProvider)
```

The app-wiring test additionally patches the shared boto3 `Session`: local startup must make no `secretsmanager` call, while Stripe startup makes exactly one and injects the resulting provider into both API and bounded worker dependency bundles. Assert the existing AWS `create_aws_clients` call signature remains unchanged.

- [ ] **Step 2: Run test to verify RED**

Run: `uv run pytest packages/cnes_infra/tests/billing/test_composition.py apps/central_api/tests/test_app_wiring.py -k 'billing or secret_provider' -q`

Expected: FAIL because `build_secret_provider` is missing and shared `app.py` has not mounted the routers.

- [ ] **Step 3: Apply dependency and workspace changes serially**

Add `stripe>=12.0` to `cnes-infra`; retain the boto3 dependency already owned by `CND-025`. Add `apps/billing_worker` to the uv workspace, make the worker depend on `cnes-infra`, and run `uv lock`. The new `CLAUDE.md` states that the worker owns webhook/recovery/reconciliation execution, never data processing, and must remain horizontally safe through DynamoDB leases.

- [ ] **Step 4: Wire mode-dependent dependencies and secrets**

Implement the billing-owned factory without changing the AWS Task 8 client/runtime bundle:

```python
def build_secret_provider(
    mode: BillingMode, session: SessionProtocol,
) -> SecretProviderPort | None:
    if mode is BillingMode.DISABLED:
        return None
    if mode is BillingMode.STRIPE:
        return SecretsManagerSecretProvider(session.client("secretsmanager"))
    raise BillingConfigurationError(f"billing_mode_unknown mode={mode}")
```

In `disabled`, call this factory before the local billing bundle only to obtain `None`; do not construct Stripe, Secrets Manager or worker network clients. Billing and webhook routes return the documented disabled response before accessing Stripe-specific app state. In `stripe`, call it once with the same boto3 `Session` already accepted by the integrated AWS `build_runtime`, require a non-`None` provider, resolve `STRIPE_SECRET_KEY_SECRET_ARN` and `STRIPE_WEBHOOK_SECRET_SECRET_ARN`, and pass the returned text only to `stripe.StripeClient(api_key)` and `StripeWebhookVerifier`; never store either value on settings, `RuntimeComponents`, `app.state`, dataclasses, or logs. Resolve the table binding and three allowlisted return URLs; attach `billing_catalog`, `entitlement_gate`, `stripe_gateway`, `stripe_webhook_verifier`, and `webhook_inbox` to app state. Mount both routers. The bounded worker uses the same billing-owned factory; no code adds Secrets Manager to `AwsClients` or `create_aws_clients`.

Create the worker with deterministic CLI commands `billing-worker inbox --limit 100` and `billing-worker recover`. `main(argv: Sequence[str] | None = None) -> int` returns `0` after a successful bounded cycle, `1` when durable retry state could not be recorded, and `2` for invalid command input. Scheduling belongs to deployment/IaC and is not created here.

- [ ] **Step 5: Export API schema and add exact env documentation**

Document `BILLING_MODE`, `BILLING_ENFORCEMENT_MODE=off`, `BILLING_CACHE_TTL_SECONDS=60`, secret ARNs, `BILLING_SUCCESS_URL`, `BILLING_CANCEL_URL`, `BILLING_PORTAL_RETURN_URL`, `BILLING_RETURN_ORIGINS`, `STRIPE_RECOVERY_LOOKBACK_HOURS=72`, `STRIPE_RECOVERY_BATCH_SIZE=100`, and `STRIPE_PROCESSING_LEASE_SECONDS=300`. Run `uv run python scripts/export_openapi.py` and assert only billing paths/schemas are added.

- [ ] **Step 6: Run integration checks**

Run: `uv sync --all-packages && uv run pytest packages/cnes_infra/tests/billing/test_composition.py apps/central_api/tests/test_app_wiring.py apps/central_api/tests/routes/test_billing.py apps/central_api/tests/routes/test_stripe_webhook.py apps/billing_worker/tests -q`

Expected: PASS.

Run: `uv run ruff check .`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml packages/cnes_infra/pyproject.toml \
  apps/billing_worker/pyproject.toml apps/billing_worker/CLAUDE.md \
  apps/billing_worker/src/billing_worker apps/billing_worker/tests uv.lock \
  packages/cnes_infra/src/cnes_infra/billing/composition.py \
  packages/cnes_infra/tests/billing/test_composition.py \
  packages/cnes_infra/src/cnes_infra/__init__.py \
  apps/central_api/src/central_api/app.py apps/central_api/src/central_api/deps.py \
  apps/central_api/tests/test_app_wiring.py .env.example pytest.ini \
  docs/contracts/openapi.json
git commit -m "build(billing): integrate Stripe runtime"
```

### Task 14: BIL-022 — Immediate Revocation Service

**Files:**
- Create: `packages/cnes_domain/src/cnes_domain/billing/revocation.py`
- Test: `packages/cnes_domain/tests/billing/test_revocation.py`
- Create: `packages/cnes_infra/src/cnes_infra/billing/dynamodb_revocation.py`
- Test: `packages/cnes_infra/tests/billing/test_dynamodb_revocation.py`

**Interfaces:**
- Consumes: `EntitlementSnapshot`, upstream canonical Run/fence/control-plane contracts, `ProcessorExecutorPort`, `BillingAuditPort`.
- Produces: `ImmediateRevocationService.revoke(command: ImmediateRevocationCommand) -> RevocationResult`.

- [ ] **Step 1: Write RED ordering and idempotency tests**

```python
def test_revogacao_invalida_snapshot_e_fences_antes_de_cancelar_executor(service, calls) -> None:
    service.revoke(make_revocation_command(reason_code="fraud_confirmed"))
    assert calls == [
        "snapshot_admin_revoked",
        "run_01_fence_incremented",
        "run_02_fence_incremented",
        "executor_run_01_cancel",
        "executor_run_02_cancel",
    ]

def test_falha_step_functions_nao_restaura_fence(service, executor, control_plane) -> None:
    executor.cancel.side_effect = RuntimeError("step_functions_unavailable")
    result = service.revoke(make_revocation_command())
    assert result.cancel_failures == ("run_01", "run_02")
    assert control_plane.current_fence("run_01") > control_plane.old_fence("run_01")
```

Add a three-wave case whose normalize, reconcile and materialize bindings are sequential; revoke during materialize must cancel only the latest materialize `dispatch_id`/ref, page every remaining nonterminal unit to `CANCELED`, transition the Run to `CANCELED`, and leave both prior completed bindings immutable for audit. A retry of `revoke` performs no second fence increment or executor cancellation.

- [ ] **Step 2: Run tests to verify RED**

Run: `uv run pytest packages/cnes_domain/tests/billing/test_revocation.py packages/cnes_infra/tests/billing/test_dynamodb_revocation.py -q`

Expected: FAIL with missing revocation modules.

- [ ] **Step 3: Implement exact command/result and state-first algorithm**

```python
@dataclass(frozen=True, slots=True)
class ImmediateRevocationCommand:
    billing_account_id: str
    actor_id: str
    reason_code: str
    requested_at: datetime

@dataclass(frozen=True, slots=True)
class RevocationResult:
    entitlement_version: int
    fenced_run_ids: tuple[str, ...]
    cancel_failures: tuple[str, ...]

@dataclass(frozen=True, slots=True)
class RevokeRunCommand:
    tenant_id: str
    run_id: str
    expected_state: RunState
    expected_fencing_token: int
    reason_code: str
    requested_at: datetime

@dataclass(frozen=True, slots=True)
class RevocableRunPage:
    runs: tuple[RunBillingState, ...]
    next_cursor: str | None

@dataclass(frozen=True, slots=True)
class CancelRunUnitsCommand:
    tenant_id: str
    run_id: str
    expected_run_fencing_token: int
    limit: int
    cursor: str | None
    canceled_at: datetime

@dataclass(frozen=True, slots=True)
class CancelRunUnitsResult:
    canceled_unit_ids: tuple[str, ...]
    next_cursor: str | None
    run_canceled: bool
```

`revoke` requires a non-empty administrative reason. It first strongly reads and conditionally writes `ADMIN_REVOKED` with version+1 and `entitlement.revoked` outbox. It lists `RunBillingState` candidates, revalidates each companion, canonical Run and `get_active_run_dispatch` by base key, and in one per-Run transaction conditionally changes the Run to `CANCEL_REQUESTED`, sets `cancel_requested=True`, increments the companion `fencing_token`, preserves the active CND dispatch's wave/dispatch/ref as the cancel target without inventing another dispatch state, and writes `run.cancel_requested`. This CAS makes every permit from the prior fence unusable, so a concurrently reserved next generation cannot bind. Only then does it cancel that exact current dispatch through `executor.cancel(CancelRunExecution(..., execution_ref=dispatch.execution_ref))`; a missing active dispatch is an idempotent no-op. Repeated commands observe the already-revoked snapshot and continue unfinished Run/unit cancellation without another fence increment.

After the executor call, `cancel_run_units` conditionally changes bounded nonterminal units to `CANCELED` using the incremented Run fence and matching `dispatch_id`. Once no nonterminal unit remains it changes the canonical Run from `CANCEL_REQUESTED` to `CANCELED`, settles the scan/compute reservation without decrementing `consumed_runs`, calls `finish_run_dispatch(FinishRunDispatch(..., outcome=DispatchOutcome.CANCELED, ...))` to persist canonical `DispatchState.TERMINAL`/`terminal_outcome=CANCELED`, mirrors that terminal dispatch outcome in the companion audit state, and writes sanitized `reason_code="revoked"` only in the billing audit/outbox records. Fence-rejected worker commits cannot reopen units or the Run. Partial unit batches, executor failure, or process crash leave durable `revoke-pending` work; recovery resumes by cursor until `run_canceled=True`.

- [ ] **Step 4: Handle DynamoDB transaction limits safely**

Snapshot revocation is the global deny switch and commits first. Runs are fenced in bounded transactions of one Run/companion plus outbox; units are canceled in separate bounded pages, so neither many Runs nor many units can exceed DynamoDB transaction limits. Progress is stored under `PK=BILLING#<account>`, `SK=REVOCATION#<entitlement_version>` with the per-Run unit cursor; recovery resumes until every affected Run is `CANCELED`.

- [ ] **Step 5: Run tests and coverage**

Run: `uv run pytest packages/cnes_domain/tests/billing/test_revocation.py packages/cnes_infra/tests/billing/test_dynamodb_revocation.py --cov=cnes_domain.billing.revocation --cov-branch -q`

Expected: PASS; domain module reaches 100% branch coverage.

- [ ] **Step 6: Commit**

```bash
git add packages/cnes_domain/src/cnes_domain/billing/revocation.py \
  packages/cnes_domain/tests/billing/test_revocation.py \
  packages/cnes_infra/src/cnes_infra/billing/dynamodb_revocation.py \
  packages/cnes_infra/tests/billing/test_dynamodb_revocation.py
git commit -m "feat(billing): revoke entitlements and fence runs"
```

### Task 15: BIL-023 — Stripe Reconciliation and Drift Repair

**Files:**
- Create: `packages/cnes_infra/src/cnes_infra/billing/reconciliation.py`
- Test: `packages/cnes_infra/tests/billing/test_reconciliation.py`
- Test: `tests/chaos/test_billing_reconciliation_resume.py`

**Interfaces:**
- Consumes: account listing by base/candidate keys, `StripeGatewayPort.get_current_state`, projection CAS, revocation service, audit and metrics ports.
- Produces: `BillingReconciler.run(request: ReconciliationRequest) -> ReconciliationResult`.

- [ ] **Step 1: Write RED no-drift, drift and crash-resume tests**

```python
def test_reconciliation_corrige_drift_com_conditional_write(reconciler, projection) -> None:
    result = reconciler.run(ReconciliationRequest(limit=100, cursor=None))
    assert result.drift_found == 1
    assert result.corrected == 1
    projection.compare_and_set_snapshot.assert_called_once()

def test_reconciliation_sem_drift_nao_incrementa_versao(reconciler, projection) -> None:
    result = reconciler.run(ReconciliationRequest(limit=100, cursor=None))
    assert result.drift_found == 0
    projection.compare_and_set_snapshot.assert_not_called()
```

- [ ] **Step 2: Run tests to verify RED**

Run: `uv run pytest packages/cnes_infra/tests/billing/test_reconciliation.py tests/chaos/test_billing_reconciliation_resume.py -q`

Expected: FAIL with missing `BillingReconciler`.

- [ ] **Step 3: Implement paged current-state comparison**

For every active Stripe-backed account, retrieve current state, map to immutable PlanVersion, compare financial/access fields excluding observation timestamps, and CAS only on drift. Status `ADMIN_REVOKED` is never overwritten by ordinary Stripe state. A Stripe state requiring immediate access loss delegates to Task 14 before marking reconciliation complete. Cursor persists after each account so a crash restarts at the next unconfirmed account.

- [ ] **Step 4: Emit reconciliation evidence**

No drift emits only operational metrics. Drift writes `billing.reconciliation_drift` and `billing.reconciliation_corrected` outbox events containing account ID, prior/new snapshot hashes, source Stripe object IDs, actor `system:reconciler` and reason `stripe_projection_drift`; no secrets or full payload.

- [ ] **Step 5: Run tests**

Run: `uv run pytest packages/cnes_infra/tests/billing/test_reconciliation.py tests/chaos/test_billing_reconciliation_resume.py -q`

Expected: PASS; crash replay corrects once and retains an auditable cursor.

- [ ] **Step 6: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/billing/reconciliation.py \
  packages/cnes_infra/tests/billing/test_reconciliation.py \
  tests/chaos/test_billing_reconciliation_resume.py
git commit -m "feat(billing): reconcile Stripe projection drift"
```

### Task 16: BIL-023 — Audit Events and Billing Metrics

**Files:**
- Create: `packages/cnes_infra/src/cnes_infra/billing/metrics.py`
- Test: `packages/cnes_infra/tests/billing/test_metrics.py`
- Create: `docs/runbooks/billing-reconciliation.md`

**Interfaces:**
- Consumes: `BillingMetric`, outbox/audit adapter from `CND-024`, structured logger.
- Produces: `CloudWatchBillingMetrics.emit(metric: BillingMetric) -> None`, operational runbook and alarm contract.

- [ ] **Step 1: Write RED metric-shape and redaction tests**

```python
def test_metric_emf_nao_contem_identificador_sensivel(metric_sink, caplog) -> None:
    metric_sink.emit(BillingMetric.webhook_failure("signature_invalid"))
    record = json.loads(caplog.records[-1].message)
    assert record["_aws"]["CloudWatchMetrics"][0]["Namespace"] == "CnesData/Billing"
    assert "stripe_secret" not in record
    assert "payload" not in record
```

- [ ] **Step 2: Run test to verify RED**

Run: `uv run pytest packages/cnes_infra/tests/billing/test_metrics.py -q`

Expected: FAIL with missing `CloudWatchBillingMetrics`.

- [ ] **Step 3: Implement exact metric names and dimensions**

Emit EMF metrics: `WebhookLatencyMs`, `WebhookFailures`, `WebhookDuplicates`, `RecoveryBacklog`, `ReconciliationDrift`, `EntitlementChecksDenied`, `QuotaReservationsActive`, `QuotaReservationsExpired`, `RunsCanceledByRevocation`, `EntitlementSnapshotAgeSeconds`, and `AuditOutboxFailures`. Allowed dimensions are `Environment`, `EventType`, `Reason`, and `SubscriptionStatus`; never use tenant/account/event IDs as dimensions.

- [ ] **Step 4: Document exact operational checks and alarm thresholds**

The runbook defines commands `billing-worker inbox --limit 100`, `billing-worker recover`, and `billing-worker reconcile --limit 100`, safe replay semantics, and these initial alarm contracts: `WebhookFailures >= 5` in 5 minutes; `RecoveryBacklog >= 100` for 10 minutes; active `EntitlementSnapshotAgeSeconds > 300` for 10 minutes; `ReconciliationDrift >= 1` in three consecutive runs; `AuditOutboxFailures >= 1` for 5 minutes. The document explicitly says IaC alarm resources require the separate deployment specification.

- [ ] **Step 5: Run tests and audit-event inventory check**

Run: `uv run pytest packages/cnes_infra/tests/billing/test_metrics.py packages/cnes_infra/tests/billing -q`

Expected: PASS; tests assert events for account create/transfer, checkout create, webhook receive/duplicate/apply/fail, subscription changes, entitlement changes, quota lifecycle, revocation, run cancellation and serving denial.

- [ ] **Step 6: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/billing/metrics.py \
  packages/cnes_infra/tests/billing/test_metrics.py \
  docs/runbooks/billing-reconciliation.md
git commit -m "feat(billing): audit and observe billing lifecycle"
```

### Task 17: Integration-Owned — Publisher Fence, Agent/Tenant Gates and Worker Jobs

**Serial entry gate:** Start only after Tasks 3–6, 13, and 14–16 are merged. The fixed queue already
places this after `CND-064`, AWS Task 8, and Source Task 4. The controller holds the shared
composition/control-plane lock; this task never runs beside another integration branch.

**Files:**
- Create: `packages/cnes_domain/src/cnes_domain/billing/publication.py`
- Create: `packages/cnes_domain/tests/billing/test_publication.py`
- Test: upstream `apps/data_processor/tests/orchestration/test_publisher.py`
- Test: upstream `apps/data_processor/tests/orchestration/test_unit_worker.py`
- Modify: upstream `apps/data_processor/src/data_processor/composition.py`
- Modify: upstream `apps/data_processor/tests/test_aws_composition.py`
- Modify: upstream `apps/central_api/src/central_api/composition.py`
- Modify: upstream `apps/central_api/tests/test_aws_composition.py`
- Modify: upstream `packages/cnes_domain/src/cnes_domain/ports/control_plane.py`
- Modify: upstream `packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_adapter.py`
- Modify: upstream `packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_claims.py`
- Modify: upstream `packages/cnes_infra/src/cnes_infra/control_plane/sqlite_adapter.py`
- Modify: upstream `apps/central_api/src/central_api/routes/agents.py`
- Modify: `apps/central_api/src/central_api/routes/admin.py`
- Create: `apps/central_api/src/central_api/routes/tenants.py`
- Modify: upstream `apps/central_api/src/central_api/routes/serving.py`
- Modify: `apps/central_api/src/central_api/app.py`
- Modify: `apps/central_api/tests/test_app_wiring.py`
- Modify: `packages/cnes_infra/src/cnes_infra/billing/projector.py`
- Modify: `apps/billing_worker/src/billing_worker/main.py`
- Modify: `apps/billing_worker/src/billing_worker/worker.py`
- Modify: `apps/billing_worker/tests/test_worker.py`
- Test: upstream `packages/cnes_infra/tests/control_plane/test_dynamodb_adapter.py`
- Test: upstream `packages/cnes_infra/tests/control_plane/test_sqlite_adapter.py`
- Test: `tests/integration/billing/test_enforcement.py`
- Test: `tests/chaos/test_revocation_publish_fence.py`
- Create: `tests/negative/test_no_direct_run_creation.py`

**Interfaces:**
- Consumes: merged Tasks 3–6 and 14–16, including the already-functional Task 6 permit/binding callbacks, plus upstream canonical publisher/executor/control plane and AWS Task 8 `ProcessorRuntimeComponents`.
- Produces: enforcement at every expensive boundary; `BillingPublicationPolicy.__call__(run: Run) -> PublicationPermit`; `ControlPlanePort.list_revocable_runs(billing_account_id: str, limit: int, cursor: str | None) -> RevocableRunPage`, `request_run_revocation(command: RevokeRunCommand, event: OutboxEvent) -> RunBillingState`, `cancel_run_units(command: CancelRunUnitsCommand) -> CancelRunUnitsResult`; worker commands `reconcile` and `revoke-pending`.

- [ ] **Step 1: Write RED E2E revocation/fence test**

```python
def test_revogacao_imediata_impede_publicacao_por_fence_antigo(runtime) -> None:
    run = runtime.create_run()
    stale_billing_fence = runtime.control_plane.get_run_billing_state(
        run.tenant_id, run.run_id,
    ).fencing_token
    billing_policy = runtime.publisher.publication_policy
    seen: list[PublicationPermit] = []
    def revoke_after_authorize(current: Run) -> PublicationPermit:
        permit = billing_policy(current)
        seen.append(permit)
        runtime.admin_revoke(reason_code="security_incident")
        return permit
    runtime.publisher.publication_policy = revoke_after_authorize
    with pytest.raises(PublishDenied, match="reason=(admin_revoked|stale_fence)"):
        runtime.publish(run)
    guard = seen[0].binding_context
    assert isinstance(guard, PublicationGuard)
    assert guard.expected_run_fencing_token == stale_billing_fence
    assert runtime.dataset_pointer_unchanged()
```

Keep the existing stale-unit-fence test separate: `RunUnit.fencing_token` protects `commit_run_unit`, while `RunBillingState.fencing_token` protects dispatch binding and publication. Add a race pausing after `BillingPublicationPolicy(run)` and revoking before `publish_dataset`; the transaction must reject the stale guard and preserve the pointer.

- [ ] **Step 2: Run test to verify RED**

Run: `uv run pytest tests/integration/billing/test_enforcement.py tests/chaos/test_revocation_publish_fence.py -q`

Expected: FAIL at the first boundary that does not call `EntitlementGate` or compare the current fence.

- [ ] **Step 3: Apply critical gates serially**

Agent registration calls `authorize_register_agent`; tenant creation calls `authorize_tenant_creation`; signed serving access calls `authorize_serving_access`; publisher calls `authorize_publish_run` with the run's original `plan_version_id`, `entitlement_version` and current run fence. Critical calls bypass cache. Serving may cache for 60 seconds except `ADMIN_REVOKED`, which immediately blocks new URLs/cookies. Read-only serving resolves only immutable versions whose `created_at` is inside `retention_days`; it never starts compute and never deletes older datasets as a side effect. Analytics authorization and byte reservation stay dormant/future-optional: this task adds no Athena adapter, executor, route, or request-path wiring.

Replace every production caller that creates or persists a `Run` with the Task 6
`RunAuthorizationService`. A caller first resolves the CND-060 pipeline server-side and builds the
canonical `CreateRunRequest` with dataset, competência, and dependency tuple. The negative test
scans `apps/**/src` and rejects calls to `put_run`, `create_unmetered_run`, or
`reserve_and_create_run` outside the authorization service and composition adapters. Direct
adapter calls remain allowed only in adapter tests and migrations; no HTTP body can set
`Run.dependencies`.

Agent and tenant creation first call `reserve_capacity`, persist the canonical resource with the reservation ID as idempotency key, then call `consume_capacity`; a failed canonical write calls `release_capacity`. Concurrent last-slot attempts therefore produce one reservation. The admin route adds `POST /api/v1/admin/billing/{billing_account_id}/revoke`, requires AWS-011 `AuthorizedTenant.role == "admin"`, a non-empty `reason_code` of at most 128 characters, and delegates to `ImmediateRevocationService`.

Preserve the Task 6 `ExecutionPermit`/`RunExecutionPermit` callbacks already installed in both composition roots; this task must not recreate or defer them. Its integration tests boot the final AWS API and processor runtimes and prove the same policy/binding implementation serves the initial wave and every downstream/retry dispatch. Revocation reads the current binding before incrementing the Run fence, persists `cancel_requested=True`, cancels only that latest active dispatch, then lets `revoke-pending` page unit cancellation and finish the canonical Run. Three completed sequential bindings remain auditable, while the fourth/current binding becomes the sole cancel target.

Use the canonical CND `ClaimRunUnit`, `CommitRunUnit`, and `FailRunUnit` requests unchanged, consuming their already-required `dispatch_id`; do not introduce a billing dispatch identifier. In Stripe mode, each corresponding DynamoDB transaction condition-checks the `RunBillingState` base item with `cancel_requested=False`, `execution_dispatch_id == command.dispatch_id`, the entitlement version and billing fence captured by that binding, plus an active matching canonical `RunDispatch`. SQLite applies the same disabled-mode companion checks in its transaction. Therefore revocation's fence increment/cancel flag blocks a late commit or fail immediately, even before `revoke-pending` has changed the unit or Run to `CANCELED`. Add a race that pauses after unit claim, revokes, then proves both `commit_run_unit` and `fail_run_unit` reject the stale dispatch/fence and publish remains impossible.

- [ ] **Step 4: Make publication one fail-closed commit check**

Keep canonical `PublishRequest(run, units, expected_version_id, now)` unchanged and accept no caller-supplied authorization, account ID, entitlement version or fence. Inject `BillingPublicationPolicy` as the CND `PublicationPolicy` callable into `DatasetPublisher`. Immediately after immutable object promotion/verification and immediately before constructing the final command, the publisher calls `publication_policy(run)` exactly once. The policy strongly reads `RunBillingState` and its snapshot by base key, rejects missing/mismatched account/run identity, `cancel_requested`, inactive access, stale authorization version or fence, calls `authorize_publish_run`, and returns the single imported CND `PublicationPermit(tenant_id=run.tenant_id, run_id=run.run_id, policy_version=snapshot.entitlement_version, fencing_token=state.fencing_token, binding_context=PublicationGuard(...))`.

Do not add billing fields to `PublishDataset`. The publisher validates the permit's tenant/run and passes that same object instance through canonical `PublishDataset.publication_permit`. The DynamoDB adapter validates `isinstance(command.publication_permit.binding_context, PublicationGuard)` and, in the same `publish_dataset` transaction, condition-checks the current snapshot status/version and companion Run account/run/fence/cancel flag against the permit and guard before the already-atomic Run finalization, `DatasetVersion`, `DatasetPointer`, reservation settlement and outbox mutations. SQLite receives the local disabled permit (`binding_context=None`) and validates its local companion in that same transaction; Stripe mode fails closed on a missing or wrongly typed guard. Add an identity test asserting `control_plane.calls[-1].command.publication_permit is policy.return_value`. A revocation between policy read and transaction, or any stale entitlement/fence, raises `PublishDenied`, leaves both the old pointer and nonterminal Run state intact, and keeps temporary outputs for lifecycle cleanup. Successful publication settles the remaining scan/compute reservation without incrementing `consumed_runs`.

Wire the projector so an access-losing Stripe transition that is effective now delegates to `ImmediateRevocationService`; `cancel_at_period_end=True` does not delegate before `period_end`. This serial edit occurs only after Task 14 exists, avoiding a temporary circular dependency.

- [ ] **Step 5: Wire reconciliation, pending revocation and reservation recovery commands**

Extend the worker parser with `reconcile --limit 100`, `revoke-pending --limit 100`, and `release-expired-reservations --limit 100`. `revoke-pending` resumes the stored Run/unit cursor, cancels bounded nonterminal unit pages, transitions the Run to `CANCELED`, closes its current binding and settles scan/compute usage. The coordinator's ordinary terminal-failure path performs the same reservation settlement without decrementing `consumed_runs`. Each command runs one bounded, idempotent cycle and returns nonzero only when durable state could not record retry.

- [ ] **Step 6: Run enforcement, race and chaos suites**

Run: `uv run pytest packages/cnes_domain/tests/billing/test_publication.py apps/data_processor/tests/orchestration/test_publisher.py apps/data_processor/tests/orchestration/test_unit_worker.py apps/central_api/tests/test_aws_composition.py apps/data_processor/tests/test_aws_composition.py tests/integration/billing/test_enforcement.py tests/chaos/test_revocation_publish_fence.py tests/property/test_quota_last_unit_race.py tests/negative/test_no_direct_run_creation.py -q`

Expected: PASS; three waves bind sequentially, retry uses a new persisted dispatch generation, revocation cancels the latest active binding and terminates units/Run, stale claim/commit/fail all reject after the billing fence changes, a failed executor cancellation still cannot change `DatasetPointer`, and the publication race rejects the stale billing fence rather than a unit fence.

- [ ] **Step 7: Commit**

```bash
git add packages/cnes_domain/src/cnes_domain/billing/publication.py \
  packages/cnes_domain/tests/billing/test_publication.py \
  apps/data_processor/src/data_processor/composition.py \
  apps/data_processor/tests/test_aws_composition.py \
  apps/central_api/src/central_api/composition.py \
  apps/central_api/tests/test_aws_composition.py \
  packages/cnes_domain/src/cnes_domain/ports/control_plane.py \
  packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_adapter.py \
  packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_claims.py \
  packages/cnes_infra/src/cnes_infra/control_plane/sqlite_adapter.py \
  apps/central_api/src/central_api/routes/agents.py \
  apps/central_api/src/central_api/routes/admin.py \
  apps/central_api/src/central_api/routes/tenants.py \
  apps/central_api/src/central_api/routes/serving.py \
  apps/central_api/src/central_api/app.py \
  apps/central_api/tests/test_app_wiring.py \
  packages/cnes_infra/src/cnes_infra/billing/projector.py \
  apps/billing_worker/src/billing_worker/main.py \
  apps/billing_worker/src/billing_worker/worker.py \
  apps/billing_worker/tests/test_worker.py \
  tests/integration/billing/test_enforcement.py \
  tests/chaos/test_revocation_publish_fence.py \
  tests/negative/test_no_direct_run_creation.py
git commit -m "feat(billing): enforce revocation at critical boundaries"
```

### Task 18: BIL-024 — Stripe Test-Clock Lifecycle E2E

**Files:**
- Create: `tests/e2e/billing/conftest.py`
- Create: `tests/e2e/billing/test_stripe_test_clock.py`
- Create: `tests/e2e/billing/test_immediate_revocation.py`

**Interfaces:**
- Consumes: Tasks 7–17, Stripe test account, `STRIPE_TEST_SECRET_KEY`, `STRIPE_TEST_PRICE_ID`, signed webhook forwarder, DynamoDB Local/AWS sandbox.
- Produces: deterministic E2E coverage for trial, renewal, payment failure, period-end cancellation and immediate revocation.

- [ ] **Step 1: Write the gated RED test-clock fixtures**

```python
@pytest.fixture
def stripe_test_clock(stripe_client):
    clock = stripe_client.test_helpers.test_clocks.create(
        frozen_time=int(NOW.timestamp()),
        name="cnesdata-bil-024",
    )
    yield clock
    stripe_client.test_helpers.test_clocks.delete(clock.id)

@pytest.mark.stripe
def test_trial_renewal_failure_e_cancelamento(clock_runtime) -> None:
    runtime = clock_runtime.start_trial(days=14)
    assert runtime.snapshot().subscription_status is SubscriptionStatus.TRIALING
    runtime.advance(days=14)
    assert runtime.await_snapshot().subscription_status is SubscriptionStatus.ACTIVE
    runtime.fail_next_payment()
    runtime.advance_to_next_invoice()
    assert runtime.await_snapshot().subscription_status is SubscriptionStatus.PAST_DUE
```

Tests skip only when `RUN_STRIPE_E2E != "1"`; with the flag set, missing secrets fail immediately with `stripe_test_config_missing`.

- [ ] **Step 2: Run the local collection to verify RED**

Run: `uv run pytest tests/e2e/billing --collect-only -q`

Expected: PASS collection with exactly five lifecycle tests; execution is skipped without `RUN_STRIPE_E2E=1`.

- [ ] **Step 3: Complete five deterministic scenarios**

The five tests are `test_trial_concede_plano_de_trial`, `test_renewal_move_periodo_sem_duplicar_quota`, `test_payment_failure_aplica_grace_e_depois_read_only`, `test_cancel_at_period_end_preserva_acesso_ate_period_end`, and `test_admin_revocation_invalida_fence_imediatamente`. Each advances the test clock, waits by bounded polling for the signed webhook/projector result, checks `entitlement_version`, gate decision, quota period and audit events, then deletes created test objects.

- [ ] **Step 4: Run against Stripe sandbox**

Run: `RUN_STRIPE_E2E=1 uv run pytest tests/e2e/billing -m stripe -v --maxfail=1`

Expected: `5 passed`; no test depends on wall-clock month boundaries.

- [ ] **Step 5: Run the local disabled acceptance in the same branch**

Run: `BILLING_MODE=disabled uv run pytest tests/negative/test_local_billing_has_no_remote_dependency.py -q`

Expected: PASS without Stripe secrets or network.

- [ ] **Step 6: Commit**

```bash
git add tests/e2e/billing/conftest.py \
  tests/e2e/billing/test_stripe_test_clock.py \
  tests/e2e/billing/test_immediate_revocation.py
git commit -m "test(billing): validate lifecycle with Stripe test clocks"
```

### Task 19: Integration-Owned — CI, Generated Contracts and Final Acceptance

**Serial entry gate:** Start only after Tasks 1–18 and all CND/AWS integration lanes are merged. The controller alone updates shared CI, generated contracts and rollout documentation in this final acceptance task.

**Files:**
- Modify: `.github/workflows/python-quality.yml`
- Create: `.github/workflows/stripe-billing-e2e.yml`
- Modify: `pytest.ini`
- Modify: `docs/contracts/openapi.json`
- Modify: `docs/roadmap.md`
- Create: `scripts/tests/test_billing_ci_contract.py`
- Test: full Python and billing matrix.

**Interfaces:**
- Consumes: every merged billing task and Stripe sandbox secrets configured in GitHub Actions environment.
- Produces: final green `BIL-024` gate and rollout-ready shadow enforcement.

- [ ] **Step 1: Write RED CI contract tests**

Add a repository test that parses workflow YAML and asserts: billing unit/integration suites run on billing paths; Stripe E2E is manual/scheduled and never on forks without secrets; `stripe` marker exists; OpenAPI contains account create/transfer, Checkout, Portal, status and webhook routes.

- [ ] **Step 2: Run the contract test to verify RED**

Run: `uv run pytest scripts/tests/test_billing_ci_contract.py -q`

Expected: FAIL until workflow, marker and OpenAPI changes are present.

- [ ] **Step 3: Add serial CI and generated artifacts**

The regular workflow runs domain/infra/API/worker tests, property races and revocation chaos without Stripe network. `stripe-billing-e2e.yml` uses the protected `stripe-sandbox` environment, `RUN_STRIPE_E2E=1`, official test keys, webhook secret and price ID; it runs the exact Task 18 command and uploads only pytest JUnit output, never raw webhook bodies or env dumps.

- [ ] **Step 4: Execute the complete billing acceptance matrix**

Run: `uv run ruff check .`

Expected: PASS.

Run: `uv run pytest packages/cnes_domain/tests/billing packages/cnes_infra/tests/billing apps/central_api/tests apps/billing_worker/tests tests/property/test_entitlement_projection_cas.py tests/property/test_quota_last_unit_race.py tests/property/test_stripe_event_reordering.py tests/chaos/test_quota_reservation_recovery.py tests/chaos/test_stripe_projection_failures.py tests/chaos/test_revocation_publish_fence.py tests/integration/billing -q`

Expected: PASS.

Run: `uv run pytest packages/ --cov --cov-config=pyproject.toml`

Expected: PASS with 100% branch coverage gate.

Run: `uv run pytest apps/ --cov --cov-config=.coveragerc`

Expected: PASS with at least 90% line coverage.

- [ ] **Step 5: Verify rollout modes explicitly**

Run: `BILLING_MODE=disabled uv run pytest tests/negative/test_local_billing_has_no_remote_dependency.py -q`

Expected: PASS.

Run: `BILLING_ENFORCEMENT_MODE=shadow uv run pytest tests/integration/billing -q`

Expected: PASS while denials are logged/audited but do not block the internal rollout tenant.

Run: `BILLING_ENFORCEMENT_MODE=enforce uv run pytest tests/integration/billing -q`

Expected: PASS with gates blocking every negative case.

- [ ] **Step 6: Commit**

Run: `git diff --check && git diff --stat`

Expected: no whitespace errors; only CI, marker, generated OpenAPI, roadmap and contract-test files change.

```bash
git add .github/workflows/python-quality.yml \
  .github/workflows/stripe-billing-e2e.yml pytest.ini \
  docs/contracts/openapi.json docs/roadmap.md \
  scripts/tests/test_billing_ci_contract.py
git commit -m "ci(billing): enforce billing acceptance matrix"
```

## Rollout Gate

The merged implementation follows the approved order:

1. Persist BillingAccount, PlanVersion and EntitlementSnapshot with enforcement off.
2. Run Stripe sandbox webhooks in `shadow` mode and compare projection to Stripe.
3. Pass duplicate, reorder, recovery, reconciliation and all five test-clock scenarios.
4. Enable decision logging for internal tenants.
5. Enable create-run and agent-registration gates.
6. Enable quota/budget reservations.
7. Enable publisher revalidation and immediate revocation.
8. Enable one production plan only after webhook, snapshot age, drift and audit-outbox alarms remain healthy.

## Self-Review Checklist

- [ ] `BIL-010`: immutable `BillingAccount`, `PlanVersion`, `EntitlementSnapshot` and disabled local mode are covered by Tasks 1–2 and 6.
- [ ] `BIL-011`: all named `EntitlementGate` methods and immutable `RunAuthorization` are covered by Tasks 3, 6 and 17.
- [ ] `BIL-012`: DynamoDB projection, base-key strong reads, 60-second cache and invalidation are covered by Task 4.
- [ ] `BIL-013`: atomic reserve/consume/release/recovery and final-unit race are covered by Task 5.
- [ ] `BIL-020`: hosted Checkout, Portal, owner authorization, allowlisted redirects and pending redirect are covered by Tasks 7–8 and 13.
- [ ] `BIL-021`: raw signature, allowlist, inbox dedupe, reordering, retries, recovery and worker are covered by Tasks 9–13.
- [ ] `BIL-022`: immediate snapshot revocation, run fences, Step Functions cancellation and publisher denial are covered by Tasks 14 and 17.
- [ ] `BIL-023`: reconciliation, explicit audit, metrics, alert contracts and runbook are covered by Tasks 15–17.
- [ ] `BIL-024`: trial, renewal, payment failure, period-end cancellation and immediate revocation test clocks are covered by Tasks 18–19.
- [ ] No critical correctness path depends on cache, GSI, TTL, redirect or successful workflow cancellation.
- [ ] No task stores card data, logs secrets, silently deletes datasets or introduces Redis/CloudFront invalidation.
- [ ] Integration-owned surfaces are modified only in serial Tasks 6, 13, 17 and 19.

## Execution Handoff

Plan complete and saved to `plans/2026-08-23-cnesdata-billing-entitlements-implementation-plan.md`. Two execution options:

1. **Subagent-Driven (recommended)** — dispatch one fresh agent per task, review specification compliance and code quality between tasks, and reserve Tasks 6, 13, 17 and 19 for the serial integration lane.
2. **Inline Execution** — use `superpowers:executing-plans`, execute the delivery-order batches, and stop at each serial integration gate for review.
