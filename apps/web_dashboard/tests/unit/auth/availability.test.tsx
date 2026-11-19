import { act, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { expect, test } from "vitest";

import {
  LOCAL_ME,
  ME,
  MEMBERSHIPS,
  SERVING,
  emit,
  expectPrivateContext,
  expectRequest,
  expectSdkRetained,
  failureResponse,
  guardOutcome,
  holdIdentity,
  startSession,
  refresh,
  sdkBoundary,
  selectSecondTenant,
} from "./availability.fixture";
import type { Failure, Profile } from "./availability.fixture";

import { ApiError } from "@/api/client";
import { useServingOverview } from "@/api/hooks/useServingOverview";
import { useSelectedTenant } from "@/api/hooks/useTenants";
import { getAccessToken } from "@/auth/oidc";
import { useAuth } from "@/auth/useAuth";

const sdk = sdkBoundary();

function Probe() {
  const session = useAuth();
  const { tenants, selectTenant } = useSelectedTenant();
  const serving = useServingOverview();
  const [completed, setCompleted] = useState(0);
  return (
    <>
      <output data-testid="status">{session.status}</output>
      <output data-testid="identity">{JSON.stringify(session.user)}</output>
      <output data-testid="credential">{JSON.stringify(session.accessToken)}</output>
      <output data-testid="selection">{JSON.stringify(session.selectedTenantId)}</output>
      <output data-testid="memberships">{JSON.stringify(tenants.data)}</output>
      <output data-testid="serving">{JSON.stringify(serving.data)}</output>
      <output data-testid="completed">{completed}</output>
      <button type="button" onClick={() => selectTenant("tenant-A2")}>
        Selecionar A2
      </button>
      <button
        type="button"
        onClick={() => void session.refresh().then(() => setCompleted((n) => n + 1))}
      >
        Atualizar
      </button>
    </>
  );
}

async function initialSession(profile: Profile = "aws") {
  return startSession(profile, <Probe />);
}

test.each(["503", "network"] as const)(
  "guard_aws_preserva_erro_de_identidade_%s_sem_buscar_memberships",
  async (failure) => {
    const state = await initialSession();
    const offset = state.seen.length;
    state.failure = failure;
    const outcome = await guardOutcome();
    expectRequest(state, offset);
    await expectSdkRetained();
    expect(screen.getByTestId("status")).toHaveTextContent(/^authenticated$/);
    expect(outcome.allowed).toBe(false);
    const error = "error" in outcome ? outcome.error : null;
    if (failure === "503") {
      expect(error).toBeInstanceOf(ApiError);
      expect(error).toMatchObject({ status: 503, body: { detail: "identity_unavailable" } });
    } else {
      expect(error).toBeInstanceOf(TypeError);
      expect(error).toMatchObject({ message: "Failed to fetch" });
    }
  },
);

test("guard_aws_401_permanece_falha_de_autenticacao", async () => {
  const state = await initialSession();
  const offset = state.seen.length;
  state.failure = "401";
  await expect(guardOutcome()).resolves.toMatchObject({
    allowed: false,
    error: { options: { to: "/login" } },
  });
  expectRequest(state, offset);
  await expectSdkRetained();
});

test("guard_aws_200_com_grants_permite_a_sessao_atual", async () => {
  const state = await initialSession();
  const offset = state.seen.length;
  await expect(guardOutcome()).resolves.toEqual({ allowed: true });
  expect(state.seen.slice(offset)).toEqual([
    { path: ME, bearer: "Bearer A1", tenant: null },
    { path: MEMBERSHIPS, bearer: "Bearer A1", tenant: null },
  ]);
  await expectSdkRetained();
});

test("guard_aws_preserva_503_das_memberships_apos_identidade_valida", async () => {
  const state = await initialSession();
  const offset = state.seen.length;
  state.membershipFailure = true;
  await expect(guardOutcome()).resolves.toMatchObject({
    allowed: false,
    error: { status: 503, body: { detail: "memberships_unavailable" } },
  });
  expect(state.seen.slice(offset)).toEqual([
    { path: ME, bearer: "Bearer A1", tenant: null },
    { path: MEMBERSHIPS, bearer: "Bearer A1", tenant: null },
  ]);
});

test.each(["503", "network"] as const)(
  "provider_aws_%s_fica_unavailable_limpa_dados_e_recupera_por_refresh",
  async (failure) => {
    const state = await initialSession();
    await selectSecondTenant();
    const offset = state.seen.length;
    const hold = holdIdentity(state);
    try {
      await userEvent.click(screen.getByRole("button", { name: "Atualizar" }));
      await waitFor(() => expect(hold.started).toBe(true));
      expect(screen.getByTestId("status")).toHaveTextContent(/^loading$/);
      expectPrivateContext();
      await act(async () => {
        hold.release(failureResponse(failure));
        await hold.pending;
      });
      await waitFor(() => expect(screen.getByTestId("completed")).toHaveTextContent("1"));
      expectPrivateContext();
      expectRequest(state, offset);
      await expectSdkRetained();
      const failedStatus = screen.getByTestId("status").textContent;
      state.hold = null;
      state.revision = 2;
      const recoveryOffset = state.seen.length;
      await refresh(2);
      await waitFor(() => expect(screen.getByTestId("serving")).toHaveTextContent("A1:2:tenant-A"));
      expect(screen.getByTestId("status")).toHaveTextContent(/^authenticated$/);
      expect(screen.getByTestId("identity")).toHaveTextContent('"subject":"subject-A"');
      expect(screen.getByTestId("credential")).toHaveTextContent('"A1"');
      expect(screen.getByTestId("selection")).toHaveTextContent("null");
      expect(screen.getByTestId("serving")).not.toHaveTextContent("tenant-A2");
      expect(state.seen.slice(recoveryOffset)).toEqual([
        { path: ME, bearer: "Bearer A1", tenant: null },
        { path: MEMBERSHIPS, bearer: "Bearer A1", tenant: null },
        { path: SERVING, bearer: "Bearer A1", tenant: "tenant-A" },
      ]);
      await expectSdkRetained();
      expect(failedStatus).toBe("unavailable");
    } finally {
      hold.release(failureResponse(failure));
    }
  },
);

test("provider_aws_401_limpa_contexto_e_continua_anonymous", async () => {
  const state = await initialSession();
  await selectSecondTenant();
  const offset = state.seen.length;
  state.failure = "401";
  await refresh(1);
  expect(screen.getByTestId("status")).toHaveTextContent(/^anonymous$/);
  expectPrivateContext();
  expectRequest(state, offset);
  await expectSdkRetained();
});

test.each(["missing", "expired"] as const)(
  "aws_sdk_%s_permanece_anonymous_login_sem_http",
  async (session) => {
    const state = await initialSession();
    await selectSecondTenant();
    const offset = state.seen.length;
    if (session === "missing") emit("UserUnloaded", null);
    else {
      sdk.expired = true;
      emit("AccessTokenExpired");
    }
    await waitFor(() => expect(screen.getByTestId("status")).toHaveTextContent(/^anonymous$/));
    expectPrivateContext();
    await expect(getAccessToken()).resolves.toBeNull();
    await expect(guardOutcome()).resolves.toMatchObject({
      allowed: false,
      error: { options: { to: "/login" } },
    });
    expect(state.seen.slice(offset)).toEqual([]);
    expect(sdk.signout).not.toHaveBeenCalled();
  },
);

const existingFailures: [Profile, Failure][] = [
  ["legacy", "503"],
  ["legacy", "network"],
  ["local", "503"],
  ["local", "network"],
];

test.each(existingFailures)(
  "perfil_%s_preserva_login_anonymous_para_falha_%s",
  async (profile, failure) => {
    const state = await initialSession(profile);
    const offset = state.seen.length;
    state.failure = failure;
    await expect(guardOutcome()).resolves.toMatchObject({
      allowed: false,
      error: { options: { to: "/login" } },
    });
    await refresh(1);
    expect(screen.getByTestId("status")).toHaveTextContent(/^anonymous$/);
    expectPrivateContext();
    expect(state.seen.slice(offset)).toEqual(
      Array.from({ length: 2 }, () => ({
        path: profile === "local" ? LOCAL_ME : ME,
        bearer: profile === "local" ? null : "Bearer A1",
        tenant: null,
      })),
    );
    if (profile === "local") expect(sdk.reads).toBe(0);
    else await expectSdkRetained();
    expect(sdk.signout).not.toHaveBeenCalled();
  },
);

test.each(["legacy", "local"] as const)("perfil_%s_preserva_sucesso_200", async (profile) => {
  await initialSession(profile);
  await expect(guardOutcome()).resolves.toEqual({ allowed: true });
  expect(screen.getByTestId("status")).toHaveTextContent(/^authenticated$/);
  if (profile === "local") expect(sdk.reads).toBe(0);
  else await expectSdkRetained();
});

test("guard_descarta_503_obsoleto_de_A_e_autoriza_B_atual", async () => {
  const state = await initialSession();
  const offset = state.seen.length;
  const hold = holdIdentity(state);
  const outcome = guardOutcome();
  try {
    await waitFor(() => expect(hold.started).toBe(true));
    emit("UserLoaded", "B");
    await waitFor(() => expect(screen.getByTestId("serving")).toHaveTextContent("B:1:tenant-B"));
    await act(async () => {
      hold.release(failureResponse("503"));
      await hold.pending;
    });
    await expect(outcome).resolves.toEqual({ allowed: true });
    expect(state.seen.slice(offset).filter((r) => r.bearer === "Bearer A1")).toEqual([
      { path: ME, bearer: "Bearer A1", tenant: null },
    ]);
    expect(state.seen.slice(-2)).toEqual([
      { path: ME, bearer: "Bearer B", tenant: null },
      { path: MEMBERSHIPS, bearer: "Bearer B", tenant: null },
    ]);
    await expectSdkRetained("B");
  } finally {
    hold.release(failureResponse("503"));
  }
});

test("provider_descarta_503_obsoleto_de_A_sem_apagar_contexto_e_produto_de_B", async () => {
  const state = await initialSession();
  await selectSecondTenant();
  const hold = holdIdentity(state);
  try {
    await userEvent.click(screen.getByRole("button", { name: "Atualizar" }));
    await waitFor(() => expect(hold.started).toBe(true));
    emit("UserLoaded", "B");
    await waitFor(() => expect(screen.getByTestId("serving")).toHaveTextContent("B:1:tenant-B"));
    const afterB = state.seen.length;
    await act(async () => {
      hold.release(failureResponse("503"));
      await hold.pending;
    });
    await waitFor(() => expect(screen.getByTestId("completed")).toHaveTextContent("1"));
    expect(screen.getByTestId("status")).toHaveTextContent(/^authenticated$/);
    expect(screen.getByTestId("identity")).toHaveTextContent('"subject":"subject-B"');
    expect(screen.getByTestId("credential")).toHaveTextContent('"B"');
    expect(screen.getByTestId("selection")).toHaveTextContent("null");
    expect(screen.getByTestId("memberships")).toHaveTextContent('"tenant_id":"tenant-B"');
    expect(screen.getByTestId("serving")).toHaveTextContent("B:1:tenant-B");
    expect(state.seen.slice(afterB)).toEqual([]);
    await expectSdkRetained("B");
  } finally {
    hold.release(failureResponse("503"));
  }
});
