import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import { User } from "oidc-client-ts";
import type * as OidcModule from "oidc-client-ts";
import { type Mock, beforeEach, expect, test, vi } from "vitest";

import { server } from "../../mocks/server";

import { useServingOverview } from "@/api/hooks/useServingOverview";
import { useTenants } from "@/api/hooks/useTenants";
import { AuthProvider } from "@/auth/AuthProvider";
import { logout } from "@/auth/oidc";
import { useAuth } from "@/auth/useAuth";
import { env } from "@/lib/env";
import { Route } from "@/routes/_app";

type SessionEvent = "UserLoaded" | "UserUnloaded" | "AccessTokenExpired";
type Callback = (user?: OidcModule.User) => void;
type SdkBoundary = {
  token: string | null;
  expired: boolean;
  currentUser: () => OidcModule.User | null;
  events: Record<SessionEvent, Set<Callback>>;
  signout: Mock<() => void>;
};
const sdk = vi.hoisted(
  (): SdkBoundary => ({
    token: "A1",
    expired: false,
    currentUser: () => null,
    events: {
      UserLoaded: new Set<Callback>(),
      UserUnloaded: new Set<Callback>(),
      AccessTokenExpired: new Set<Callback>(),
    },
    signout: vi.fn<() => void>(),
  }),
);

vi.mock("oidc-client-ts", async (importOriginal) => {
  const actual = await importOriginal<typeof OidcModule>();
  const register = (event: SessionEvent, callback: Callback) => {
    sdk.events[event].add(callback);
    return () => sdk.events[event].delete(callback);
  };
  return {
    ...actual,
    UserManager: class {
      events = {
        addUserLoaded: (callback: Callback) => register("UserLoaded", callback),
        addUserUnloaded: (callback: Callback) => register("UserUnloaded", callback),
        addAccessTokenExpired: (callback: Callback) => register("AccessTokenExpired", callback),
      };
      getUser() {
        return Promise.resolve(sdk.currentUser());
      }
      signoutRedirect() {
        sdk.signout();
        sdk.token = null;
        sdk.events.UserUnloaded.forEach((callback) => callback());
        return Promise.resolve();
      }
    },
  };
});

type SeenRequest = { path: string; bearer: string | null; tenant: string | null };
type GuardResult = { allowed: boolean; error?: unknown };
const ME_PATH = "/api/v1/dashboard/auth/me";
const MEMBERSHIPS_PATH = "/api/v1/dashboard/memberships";
const SERVING_PATH = "/api/v1/dashboard/serving/cnes/overview";
const guard = Route.options.beforeLoad as () => Promise<unknown>;
const identities = {
  A1: { subject: "same-subject", issuer: "https://A.idp.test", email: "A@test", display_name: "A" },
  A2: { subject: "same-subject", issuer: "https://A.idp.test", email: "A@test", display_name: "A" },
  B: { subject: "same-subject", issuer: "https://B.idp.test", email: "B@test", display_name: "B" },
};

function currentSdkUser() {
  if (!sdk.token) return null;
  const identity = identities[sdk.token as keyof typeof identities];
  return new User({
    access_token: sdk.token,
    token_type: "Bearer",
    expires_at: sdk.expired ? 1 : 4102444800,
    profile: {
      sub: identity.subject,
      iss: identity.issuer,
      aud: "dashboard",
      iat: 1,
      exp: 4102444800,
      email: identity.email,
      name: identity.display_name,
    },
  });
}

beforeEach(() => {
  sdk.token = "A1";
  sdk.expired = false;
  sdk.signout.mockClear();
  sdk.currentUser = currentSdkUser;
  env.VITE_AUTH_MODE = "oidc";
  env.VITE_RUNTIME_POLICY = "generic";
  env.VITE_OIDC_AUTHORITY = "https://issuer.example.test";
  env.VITE_OIDC_CLIENT_ID = "dashboard";
  env.VITE_OIDC_REDIRECT_URI = "http://localhost/auth/callback";
});

function Probe() {
  const { user, status, accessToken } = useAuth();
  const tenants = useTenants();
  const serving = useServingOverview();
  return (
    <>
      <output data-testid="session">
        {status}|{user?.email}
      </output>
      <output data-testid="identity">{JSON.stringify(user)}</output>
      <output data-testid="credential">{JSON.stringify(accessToken)}</output>
      <output data-testid="memberships">{JSON.stringify(tenants.data)}</output>
      <output data-testid="serving">{JSON.stringify(serving.data)}</output>
      <button onClick={() => void logout()}>Sair</button>
    </>
  );
}

function mount() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={client}>
      <AuthProvider>
        <Probe />
      </AuthProvider>
    </QueryClientProvider>,
  );
}

function deferredResponse() {
  let release: (response: Response) => void = () => undefined;
  const pending = new Promise<Response>((resolve) => {
    release = resolve;
  });
  return { pending, release };
}

function grants(token: string, bHasGrant: boolean) {
  if (token === "B" && !bHasGrant) return [];
  const id = token === "B" ? "B" : "A";
  return [
    {
      tenant_id: `tenant-${id}`,
      municipality_name: `Município ${id}`,
      role: id === "B" ? "leitor" : "gestor",
    },
  ];
}

function overview(token: string, tenantId: string | null) {
  return {
    schema_version: "cnes-serving-v1",
    tenant_id: tenantId,
    run_id: token,
    generated_at: "2026-10-25T00:00:00Z",
    competencia: "2026-10",
    kpis: {
      match_count: 1,
      local_only_count: 0,
      national_only_count: 0,
      conflict_count: 0,
      reconciled_row_count: 1,
      active_professional_count: 1,
    },
    divergence_counts: {},
    missing_sources: [],
  };
}

function network(bHasGrant: boolean) {
  const state = { ...deferredResponse(), seen: [] as SeenRequest[], holdA: false, started: false };
  const record = (request: Request) => {
    const bearer = request.headers.get("authorization");
    state.seen.push({
      path: new URL(request.url).pathname,
      bearer,
      tenant: request.headers.get("x-tenant-id"),
    });
    return bearer?.slice(7) ?? "";
  };
  server.use(
    http.get(ME_PATH, ({ request }) => {
      const token = record(request) as keyof typeof identities;
      return identities[token]
        ? HttpResponse.json(identities[token])
        : HttpResponse.json({ detail: "auth_required" }, { status: 401 });
    }),
    http.get(MEMBERSHIPS_PATH, ({ request }) => {
      const token = record(request);
      if (!(token in identities)) {
        return HttpResponse.json({ detail: "auth_required" }, { status: 401 });
      }
      if (state.holdA && token === "A1") {
        state.started = true;
        return state.pending;
      }
      return HttpResponse.json(grants(token, bHasGrant));
    }),
    http.get(SERVING_PATH, ({ request }) =>
      HttpResponse.json(overview(record(request), request.headers.get("x-tenant-id"))),
    ),
  );
  return state;
}

type Network = ReturnType<typeof network>;
type GuardRace = { network: Network; outcome: Promise<GuardResult> };

async function pendingGuard(bHasGrant = false): Promise<GuardRace> {
  const state = network(bHasGrant);
  mount();
  await waitFor(() => expect(screen.getByTestId("serving")).toHaveTextContent('"run_id":"A1"'));
  expect(screen.getByTestId("session")).toHaveTextContent("authenticated|A@test");
  state.holdA = true;
  const outcome = guard().then(
    () => ({ allowed: true }),
    (error: unknown) => ({ allowed: false, error }),
  );
  try {
    await waitFor(() => expect(state.started).toBe(true));
  } catch (error) {
    state.release(HttpResponse.json(grants("A1", true)));
    throw error;
  }
  expect(state.seen).toContainEqual({ path: MEMBERSHIPS_PATH, bearer: "Bearer A1", tenant: null });
  return { network: state, outcome };
}

function emitLoaded(token: string) {
  sdk.token = token;
  act(() => {
    sdk.events.UserLoaded.forEach((callback) => callback(sdk.currentUser() ?? undefined));
  });
}

function releaseA(race: GuardRace) {
  race.network.release(HttpResponse.json(grants("A1", true)));
}

async function finishA(race: GuardRace) {
  await act(async () => {
    releaseA(race);
    await race.network.pending;
  });
  return race.outcome;
}

function expectDiscoveryHeaders(state: Network) {
  expect(
    state.seen
      .filter((request) => request.path !== SERVING_PATH)
      .every((request) => request.tenant === null),
  ).toBe(true);
}

test("guard_redireciona_B_sem_grants_apos_resposta_atrasada_de_A", async () => {
  const race = await pendingGuard();
  try {
    emitLoaded("B");
    await waitFor(() =>
      expect(screen.getByTestId("session")).toHaveTextContent("authenticated|B@test"),
    );
    await waitFor(() => expect(screen.getByTestId("memberships")).toHaveTextContent("[]"));
    expect(screen.getByTestId("credential")).toHaveTextContent('"B"');
    expect(race.network.seen).toContainEqual({
      path: MEMBERSHIPS_PATH,
      bearer: "Bearer B",
      tenant: null,
    });
    expect(screen.getByTestId("serving")).toBeEmptyDOMElement();
    await expect(finishA(race)).resolves.toMatchObject({
      allowed: false,
      error: { options: { to: "/access-pending" } },
    });
    expect(screen.getByTestId("session")).toHaveTextContent("authenticated|B@test");
    expect(screen.getByTestId("memberships")).toHaveTextContent("[]");
    expectDiscoveryHeaders(race.network);
  } finally {
    releaseA(race);
  }
});

test("guard_redireciona_login_se_logout_antecede_resposta_atrasada_de_A", async () => {
  const race = await pendingGuard();
  try {
    await userEvent.click(screen.getByRole("button", { name: "Sair" }));
    expect(sdk.signout).toHaveBeenCalledOnce();
    await waitFor(() => expect(screen.getByTestId("session")).toHaveTextContent("anonymous|"));
    expect(screen.getByTestId("credential")).toHaveTextContent("null");
    expect(screen.getByTestId("memberships")).toBeEmptyDOMElement();
    await expect(finishA(race)).resolves.toMatchObject({
      allowed: false,
      error: { options: { to: "/login" } },
    });
    expect(screen.getByTestId("session")).toHaveTextContent("anonymous|");
    expect(screen.getByTestId("serving")).toBeEmptyDOMElement();
    expectDiscoveryHeaders(race.network);
  } finally {
    releaseA(race);
  }
});

test("guard_permite_resposta_atrasada_quando_A_permanece_autorizado", async () => {
  const race = await pendingGuard();
  try {
    await expect(finishA(race)).resolves.toEqual({ allowed: true });
    expect(screen.getByTestId("session")).toHaveTextContent("authenticated|A@test");
    expect(screen.getByTestId("credential")).toHaveTextContent('"A1"');
    expectDiscoveryHeaders(race.network);
  } finally {
    releaseA(race);
  }
});

test.each(["B", "A2"] as const)(
  "guard_permite_%s_com_grants_validos_apos_mudanca_de_token",
  async (token) => {
    const race = await pendingGuard(true);
    try {
      emitLoaded(token);
      await waitFor(() =>
        expect(screen.getByTestId("credential")).toHaveTextContent(JSON.stringify(token)),
      );
      await waitFor(() =>
        expect(screen.getByTestId("serving")).toHaveTextContent(`"run_id":"${token}"`),
      );
      const id = token === "B" ? "B" : "A";
      expect(screen.getByTestId("session")).toHaveTextContent(`authenticated|${id}@test`);
      expect(race.network.seen).toContainEqual({
        path: MEMBERSHIPS_PATH,
        bearer: `Bearer ${token}`,
        tenant: null,
      });
      expect(race.network.seen).toContainEqual({
        path: SERVING_PATH,
        bearer: `Bearer ${token}`,
        tenant: `tenant-${id}`,
      });
      await expect(finishA(race)).resolves.toEqual({ allowed: true });
      expect(screen.getByTestId("credential")).toHaveTextContent(JSON.stringify(token));
      expect(screen.getByTestId("serving")).toHaveTextContent(`"run_id":"${token}"`);
      expect(race.network.seen).not.toContainEqual({
        path: SERVING_PATH,
        bearer: "Bearer B",
        tenant: "tenant-A",
      });
      expectDiscoveryHeaders(race.network);
    } finally {
      releaseA(race);
    }
  },
);
