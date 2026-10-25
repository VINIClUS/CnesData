import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import type * as OidcModule from "oidc-client-ts";
import { type Mock, beforeEach, expect, test, vi } from "vitest";

import { server } from "../../mocks/server";

import { useServingOverview } from "@/api/hooks/useServingOverview";
import { useTenants } from "@/api/hooks/useTenants";
import { AuthProvider } from "@/auth/AuthProvider";
import { logout } from "@/auth/oidc";
import { useAuth } from "@/auth/useAuth";
import { env } from "@/lib/env";

type SessionEvent = "UserLoaded" | "UserUnloaded" | "AccessTokenExpired";
type Callback = () => void;
type SdkBoundary = {
  token: string | null;
  expired: boolean;
  events: Record<SessionEvent, Set<Callback>>;
  signout: Mock<() => void>;
};
const sdk = vi.hoisted(
  (): SdkBoundary => ({
    token: "A1",
    expired: false,
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
        return Promise.resolve(
          sdk.token ? { access_token: sdk.token, expired: sdk.expired } : null,
        );
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
const ME_PATH = "/api/v1/dashboard/auth/me";
const MEMBERSHIPS_PATH = "/api/v1/dashboard/memberships";
const SERVING_PATH = "/api/v1/dashboard/serving/cnes/overview";
const identities = {
  A1: { subject: "same-subject", issuer: "https://A.idp.test", email: "A@test", display_name: "A" },
  A2: { subject: "same-subject", issuer: "https://A.idp.test", email: "A@test", display_name: "A" },
  B: { subject: "same-subject", issuer: "https://B.idp.test", email: "B@test", display_name: "B" },
};

beforeEach(() => {
  sdk.token = "A1";
  sdk.expired = false;
  sdk.signout.mockClear();
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

function network() {
  const seen: SeenRequest[] = [];
  const record = (request: Request) => {
    const bearer = request.headers.get("authorization");
    seen.push({
      path: new URL(request.url).pathname,
      bearer,
      tenant: request.headers.get("x-tenant-id"),
    });
    return bearer?.slice(7) as keyof typeof identities;
  };
  server.use(
    http.get(ME_PATH, ({ request }) => HttpResponse.json(identities[record(request)])),
    http.get(MEMBERSHIPS_PATH, ({ request }) => {
      const token = record(request);
      const id = token === "B" ? "B" : "A";
      return HttpResponse.json([
        {
          tenant_id: `tenant-${id}`,
          municipality_name: `Município ${id}`,
          role: id === "B" ? "leitor" : "gestor",
        },
      ]);
    }),
    http.get(SERVING_PATH, ({ request }) => {
      const token = record(request);
      return HttpResponse.json({ tenant_id: request.headers.get("x-tenant-id"), run_id: token });
    }),
  );
  return seen;
}

async function initialSession() {
  const seen = network();
  mount();
  await waitFor(() => expect(screen.getByTestId("serving")).toHaveTextContent('"run_id":"A1"'));
  expect(screen.getByTestId("credential")).toHaveTextContent('"A1"');
  expect(screen.getByTestId("session")).toHaveTextContent("authenticated|A@test");
  return seen;
}

function emit(event: SessionEvent, token = sdk.token) {
  sdk.token = token;
  act(() => {
    sdk.events[event].forEach((callback) => callback());
  });
}

function expectRequests(seen: SeenRequest[], token: string, tenant: string) {
  expect(seen).toContainEqual({ path: ME_PATH, bearer: `Bearer ${token}`, tenant: null });
  expect(seen).toContainEqual({ path: MEMBERSHIPS_PATH, bearer: `Bearer ${token}`, tenant: null });
  expect(seen).toContainEqual({ path: SERVING_PATH, bearer: `Bearer ${token}`, tenant });
}

test("UserLoaded_renova_bearer_do_mesmo_issuer_subject_e_refaz_descoberta_e_produto", async () => {
  const seen = await initialSession();
  expectRequests(seen, "A1", "tenant-A");
  const offset = seen.length;
  emit("UserLoaded", "A2");
  await waitFor(() => expect(screen.getByTestId("serving")).toHaveTextContent('"run_id":"A2"'));
  expect(screen.getByTestId("session")).toHaveTextContent("authenticated|A@test");
  expect(screen.getByTestId("identity")).toHaveTextContent('"issuer":"https://A.idp.test"');
  expect(screen.getByTestId("identity")).toHaveTextContent('"subject":"same-subject"');
  expect(screen.getByTestId("credential")).toHaveTextContent('"A2"');
  expectRequests(seen.slice(offset), "A2", "tenant-A");
  expect(seen.slice(offset).every((request) => request.bearer === "Bearer A2")).toBe(true);
});

test("UserLoaded_troca_o_principal_sem_reusar_tenant_papel_ou_produto_anterior", async () => {
  const seen = await initialSession();
  emit("UserLoaded", "B");
  await waitFor(() =>
    expect(screen.getByTestId("session")).toHaveTextContent("authenticated|B@test"),
  );
  expect(screen.getByTestId("memberships")).not.toHaveTextContent("Município A");
  expect(screen.getByTestId("serving")).not.toHaveTextContent('"run_id":"A1"');
  await waitFor(() => expect(screen.getByTestId("serving")).toHaveTextContent('"run_id":"B"'));
  expect(screen.getByTestId("identity")).toHaveTextContent('"issuer":"https://B.idp.test"');
  expect(screen.getByTestId("credential")).toHaveTextContent('"B"');
  expect(screen.getByTestId("memberships")).toHaveTextContent('"role":"leitor"');
  expectRequests(seen, "B", "tenant-B");
  expect(seen).not.toContainEqual({ path: SERVING_PATH, bearer: "Bearer B", tenant: "tenant-A" });
});

test.each(["UserUnloaded", "AccessTokenExpired"] as const)(
  "%s_limpa_identidade_bearer_memberships_e_produto",
  async (event) => {
    const seen = await initialSession();
    const offset = seen.length;
    if (event === "UserUnloaded") {
      await userEvent.click(screen.getByRole("button", { name: "Sair" }));
      expect(sdk.signout).toHaveBeenCalledOnce();
    } else {
      sdk.expired = true;
      emit(event);
    }
    await waitFor(() => expect(screen.getByTestId("session")).toHaveTextContent("anonymous|"));
    expect(screen.getByTestId("identity")).toHaveTextContent("null");
    expect(screen.getByTestId("credential")).toHaveTextContent("null");
    expect(screen.getByTestId("memberships")).toBeEmptyDOMElement();
    expect(screen.getByTestId("serving")).toBeEmptyDOMElement();
    expect(seen.slice(offset)).toEqual([]);
  },
);
