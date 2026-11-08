import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import { User } from "oidc-client-ts";
import type * as OidcModule from "oidc-client-ts";
import type { ReactNode } from "react";
import { type Mock, afterEach, beforeEach, expect, vi } from "vitest";

import { server } from "../../mocks/server";

import { AuthProvider } from "@/auth/AuthProvider";
import { getAccessToken } from "@/auth/oidc";
import { env, parseEnv } from "@/lib/env";
import { Route } from "@/routes/_app";

type SessionEvent = "UserLoaded" | "UserUnloaded" | "AccessTokenExpired";
type Callback = () => void;
type SdkBoundary = {
  token: string | null;
  expired: boolean;
  reads: number;
  currentUser: () => OidcModule.User | null;
  signout: Mock<() => void>;
  erase: Mock<() => void>;
  store: Mock<(user: OidcModule.User | null) => void>;
  events: Record<SessionEvent, Set<Callback>>;
};
const sdk = vi.hoisted(
  (): SdkBoundary => ({
    token: "A1",
    expired: false,
    reads: 0,
    currentUser: (): OidcModule.User | null => null,
    signout: vi.fn<() => void>(),
    erase: vi.fn<() => void>(),
    store: vi.fn<(user: OidcModule.User | null) => void>(),
    events: {
      UserLoaded: new Set<Callback>(),
      UserUnloaded: new Set<Callback>(),
      AccessTokenExpired: new Set<Callback>(),
    },
  }),
);

export function sdkBoundary() {
  return sdk;
}

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
        sdk.reads++;
        return Promise.resolve(sdk.currentUser());
      }
      removeUser() {
        sdk.erase();
        sdk.token = null;
        sdk.events.UserUnloaded.forEach((callback) => callback());
        return Promise.resolve();
      }
      storeUser(user: OidcModule.User | null) {
        sdk.store(user);
        sdk.token = user?.access_token ?? null;
        return Promise.resolve();
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

export type Profile = "aws" | "legacy" | "local";
export type Failure = "503" | "network" | "401";
export type Seen = { path: string; bearer: string | null; tenant: string | null };
export const ME = "/api/v1/dashboard/auth/me";
export const MEMBERSHIPS = "/api/v1/dashboard/memberships";
export const TENANTS = "/api/v1/dashboard/tenants";
export const SERVING = "/api/v1/dashboard/serving/cnes/overview";
export const LOCAL_ME = "/api/v1/auth/me";
const ISSUER = "https://cognito-idp.us-east-2.amazonaws.com/us-east-2_Example";
const identities = {
  A1: { subject: "subject-A", issuer: ISSUER, email: "A@test", display_name: "A" },
  B: { subject: "subject-B", issuer: ISSUER, email: "B@test", display_name: "B" },
};
const originalEnv = { ...env };
const clients: QueryClient[] = [];

function sdkUser() {
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
    },
  });
}

beforeEach(() => {
  sdk.token = "A1";
  sdk.expired = false;
  sdk.reads = 0;
  sdk.currentUser = sdkUser;
  sdk.signout.mockClear();
  sdk.erase.mockClear();
  sdk.store.mockClear();
});

afterEach(() => {
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
  Object.assign(env, originalEnv);
});

function configure(profile: Profile) {
  Object.assign(
    env,
    parseEnv({
      VITE_RUNTIME_POLICY: profile === "aws" ? "production" : "generic",
      VITE_AUTH_MODE: profile === "local" ? "local" : "oidc",
      VITE_API_BASE_URL: profile === "aws" ? "https://api.cnesdata.com.br/api/v1" : "/api/v1",
      VITE_OIDC_AUTHORITY: ISSUER,
      VITE_OIDC_CLIENT_ID: "dashboard",
      VITE_OIDC_REDIRECT_URI: "http://localhost/auth/callback",
      VITE_OIDC_DOMAIN: "https://example.auth.us-east-2.amazoncognito.com",
    }),
  );
}

export function failureResponse(failure: Failure) {
  if (failure === "network") return HttpResponse.error();
  return HttpResponse.json(
    { detail: failure === "503" ? "identity_unavailable" : "token_invalid" },
    { status: Number(failure) },
  );
}

function deferred() {
  let release: (response: Response) => void = () => undefined;
  const pending = new Promise<Response>((resolve) => {
    release = resolve;
  });
  return { pending, release, started: false };
}

export type Network = {
  profile: Profile;
  seen: Seen[];
  failure: Failure | null;
  membershipFailure: boolean;
  revision: number;
  hold: ReturnType<typeof deferred> | null;
};

function record(state: Network, request: Request) {
  const bearer = request.headers.get("authorization");
  state.seen.push({
    path: new URL(request.url).pathname,
    bearer,
    tenant: request.headers.get("x-tenant-id"),
  });
  if (state.profile === "local") {
    expect(bearer).toBeNull();
    return "A1";
  }
  expect(bearer === "Bearer A1" || bearer === "Bearer B").toBe(true);
  return bearer!.slice(7);
}

function identityResponse(state: Network, request: Request) {
  const token = record(state, request) as keyof typeof identities;
  if (state.hold && token === "A1") {
    state.hold.started = true;
    return state.hold.pending;
  }
  if (state.failure) return failureResponse(state.failure);
  if (state.profile === "local") {
    return HttpResponse.json({
      user_id: "local-A",
      email: "A@test",
      tenant_id: "354130",
      role: "gestor",
    });
  }
  return HttpResponse.json(
    state.profile === "aws"
      ? identities[token]
      : {
          user_id: "legacy-A",
          email: "A@test",
          display_name: "A",
          role: "gestor",
          tenant_ids: ["354130"],
          has_pending_request: false,
        },
  );
}

function membershipResponse(state: Network, request: Request) {
  const token = record(state, request);
  if (state.membershipFailure) {
    return HttpResponse.json({ detail: "memberships_unavailable" }, { status: 503 });
  }
  const ids = token === "B" ? ["tenant-B"] : ["tenant-A", "tenant-A2"];
  return HttpResponse.json(
    ids.map((tenant_id) => ({
      tenant_id,
      municipality_name: `Município ${tenant_id}`,
      role: "gestor",
    })),
  );
}

function network(profile: Profile): Network {
  const state: Network = {
    profile,
    seen: [],
    failure: null,
    membershipFailure: false,
    revision: 1,
    hold: null,
  };
  server.use(
    http.get(`*${ME}`, ({ request }) => identityResponse(state, request)),
    http.get(`*${LOCAL_ME}`, ({ request }) => identityResponse(state, request)),
    http.get(`*${MEMBERSHIPS}`, ({ request }) => membershipResponse(state, request)),
    http.get(`*${TENANTS}`, ({ request }) => {
      record(state, request);
      return HttpResponse.json([{ ibge6: "354130", ibge7: "3541308", nome: "Epitácio", uf: "SP" }]);
    }),
    http.get(`*${SERVING}`, ({ request }) => {
      const token = record(state, request);
      return HttpResponse.json({
        tenant_id: request.headers.get("x-tenant-id"),
        run_id: `${token}:${state.revision}:${request.headers.get("x-tenant-id")}`,
      });
    }),
  );
  return state;
}

export async function startSession(profile: Profile, probe: ReactNode) {
  configure(profile);
  const state = network(profile);
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  clients.push(client);
  render(
    <QueryClientProvider client={client}>
      <AuthProvider>{probe}</AuthProvider>
    </QueryClientProvider>,
  );
  await waitFor(() => expect(screen.getByTestId("status")).toHaveTextContent(/^authenticated$/));
  await waitFor(() => expect(screen.getByTestId("serving")).toHaveTextContent('"run_id":"A1:1:'));
  expect(screen.getByTestId("identity")).toHaveTextContent('"email":"A@test"');
  expect(screen.getByTestId("credential")).toHaveTextContent(profile === "local" ? "null" : '"A1"');
  if (profile === "local") expect(sdk.reads).toBe(0);
  else expect(await getAccessToken()).toBe("A1");
  return state;
}

export async function selectSecondTenant() {
  await userEvent.click(screen.getByRole("button", { name: "Selecionar A2" }));
  await waitFor(() => expect(screen.getByTestId("serving")).toHaveTextContent("A1:1:tenant-A2"));
  expect(screen.getByTestId("selection")).toHaveTextContent('"tenant-A2"');
}

export function holdIdentity(state: Network) {
  const hold = deferred();
  state.hold = hold;
  return hold;
}

export function emit(event: SessionEvent, token = sdk.token) {
  sdk.token = token;
  act(() => sdk.events[event].forEach((callback) => callback()));
}

export async function refresh(completed: number) {
  await userEvent.click(screen.getByRole("button", { name: "Atualizar" }));
  await waitFor(() => expect(screen.getByTestId("completed")).toHaveTextContent(String(completed)));
}

export function expectPrivateContext() {
  for (const name of ["identity", "credential", "selection"]) {
    expect(screen.getByTestId(name)).toHaveTextContent("null");
  }
  expect(screen.getByTestId("memberships")).toBeEmptyDOMElement();
  expect(screen.getByTestId("serving")).toBeEmptyDOMElement();
}

export function expectRequest(state: Network, offset: number, path = ME) {
  expect(state.seen.slice(offset)).toEqual([{ path, bearer: "Bearer A1", tenant: null }]);
}

const guard = Route.options.beforeLoad as () => Promise<unknown>;
export function guardOutcome() {
  return guard().then(
    () => ({ allowed: true as const }),
    (error: unknown) => ({ allowed: false as const, error }),
  );
}

export async function expectSdkRetained(token = "A1") {
  expect(await getAccessToken()).toBe(token);
  expect(sdk.currentUser()?.access_token).toBe(token);
  expect(sdk.signout).not.toHaveBeenCalled();
  expect(sdk.erase).not.toHaveBeenCalled();
  expect(sdk.store).not.toHaveBeenCalled();
}
