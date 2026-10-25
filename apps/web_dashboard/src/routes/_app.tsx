import { Outlet, createFileRoute, redirect } from "@tanstack/react-router";

import { apiFetch } from "@/api/client";
import type { Membership } from "@/api/hooks/useTenants";
import { getLocalPrincipal } from "@/auth/local";
import { getAccessToken } from "@/auth/oidc";
import { sessionGeneration } from "@/auth/sessionEvents";
import { type Me, isAwsMe } from "@/auth/types";
import { Shell } from "@/components/layout/Shell";
import { env } from "@/lib/env";

export const Route = createFileRoute("/_app")({
  beforeLoad: async () => {
    if (env.VITE_AUTH_MODE === "local") {
      try {
        await getLocalPrincipal();
      } catch {
        // eslint-disable-next-line @typescript-eslint/only-throw-error -- TanStack Router pattern
        throw redirect({ to: "/login" });
      }
      return;
    }
    for (;;) {
      const generation = sessionGeneration();
      const token = await getAccessToken().catch(() => null);
      if (generation !== sessionGeneration()) continue;
      if (!token) {
        // eslint-disable-next-line @typescript-eslint/only-throw-error -- TanStack Router pattern
        throw redirect({ to: "/login" });
      }
      const outcome = await tenantAccess(token);
      const currentToken = await getAccessToken().catch(() => null);
      if (generation !== sessionGeneration() || token !== currentToken) continue;
      if ("error" in outcome) throw outcome.error;
      if (!outcome.allowed) {
        // eslint-disable-next-line @typescript-eslint/only-throw-error -- TanStack Router pattern
        throw redirect({ to: "/access-pending" });
      }
      return;
    }
  },
  component: () => (
    <Shell>
      <Outlet />
    </Shell>
  ),
});

async function tenantAccess(accessToken: string) {
  try {
    const me = await identity(accessToken);
    const tenants = isAwsMe(me)
      ? await apiFetch<Membership[]>("/dashboard/memberships", { accessToken })
      : me.tenant_ids;
    return { allowed: tenants.length > 0 };
  } catch (error) {
    return { error };
  }
}

async function identity(accessToken: string): Promise<Me> {
  try {
    return await apiFetch<Me>("/dashboard/auth/me", { accessToken });
  } catch {
    // eslint-disable-next-line @typescript-eslint/only-throw-error -- TanStack Router pattern
    throw redirect({ to: "/login" });
  }
}
