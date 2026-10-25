import { Outlet, createFileRoute, redirect } from "@tanstack/react-router";

import { apiFetch } from "@/api/client";
import type { Membership } from "@/api/hooks/useTenants";
import { getLocalPrincipal } from "@/auth/local";
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
    let me: Me;
    try {
      me = await apiFetch<Me>("/dashboard/auth/me");
    } catch {
      // eslint-disable-next-line @typescript-eslint/only-throw-error -- TanStack Router pattern
      throw redirect({ to: "/login" });
    }
    const tenantIds = isAwsMe(me)
      ? await apiFetch<Membership[]>("/dashboard/memberships")
      : me.tenant_ids;
    if (tenantIds.length === 0) {
      // eslint-disable-next-line @typescript-eslint/only-throw-error -- TanStack Router pattern
      throw redirect({ to: "/access-pending" });
    }
  },
  component: () => (
    <Shell>
      <Outlet />
    </Shell>
  ),
});
