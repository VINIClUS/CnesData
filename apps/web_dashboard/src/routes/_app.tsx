import { Outlet, createFileRoute, redirect } from "@tanstack/react-router";

import { apiFetch } from "@/api/client";
import { getLocalPrincipal } from "@/auth/local";
import type { Me } from "@/auth/types";
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
    if (me.tenant_ids.length === 0) {
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
