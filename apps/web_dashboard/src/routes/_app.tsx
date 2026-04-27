import { Outlet, createFileRoute, redirect } from "@tanstack/react-router";

import { Shell } from "@/components/layout/Shell";

export const Route = createFileRoute("/_app")({
  beforeLoad: async () => {
    let r: Response;
    try {
      r = await fetch("/api/v1/dashboard/auth/me", { credentials: "include" });
    } catch {
      // eslint-disable-next-line @typescript-eslint/only-throw-error -- TanStack Router pattern
      throw redirect({ to: "/login" });
    }
    if (!r.ok) {
      // eslint-disable-next-line @typescript-eslint/only-throw-error -- TanStack Router pattern
      throw redirect({ to: "/login" });
    }
    const me = (await r.json()) as { tenant_ids: string[] };
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
