import { Outlet, createFileRoute, redirect } from "@tanstack/react-router";

import { Shell } from "@/components/layout/Shell";

export const Route = createFileRoute("/_app")({
  beforeLoad: async () => {
    const r = await fetch("/api/v1/dashboard/auth/me", { credentials: "include" });
    if (r.status === 401) {
      // eslint-disable-next-line @typescript-eslint/only-throw-error -- TanStack Router pattern
      throw redirect({ to: "/login" });
    }
  },
  component: () => (
    <Shell>
      <Outlet />
    </Shell>
  ),
});
