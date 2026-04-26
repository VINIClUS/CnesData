import { Outlet, createFileRoute, redirect } from "@tanstack/react-router";

export const Route = createFileRoute("/_app")({
  beforeLoad: async () => {
    const r = await fetch("/api/v1/dashboard/auth/me", { credentials: "include" });
    if (r.status === 401) throw redirect({ to: "/login" });
  },
  component: () => <Outlet />,
});
