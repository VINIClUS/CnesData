import { createFileRoute } from "@tanstack/react-router";

import { LoginPage } from "@/components/login/LoginPage";

export const Route = createFileRoute("/login")({
  head: () => ({ meta: [{ title: "Entrar | CnesData" }] }),
  component: LoginPage,
});
