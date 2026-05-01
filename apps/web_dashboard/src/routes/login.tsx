import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { useEffect } from "react";

import { startLogin } from "@/auth/oidc";
import { useAuth } from "@/auth/useAuth";
import { Button } from "@/components/ui/button";

export const Route = createFileRoute("/login")({
  component: LoginPage,
});

function LoginPage() {
  const { status } = useAuth();
  const navigate = useNavigate();

  useEffect(() => {
    if (status === "authenticated") void navigate({ to: "/agentes" });
  }, [status, navigate]);

  return (
    <main className="flex min-h-screen items-center justify-center bg-background">
      <div className="w-[360px] rounded-xl border bg-card p-8 shadow">
        <h1 className="text-2xl font-semibold">CnesData</h1>
        <p className="mb-6 text-sm text-muted-foreground">Painel municipal</p>
        <Button onClick={() => void startLogin()} className="w-full">
          Entrar
        </Button>
      </div>
    </main>
  );
}
