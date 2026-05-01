import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { useEffect, useState } from "react";

import { completeLogin } from "@/auth/oidc";
import { useAuth } from "@/auth/useAuth";

export const Route = createFileRoute("/auth/callback")({
  component: AuthCallback,
});

function AuthCallback() {
  const navigate = useNavigate();
  const { refresh } = useAuth();
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    completeLogin()
      .then(refresh)
      .then(() => navigate({ to: "/agentes" }))
      .catch((e: unknown) => setError(String(e)));
  }, [navigate, refresh]);

  if (error) {
    return (
      <main className="p-8">
        <p>
          Erro ao completar login. <a href="/login">Tentar novamente</a>
        </p>
      </main>
    );
  }
  return <main className="p-8">Concluindo login...</main>;
}
