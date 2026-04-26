import { createFileRoute } from "@tanstack/react-router";
import { useEffect } from "react";

import { useAccessRequests } from "@/api/hooks/useAccessRequests";
import { useAuth } from "@/auth/useAuth";
import { AccessRequestForm } from "@/components/signup/AccessRequestForm";
import { PendingRequestsList } from "@/components/signup/PendingRequestsList";

export const Route = createFileRoute("/access-pending")({
  component: AccessPendingPage,
});

function AccessPendingPage() {
  const { user, refresh } = useAuth();
  const requests = useAccessRequests();

  useEffect(() => {
    if (user && user.tenant_ids.length > 0) {
      window.location.href = "/overview";
    }
  }, [user]);

  useEffect(() => {
    const t = setInterval(() => void refresh(), 30_000);
    return () => clearInterval(t);
  }, [refresh]);

  return (
    <main className="mx-auto max-w-2xl p-8">
      <h1 className="mb-2 text-2xl font-semibold">Conta criada</h1>
      <p className="mb-6 text-sm text-muted-foreground">
        Solicite acesso a um município. O administrador revisará seu pedido.
      </p>
      <section className="mb-8">
        <h2 className="mb-3 text-sm font-semibold">Suas solicitações</h2>
        {requests.data ? (
          <PendingRequestsList requests={requests.data} />
        ) : (
          <p className="text-sm">Carregando...</p>
        )}
      </section>
      <section>
        <h2 className="mb-3 text-sm font-semibold">Nova solicitação</h2>
        <AccessRequestForm />
      </section>
    </main>
  );
}
