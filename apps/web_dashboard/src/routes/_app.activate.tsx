import { createFileRoute } from "@tanstack/react-router";

import { useTenants } from "@/api/hooks/useTenants";
import { ActivateForm } from "@/components/activate/ActivateForm";

export const Route = createFileRoute("/_app/activate")({
  component: ActivatePage,
});

function ActivatePage() {
  const tenants = useTenants();

  if (tenants.isLoading) {
    return <p>Carregando...</p>;
  }
  if (tenants.isError || !tenants.data) {
    return <p className="text-red-600">Erro ao carregar municípios.</p>;
  }

  return (
    <section className="max-w-2xl">
      <h1 className="mb-1 text-2xl font-semibold">Ativar novo agente</h1>
      <p className="mb-6 text-sm text-muted-foreground">
        Cole o código mostrado pelo <code>agent.exe register</code>. O código expira em 10 minutos.
      </p>
      <ActivateForm tenants={tenants.data} />
    </section>
  );
}
