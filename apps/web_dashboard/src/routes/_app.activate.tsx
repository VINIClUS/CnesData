import { useTenants } from "@/api/hooks/useTenants";
import { ActivateForm } from "@/components/activate/ActivateForm";
import { createFileRoute } from "@tanstack/react-router";

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
      <h1 className="text-2xl font-semibold mb-1">Ativar novo agente</h1>
      <p className="text-sm text-muted-foreground mb-6">
        Cole o código mostrado pelo <code>agent.exe register</code>. O código expira em 10 minutos.
      </p>
      <ActivateForm tenants={tenants.data} />
    </section>
  );
}
