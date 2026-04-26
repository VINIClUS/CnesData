import { useState, type FormEvent } from "react";

import { ApiError } from "@/api/client";
import { useAvailableTenants } from "@/api/hooks/useAvailableTenants";
import { useSubmitAccessRequest } from "@/api/hooks/useSubmitAccessRequest";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

const _MOTIVATION_PLACEHOLDER =
  "Descreva por que precisa de acesso. NÃO inclua dados pessoais (CPF, nome).";

function _isDuplicate(err: Error): boolean {
  if (err instanceof ApiError) {
    const body = err.body as { detail?: string } | null;
    return body?.detail === "duplicate_request" || err.status === 409;
  }
  return err.message.includes("duplicate");
}

export function AccessRequestForm() {
  const tenants = useAvailableTenants();
  const submit = useSubmitAccessRequest();
  const [tenantId, setTenantId] = useState("");
  const [motivation, setMotivation] = useState("");

  const onSubmit = (e: FormEvent) => {
    e.preventDefault();
    submit.mutate({ tenant_id: tenantId, motivation });
  };

  if (submit.isSuccess) {
    return (
      <div className="rounded border bg-green-50 p-4 text-sm">
        Solicitação enviada. Aguarde aprovação do administrador.
      </div>
    );
  }

  return (
    <form onSubmit={onSubmit} className="space-y-4">
      <div>
        <Label>Município</Label>
        <Select value={tenantId} onValueChange={setTenantId}>
          <SelectTrigger>
            <SelectValue placeholder="Selecione um município" />
          </SelectTrigger>
          <SelectContent>
            {tenants.data?.map((t) => (
              <SelectItem key={t.ibge6} value={t.ibge6}>
                {t.nome} / {t.uf} — {t.ibge6}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
      <div>
        <Label htmlFor="motivation">Justificativa</Label>
        <textarea
          id="motivation"
          value={motivation}
          onChange={(e) => setMotivation(e.target.value.slice(0, 500))}
          placeholder={_MOTIVATION_PLACEHOLDER}
          required
          maxLength={500}
          rows={4}
          className="w-full rounded border bg-background px-3 py-2 text-sm"
        />
        <p className="mt-1 text-xs text-muted-foreground">{motivation.length} / 500</p>
      </div>
      <Button type="submit" disabled={!tenantId || !motivation || submit.isPending}>
        {submit.isPending ? "Enviando..." : "Solicitar acesso"}
      </Button>
      {submit.isError && (
        <p className="text-sm text-red-600">
          {_isDuplicate(submit.error)
            ? "Você já tem solicitação para esse município."
            : `Erro: ${submit.error.message}`}
        </p>
      )}
    </form>
  );
}
