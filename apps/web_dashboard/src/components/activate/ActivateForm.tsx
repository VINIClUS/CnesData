import { useActivate } from "@/api/hooks/useActivate";
import type { Tenant } from "@/api/hooks/useTenants";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { type FormEvent, useState } from "react";

type Props = { tenants: Tenant[] };

export function ActivateForm({ tenants }: Props) {
  const [userCode, setUserCode] = useState("");
  const [tenantId, setTenantId] = useState(tenants[0]?.ibge6 ?? "");
  const activate = useActivate();

  const submit = (e: FormEvent) => {
    e.preventDefault();
    activate.mutate({ user_code: userCode, tenant_id: tenantId });
  };

  if (activate.isSuccess) {
    return (
      <div className="rounded border bg-green-50 p-4 text-sm">
        <strong>Agente aprovado.</strong> O agente receberá o certificado mTLS no próximo poll.
      </div>
    );
  }

  return (
    <form onSubmit={submit} className="max-w-md space-y-4">
      <div>
        <Label htmlFor="user_code">Código de ativação</Label>
        <Input
          id="user_code"
          value={userCode}
          onChange={(e) => setUserCode(e.target.value.toUpperCase())}
          placeholder="WDJB-MJHT"
          className="font-mono tracking-widest"
          pattern="[A-Z0-9-]{8,10}"
          required
        />
      </div>
      <div>
        <Label>Município (tenant)</Label>
        <Select value={tenantId} onValueChange={setTenantId}>
          <SelectTrigger>
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {tenants.map((t) => (
              <SelectItem key={t.ibge6} value={t.ibge6}>
                {t.nome} / {t.uf} — {t.ibge6}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
      <Button type="submit" disabled={activate.isPending}>
        {activate.isPending ? "Aprovando..." : "Aprovar"}
      </Button>
      {activate.isError && (
        <p className="text-sm text-red-600">
          {activate.error.message === "invalid_or_expired_user_code"
            ? "Código inválido ou expirado. Peça novo `agent.exe register`."
            : `Erro: ${activate.error.message}`}
        </p>
      )}
    </form>
  );
}
