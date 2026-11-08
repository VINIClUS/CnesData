import { type FormEvent, useState } from "react";

import { useActivate } from "@/api/hooks/useActivate";
import {
  type AuthorizedMunicipality,
  tenantId as municipalityId,
  tenantName,
} from "@/api/hooks/useTenants";
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

type Props = { tenants: AuthorizedMunicipality[] };

export function ActivateForm({ tenants }: Props) {
  const [userCode, setUserCode] = useState("");
  const [tenantId, setTenantId] = useState(tenants[0] ? municipalityId(tenants[0]) : "");
  const authorizedId = tenants.find((tenant) => municipalityId(tenant) === tenantId)
    ? tenantId
    : tenants[0]
      ? municipalityId(tenants[0])
      : "";
  const activate = useActivate();

  const submit = (e: FormEvent) => {
    e.preventDefault();
    if (authorizedId) activate.mutate({ user_code: userCode, tenant_id: authorizedId });
  };

  if (activate.isSuccess) return <ActivationSuccess />;

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
      <MunicipalitySelect tenants={tenants} value={authorizedId} onChange={setTenantId} />
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

function ActivationSuccess() {
  return (
    <div className="rounded border bg-green-50 p-4 text-sm">
      <strong>Agente aprovado.</strong> O agente receberá o certificado mTLS no próximo poll.
    </div>
  );
}

type MunicipalitySelectProps = Props & { value: string; onChange: (value: string) => void };

function MunicipalitySelect({ tenants, value, onChange }: MunicipalitySelectProps) {
  return (
    <div>
      <Label>Município (tenant)</Label>
      <Select value={value} onValueChange={onChange}>
        <SelectTrigger>
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {tenants.map((t) => (
            <SelectItem key={municipalityId(t)} value={municipalityId(t)}>
              {tenantName(t)}
              {"uf" in t ? ` / ${t.uf} — ${t.ibge6}` : ""}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
}
