import type { AccessRequest } from "@/api/hooks/useAccessRequests";
import { formatRelativeTime } from "@/lib/format";

const _BADGE: Record<AccessRequest["status"], string> = {
  pending: "bg-yellow-100 text-yellow-800",
  approved: "bg-green-100 text-green-800",
  rejected: "bg-red-100 text-red-800",
};

const _LABEL: Record<AccessRequest["status"], string> = {
  pending: "Pendente",
  approved: "Aprovado",
  rejected: "Rejeitado",
};

export function PendingRequestsList({ requests }: { requests: AccessRequest[] }) {
  if (requests.length === 0) {
    return <p className="text-sm text-muted-foreground">Nenhuma solicitação ainda.</p>;
  }
  return (
    <ul className="space-y-2">
      {requests.map((r) => (
        <li key={r.id} className="flex items-center justify-between rounded border p-3">
          <div>
            <div className="text-sm font-medium">{r.tenant_nome ?? r.tenant_id}</div>
            <div className="text-xs text-muted-foreground">
              {formatRelativeTime(new Date(r.requested_at))}
            </div>
          </div>
          <span className={`rounded-full px-2 py-0.5 text-xs ${_BADGE[r.status]}`}>
            {_LABEL[r.status]}
          </span>
        </li>
      ))}
    </ul>
  );
}
