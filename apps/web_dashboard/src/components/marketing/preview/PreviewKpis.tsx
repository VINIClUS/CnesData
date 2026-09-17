import { Building2, MapPin, TrendingUp, UserRound, Users } from "lucide-react";
import type { LucideIcon } from "lucide-react";

const _KPIS: { icon: LucideIcon; label: string; value: string; delta: string; up: boolean }[] = [
  { icon: Building2, label: "Estabelecimentos", value: "17.842", delta: "2,4%", up: true },
  { icon: UserRound, label: "Profissionais", value: "293.102", delta: "1,8%", up: true },
  { icon: Users, label: "Equipes", value: "48.771", delta: "3,1%", up: true },
  { icon: MapPin, label: "Municípios", value: "645", delta: "100% SP", up: false },
];

export function PreviewKpis() {
  return (
    <ul className="grid grid-cols-4 gap-3">
      {_KPIS.map(({ icon: Icon, label, value, delta, up }) => (
        <li key={label} className="flex items-center gap-3 rounded-lg border border-slate-200 p-3">
          <span className="flex size-9 items-center justify-center rounded-full bg-blue-50 text-blue-600">
            <Icon aria-hidden="true" className="size-4" />
          </span>
          <div>
            <div className="text-[10px] text-slate-500">{label}</div>
            <div className="text-base font-semibold text-slate-900">{value}</div>
            <div className="flex items-center gap-1 text-[10px] text-slate-500">
              {up && <TrendingUp aria-hidden="true" className="size-3 text-emerald-500" />}
              {delta}
            </div>
          </div>
        </li>
      ))}
    </ul>
  );
}
