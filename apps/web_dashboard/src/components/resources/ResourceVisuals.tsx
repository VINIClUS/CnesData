import type { ReactNode } from "react";

import { PreviewDonut } from "@/components/marketing/preview/PreviewDonut";
import { PreviewKpis } from "@/components/marketing/preview/PreviewKpis";
import { PreviewLineChart } from "@/components/marketing/preview/PreviewLineChart";
import { Badge } from "@/components/ui/badge";
import { resources } from "@/i18n/resources";

function IllustrativePanel({ caption, children }: { caption: string; children: ReactNode }) {
  return (
    <figure className="min-w-0 rounded-2xl border bg-card p-4 shadow-sm">
      <div className="overflow-hidden rounded-xl bg-white p-4 text-slate-900">{children}</div>
      <figcaption className="mt-3 text-center text-xs text-muted-foreground">{caption}</figcaption>
    </figure>
  );
}

export function KpisVisual() {
  return (
    <IllustrativePanel caption={resources.visuals.kpis}>
      <PreviewKpis />
    </IllustrativePanel>
  );
}

export function DivergenceVisual() {
  const d = resources.visuals.divergence;
  return (
    <IllustrativePanel caption={d.caption}>
      <table className="w-full text-left text-xs">
        <thead className="text-slate-500">
          <tr>
            {d.columns.map((c) => (
              <th key={c} scope="col" className="pb-2 font-medium">
                {c}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {d.rows.map(([field, local, national, status]) => (
            <tr key={field} className="border-t border-slate-200">
              <th scope="row" className="py-2 font-medium">
                {field}
              </th>
              <td className="py-2 tabular-nums">{local}</td>
              <td className="py-2 tabular-nums">{national}</td>
              <td className="py-2">
                <Badge variant={status === "Revisar" ? "default" : "secondary"}>{status}</Badge>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </IllustrativePanel>
  );
}

export function ChartsVisual() {
  return (
    <IllustrativePanel caption={resources.visuals.charts}>
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-[1.2fr_1fr]">
        <PreviewLineChart />
        <PreviewDonut />
      </div>
    </IllustrativePanel>
  );
}
