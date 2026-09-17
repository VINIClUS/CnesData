import { CalendarDays } from "lucide-react";

import { PreviewDonut } from "./PreviewDonut";
import { PreviewKpis } from "./PreviewKpis";
import { PreviewLineChart } from "./PreviewLineChart";
import { PreviewSidebar } from "./PreviewSidebar";

import { landing } from "@/i18n/landing";

export function DashboardPreview() {
  return (
    <div className="grid grid-cols-[10rem_1fr] overflow-hidden rounded-2xl border border-white/10 bg-navy-card shadow-2xl shadow-black/40">
      <PreviewSidebar />
      <div className="bg-white p-4 text-slate-900">
        <div className="flex items-start justify-between">
          <div>
            <h3 className="text-base font-semibold">{landing.preview.title}</h3>
            <p className="text-[10px] text-slate-500">{landing.preview.subtitle}</p>
          </div>
          <div className="flex items-center gap-2 rounded-lg border border-slate-200 px-2.5 py-1.5">
            <CalendarDays aria-hidden="true" className="size-3.5 text-blue-600" />
            <div className="text-[9px] leading-tight text-slate-500">
              {landing.preview.updated}
              <div className="font-semibold text-slate-900">{landing.preview.updatedAt}</div>
            </div>
          </div>
        </div>
        <div className="mt-4">
          <PreviewKpis />
        </div>
        <div className="mt-3 grid grid-cols-[1.2fr_1fr] gap-3">
          <PreviewLineChart />
          <PreviewDonut />
        </div>
      </div>
    </div>
  );
}
