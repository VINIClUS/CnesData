import { ChevronDown } from "lucide-react";

import { landing } from "@/i18n/landing";

const _MONTHS = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set"] as const;
const _POINTS = [14.2, 15.0, 15.9, 16.6, 17.4, 18.3, 19.1, 20.2, 21.6] as const;
const _TICKS = ["25k", "20k", "15k", "10k"] as const;
const _W = 300;
const _H = 110;

function _coords(): { x: number; y: number }[] {
  const step = _W / (_POINTS.length - 1);
  return _POINTS.map((v, i) => ({ x: i * step, y: _H - ((v - 10) / 15) * _H }));
}

export function PreviewLineChart() {
  const pts = _coords();
  const line = pts.map((p) => `${p.x},${p.y}`).join(" ");
  const area = `0,${_H} ${line} ${_W},${_H}`;
  return (
    <div className="rounded-lg border border-slate-200 p-3">
      <div className="flex items-center justify-between">
        <span className="whitespace-nowrap text-[11px] font-semibold text-slate-900">
          {landing.preview.lineTitle}
        </span>
        <span className="flex items-center gap-1 whitespace-nowrap rounded border border-slate-200 px-1.5 py-0.5 text-[8px] text-slate-500">
          {landing.preview.linePeriod}
          <ChevronDown aria-hidden="true" className="size-2.5" />
        </span>
      </div>
      <div className="mt-3 flex gap-2">
        <div className="flex flex-col justify-between text-[8px] text-slate-400">
          {_TICKS.map((t) => (
            <span key={t}>{t}</span>
          ))}
        </div>
        <svg viewBox={`0 0 ${_W} ${_H}`} aria-hidden="true" className="h-24 w-full">
          <defs>
            <linearGradient id="preview-area" x1="0" x2="0" y1="0" y2="1">
              <stop offset="0%" stopColor="#3b82f6" stopOpacity="0.25" />
              <stop offset="100%" stopColor="#3b82f6" stopOpacity="0" />
            </linearGradient>
          </defs>
          <polygon points={area} fill="url(#preview-area)" />
          <polyline points={line} fill="none" stroke="#2563eb" strokeWidth="2" />
          {pts.map((p) => (
            <circle key={p.x} cx={p.x} cy={p.y} r="2.5" fill="#2563eb" />
          ))}
        </svg>
      </div>
      <div className="ml-6 mt-1 flex justify-between text-[8px] text-slate-400">
        {_MONTHS.map((m) => (
          <span key={m}>{m}</span>
        ))}
      </div>
    </div>
  );
}
