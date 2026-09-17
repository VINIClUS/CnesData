import { landing } from "@/i18n/landing";

const _SLICES = [
  { label: "UBS", pct: 42, color: "#2563eb" },
  { label: "Hospital", pct: 18, color: "#22c55e" },
  { label: "CAPS", pct: 8, color: "#eab308" },
  { label: "Policlínica", pct: 7, color: "#ef4444" },
  { label: "Outros", pct: 25, color: "#a855f7" },
] as const;

function _offsets(): number[] {
  let acc = 0;
  return _SLICES.map((s) => {
    const o = acc;
    acc += s.pct;
    return o;
  });
}

export function PreviewDonut() {
  const offsets = _offsets();
  return (
    <div className="rounded-lg border border-slate-200 p-3">
      <div className="flex items-center justify-between">
        <span className="text-[11px] font-semibold text-slate-900">
          {landing.preview.donutTitle}
        </span>
        <span className="text-[9px] text-blue-600">{landing.preview.donutLink}</span>
      </div>
      <div className="mt-3 flex items-center gap-4">
        <svg viewBox="0 0 40 40" aria-hidden="true" className="size-24 -rotate-90">
          {_SLICES.map((s, i) => (
            <circle
              key={s.label}
              cx="20"
              cy="20"
              r="15"
              fill="none"
              stroke={s.color}
              strokeWidth="7"
              pathLength={100}
              strokeDasharray={`${s.pct} ${100 - s.pct}`}
              strokeDashoffset={-(offsets[i] ?? 0)}
            />
          ))}
        </svg>
        <ul className="flex-1 space-y-1.5">
          {_SLICES.map((s) => (
            <li key={s.label} className="flex items-center justify-between gap-2 text-[10px]">
              <span className="flex items-center gap-1.5 text-slate-600">
                <span className="size-2 rounded-full" style={{ backgroundColor: s.color }} />
                {s.label}
              </span>
              <span className="font-semibold text-slate-900">{s.pct}%</span>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}
