type Props = {
  label: string;
  value: string;
  delta?: { value: string; direction: "up" | "down" | "warn" };
  context?: string;
};

const _COLOR: Record<NonNullable<Props["delta"]>["direction"], string> = {
  up: "text-green-600",
  down: "text-red-600",
  warn: "text-yellow-600",
};

const _ARROW: Record<NonNullable<Props["delta"]>["direction"], string> = {
  up: "▲",
  down: "▼",
  warn: "▲",
};

export function KpiCard({ label, value, delta, context }: Props) {
  return (
    <article className="rounded-lg border bg-card p-4">
      <div className="text-xs uppercase tracking-wide text-muted-foreground">{label}</div>
      <div className="mt-1 text-2xl font-semibold">{value}</div>
      {delta && (
        <div className={`text-xs ${_COLOR[delta.direction]}`}>
          {_ARROW[delta.direction]} {delta.value}
        </div>
      )}
      {context && <div className="mt-1 text-[11px] text-muted-foreground">{context}</div>}
    </article>
  );
}
