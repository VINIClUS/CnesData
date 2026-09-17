import { cn } from "@/lib/utils";

export type LogoSize = "sm" | "md" | "lg";

const _MARK: Record<LogoSize, string> = { sm: "size-5", md: "size-7", lg: "size-9" };
const _TEXT: Record<LogoSize, string> = { sm: "text-base", md: "text-xl", lg: "text-2xl" };

const _BARS = [
  { x: 2, y: 14, h: 8, className: "fill-brand-bright/60" },
  { x: 9, y: 8, h: 14, className: "fill-brand-bright" },
  { x: 16, y: 2, h: 20, className: "fill-brand" },
] as const;

export function LogoMark({ size = "md", className }: { size?: LogoSize; className?: string }) {
  return (
    <svg
      viewBox="0 0 24 24"
      aria-hidden="true"
      className={cn(_MARK[size], className)}
      data-testid="logo-mark"
    >
      {_BARS.map((b) => (
        <rect key={b.x} x={b.x} y={b.y} width={6} height={b.h} rx={1.5} className={b.className} />
      ))}
    </svg>
  );
}

export function Logo({ size = "md", className }: { size?: LogoSize; className?: string }) {
  return (
    <span className={cn("inline-flex items-center gap-2", className)}>
      <LogoMark size={size} />
      <span className={cn("font-semibold tracking-tight", _TEXT[size])}>
        Cnes<span className="text-primary">Data</span>
      </span>
    </span>
  );
}
