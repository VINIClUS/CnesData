const _FACETS = [
  { points: "0,0 420,0 180,260", className: "fill-white/[0.035]" },
  { points: "420,0 900,0 640,190", className: "fill-brand/[0.08]" },
  { points: "180,260 640,190 380,520", className: "fill-white/[0.025]" },
  { points: "0,300 180,260 60,620", className: "fill-brand/[0.06]" },
  { points: "900,0 1600,0 1250,220", className: "fill-white/[0.03]" },
  { points: "1250,220 1600,120 1600,520", className: "fill-brand/[0.07]" },
  { points: "640,190 1250,220 980,480", className: "fill-white/[0.02]" },
  { points: "380,520 980,480 620,900", className: "fill-brand/[0.05]" },
  { points: "980,480 1600,520 1600,900 1180,900", className: "fill-white/[0.03]" },
  { points: "0,620 380,520 200,900 0,900", className: "fill-white/[0.02]" },
] as const;

export function HeroBackground() {
  return (
    <svg
      aria-hidden="true"
      viewBox="0 0 1600 900"
      preserveAspectRatio="none"
      className="pointer-events-none absolute inset-0 size-full"
    >
      {_FACETS.map((f) => (
        <polygon key={f.points} points={f.points} className={f.className} />
      ))}
    </svg>
  );
}
