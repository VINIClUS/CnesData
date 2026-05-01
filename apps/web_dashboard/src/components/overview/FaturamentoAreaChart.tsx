import { lazy, Suspense } from "react";

import type { FaturamentoChart } from "@/api/hooks/useFaturamentoChart";
import { formatBRL } from "@/lib/format";

const _AreaChartLazy = lazy(async () => {
  const { AreaChart } = await import("@tremor/react");
  return { default: AreaChart };
});

const _COLORS = [
  "blue",
  "cyan",
  "indigo",
  "violet",
  "fuchsia",
  "pink",
  "rose",
  "orange",
  "amber",
  "yellow",
  "gray",
] as const;

export function FaturamentoAreaChart({ data }: { data: FaturamentoChart }) {
  return (
    <Suspense fallback={<div className="h-[260px] animate-pulse rounded bg-muted" />}>
      <_AreaChartLazy
        data={data.series}
        index="competencia"
        categories={data.categories}
        colors={[..._COLORS].slice(0, data.categories.length)}
        valueFormatter={(v: number) => formatBRL(v)}
        stack
        showLegend
        className="h-[260px]"
      />
    </Suspense>
  );
}
