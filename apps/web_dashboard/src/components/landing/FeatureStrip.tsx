import { ChartNoAxesColumnIncreasing, Database, ShieldCheck, Users } from "lucide-react";
import type { LucideIcon } from "lucide-react";

import { FeatureItem } from "@/components/marketing/FeatureItem";
import { landing } from "@/i18n/landing";

const _ICONS: LucideIcon[] = [Database, ChartNoAxesColumnIncreasing, ShieldCheck, Users];

export function FeatureStrip() {
  return (
    <section id="recursos" className="bg-background">
      <div className="mx-auto grid w-full max-w-[1320px] grid-cols-1 gap-8 px-6 py-10 md:grid-cols-2 lg:grid-cols-4">
        {landing.features.map((f, i) => (
          <FeatureItem key={f.title} icon={_ICONS[i] ?? Database} {...f} />
        ))}
      </div>
    </section>
  );
}
