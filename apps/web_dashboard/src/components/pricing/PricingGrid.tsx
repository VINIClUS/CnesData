import { ChartNoAxesColumnIncreasing, UserRound, Users } from "lucide-react";
import type { LucideIcon } from "lucide-react";

import { PricingCard } from "./PricingCard";

import { pricing } from "@/i18n/pricing";

const _ICONS: Record<(typeof pricing.plans)[number]["id"], LucideIcon> = {
  basico: UserRound,
  profissional: ChartNoAxesColumnIncreasing,
  enterprise: Users,
};

export function PricingGrid() {
  return (
    <section className="bg-background">
      <div className="mx-auto grid w-full max-w-[1320px] grid-cols-1 gap-6 px-6 py-10 lg:grid-cols-3">
        {pricing.plans.map((plan) => (
          <PricingCard
            key={plan.id}
            plan={plan}
            icon={_ICONS[plan.id]}
            highlighted={plan.id === "profissional"}
          />
        ))}
      </div>
    </section>
  );
}
