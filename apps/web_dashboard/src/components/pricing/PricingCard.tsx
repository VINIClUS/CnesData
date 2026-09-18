import { Link } from "@tanstack/react-router";
import { ArrowRight } from "lucide-react";
import type { LucideIcon } from "lucide-react";

import { CheckBullet } from "@/components/marketing/CheckBullet";
import { IconBadge } from "@/components/marketing/IconBadge";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { type Plan, pricing } from "@/i18n/pricing";
import { cn } from "@/lib/utils";

type Props = { plan: Plan; icon: LucideIcon; highlighted?: boolean };

function PriceBlock({ plan }: { plan: Plan }) {
  const l = pricing.priceLabels;
  return (
    <div className="mt-6">
      <div className="text-sm text-muted-foreground line-through">
        {l.from} {plan.originalPrice}
      </div>
      <div className="text-sm text-muted-foreground">{l.perOnly}</div>
      <div className="flex items-baseline gap-2 text-primary">
        <span className="text-2xl font-semibold">{l.installments}</span>
        <span className="text-4xl font-bold tracking-tight">{plan.price}</span>
      </div>
    </div>
  );
}

function PlanCta({ plan, highlighted }: { plan: Plan; highlighted: boolean }) {
  const variant = highlighted ? "default" : "outline";
  const cls = cn("mt-6 w-full", !highlighted && "border-primary/40 text-primary");
  if (plan.cta.href.startsWith("/")) {
    return (
      <Button variant={variant} className={cls} asChild>
        <Link to="/login">
          {plan.cta.label}
          <ArrowRight aria-hidden="true" />
        </Link>
      </Button>
    );
  }
  return (
    <Button variant={variant} className={cls} asChild>
      <a href={plan.cta.href}>
        {plan.cta.label}
        <ArrowRight aria-hidden="true" />
      </a>
    </Button>
  );
}

export function PricingCard({ plan, icon, highlighted = false }: Props) {
  return (
    <article
      data-testid={`plan-${plan.id}`}
      className={cn(
        "relative flex flex-col rounded-2xl border bg-card p-6 shadow-sm",
        highlighted && "border-primary shadow-xl shadow-primary/10",
      )}
    >
      <Badge className={cn("absolute right-5 top-5", !highlighted && "bg-brand-bright")}>
        {plan.badge}
      </Badge>
      <div className="flex items-center gap-3">
        <IconBadge icon={icon} size="sm" />
        <div>
          <h3 className="text-lg font-semibold">{plan.name}</h3>
          <p className="text-xs text-muted-foreground">{plan.description}</p>
        </div>
      </div>
      <PriceBlock plan={plan} />
      <ul className="mt-5 flex-1 space-y-2">
        {plan.features.map((f) => (
          <CheckBullet key={f}>{f}</CheckBullet>
        ))}
      </ul>
      <PlanCta plan={plan} highlighted={highlighted} />
    </article>
  );
}
