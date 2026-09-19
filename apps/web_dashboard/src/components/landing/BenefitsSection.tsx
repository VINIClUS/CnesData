import { Link } from "@tanstack/react-router";
import { ArrowRight, ChartNoAxesColumnIncreasing, FileText, Users } from "lucide-react";
import type { LucideIcon } from "lucide-react";

import { FeatureItem } from "@/components/marketing/FeatureItem";
import { SectionHeading } from "@/components/marketing/SectionHeading";
import { Button } from "@/components/ui/button";
import { landing } from "@/i18n/landing";

const _ICONS: LucideIcon[] = [ChartNoAxesColumnIncreasing, Users, FileText];

export function BenefitsSection() {
  const b = landing.benefits;
  return (
    <section id="sobre" className="border-t bg-muted/40">
      <div className="mx-auto grid w-full max-w-[1320px] grid-cols-1 gap-10 px-6 py-14 lg:grid-cols-[1fr_2fr]">
        <div className="space-y-6">
          <SectionHeading eyebrow={b.eyebrow} title={b.title} description={b.description} />
          <Button variant="outline" className="text-primary" asChild>
            <Link to="/recursos">
              {b.button}
              <ArrowRight aria-hidden="true" />
            </Link>
          </Button>
        </div>
        <div className="grid grid-cols-1 gap-5 md:grid-cols-3">
          {b.cards.map((c, i) => (
            <FeatureItem key={c.title} icon={_ICONS[i] ?? FileText} layout="card" {...c} />
          ))}
        </div>
      </div>
    </section>
  );
}
