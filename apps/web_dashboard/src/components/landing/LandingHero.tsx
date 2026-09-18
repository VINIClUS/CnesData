import { Link } from "@tanstack/react-router";
import { ArrowRight } from "lucide-react";

import { CheckBullet } from "@/components/marketing/CheckBullet";
import { Eyebrow } from "@/components/marketing/Eyebrow";
import { HeroTitle } from "@/components/marketing/HeroTitle";
import { StageBadge } from "@/components/marketing/StageBadge";
import { ACCESS_SEARCH } from "@/components/marketing/marketingNavigation";
import { DashboardPreview } from "@/components/marketing/preview/DashboardPreview";
import { Button } from "@/components/ui/button";
import { landing } from "@/i18n/landing";
import { marketing } from "@/i18n/marketing";

export function LandingHero() {
  const h = landing.hero;
  return (
    <section className="mx-auto grid w-full max-w-[1320px] grid-cols-1 items-center gap-8 px-6 pb-10 pt-4 lg:grid-cols-[1.05fr_1fr]">
      <div className="space-y-5">
        <div className="flex flex-wrap items-center gap-3">
          <Eyebrow>{h.eyebrow}</Eyebrow>
          <StageBadge />
        </div>
        <HeroTitle lead={h.lead} highlight={h.highlight} />
        <p className="max-w-xl text-base leading-relaxed text-muted-foreground">{h.description}</p>
        <div className="flex flex-wrap items-center gap-3">
          <Button size="lg" asChild>
            <Link to="/contato" search={ACCESS_SEARCH}>
              {h.primary}
              <ArrowRight aria-hidden="true" />
            </Link>
          </Button>
          <Button size="lg" variant="outline" className="border-white/15 bg-transparent" asChild>
            <Link to="/recursos">{h.secondary}</Link>
          </Button>
        </div>
        <ul className="flex flex-wrap gap-6">
          {h.bullets.map((b) => (
            <CheckBullet key={b}>{b}</CheckBullet>
          ))}
        </ul>
      </div>
      <figure className="min-w-0">
        <DashboardPreview />
        <figcaption className="mt-3 text-center text-xs text-muted-foreground">
          {marketing.preview.caption}
        </figcaption>
      </figure>
    </section>
  );
}
