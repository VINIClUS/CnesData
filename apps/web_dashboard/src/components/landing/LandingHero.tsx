import { Link } from "@tanstack/react-router";
import { ArrowRight, Play } from "lucide-react";

import { CheckBullet } from "@/components/marketing/CheckBullet";
import { Eyebrow } from "@/components/marketing/Eyebrow";
import { HeroTitle } from "@/components/marketing/HeroTitle";
import { DashboardPreview } from "@/components/marketing/preview/DashboardPreview";
import { Button } from "@/components/ui/button";
import { landing } from "@/i18n/landing";

export function LandingHero() {
  const h = landing.hero;
  return (
    <section className="mx-auto grid w-full max-w-[1320px] grid-cols-1 items-center gap-8 px-6 pb-10 pt-4 lg:grid-cols-[1.05fr_1fr]">
      <div className="space-y-5">
        <Eyebrow>{h.eyebrow}</Eyebrow>
        <HeroTitle lead={h.lead} highlight={h.highlight} />
        <p className="max-w-xl text-base leading-relaxed text-muted-foreground">{h.description}</p>
        <div className="flex flex-wrap items-center gap-3">
          <Button size="lg" asChild>
            <Link to="/login">
              {h.primary}
              <ArrowRight aria-hidden="true" />
            </Link>
          </Button>
          <Button size="lg" variant="outline" className="border-white/15 bg-transparent" asChild>
            <Link to="/login">
              <Play aria-hidden="true" className="fill-current" />
              {h.secondary}
            </Link>
          </Button>
        </div>
        <ul className="flex flex-wrap gap-6">
          {h.bullets.map((b) => (
            <CheckBullet key={b}>{b}</CheckBullet>
          ))}
        </ul>
      </div>
      <DashboardPreview />
    </section>
  );
}
