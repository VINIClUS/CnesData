import { PricingHeroCard } from "./PricingHeroCard";

import { CheckBullet } from "@/components/marketing/CheckBullet";
import { Eyebrow } from "@/components/marketing/Eyebrow";
import { HeroTitle } from "@/components/marketing/HeroTitle";
import { pricing } from "@/i18n/pricing";

export function PricingHero() {
  const h = pricing.hero;
  return (
    <section className="mx-auto grid w-full max-w-[1320px] grid-cols-1 items-start gap-10 px-6 pb-8 pt-2 lg:grid-cols-[1.4fr_1fr]">
      <div className="space-y-4">
        <Eyebrow>{h.eyebrow}</Eyebrow>
        <HeroTitle lead={h.lead} highlight={h.highlight} />
        <p className="max-w-xl text-[15px] leading-relaxed text-muted-foreground">
          {h.description}
        </p>
        <ul className="flex flex-wrap gap-6">
          {h.bullets.map((b) => (
            <CheckBullet key={b}>{b}</CheckBullet>
          ))}
        </ul>
      </div>
      <PricingHeroCard />
    </section>
  );
}
