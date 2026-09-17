import { ChartNoAxesColumnIncreasing, ShieldCheck, Users } from "lucide-react";
import type { LucideIcon } from "lucide-react";

import { Eyebrow } from "@/components/marketing/Eyebrow";
import { FeatureItem } from "@/components/marketing/FeatureItem";
import { HeroTitle } from "@/components/marketing/HeroTitle";
import { login } from "@/i18n/login";

const _ICONS: LucideIcon[] = [ChartNoAxesColumnIncreasing, Users, ShieldCheck];

export function LoginHero() {
  const h = login.hero;
  return (
    <div className="max-w-xl space-y-6">
      <Eyebrow>{h.eyebrow}</Eyebrow>
      <HeroTitle lead={h.lead} highlight={h.highlight} />
      <p className="text-base leading-relaxed text-muted-foreground">{h.description}</p>
      <ul className="space-y-6 pt-2">
        {h.features.map((f, i) => (
          <li key={f.title}>
            <FeatureItem icon={_ICONS[i] ?? ShieldCheck} badgeSize="lg" {...f} />
          </li>
        ))}
      </ul>
      <div className="flex items-center gap-4 pt-4">
        <span className="h-px w-12 bg-primary" />
        <Eyebrow>{h.tagline}</Eyebrow>
      </div>
    </div>
  );
}
