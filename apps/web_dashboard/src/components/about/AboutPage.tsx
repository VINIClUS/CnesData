import { AboutBody } from "./AboutBody";

import { CtaBand } from "@/components/marketing/CtaBand";
import { HeroSurface } from "@/components/marketing/HeroSurface";
import { MarketingFooter } from "@/components/marketing/MarketingFooter";
import { MarketingNavbar } from "@/components/marketing/MarketingNavbar";
import { PageHero } from "@/components/marketing/PageHero";
import { about } from "@/i18n/about";
import { marketing } from "@/i18n/marketing";

export function AboutPage() {
  const h = about.hero;
  return (
    <div className="min-h-screen bg-background text-foreground">
      <HeroSurface>
        <MarketingNavbar />
        <PageHero
          eyebrow={h.eyebrow}
          lead={h.lead}
          highlight={h.highlight}
          description={h.description}
          stage="desenvolvimento"
        />
      </HeroSurface>
      <AboutBody />
      <CtaBand
        title={about.cta.title}
        subtitle={about.cta.subtitle}
        button={marketing.cta.buttonPiloto}
        interesse="piloto"
      />
      <MarketingFooter />
    </div>
  );
}
