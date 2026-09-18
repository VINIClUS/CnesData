import { BenefitsSection } from "./BenefitsSection";
import { FeatureStrip } from "./FeatureStrip";
import { LandingHero } from "./LandingHero";

import { CtaBand } from "@/components/marketing/CtaBand";
import { HeroSurface } from "@/components/marketing/HeroSurface";
import { MarketingFooter } from "@/components/marketing/MarketingFooter";
import { MarketingNavbar } from "@/components/marketing/MarketingNavbar";
import { landing } from "@/i18n/landing";

export function LandingPage() {
  return (
    <div className="min-h-screen bg-background text-foreground">
      <HeroSurface>
        <MarketingNavbar />
        <LandingHero />
      </HeroSurface>
      <FeatureStrip />
      <BenefitsSection />
      <CtaBand title={landing.cta} />
      <MarketingFooter />
    </div>
  );
}
