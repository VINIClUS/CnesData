import { FaqSection } from "./FaqSection";
import { PricingGrid } from "./PricingGrid";
import { PricingHero } from "./PricingHero";
import { TrustStrip } from "./TrustStrip";

import { CtaBand } from "@/components/marketing/CtaBand";
import { HeroSurface } from "@/components/marketing/HeroSurface";
import { MarketingFooter } from "@/components/marketing/MarketingFooter";
import { MarketingNavbar } from "@/components/marketing/MarketingNavbar";
import { pricing } from "@/i18n/pricing";

export function PricingPage() {
  return (
    <div className="min-h-screen bg-background text-foreground">
      <HeroSurface>
        <MarketingNavbar />
        <PricingHero />
      </HeroSurface>
      <PricingGrid />
      <TrustStrip />
      <FaqSection />
      <CtaBand title={pricing.cta} />
      <MarketingFooter />
    </div>
  );
}
