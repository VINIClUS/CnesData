import { ResourceBlock } from "./ResourceBlock";
import { ChartsVisual, DivergenceVisual, KpisVisual } from "./ResourceVisuals";
import { ResourcesFlow } from "./ResourcesFlow";

import { CtaBand } from "@/components/marketing/CtaBand";
import { HeroSurface } from "@/components/marketing/HeroSurface";
import { MarketingFooter } from "@/components/marketing/MarketingFooter";
import { MarketingNavbar } from "@/components/marketing/MarketingNavbar";
import { PageHero } from "@/components/marketing/PageHero";
import { marketing } from "@/i18n/marketing";
import { resources } from "@/i18n/resources";

const _VISUALS = [
  <KpisVisual key="kpis" />,
  <DivergenceVisual key="div" />,
  <ChartsVisual key="charts" />,
];

export function ResourcesPage() {
  const h = resources.hero;
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
      {resources.blocks.map((b, i) => (
        <ResourceBlock key={b.id} block={b} reversed={i % 2 === 1} visual={_VISUALS[i]} />
      ))}
      <ResourcesFlow />
      <CtaBand
        title={resources.cta.title}
        subtitle={resources.cta.subtitle}
        button={marketing.cta.buttonPiloto}
        interesse="piloto"
      />
      <MarketingFooter />
    </div>
  );
}
