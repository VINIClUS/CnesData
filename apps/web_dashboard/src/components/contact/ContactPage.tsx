import { ContactIntro } from "./ContactIntro";
import { LeadForm } from "./LeadForm";
import type { Interesse } from "./contactInterest";

import { HeroSurface } from "@/components/marketing/HeroSurface";
import { MarketingFooter } from "@/components/marketing/MarketingFooter";
import { MarketingNavbar } from "@/components/marketing/MarketingNavbar";
import { PageHero } from "@/components/marketing/PageHero";
import { contact } from "@/i18n/contact";

export function ContactPage({ interesse }: { interesse: Interesse }) {
  const h = contact.hero[interesse];
  return (
    <div className="min-h-screen bg-background text-foreground">
      <HeroSurface>
        <MarketingNavbar />
        <PageHero
          eyebrow={contact.eyebrow}
          lead={h.lead}
          highlight={h.highlight}
          description={h.description}
          stage="desenvolvimento"
        />
      </HeroSurface>
      <section className="mx-auto grid w-full max-w-[1320px] grid-cols-1 gap-10 px-6 py-14 lg:grid-cols-[1fr_1.1fr]">
        <ContactIntro />
        <div className="rounded-2xl border bg-card p-6 shadow-sm lg:p-8">
          <LeadForm key={interesse} interesse={interesse} />
        </div>
      </section>
      <MarketingFooter />
    </div>
  );
}
