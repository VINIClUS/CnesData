import { Link } from "@tanstack/react-router";
import { ArrowRight, Mail } from "lucide-react";

import { HeroSurface } from "@/components/marketing/HeroSurface";
import { MarketingFooter } from "@/components/marketing/MarketingFooter";
import { MarketingNavbar } from "@/components/marketing/MarketingNavbar";
import { PageHero } from "@/components/marketing/PageHero";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import type { LegalDoc, LegalSection } from "@/i18n/legal";
import { legal } from "@/i18n/legal";
import { marketing } from "@/i18n/marketing";

function Section({ section }: { section: LegalSection }) {
  return (
    <section className="space-y-3">
      <div className="flex flex-wrap items-center gap-3">
        <h2 className="text-xl font-semibold leading-tight tracking-tight">{section.title}</h2>
        {section.pending && (
          <Badge
            variant="outline"
            className="border-amber-500/50 text-amber-600 dark:text-amber-400"
          >
            {legal.pendingLabel}
          </Badge>
        )}
      </div>
      {section.paragraphs.map((p) => (
        <p key={p} className="text-base leading-relaxed text-muted-foreground">
          {p}
        </p>
      ))}
      {section.pending && (
        <p className="text-sm italic text-muted-foreground">{legal.pendingNote}</p>
      )}
    </section>
  );
}

export function LegalPage({ doc }: { doc: LegalDoc }) {
  return (
    <div className="min-h-screen bg-background text-foreground">
      <HeroSurface>
        <MarketingNavbar />
        <PageHero
          eyebrow={doc.eyebrow}
          lead={doc.lead}
          highlight={doc.highlight}
          description={doc.description}
        >
          <p className="text-xs text-muted-foreground">
            {legal.updatedLabel}: {doc.updatedAt}
          </p>
        </PageHero>
      </HeroSurface>
      <div className="mx-auto w-full max-w-[1320px] px-6 py-14">
        <article className="mx-auto max-w-[720px] space-y-10">
          {doc.sections.map((s) => (
            <Section key={s.title} section={s} />
          ))}
          <div className="flex flex-wrap gap-3 border-t pt-8">
            <Button variant="outline" asChild>
              <a href={`mailto:${marketing.contactEmail}`}>
                <Mail aria-hidden="true" />
                {marketing.contactEmail}
              </a>
            </Button>
            <Button asChild>
              <Link to="/contato">
                {legal.backToContact}
                <ArrowRight aria-hidden="true" />
              </Link>
            </Button>
          </div>
        </article>
      </div>
      <MarketingFooter />
    </div>
  );
}
