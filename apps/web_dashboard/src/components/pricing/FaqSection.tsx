import { Link } from "@tanstack/react-router";
import { ArrowRight } from "lucide-react";

import { FaqList } from "./FaqList";

import { SectionHeading } from "@/components/marketing/SectionHeading";
import { Button } from "@/components/ui/button";
import { pricing } from "@/i18n/pricing";

export function FaqSection() {
  const f = pricing.faq;
  return (
    <section id="faq" className="bg-background">
      <div className="mx-auto grid w-full max-w-[1320px] grid-cols-1 gap-10 px-6 pb-14 pt-4 lg:grid-cols-[1fr_3fr]">
        <div className="space-y-6">
          <SectionHeading eyebrow={f.eyebrow} title={f.title} description={f.description} />
          <Button variant="outline" className="text-primary" asChild>
            <Link to="/precos" hash="faq">
              {f.button}
              <ArrowRight aria-hidden="true" />
            </Link>
          </Button>
        </div>
        <FaqList items={f.items} />
      </div>
    </section>
  );
}
