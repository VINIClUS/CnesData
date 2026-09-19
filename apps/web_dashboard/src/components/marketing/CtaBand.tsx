import { Link } from "@tanstack/react-router";
import { ArrowRight } from "lucide-react";

import { LogoMark } from "@/components/brand/Logo";
import type { Interesse } from "@/components/contact/contactInterest";
import { Button } from "@/components/ui/button";
import { marketing } from "@/i18n/marketing";

type Props = {
  title: string;
  subtitle?: string;
  button?: string;
  interesse?: Interesse;
};

export function CtaBand({
  title,
  subtitle = marketing.cta.subtitle,
  button = marketing.cta.button,
  interesse = "acesso-antecipado",
}: Props) {
  return (
    <section className="bg-cta-blue dark text-white">
      <div className="mx-auto flex w-full max-w-[1320px] flex-wrap items-center justify-between gap-8 px-6 py-7">
        <div className="flex items-center gap-5">
          <LogoMark size="lg" className="[&_rect]:fill-white" />
          <div>
            <h2 className="text-lg font-semibold">{title}</h2>
            <p className="mt-1 max-w-xl text-sm text-white/80">{subtitle}</p>
          </div>
        </div>
        <div className="flex flex-col items-center gap-1.5">
          <Button size="lg" className="bg-brand-bright text-white hover:bg-brand" asChild>
            <Link to="/contato" search={{ interesse }}>
              {button}
              <ArrowRight aria-hidden="true" />
            </Link>
          </Button>
          <span className="text-[11px] text-white/70">{marketing.cta.note}</span>
        </div>
      </div>
    </section>
  );
}
