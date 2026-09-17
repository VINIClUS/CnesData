import { LogoMark } from "@/components/brand/Logo";
import { pricing } from "@/i18n/pricing";

export function PricingHeroCard() {
  const c = pricing.hero.card;
  return (
    <aside className="flex gap-5 rounded-2xl border border-white/10 bg-navy-card/70 p-7 shadow-2xl shadow-black/30 backdrop-blur">
      <LogoMark size="lg" className="mt-1 shrink-0" />
      <div>
        <h2 className="text-base font-semibold leading-snug">{c.title}</h2>
        <p className="mt-3 text-sm leading-relaxed text-muted-foreground">{c.description}</p>
      </div>
    </aside>
  );
}
