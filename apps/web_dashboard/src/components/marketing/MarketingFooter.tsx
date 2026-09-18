import { Link } from "@tanstack/react-router";

import { SocialLinks } from "./SocialLinks";
import { PUBLIC_NAV } from "./marketingNavigation";

import { Logo } from "@/components/brand/Logo";
import { marketing } from "@/i18n/marketing";

const _LINK = "text-xs text-muted-foreground transition-colors hover:text-foreground";

type FooterRoute = {
  label: string;
  to: "/" | "/recursos" | "/sobre" | "/contato" | "/termos" | "/privacidade" | "/ajuda";
};

const _LEGAL: FooterRoute[] = [
  { label: marketing.footer.termos, to: "/termos" },
  { label: marketing.footer.privacidade, to: "/privacidade" },
];

const _COMPACT: FooterRoute[] = [
  { label: marketing.footer.ajuda, to: "/ajuda" },
  { label: marketing.footer.privacidade, to: "/privacidade" },
  { label: marketing.footer.termos, to: "/termos" },
  { label: marketing.nav.contato, to: "/contato" },
];

function FooterBrand() {
  return (
    <div className="flex items-center gap-4">
      <Logo size="sm" />
      <span className="text-xs text-muted-foreground">{marketing.footer.tagline}</span>
    </div>
  );
}

function FooterLinks({ items }: { items: readonly FooterRoute[] }) {
  return (
    <>
      {items.map((l) => (
        <Link key={l.to} to={l.to} className={_LINK}>
          {l.label}
        </Link>
      ))}
    </>
  );
}

export function MarketingFooter({ variant = "full" }: { variant?: "full" | "compact" }) {
  if (variant === "compact") {
    return (
      <footer className="dark mx-auto flex w-full max-w-[1320px] items-center justify-between px-6 py-5 text-foreground">
        <FooterBrand />
        <div className="flex items-center gap-8">
          <nav aria-label="Rodapé" className="flex items-center gap-6">
            <FooterLinks items={_COMPACT} />
          </nav>
          <SocialLinks />
        </div>
      </footer>
    );
  }
  return (
    <footer className="dark bg-navy-deep text-foreground">
      <div className="mx-auto flex w-full max-w-[1320px] flex-wrap items-center justify-between gap-4 px-6 py-5">
        <FooterBrand />
        <nav aria-label="Rodapé" className="hidden items-center gap-6 lg:flex">
          <FooterLinks items={PUBLIC_NAV} />
          <FooterLinks items={_LEGAL} />
        </nav>
        <div className="flex flex-wrap items-center gap-4">
          <SocialLinks />
          <span className="max-w-[140px] text-[10px] leading-tight text-muted-foreground">
            {marketing.footer.developedFor}
          </span>
        </div>
      </div>
    </footer>
  );
}
