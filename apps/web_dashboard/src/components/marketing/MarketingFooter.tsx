import { Link } from "@tanstack/react-router";

import { SocialLinks } from "./SocialLinks";

import { Logo } from "@/components/brand/Logo";
import { marketing } from "@/i18n/marketing";

const _MAIL = `mailto:${marketing.contactEmail}`;
const _LINK = "text-xs text-muted-foreground transition-colors hover:text-foreground";

type RouteLink = { label: string; to: "/" | "/precos"; hash?: string };
type ExternalLink = { label: string; href: string };

const _FULL_ROUTES: RouteLink[] = [
  { label: marketing.nav.inicio, to: "/" },
  { label: marketing.nav.recursos, to: "/", hash: "recursos" },
  { label: marketing.nav.precos, to: "/precos" },
  { label: marketing.nav.sobre, to: "/", hash: "sobre" },
];

const _FULL_EXTERNAL: ExternalLink[] = [
  { label: marketing.nav.contato, href: _MAIL },
  { label: marketing.footer.termos, href: `${_MAIL}?subject=Termos` },
  { label: marketing.footer.privacidade, href: `${_MAIL}?subject=Privacidade` },
];

const _COMPACT: ExternalLink[] = [
  { label: marketing.footer.ajuda, href: `${_MAIL}?subject=Ajuda` },
  { label: marketing.footer.privacidade, href: `${_MAIL}?subject=Privacidade` },
  { label: marketing.footer.termos, href: `${_MAIL}?subject=Termos` },
  { label: marketing.nav.contato, href: _MAIL },
];

function FooterBrand() {
  return (
    <div className="flex items-center gap-4">
      <Logo size="sm" />
      <span className="text-xs text-muted-foreground">{marketing.footer.tagline}</span>
    </div>
  );
}

function FooterLinks({ items }: { items: ExternalLink[] }) {
  return (
    <>
      {items.map((l) => (
        <a key={l.label} href={l.href} className={_LINK}>
          {l.label}
        </a>
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
      <div className="mx-auto flex w-full max-w-[1320px] items-center justify-between px-6 py-5">
        <FooterBrand />
        <nav aria-label="Rodapé" className="hidden items-center gap-6 lg:flex">
          {_FULL_ROUTES.map((l) => (
            <Link key={l.label} to={l.to} hash={l.hash} className={_LINK}>
              {l.label}
            </Link>
          ))}
          <FooterLinks items={_FULL_EXTERNAL} />
        </nav>
        <div className="flex items-center gap-4">
          <SocialLinks />
          <span className="max-w-[140px] text-[10px] leading-tight text-muted-foreground">
            {marketing.footer.developedFor}
          </span>
        </div>
      </div>
    </footer>
  );
}
